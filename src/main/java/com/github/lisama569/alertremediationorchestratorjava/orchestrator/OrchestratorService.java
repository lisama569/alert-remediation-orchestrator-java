package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.domain.RemediationOutcome;
import com.github.lisama569.alertremediationorchestratorjava.execution.ActionResolver;
import com.github.lisama569.alertremediationorchestratorjava.execution.ExecutionResult;
import com.github.lisama569.alertremediationorchestratorjava.execution.RemediationAction;
import com.github.lisama569.alertremediationorchestratorjava.policy.MaintenanceWindow;
import com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * 编排门控：维护窗口延迟、冻结期拒绝、高风险审批，全部判定写入事件历史。
 *
 * 自动驱动：
 * - 受理后由 EventService 经 {@link #autoDrive()} 自动走一遍门控+处置，无需手动触发；
 * - DELAYED 事件在时钟推进（或窗口调整）到窗口结束后自动恢复，语义仍为半开区间 [start,end)；
 * - 高风险事件审批决定后自动继续：批准->执行，拒绝->终态 UNAUTHORIZED。
 * 自动链路与原手动 remediate 走同一套门控，语义完全一致。
 */
public class OrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorService.class);

    /** 本地默认动作：no-op 成功；生产可通过 setActionResolver 替换为按事件策略快照解析的动作。 */
    private static final RemediationAction DEFAULT_ACTION =
            (key, attempt) -> ExecutionResult.ok("default no-op remediation");

    private final EventService eventService;
    private final ApprovalService approvalService;
    private final RemediationExecutor executor;
    private final MutableClock clock;
    private final List<MaintenanceWindow> windows = new CopyOnWriteArrayList<>();
    private volatile ActionResolver actionResolver = event -> DEFAULT_ACTION;

    public OrchestratorService(EventService eventService, ApprovalService approvalService,
                               RemediationExecutor executor, MutableClock clock) {
        this.eventService = eventService;
        this.approvalService = approvalService;
        this.executor = executor;
        this.clock = clock;
        wireAutoDrive();
    }

    /**
     * 装配自动驱动：接入即处置、时钟推进自愈、审批决定后续跑。
     * 所有回调均在事件监视器之外执行（仅回调内部短暂加锁），不存在锁嵌套死锁。
     */
    private void wireAutoDrive() {
        eventService.setAutoDriver(this::driveEvent);
        clock.onTick(at -> resumeDueDelayed());
        approvalService.addListener((eventKey, state) -> {
            AlertEvent e = eventService.getEvent(eventKey);
            if (e != null) {
                log.info("approval decision drives pipeline: eventKey={} state={}", eventKey, state);
                driveEvent(e);
            }
        });
    }

    public void setActionResolver(ActionResolver resolver) {
        this.actionResolver = resolver == null ? event -> DEFAULT_ACTION : resolver;
    }

    public void setWindows(List<MaintenanceWindow> newWindows) {
        windows.clear();
        windows.addAll(newWindows);
        // 窗口调整后立即重新评估被延迟的事件（可能有事件恰好因此放行）
        resumeDueDelayed();
    }

    /** 自动驱动入口：按事件上粘住的策略快照解析动作并执行门控+处置。 */
    private RemediationOutcome driveEvent(AlertEvent event) {
        return remediate(event.getEventKey(), actionResolver.resolve(event));
    }

    /** 供外部（Function 形式）使用的自动驱动。 */
    public Function<AlertEvent, RemediationOutcome> autoDrive() {
        return this::driveEvent;
    }

    /** 手动触发入口（兼容）：同样按事件上的策略快照解析动作，与自动链路门控完全一致。 */
    public RemediationOutcome remediateManually(String eventKey) {
        AlertEvent event = eventService.getEvent(eventKey);
        if (event == null) {
            return RemediationOutcome.rejected(RejectionReason.POLICY_MISMATCH, "unknown event");
        }
        return remediate(eventKey, actionResolver.resolve(event));
    }

    /** 请求对某事件执行处置。自动链路与手动请求共用此门控。 */
    public RemediationOutcome remediate(String eventKey, RemediationAction action) {
        AlertEvent event = eventService.getEvent(eventKey);
        if (event == null) {
            log.info("remediate rejected: eventKey={} reason=unknown event", eventKey);
            return RemediationOutcome.rejected(RejectionReason.POLICY_MISMATCH, "unknown event");
        }
        synchronized (event) {
            EventStatus status = event.getStatus();
            if (isTerminal(status)) {
                // 终态事件：原样复现当时的拒绝原因，不因再次触发而改口
                RejectionReason reason = status == EventStatus.REJECTED && event.getRejection() != null
                        ? event.getRejection() : RejectionReason.ALREADY_CLOSED;
                event.recordNote(clock.now(),
                        "remediation skipped: terminal status=" + status + " reason=" + reason);
                log.info("remediate rejected: eventKey={} reason={} status={}", eventKey, reason, status);
                return RemediationOutcome.rejected(reason, "event in terminal status " + status);
            }
            RemediationPolicy policy = event.getPolicy();
            if (policy == null) {
                return RemediationOutcome.rejected(RejectionReason.POLICY_MISMATCH,
                        "no policy snapshot on event");
            }
            Instant now = clock.now();
            for (MaintenanceWindow w : windows) {
                if (!w.contains(now)) {
                    continue;
                }
                if (w.kind() == MaintenanceWindow.Kind.FREEZE) {
                    event.recordNote(now, "freeze hit: window=" + w.name()
                            + " [" + w.start() + "," + w.end() + ") -> operation rejected");
                    log.info("remediate rejected: eventKey={} reason=CHANGE_FREEZE window={}",
                            eventKey, w.name());
                    return RemediationOutcome.rejected(RejectionReason.CHANGE_FREEZE,
                            "change freeze window " + w.name());
                }
                event.transition(now, EventStatus.DELAYED,
                        "maintenance window " + w.name() + " hit: delayed until " + w.end());
                log.info("remediate delayed: eventKey={} reason=MAINTENANCE_WINDOW window={} until={}",
                        eventKey, w.name(), w.end());
                return RemediationOutcome.delayed(
                        "maintenance window " + w.name() + " until " + w.end());
            }
            if (policy.highRisk() && !approvalService.isApproved(eventKey)) {
                var rec = approvalService.getRecord(eventKey);
                if (rec != null && rec.state() == ApprovalService.ApprovalState.REJECTED) {
                    event.reject(now, RejectionReason.UNAUTHORIZED,
                            "approval rejected by " + rec.approver());
                    log.info("remediate rejected: eventKey={} reason=UNAUTHORIZED approver={}",
                            eventKey, rec.approver());
                    return RemediationOutcome.rejected(RejectionReason.UNAUTHORIZED,
                            "approval rejected");
                }
                approvalService.requestApproval(eventKey);
                event.transition(now, EventStatus.AWAITING_APPROVAL,
                        "high-risk policy " + policy.policyId() + ": approval required");
                log.info("remediate pending: eventKey={} reason=APPROVAL_REQUIRED policy={}",
                        eventKey, policy.policyId());
                return RemediationOutcome.rejected(RejectionReason.APPROVAL_REQUIRED,
                        "high-risk operation requires approval");
            }
        }
        boolean taken = executor.trigger(event, action, policyMaxRetries(event), policyTimeout(event));
        if (!taken) {
            log.info("remediate deduplicated: eventKey={} already executing or terminal", eventKey);
            return RemediationOutcome.duplicate("execution already in progress or event terminal");
        }
        log.info("remediate executed: eventKey={} finalStatus={}", eventKey, event.getStatus());
        return RemediationOutcome.executed("finalStatus=" + event.getStatus());
    }

    private boolean isTerminal(EventStatus status) {
        return status == EventStatus.CLOSED || status == EventStatus.RESOLVED
                || status == EventStatus.REJECTED || status == EventStatus.FAILED_FINAL;
    }

    /**
     * 恢复所有已到点的延迟事件：仍在维护窗口内则继续等待（判定留痕），
     * 窗口结束（end 时刻按半开区间放行）则转 NEW 并自动续跑，且执行层保证只真正执行一次。
     * 由时钟推进/窗口调整自动调用，也保留手动入口。
     */
    public int resumeDueDelayed() {
        return processDelayed(key -> {
            AlertEvent e = eventService.getEvent(key);
            return actionResolver.resolve(e);
        });
    }

    /** 推进延迟事件：维护窗口结束后重新进入可执行状态并立即继续处置。 */
    public int processDelayed(Function<String, RemediationAction> actionFactory) {
        int resumed = 0;
        for (AlertEvent e : eventService.listEvents()) {
            boolean due = false;
            synchronized (e) {
                if (e.getStatus() != EventStatus.DELAYED) {
                    continue;
                }
                Instant now = clock.now();
                boolean stillInWindow = windows.stream()
                        .anyMatch(w -> w.kind() == MaintenanceWindow.Kind.MAINTENANCE && w.contains(now));
                if (stillInWindow) {
                    e.recordNote(now, "still within maintenance window, remain DELAYED");
                    log.info("delayed event keeps waiting: eventKey={} at={}", e.getEventKey(), now);
                    continue;
                }
                e.transition(now, EventStatus.NEW, "maintenance window ended, resuming");
                due = true;
            }
            if (due) {
                RemediationOutcome outcome = remediate(e.getEventKey(),
                        actionFactory.apply(e.getEventKey()));
                resumed++;
                log.info("delayed event resumed: eventKey={} outcome={} finalStatus={}",
                        e.getEventKey(), outcome.kind(), e.getStatus());
            }
        }
        return resumed;
    }

    /** 人工闭环。 */
    public boolean close(String eventKey, String operator) {
        AlertEvent event = eventService.getEvent(eventKey);
        if (event == null) {
            return false;
        }
        event.transition(clock.now(), EventStatus.CLOSED, "closed manually by " + operator);
        log.info("event closed: eventKey={} operator={}", eventKey, operator);
        return true;
    }

    private int policyMaxRetries(AlertEvent event) {
        RemediationPolicy p = event.getPolicy();
        return p != null ? p.maxRetries() : 0;
    }

    private java.time.Duration policyTimeout(AlertEvent event) {
        RemediationPolicy p = event.getPolicy();
        return p != null ? p.timeout() : java.time.Duration.ofSeconds(30);
    }
}
