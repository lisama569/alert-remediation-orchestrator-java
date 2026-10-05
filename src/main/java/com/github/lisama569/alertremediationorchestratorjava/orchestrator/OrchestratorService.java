package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.IngestResult;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.execution.RemediationAction;
import com.github.lisama569.alertremediationorchestratorjava.policy.MaintenanceWindow;
import com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 编排门控：维护窗口延迟、冻结期拒绝、高风险审批，全部判定写入事件历史。
 * 自动驱动：ingestAndDrive 让告警接入后无需手动触发即走到处置结论；
 * enableAutoResume 后，时钟推进过维护窗口结束点时延迟事件自动恢复，无需手动唤醒。
 */
public class OrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorService.class);

    private final EventService eventService;
    private final ApprovalService approvalService;
    private final RemediationExecutor executor;
    private final MutableClock clock;
    private final List<MaintenanceWindow> windows = new CopyOnWriteArrayList<>();
    /** 自动驱动与延迟自愈使用的默认动作工厂 */
    private volatile java.util.function.Function<String, RemediationAction> defaultActionFactory =
            key -> (k, attempt) -> com.github.lisama569.alertremediationorchestratorjava.execution.ExecutionResult
                    .ok("default no-op remediation");

    public OrchestratorService(EventService eventService, ApprovalService approvalService,
                               RemediationExecutor executor, MutableClock clock) {
        this.eventService = eventService;
        this.approvalService = approvalService;
        this.executor = executor;
        this.clock = clock;
    }

    public void setWindows(List<MaintenanceWindow> newWindows) {
        windows.clear();
        windows.addAll(newWindows);
    }

    /** 设置自动驱动 / 延迟自愈使用的默认动作工厂。 */
    public void setDefaultActionFactory(
            java.util.function.Function<String, RemediationAction> factory) {
        this.defaultActionFactory = factory;
    }

    /**
     * 告警接入并自动驱动到处置结论，无需手动触发。
     * 接入未受理（抑制 / 终态 / 暂未匹配策略）时不触发处置，remediation 为 null；
     * 重复 / 并发投递由执行器去重，只会真正执行一次，其余得到明确的 DUPLICATE 结果。
     */
    public IngestOutcome ingestAndDrive(com.github.lisama569.alertremediationorchestratorjava.domain.Alert alert) {
        IngestResult ingest = eventService.ingest(alert);
        if (!ingest.accepted()) {
            log.info("auto-drive skipped: eventKey={} ingest rejected reason={}",
                    alert.eventKey(), ingest.rejection());
            return new IngestOutcome(ingest, null);
        }
        RemediationOutcome outcome = remediate(alert.eventKey(),
                defaultActionFactory.apply(alert.eventKey()));
        log.info("auto-drive finished: eventKey={} ingest={} remediation={}",
                alert.eventKey(), ingest.accepted(), outcome.kind());
        return new IngestOutcome(ingest, outcome);
    }

    /**
     * 开启延迟自愈：时钟每次推进后自动检查并恢复窗口已结束的延迟事件，
     * 无需手动调用 processDelayed。恢复判定与执行均写入事件历史，且只执行一次。
     */
    public void enableAutoResume() {
        clock.addListener(t -> {
            int resumed = processDelayed();
            if (resumed > 0) {
                log.info("auto-resume on clock advance: now={} resumed={}", t, resumed);
            }
        });
    }

    /** 使用默认动作工厂推进延迟事件。 */
    public int processDelayed() {
        return processDelayed(defaultActionFactory);
    }

    /** 请求对某事件执行处置。 */
    public RemediationOutcome remediate(String eventKey, RemediationAction action) {
        AlertEvent event = eventService.getEvent(eventKey);
        if (event == null) {
            log.info("remediate rejected: eventKey={} reason=unknown event", eventKey);
            return RemediationOutcome.rejected(RejectionReason.POLICY_MISMATCH, "unknown event");
        }
        synchronized (event) {
            EventStatus status = event.getStatus();
            if (status == EventStatus.CLOSED || status == EventStatus.RESOLVED
                    || status == EventStatus.REJECTED || status == EventStatus.FAILED_FINAL) {
                log.info("remediate rejected: eventKey={} reason=ALREADY_CLOSED status={}",
                        eventKey, status);
                return RemediationOutcome.rejected(RejectionReason.ALREADY_CLOSED,
                        "event in terminal status " + status);
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
                    event.transition(now, status, "freeze hit: window=" + w.name()
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
            log.info("remediate deduplicated: eventKey={} already executing", eventKey);
            return RemediationOutcome.duplicate("execution already in progress");
        }
        log.info("remediate executed: eventKey={} finalStatus={}", eventKey, event.getStatus());
        return RemediationOutcome.executed("finalStatus=" + event.getStatus());
    }

    /** 推进延迟事件：维护窗口结束后重新进入可执行状态。 */
    public int processDelayed(java.util.function.Function<String, RemediationAction> actionFactory) {
        int resumed = 0;
        for (AlertEvent e : eventService.listEvents()) {
            synchronized (e) {
                if (e.getStatus() != EventStatus.DELAYED) {
                    continue;
                }
                Instant now = clock.now();
                boolean stillInWindow = windows.stream()
                        .anyMatch(w -> w.kind() == MaintenanceWindow.Kind.MAINTENANCE && w.contains(now));
                if (stillInWindow) {
                    continue;
                }
                e.transition(now, EventStatus.NEW, "maintenance window ended, resuming");
            }
            remediate(e.getEventKey(), actionFactory.apply(e.getEventKey()));
            resumed++;
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
