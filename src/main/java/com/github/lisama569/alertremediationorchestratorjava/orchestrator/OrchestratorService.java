package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
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
 */
public class OrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorService.class);

    private final EventService eventService;
    private final ApprovalService approvalService;
    private final RemediationExecutor executor;
    private final MutableClock clock;
    private final List<MaintenanceWindow> windows = new CopyOnWriteArrayList<>();

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
