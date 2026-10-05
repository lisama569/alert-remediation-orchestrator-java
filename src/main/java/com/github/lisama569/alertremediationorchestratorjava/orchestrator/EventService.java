package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.Alert;
import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.IngestResult;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.domain.RemediationOutcome;
import com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * 告警接入与事件归并。
 * 判定顺序：已闭环（原样复现历史原因） > 策略匹配（不受理但不终结事件） > 抑制（终态拒绝）。
 * 命中策略的 id 与判定依据以快照粘在事件上：事件一旦受理，后续规则调整不影响该事件，
 * 历史结论（含执行参数）原样可复现；而仅因当时无规则而未受理的事件不被钉死，规则补齐后可继续。
 *
 * 受理（首次/归并）后通过自动驱动钩子一路走完门控与处置，无需再手动触发；
 * 钩子由编排层注册，执行在事件监视器之外进行，避免锁内回调造成死锁。
 */
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    private final Map<String, AlertEvent> events = new ConcurrentHashMap<>();
    private final List<RemediationPolicy> policies = new CopyOnWriteArrayList<>();
    private final MutableClock clock;
    /** 受理后自动驱动：eventKey -> 自动走一遍门控+处置；默认无驱动（仅接入）。 */
    private volatile Function<AlertEvent, RemediationOutcome> autoDriver = e -> null;

    public EventService(MutableClock clock) {
        this.clock = clock;
    }

    public void setAutoDriver(Function<AlertEvent, RemediationOutcome> driver) {
        this.autoDriver = driver == null ? e -> null : driver;
    }

    /** 全量替换策略（策略调整不影响已受理事件上粘住的快照与历史结论）。 */
    public void setPolicies(List<RemediationPolicy> newPolicies) {
        policies.clear();
        policies.addAll(newPolicies);
    }

    public IngestResult ingest(Alert alert) {
        AlertEvent event = events.computeIfAbsent(alert.eventKey(),
                k -> new AlertEvent(k, clock.now()));
        IngestResult result;
        boolean driveAfter;
        synchronized (event) {
            EventStatus status = event.getStatus();
            Instant now = clock.now();
            if (isTerminalStatus(status)) {
                // 终态事件的重复投递：接入侧先按当时原因给出结论，随后仍走一遍自动门控，
                // 门控原样复现拒绝原因（不重复执行、不改口），drive 中可观测到明确去重结果
                RejectionReason reason = terminalRejection(event, status);
                event.recordNote(now, "alert redelivered to terminal event: status=" + status
                        + " reason=" + reason + " alertId=" + alert.alertId());
                log.info("ingest rejected: alert={} eventKey={} reason={} status={}",
                        alert.alertId(), alert.eventKey(), reason, status);
                result = IngestResult.rejected(alert.eventKey(), reason,
                        "event already in terminal status " + status, status, event.getAlertCount());
                driveAfter = true;
            } else {
                RemediationPolicy policy = policies.stream()
                        .filter(p -> p.matches(alert.labels()))
                        .findFirst()
                        .orElse(null);
                if (policy == null) {
                    // 不受理，但不终结事件：留备注，规则补齐后同样告警再投可正常走下去；
                    // 事件未受理，不进入自动驱动
                    event.recordNote(now, "alert not accepted: no policy matched labels="
                            + alert.labels() + " alertId=" + alert.alertId());
                    log.info("ingest not accepted: alert={} eventKey={} reason=POLICY_MISMATCH labels={}",
                            alert.alertId(), alert.eventKey(), alert.labels());
                    result = IngestResult.rejected(alert.eventKey(), RejectionReason.POLICY_MISMATCH,
                            "no policy matched", status, event.getAlertCount());
                    driveAfter = false;
                } else {
                    if (event.getPolicy() == null) {
                        // 首次受理：策略快照粘在事件上，之后规则改动不影响本事件
                        event.setPolicy(policy);
                        event.setPolicySnapshot("policyId=" + policy.policyId()
                                + ", suppressed=" + policy.suppressed()
                                + ", highRisk=" + policy.highRisk()
                                + ", maxRetries=" + policy.maxRetries()
                                + ", timeout=" + policy.timeout());
                    }
                    if (event.getPolicy().suppressed()) {
                        event.reject(now, RejectionReason.SUPPRESSED,
                                "suppressed by policy " + event.getPolicy().policyId());
                        log.info("ingest rejected: alert={} eventKey={} reason=SUPPRESSED policy={}",
                                alert.alertId(), alert.eventKey(), event.getPolicy().policyId());
                        result = IngestResult.rejected(alert.eventKey(), RejectionReason.SUPPRESSED,
                                "suppressed by policy " + event.getPolicy().policyId(),
                                EventStatus.REJECTED, event.getAlertCount());
                        // 已形成终态，走自动门控复现一次明确的终态去重结论
                        driveAfter = true;
                    } else {
                        boolean firstAccepted = event.getStatus() == EventStatus.NEW
                                && event.getAlertCount() == 0;
                        int count = event.mergeAlert(now, alert.alertId());
                        String detail = (firstAccepted ? "accepted new event" : "merged duplicate")
                                + ", policy=" + event.getPolicy().policyId() + ", alertCount=" + count;
                        log.info("ingest accepted: alert={} eventKey={} disposition={} policy={} alertCount={}",
                                alert.alertId(), alert.eventKey(),
                                firstAccepted ? "NEW_ACCEPTED" : "MERGED",
                                event.getPolicy().policyId(), count);
                        result = IngestResult.accepted(alert.eventKey(),
                                firstAccepted ? IngestResult.Disposition.NEW_ACCEPTED
                                        : IngestResult.Disposition.MERGED,
                                detail, null, event.getStatus(), count);
                        driveAfter = true;
                    }
                }
            }
        }
        if (driveAfter) {
            // 受理后自动一路驱动到结论；终态/抑制重复投递由门控复现原因，执行层保证只执行一次
            RemediationOutcome drive = autoDriver.apply(event);
            synchronized (event) {
                result = new IngestResult(result.accepted(), result.disposition(), result.eventKey(),
                        result.rejection(), result.detail(), drive,
                        event.getStatus(), event.getAlertCount());
            }
            log.info("auto-drive after ingest: eventKey={} disposition={} drive={} finalStatus={} alertCount={}",
                    alert.eventKey(), result.disposition(), drive,
                    result.status(), result.alertCount());
        }
        return result;
    }

    private boolean isTerminalStatus(EventStatus status) {
        return status == EventStatus.CLOSED || status == EventStatus.RESOLVED
                || status == EventStatus.REJECTED || status == EventStatus.FAILED_FINAL;
    }

    /** 终态事件重复投递时复现的原因：保留事件自身拒绝原因；非拒绝终态为 ALREADY_CLOSED。 */
    private RejectionReason terminalRejection(AlertEvent event, EventStatus status) {
        if (status == EventStatus.REJECTED && event.getRejection() != null) {
            return event.getRejection();
        }
        return RejectionReason.ALREADY_CLOSED;
    }

    public AlertEvent getEvent(String eventKey) {
        return events.get(eventKey);
    }

    public List<AlertEvent> listEvents() {
        return List.copyOf(events.values());
    }
}
