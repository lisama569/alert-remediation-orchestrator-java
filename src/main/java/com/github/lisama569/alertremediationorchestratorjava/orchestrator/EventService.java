package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.Alert;
import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.IngestResult;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 告警接入与事件归并。
 * 判定顺序：终态 > 已记录的终态拒绝 > 策略匹配（仅首次）> 抑制。拒绝原因可区分且前后一致：
 * 终态拒绝（抑制/未授权）再次投递时原样返回当时记录的原因；
 * 策略不匹配只是非终态的暂时结论，补齐策略后同一告警再投可正常受理；
 * 已命中策略的事件不会被后续策略调整改判（policy 引用即快照）。
 */
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    private final Map<String, AlertEvent> events = new ConcurrentHashMap<>();
    private final List<RemediationPolicy> policies = new CopyOnWriteArrayList<>();
    private final MutableClock clock;

    public EventService(MutableClock clock) {
        this.clock = clock;
    }

    /** 全量替换策略（策略调整不影响已落库的历史结论）。 */
    public void setPolicies(List<RemediationPolicy> newPolicies) {
        policies.clear();
        policies.addAll(newPolicies);
    }

    public IngestResult ingest(Alert alert) {
        AlertEvent event = events.computeIfAbsent(alert.eventKey(),
                k -> new AlertEvent(k, clock.now()));
        synchronized (event) {
            EventStatus status = event.getStatus();
            if (status == EventStatus.CLOSED || status == EventStatus.RESOLVED
                    || status == EventStatus.FAILED_FINAL) {
                log.info("ingest rejected: alert={} eventKey={} reason=ALREADY_CLOSED status={}",
                        alert.alertId(), alert.eventKey(), status);
                return IngestResult.rejected(alert.eventKey(), RejectionReason.ALREADY_CLOSED,
                        "event already in terminal status " + status);
            }
            if (status == EventStatus.REJECTED) {
                // 终态拒绝：原样返回当时记录的原因，保证重复投递结论一致
                RejectionReason recorded = event.getRejection() != null
                        ? event.getRejection() : RejectionReason.ALREADY_CLOSED;
                log.info("ingest rejected: alert={} eventKey={} reason={} (recorded, terminal)",
                        alert.alertId(), alert.eventKey(), recorded);
                return IngestResult.rejected(alert.eventKey(), recorded,
                        "event already rejected: " + recorded);
            }
            RemediationPolicy policy = event.getPolicy();
            if (policy == null) {
                // 仅在事件尚未命中过策略时才重新匹配；已命中的事件不被策略调整改判
                policy = policies.stream()
                        .filter(p -> p.matches(alert.labels()))
                        .findFirst()
                        .orElse(null);
                if (policy == null) {
                    // 非终态：不钉死事件，补齐策略后同一告警可再次受理
                    event.noteTransientRejection(clock.now(), RejectionReason.POLICY_MISMATCH,
                            "no policy matched labels=" + alert.labels());
                    log.info("ingest rejected: alert={} eventKey={} reason=POLICY_MISMATCH labels={}",
                            alert.alertId(), alert.eventKey(), alert.labels());
                    return IngestResult.rejected(alert.eventKey(), RejectionReason.POLICY_MISMATCH,
                            "no policy matched");
                }
                event.setPolicy(policy);
                event.setPolicySnapshot("policyId=" + policy.policyId()
                        + ", suppressed=" + policy.suppressed()
                        + ", highRisk=" + policy.highRisk()
                        + ", maxRetries=" + policy.maxRetries()
                        + ", timeout=" + policy.timeout());
                if (policy.suppressed()) {
                    event.reject(clock.now(), RejectionReason.SUPPRESSED,
                            "suppressed by policy " + policy.policyId());
                    log.info("ingest rejected: alert={} eventKey={} reason=SUPPRESSED policy={}",
                            alert.alertId(), alert.eventKey(), policy.policyId());
                    return IngestResult.rejected(alert.eventKey(), RejectionReason.SUPPRESSED,
                            "suppressed by policy " + policy.policyId());
                }
            }
            event.clearRejection();
            int count = event.mergeAlert(clock.now(), alert.alertId());
            log.info("ingest accepted: alert={} eventKey={} policy={} alertCount={}",
                    alert.alertId(), alert.eventKey(), policy.policyId(), count);
            return IngestResult.accepted(alert.eventKey(),
                    "merged into event, policy=" + policy.policyId() + ", alertCount=" + count);
        }
    }

    public AlertEvent getEvent(String eventKey) {
        return events.get(eventKey);
    }

    public List<AlertEvent> listEvents() {
        return List.copyOf(events.values());
    }
}
