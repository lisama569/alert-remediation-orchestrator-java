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
 * 判定顺序：已闭环 > 策略匹配 > 抑制。拒绝原因可区分；
 * 命中策略的 id 与判定依据写入事件快照，策略调整后历史结论仍可复现。
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
                    || status == EventStatus.REJECTED) {
                log.info("ingest rejected: alert={} eventKey={} reason=ALREADY_CLOSED status={}",
                        alert.alertId(), alert.eventKey(), status);
                return IngestResult.rejected(alert.eventKey(), RejectionReason.ALREADY_CLOSED,
                        "event already in terminal status " + status);
            }
            RemediationPolicy policy = policies.stream()
                    .filter(p -> p.matches(alert.labels()))
                    .findFirst()
                    .orElse(null);
            if (policy == null) {
                event.reject(clock.now(), RejectionReason.POLICY_MISMATCH,
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
