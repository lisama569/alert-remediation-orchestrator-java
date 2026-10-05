package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.Alert;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.IngestResult;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventServiceTest {

    private static final Logger log = LoggerFactory.getLogger(EventServiceTest.class);

    private MutableClock clock;
    private EventService service;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        service = new EventService(clock);
        service.setPolicies(List.of(
                new RemediationPolicy("p-restart", Map.of("app", "web"), false, false, 2,
                        Duration.ofSeconds(30)),
                new RemediationPolicy("p-noise", Map.of("app", "noise"), true, false, 0,
                        Duration.ofSeconds(30))));
    }

    private Alert alert(String id, String key, Map<String, String> labels) {
        return new Alert(id, key, "critical", "prometheus", labels, clock.now());
    }

    @Test
    void duplicateAlertsMergeIntoSingleEvent() {
        IngestResult r1 = service.ingest(alert("a1", "evt-1", Map.of("app", "web")));
        IngestResult r2 = service.ingest(alert("a2", "evt-1", Map.of("app", "web")));
        IngestResult r3 = service.ingest(alert("a3", "evt-1", Map.of("app", "web")));
        log.info("input: 3 alerts same eventKey=evt-1; results: r1={} r2={} r3={}",
                r1.accepted(), r2.accepted(), r3.accepted());
        assertTrue(r1.accepted() && r2.accepted() && r3.accepted());
        assertEquals(1, service.listEvents().size());
        assertEquals(3, service.getEvent("evt-1").getAlertCount());
        log.info("decision basis: alertCount={} history={}",
                service.getEvent("evt-1").getAlertCount(), service.getEvent("evt-1").getHistory());
    }

    @Test
    void suppressedAlertRejectedWithDistinguishableReason() {
        IngestResult r = service.ingest(alert("a1", "evt-2", Map.of("app", "noise")));
        log.info("input: alert matching suppressed policy p-noise; result={}", r);
        assertFalse(r.accepted());
        assertEquals(RejectionReason.SUPPRESSED, r.rejection());
    }

    @Test
    void unmatchedPolicyRejectedButEventNotPermanentlyPinned() {
        IngestResult r = service.ingest(alert("a1", "evt-3", Map.of("app", "unknown")));
        log.info("input: alert with no matching policy; result={}", r);
        assertFalse(r.accepted());
        assertEquals(RejectionReason.POLICY_MISMATCH, r.rejection());
        // 不受理不等于终态拒绝：事件保持 NEW，不计数，判定留痕，等规则补齐可继续
        assertEquals(EventStatus.NEW, service.getEvent("evt-3").getStatus());
        assertEquals(0, service.getEvent("evt-3").getAlertCount());
        assertTrue(service.getEvent("evt-3").getHistory().stream()
                        .anyMatch(h -> h.detail().contains("no policy matched")),
                "each policy-mismatch decision must be recorded");

        // 规则补齐后，同样的告警再投进来正常受理（此处未接自动驱动钩子，只验证接入语义）
        service.setPolicies(List.of(
                new RemediationPolicy("p-web", Map.of("app", "web"), false, false, 2,
                        Duration.ofSeconds(30)),
                new RemediationPolicy("p-unknown", Map.of("app", "unknown"), false, false, 1,
                        Duration.ofSeconds(30))));
        IngestResult r2 = service.ingest(alert("a2", "evt-3", Map.of("app", "unknown")));
        log.info("input: same alert redelivered after rule added; result={} status={} snapshot={}",
                r2, service.getEvent("evt-3").getStatus(),
                service.getEvent("evt-3").getPolicySnapshot());
        assertTrue(r2.accepted());
        assertEquals(IngestResult.Disposition.NEW_ACCEPTED, r2.disposition());
        assertEquals(1, service.getEvent("evt-3").getAlertCount());
        assertTrue(service.getEvent("evt-3").getPolicySnapshot().contains("p-unknown"));
    }

    @Test
    void closedEventRejectsFurtherAlerts() {
        service.ingest(alert("a1", "evt-4", Map.of("app", "web")));
        service.getEvent("evt-4").transition(clock.now(),
                com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus.CLOSED,
                "manual close");
        IngestResult r = service.ingest(alert("a2", "evt-4", Map.of("app", "web")));
        log.info("input: alert for closed event evt-4; result={}", r);
        assertFalse(r.accepted());
        assertEquals(RejectionReason.ALREADY_CLOSED, r.rejection());
    }

    @Test
    void policyChangeDoesNotRewriteHistoricalConclusion() {
        service.ingest(alert("a1", "evt-5", Map.of("app", "web")));
        String snapshotBefore = service.getEvent("evt-5").getPolicySnapshot();
        // 策略调整：web 改为抑制
        service.setPolicies(List.of(
                new RemediationPolicy("p-restart-v2", Map.of("app", "web"), true, false, 0,
                        Duration.ofSeconds(30))));
        String snapshotAfter = service.getEvent("evt-5").getPolicySnapshot();
        log.info("input: policy replaced after ingest; snapshotBefore={} snapshotAfter={}",
                snapshotBefore, snapshotAfter);
        assertEquals(snapshotBefore, snapshotAfter);
        assertTrue(snapshotAfter.contains("p-restart"));
    }
}
