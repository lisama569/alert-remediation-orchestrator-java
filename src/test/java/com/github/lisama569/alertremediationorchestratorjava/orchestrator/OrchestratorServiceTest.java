package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.Alert;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.execution.ExecutionResult;
import com.github.lisama569.alertremediationorchestratorjava.execution.RemediationAction;
import com.github.lisama569.alertremediationorchestratorjava.policy.MaintenanceWindow;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrchestratorServiceTest {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorServiceTest.class);

    private static final Instant T0 = Instant.parse("2026-09-25T00:00:00Z");

    private MutableClock clock;
    private EventService eventService;
    private ApprovalService approvalService;
    private OrchestratorService orchestrator;

    private final RemediationAction okAction = (key, attempt) -> ExecutionResult.ok("ok");

    @BeforeEach
    void setUp() {
        clock = new MutableClock(T0);
        eventService = new EventService(clock);
        approvalService = new ApprovalService(clock);
        orchestrator = new OrchestratorService(eventService, approvalService,
                new RemediationExecutor(clock), clock);
    }

    private void usePolicy(boolean highRisk) {
        eventService.setPolicies(List.of(new RemediationPolicy(
                "p1", Map.of("app", "web"), false, highRisk, 2, Duration.ofSeconds(30))));
    }

    private void ingest(String key) {
        eventService.ingest(new Alert("a-" + key, key, "critical", "test",
                Map.of("app", "web"), clock.now()));
    }

    @Test
    void maintenanceWindowDelaysAndResumesAfterEnd() {
        usePolicy(false);
        ingest("evt-m");
        orchestrator.setWindows(List.of(new MaintenanceWindow("nightly",
                T0, T0.plusSeconds(3600), MaintenanceWindow.Kind.MAINTENANCE)));
        RemediationOutcome o1 = orchestrator.remediate("evt-m", okAction);
        log.info("input: remediate inside window [{},{}); outcome={} status={}",
                T0, T0.plusSeconds(3600), o1, eventService.getEvent("evt-m").getStatus());
        assertEquals(RemediationOutcome.Kind.DELAYED, o1.kind());
        assertEquals(EventStatus.DELAYED, eventService.getEvent("evt-m").getStatus());
        assertEquals(0, eventService.getEvent("evt-m").getExecutionCount());

        // 推进到窗口结束后，延迟事件恢复执行
        clock.advanceMillis(3_600_000);
        int resumed = orchestrator.processDelayed(k -> okAction);
        log.info("input: clock advanced past window end; resumed={} status={} executionCount={}",
                resumed, eventService.getEvent("evt-m").getStatus(),
                eventService.getEvent("evt-m").getExecutionCount());
        assertEquals(1, resumed);
        assertEquals(EventStatus.RESOLVED, eventService.getEvent("evt-m").getStatus());
        assertEquals(1, eventService.getEvent("evt-m").getExecutionCount());
    }

    @Test
    void maintenanceWindowBoundaryIsHalfOpen() {
        usePolicy(false);
        ingest("evt-b");
        orchestrator.setWindows(List.of(new MaintenanceWindow("w",
                T0.plusSeconds(10), T0.plusSeconds(20), MaintenanceWindow.Kind.MAINTENANCE)));
        // end 边界（不含）：窗口外，应直接执行
        clock.set(T0.plusSeconds(20));
        RemediationOutcome atEnd = orchestrator.remediate("evt-b", okAction);
        log.info("input: remediate at window end (exclusive); outcome={}", atEnd);
        assertEquals(RemediationOutcome.Kind.EXECUTED, atEnd.kind());

        // start 边界（含）：窗口内，应延迟
        ingest("evt-b2");
        clock.set(T0.plusSeconds(10));
        RemediationOutcome atStart = orchestrator.remediate("evt-b2", okAction);
        log.info("input: remediate at window start (inclusive); outcome={}", atStart);
        assertEquals(RemediationOutcome.Kind.DELAYED, atStart.kind());
    }

    @Test
    void freezeWindowRejectsOperation() {
        usePolicy(false);
        ingest("evt-f");
        orchestrator.setWindows(List.of(new MaintenanceWindow("freeze",
                T0, T0.plusSeconds(3600), MaintenanceWindow.Kind.FREEZE)));
        RemediationOutcome o = orchestrator.remediate("evt-f", okAction);
        log.info("input: remediate inside freeze window; outcome={} status={}",
                o, eventService.getEvent("evt-f").getStatus());
        assertEquals(RemediationOutcome.Kind.REJECTED, o.kind());
        assertEquals(RejectionReason.CHANGE_FREEZE, o.rejection());
        assertEquals(0, eventService.getEvent("evt-f").getExecutionCount());
        assertTrue(eventService.getEvent("evt-f").getHistory().toString().contains("freeze"),
                "freeze decision must be traceable in history");
    }

    @Test
    void highRiskRequiresApprovalAndUnauthorizedIsRejected() {
        usePolicy(true);
        ingest("evt-h");
        RemediationOutcome o1 = orchestrator.remediate("evt-h", okAction);
        log.info("input: high-risk remediate without approval; outcome={} status={}",
                o1, eventService.getEvent("evt-h").getStatus());
        assertEquals(RemediationOutcome.Kind.REJECTED, o1.kind());
        assertEquals(RejectionReason.APPROVAL_REQUIRED, o1.rejection());
        assertEquals(EventStatus.AWAITING_APPROVAL, eventService.getEvent("evt-h").getStatus());
        assertEquals(0, eventService.getEvent("evt-h").getExecutionCount());

        // 审批被拒 -> 未授权，终态拒绝
        approvalService.reject("evt-h", "security-officer");
        RemediationOutcome o2 = orchestrator.remediate("evt-h", okAction);
        log.info("input: remediate after approval rejected; outcome={} status={}",
                o2, eventService.getEvent("evt-h").getStatus());
        assertEquals(RejectionReason.UNAUTHORIZED, o2.rejection());
        assertEquals(EventStatus.REJECTED, eventService.getEvent("evt-h").getStatus());
    }

    @Test
    void highRiskExecutesAfterApproval() {
        usePolicy(true);
        ingest("evt-a");
        orchestrator.remediate("evt-a", okAction); // -> AWAITING_APPROVAL
        approvalService.approve("evt-a", "team-lead");
        RemediationOutcome o = orchestrator.remediate("evt-a", okAction);
        log.info("input: remediate after approval granted; outcome={} status={} executionCount={}",
                o, eventService.getEvent("evt-a").getStatus(),
                eventService.getEvent("evt-a").getExecutionCount());
        assertEquals(RemediationOutcome.Kind.EXECUTED, o.kind());
        assertEquals(EventStatus.RESOLVED, eventService.getEvent("evt-a").getStatus());
        assertEquals(1, eventService.getEvent("evt-a").getExecutionCount());
    }

    @Test
    void concurrentRemediationThroughOrchestratorRunsOnce() throws InterruptedException {
        usePolicy(false);
        ingest("evt-cc");
        AtomicInteger runs = new AtomicInteger();
        RemediationAction counting = (key, attempt) -> {
            runs.incrementAndGet();
            clock.advanceMillis(50);
            return ExecutionResult.ok("ok");
        };
        int threads = 12;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        var go = new java.util.concurrent.CountDownLatch(1);
        List<RemediationOutcome> outcomes = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    go.await();
                    outcomes.add(orchestrator.remediate("evt-cc", counting));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
        long executed = outcomes.stream()
                .filter(o -> o.kind() == RemediationOutcome.Kind.EXECUTED).count();
        log.info("input: {} concurrent remediate calls; executed={} duplicate={} runs={} status={}",
                threads, executed,
                outcomes.stream().filter(o -> o.kind() == RemediationOutcome.Kind.DUPLICATE).count(),
                runs.get(), eventService.getEvent("evt-cc").getStatus());
        assertEquals(1, runs.get());
        assertEquals(1, eventService.getEvent("evt-cc").getExecutionCount());
        assertEquals(EventStatus.RESOLVED, eventService.getEvent("evt-cc").getStatus());
    }
}
