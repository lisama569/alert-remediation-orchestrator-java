package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.Alert;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.IngestResult;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.domain.RemediationOutcome;
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

/**
 * 编排门控测试。接入后自动驱动到结论（无需手动 remediate），
 * 维护窗口靠时钟推进自愈，高风险靠审批决定后续跑。
 */
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

    private IngestResult ingest(String key) {
        return eventService.ingest(new Alert("a-" + key, key, "critical", "test",
                Map.of("app", "web"), clock.now()));
    }

    @Test
    void maintenanceWindowDelaysAndAutoResumesAfterEnd() {
        usePolicy(false);
        orchestrator.setWindows(List.of(new MaintenanceWindow("nightly",
                T0, T0.plusSeconds(3600), MaintenanceWindow.Kind.MAINTENANCE)));
        // 接入即自动驱动：窗口内 -> DELAYED，不执行
        IngestResult ir = ingest("evt-m");
        log.info("[维护窗口延迟自愈] input: ingest inside window [{},{}); drive={} status={}",
                T0, T0.plusSeconds(3600), ir.drive(), ir.status());
        assertEquals(RemediationOutcome.Kind.DELAYED, ir.drive().kind());
        assertEquals(EventStatus.DELAYED, eventService.getEvent("evt-m").getStatus());
        assertEquals(0, eventService.getEvent("evt-m").getExecutionCount());

        // 推进到窗口结束时刻（end 半开放行）：无需任何手动唤醒，自动恢复并执行
        clock.advanceMillis(3_600_000);
        log.info("[维护窗口延迟自愈] basis: clock advanced to {}; status={} executionCount={}",
                clock.now(), eventService.getEvent("evt-m").getStatus(),
                eventService.getEvent("evt-m").getExecutionCount());
        assertEquals(EventStatus.RESOLVED, eventService.getEvent("evt-m").getStatus());
        assertEquals(1, eventService.getEvent("evt-m").getExecutionCount());
        assertTrue(eventService.getEvent("evt-m").getHistory().stream()
                        .anyMatch(h -> h.detail().contains("delayed")),
                "each delay decision must be recorded");
        assertTrue(eventService.getEvent("evt-m").getHistory().stream()
                        .anyMatch(h -> h.detail().contains("window ended, resuming")),
                "resume decision must be recorded");
    }

    @Test
    void maintenanceWindowBoundaryIsHalfOpen() {
        usePolicy(false);
        orchestrator.setWindows(List.of(new MaintenanceWindow("w",
                T0.plusSeconds(10), T0.plusSeconds(20), MaintenanceWindow.Kind.MAINTENANCE)));
        // end 边界（不含）：窗口外，接入即执行
        clock.set(T0.plusSeconds(20));
        IngestResult atEnd = ingest("evt-b");
        log.info("[窗口半开边界] input: ingest at window end (exclusive) t={}; drive={}",
                clock.now(), atEnd.drive());
        assertEquals(RemediationOutcome.Kind.EXECUTED, atEnd.drive().kind());

        // start 边界（含）：窗口内，接入即延迟
        clock.set(T0.plusSeconds(10));
        IngestResult atStart = ingest("evt-b2");
        log.info("[窗口半开边界] input: ingest at window start (inclusive) t={}; drive={}",
                clock.now(), atStart.drive());
        assertEquals(RemediationOutcome.Kind.DELAYED, atStart.drive().kind());
    }

    @Test
    void freezeWindowRejectsOperation() {
        usePolicy(false);
        orchestrator.setWindows(List.of(new MaintenanceWindow("freeze",
                T0, T0.plusSeconds(3600), MaintenanceWindow.Kind.FREEZE)));
        IngestResult ir = ingest("evt-f");
        log.info("[冻结期拒绝] input: ingest inside freeze window; drive={} status={}",
                ir.drive(), eventService.getEvent("evt-f").getStatus());
        assertEquals(RemediationOutcome.Kind.REJECTED, ir.drive().kind());
        assertEquals(RejectionReason.CHANGE_FREEZE, ir.drive().rejection());
        // 冻结拒绝语义与手动一致：不执行，事件不终结（非维护时段可继续）
        assertEquals(0, eventService.getEvent("evt-f").getExecutionCount());
        assertEquals(EventStatus.NEW, eventService.getEvent("evt-f").getStatus());
        assertTrue(eventService.getEvent("evt-f").getHistory().toString().contains("freeze"),
                "freeze decision must be traceable in history");
    }

    @Test
    void highRiskRequiresApprovalAndUnauthorizedIsRejected() {
        usePolicy(true);
        IngestResult ir = ingest("evt-h");
        log.info("[高风险审批] input: high-risk ingest without approval; drive={} status={}",
                ir.drive(), eventService.getEvent("evt-h").getStatus());
        assertEquals(RemediationOutcome.Kind.REJECTED, ir.drive().kind());
        assertEquals(RejectionReason.APPROVAL_REQUIRED, ir.drive().rejection());
        assertEquals(EventStatus.AWAITING_APPROVAL, eventService.getEvent("evt-h").getStatus());
        assertEquals(0, eventService.getEvent("evt-h").getExecutionCount());

        // 审批被拒 -> 自动续跑到终态拒绝（UNAUTHORIZED），无需再手动触发
        approvalService.reject("evt-h", "security-officer");
        log.info("[高风险审批] basis: approval rejected; status={} rejection={} executionCount={}",
                eventService.getEvent("evt-h").getStatus(),
                eventService.getEvent("evt-h").getRejection(),
                eventService.getEvent("evt-h").getExecutionCount());
        assertEquals(EventStatus.REJECTED, eventService.getEvent("evt-h").getStatus());
        assertEquals(RejectionReason.UNAUTHORIZED, eventService.getEvent("evt-h").getRejection());
    }

    @Test
    void highRiskExecutesAfterApproval() {
        usePolicy(true);
        ingest("evt-a"); // 自动驱动 -> AWAITING_APPROVAL
        // 批准 -> 自动续跑执行
        approvalService.approve("evt-a", "team-lead");
        log.info("[高风险审批] input: approval granted; status={} executionCount={}",
                eventService.getEvent("evt-a").getStatus(),
                eventService.getEvent("evt-a").getExecutionCount());
        assertEquals(EventStatus.RESOLVED, eventService.getEvent("evt-a").getStatus());
        assertEquals(1, eventService.getEvent("evt-a").getExecutionCount());
    }

    @Test
    void concurrentRemediationThroughOrchestratorRunsOnce() throws InterruptedException {
        usePolicy(false);
        int threads = 12;
        AtomicInteger runs = new AtomicInteger();
        // 所有并发投递都进入自动驱动门控后才放行真正执行，模拟最大重叠，
        // 证明：全部告警都归并计数，但处置只真正执行一次
        var allDrove = new java.util.concurrent.CountDownLatch(threads);
        orchestrator.setActionResolver(event -> (key, attempt) -> {
            try {
                allDrove.await(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            runs.incrementAndGet();
            clock.advanceMillis(50);
            return ExecutionResult.ok("ok");
        });
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        var go = new java.util.concurrent.CountDownLatch(1);
        List<IngestResult> results = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int n = i;
            pool.submit(() -> {
                try {
                    go.await();
                    IngestResult r = eventService.ingest(new Alert("a" + n, "evt-cc", "critical",
                            "test", Map.of("app", "web"), clock.now()));
                    results.add(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    allDrove.countDown();
                }
            });
        }
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(allDrove.await(5, java.util.concurrent.TimeUnit.SECONDS));
        long executed = results.stream()
                .filter(r -> r.drive() != null
                        && r.drive().kind() == RemediationOutcome.Kind.EXECUTED).count();
        log.info("[并发接入同一事件] input: {} concurrent ingests on evt-cc; newAccepted={} merged={}"
                        + " driveExecuted={} runs={} finalStatus={} alertCount={} executionCount={}",
                threads,
                results.stream().filter(r -> r.disposition() == IngestResult.Disposition.NEW_ACCEPTED).count(),
                results.stream().filter(r -> r.disposition() == IngestResult.Disposition.MERGED).count(),
                executed, runs.get(), eventService.getEvent("evt-cc").getStatus(),
                eventService.getEvent("evt-cc").getAlertCount(),
                eventService.getEvent("evt-cc").getExecutionCount());
        assertEquals(1, runs.get());
        assertEquals(1, eventService.getEvent("evt-cc").getExecutionCount());
        assertEquals(EventStatus.RESOLVED, eventService.getEvent("evt-cc").getStatus());
        assertEquals(threads, eventService.getEvent("evt-cc").getAlertCount());
    }
}
