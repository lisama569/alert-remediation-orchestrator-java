package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.Alert;
import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.HistoryEntry;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.execution.ExecutionResult;
import com.github.lisama569.alertremediationorchestratorjava.execution.FailureType;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 自动驱动 / 延迟自愈 / 结论一致性 的自动化测试。
 * 时间推进全部通过 MutableClock，不依赖真实等待；
 * 每个用例打印输入与判定依据，便于从日志核对覆盖的业务。
 */
class AutoOrchestrationTest {

    private static final Logger log = LoggerFactory.getLogger(AutoOrchestrationTest.class);

    private static final Instant T0 = Instant.parse("2026-10-05T00:00:00Z");

    private MutableClock clock;
    private EventService eventService;
    private ApprovalService approvalService;
    private OrchestratorService orchestrator;

    private final AtomicInteger actionRuns = new AtomicInteger();
    private final RemediationAction okAction = (key, attempt) -> {
        actionRuns.incrementAndGet();
        return ExecutionResult.ok("ok attempt=" + attempt);
    };

    @BeforeEach
    void setUp() {
        clock = new MutableClock(T0);
        eventService = new EventService(clock);
        approvalService = new ApprovalService(clock);
        orchestrator = new OrchestratorService(eventService, approvalService,
                new RemediationExecutor(clock), clock);
        actionRuns.set(0);
    }

    private void usePolicy(String policyId, boolean suppressed, boolean highRisk, int maxRetries) {
        eventService.setPolicies(List.of(new RemediationPolicy(
                policyId, Map.of("app", "web"), suppressed, highRisk, maxRetries,
                Duration.ofSeconds(30))));
    }

    private Alert alert(String id, String key) {
        return new Alert(id, key, "critical", "test", Map.of("app", "web"), clock.now());
    }

    private AlertEvent event(String key) {
        return eventService.getEvent(key);
    }

    // ---------- 自动驱动 ----------

    @Test
    void ingestAutoDrivesToTerminalWithoutManualTrigger() {
        usePolicy("p1", false, false, 2);
        orchestrator.setDefaultActionFactory(k -> okAction);
        IngestOutcome o = orchestrator.ingestAndDrive(alert("a1", "evt-auto"));
        log.info("input: ingestAndDrive normal policy; outcome={} status={} executionCount={}",
                o, event("evt-auto").getStatus(), event("evt-auto").getExecutionCount());
        log.info("decision basis: no window/freeze/approval gate -> auto executed once");
        assertTrue(o.accepted());
        assertEquals(RemediationOutcome.Kind.EXECUTED, o.remediation().kind());
        assertEquals(EventStatus.RESOLVED, event("evt-auto").getStatus());
        assertEquals(1, event("evt-auto").getExecutionCount());
        assertEquals(1, actionRuns.get());
    }

    @Test
    void sequentialRedeliveryKeepsSingleExecutionAndStableConclusion() {
        usePolicy("p1", false, false, 2);
        orchestrator.setDefaultActionFactory(k -> okAction);
        IngestOutcome first = orchestrator.ingestAndDrive(alert("a1", "evt-seq"));
        IngestOutcome second = orchestrator.ingestAndDrive(alert("a1", "evt-seq"));
        IngestOutcome third = orchestrator.ingestAndDrive(alert("a1", "evt-seq"));
        log.info("input: same alert delivered 3 times; first={} second={} third={}",
                first.remediation().kind(), second.ingest().rejection(), third.ingest().rejection());
        log.info("decision basis: terminal RESOLVED -> redeliveries ALREADY_CLOSED, no re-execution");
        assertEquals(RemediationOutcome.Kind.EXECUTED, first.remediation().kind());
        assertEquals(RejectionReason.ALREADY_CLOSED, second.ingest().rejection());
        assertEquals(RejectionReason.ALREADY_CLOSED, third.ingest().rejection());
        assertNull(second.remediation());
        assertEquals(1, event("evt-seq").getExecutionCount());
        assertEquals(1, actionRuns.get());
        assertEquals(EventStatus.RESOLVED, event("evt-seq").getStatus());
    }

    @Test
    void concurrentIngestAndDriveExecutesExactlyOnce() throws InterruptedException {
        usePolicy("p1", false, false, 2);
        int threads = 12;
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger runs = new AtomicInteger();
        RemediationAction blocking = (key, attempt) -> {
            runs.incrementAndGet();
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ExecutionResult.ok("ok");
        };
        orchestrator.setDefaultActionFactory(k -> blocking);

        List<IngestOutcome> outcomes = new CopyOnWriteArrayList<>();
        var pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            String id = "a-" + i;
            pool.submit(() -> {
                try {
                    go.await();
                    outcomes.add(orchestrator.ingestAndDrive(alert(id, "evt-conc")));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        go.countDown();
        assertTrue(entered.await(10, TimeUnit.SECONDS), "first driver must enter action");
        // 等所有线程完成归并、且其余驱动请求都拿到去重结果后再放行执行
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((event("evt-conc").getAlertCount() < threads || outcomes.size() < threads - 1)
                && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        release.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long executed = outcomes.stream()
                .filter(o -> o.remediation() != null
                        && o.remediation().kind() == RemediationOutcome.Kind.EXECUTED).count();
        long duplicate = outcomes.stream()
                .filter(o -> o.remediation() != null
                        && o.remediation().kind() == RemediationOutcome.Kind.DUPLICATE).count();
        log.info("input: {} threads ingestAndDrive same eventKey concurrently; "
                        + "executed={} duplicate={} alertCount={} runs={} executionCount={} status={}",
                threads, executed, duplicate, event("evt-conc").getAlertCount(), runs.get(),
                event("evt-conc").getExecutionCount(), event("evt-conc").getStatus());
        log.info("decision basis: all alerts merged into one event; executor running-set dedups "
                + "concurrent drives -> exactly one EXECUTED, rest DUPLICATE");
        assertEquals(threads, outcomes.size());
        assertEquals(threads, event("evt-conc").getAlertCount());
        assertEquals(1, executed);
        assertEquals(threads - 1, duplicate);
        assertEquals(1, runs.get());
        assertEquals(1, event("evt-conc").getExecutionCount());
        assertEquals(EventStatus.RESOLVED, event("evt-conc").getStatus());
    }

    // ---------- 自动链路上的既有门控 ----------

    @Test
    void autoDriveKeepsSuppressionGatingAndStableReason() {
        usePolicy("p-sup", true, false, 0);
        orchestrator.setDefaultActionFactory(k -> okAction);
        IngestOutcome first = orchestrator.ingestAndDrive(alert("a1", "evt-sup"));
        IngestOutcome second = orchestrator.ingestAndDrive(alert("a1", "evt-sup"));
        log.info("input: suppressed policy, alert delivered twice; first={} second={}",
                first.ingest().rejection(), second.ingest().rejection());
        log.info("decision basis: suppression is a terminal rejection; redelivery must "
                + "reproduce the same SUPPRESSED reason, never executed");
        assertFalse(first.accepted());
        assertEquals(RejectionReason.SUPPRESSED, first.ingest().rejection());
        assertNull(first.remediation());
        assertEquals(RejectionReason.SUPPRESSED, second.ingest().rejection(),
                "rejection reason must be stable across redeliveries");
        assertEquals(0, event("evt-sup").getExecutionCount());
        assertEquals(0, actionRuns.get());
    }

    @Test
    void autoDriveKeepsFreezeGating() {
        usePolicy("p1", false, false, 2);
        orchestrator.setDefaultActionFactory(k -> okAction);
        orchestrator.setWindows(List.of(new MaintenanceWindow("freeze",
                T0, T0.plusSeconds(3600), MaintenanceWindow.Kind.FREEZE)));
        IngestOutcome o = orchestrator.ingestAndDrive(alert("a1", "evt-frz"));
        log.info("input: ingestAndDrive inside freeze window [{},{}); outcome={} status={}",
                T0, T0.plusSeconds(3600), o, event("evt-frz").getStatus());
        log.info("decision basis: freeze window rejects with CHANGE_FREEZE, no execution");
        assertTrue(o.accepted());
        assertEquals(RemediationOutcome.Kind.REJECTED, o.remediation().kind());
        assertEquals(RejectionReason.CHANGE_FREEZE, o.remediation().rejection());
        assertEquals(0, event("evt-frz").getExecutionCount());
        assertEquals(0, actionRuns.get());
    }

    @Test
    void autoDriveKeepsApprovalGatingThenProceedsAfterApproval() {
        usePolicy("p-hr", false, true, 2);
        orchestrator.setDefaultActionFactory(k -> okAction);
        IngestOutcome o1 = orchestrator.ingestAndDrive(alert("a1", "evt-hr"));
        log.info("input: high-risk policy, no approval; outcome={} status={}",
                o1.remediation(), event("evt-hr").getStatus());
        assertEquals(RemediationOutcome.Kind.REJECTED, o1.remediation().kind());
        assertEquals(RejectionReason.APPROVAL_REQUIRED, o1.remediation().rejection());
        assertEquals(EventStatus.AWAITING_APPROVAL, event("evt-hr").getStatus());
        assertEquals(0, actionRuns.get());

        approvalService.approve("evt-hr", "team-lead");
        IngestOutcome o2 = orchestrator.ingestAndDrive(alert("a1", "evt-hr"));
        log.info("input: redeliver after approval granted; outcome={} status={} executionCount={}",
                o2.remediation(), event("evt-hr").getStatus(), event("evt-hr").getExecutionCount());
        log.info("decision basis: approval gate passed on redelivery -> executed exactly once");
        assertTrue(o2.accepted());
        assertEquals(RemediationOutcome.Kind.EXECUTED, o2.remediation().kind());
        assertEquals(EventStatus.RESOLVED, event("evt-hr").getStatus());
        assertEquals(1, event("evt-hr").getExecutionCount());
        assertEquals(1, actionRuns.get());
    }

    // ---------- 延迟自愈 ----------

    @Test
    void delayedEventAutoResumesWhenClockPassesWindowEnd() {
        usePolicy("p1", false, false, 2);
        orchestrator.setDefaultActionFactory(k -> okAction);
        orchestrator.enableAutoResume();
        orchestrator.setWindows(List.of(new MaintenanceWindow("nightly",
                T0, T0.plusSeconds(3600), MaintenanceWindow.Kind.MAINTENANCE)));

        IngestOutcome o1 = orchestrator.ingestAndDrive(alert("a1", "evt-dly"));
        log.info("input: ingestAndDrive inside window [{},{}); outcome={} status={}",
                T0, T0.plusSeconds(3600), o1.remediation(), event("evt-dly").getStatus());
        assertEquals(RemediationOutcome.Kind.DELAYED, o1.remediation().kind());
        assertEquals(EventStatus.DELAYED, event("evt-dly").getStatus());
        assertEquals(0, actionRuns.get());

        // 窗口内重复投递：归并 + 再次判定仍为延迟，不执行
        IngestOutcome o2 = orchestrator.ingestAndDrive(alert("a1", "evt-dly"));
        log.info("input: redeliver inside window; outcome={} alertCount={}",
                o2.remediation().kind(), event("evt-dly").getAlertCount());
        assertEquals(RemediationOutcome.Kind.DELAYED, o2.remediation().kind());
        assertEquals(2, event("evt-dly").getAlertCount());
        assertEquals(0, actionRuns.get());

        // 窗口内推进时间：仍延迟，不执行
        clock.advanceMillis(1_800_000);
        log.info("input: clock advanced 30min (still inside window); status={} runs={}",
                event("evt-dly").getStatus(), actionRuns.get());
        assertEquals(EventStatus.DELAYED, event("evt-dly").getStatus());
        assertEquals(0, actionRuns.get());

        // 推进到窗口结束点（半开区间 end 不命中）：自动恢复并执行，无需手动唤醒
        clock.advanceMillis(1_800_000);
        log.info("input: clock advanced to window end; status={} runs={} executionCount={}",
                event("evt-dly").getStatus(), actionRuns.get(),
                event("evt-dly").getExecutionCount());
        assertEquals(EventStatus.RESOLVED, event("evt-dly").getStatus());
        assertEquals(1, actionRuns.get());
        assertEquals(1, event("evt-dly").getExecutionCount());

        // 延迟与恢复的判定过程均可查
        List<HistoryEntry> history = event("evt-dly").getHistory();
        log.info("decision basis: history={}", history);
        assertTrue(history.toString().contains("delayed until"),
                "delay decision must be recorded");
        assertTrue(history.toString().contains("resuming"),
                "resume decision must be recorded");

        // 恢复后继续推进时间：不会重复执行
        clock.advanceMillis(3_600_000);
        assertEquals(1, actionRuns.get());
        assertEquals(1, event("evt-dly").getExecutionCount());
    }

    // ---------- 结论一致性 ----------

    @Test
    void policyMismatchIsTransientAndRecoversAfterPolicyFix() {
        orchestrator.setDefaultActionFactory(k -> okAction);
        IngestOutcome o1 = orchestrator.ingestAndDrive(alert("a1", "evt-pm"));
        IngestOutcome o2 = orchestrator.ingestAndDrive(alert("a1", "evt-pm"));
        log.info("input: no policy configured, alert delivered twice; first={} second={} status={}",
                o1.ingest().rejection(), o2.ingest().rejection(), event("evt-pm").getStatus());
        log.info("decision basis: POLICY_MISMATCH is transient (non-terminal), reason stable, "
                + "event not pinned");
        assertEquals(RejectionReason.POLICY_MISMATCH, o1.ingest().rejection());
        assertEquals(RejectionReason.POLICY_MISMATCH, o2.ingest().rejection(),
                "same transient reason on redelivery");
        assertEquals(EventStatus.NEW, event("evt-pm").getStatus());
        assertEquals(0, actionRuns.get());

        // 补齐策略后同一告警再投：正常受理并自动走到结论
        usePolicy("p1", false, false, 2);
        IngestOutcome o3 = orchestrator.ingestAndDrive(alert("a1", "evt-pm"));
        log.info("input: policy added, redeliver same alert; outcome={} status={} runs={}",
                o3.remediation(), event("evt-pm").getStatus(), actionRuns.get());
        assertTrue(o3.accepted());
        assertEquals(RemediationOutcome.Kind.EXECUTED, o3.remediation().kind());
        assertEquals(EventStatus.RESOLVED, event("evt-pm").getStatus());
        assertEquals(1, actionRuns.get());
    }

    @Test
    void terminalConclusionReproducedWithOriginalPolicyVersion() {
        // v1：非抑制、非高风险、maxRetries=2
        usePolicy("p-v1", false, false, 2);
        orchestrator.setWindows(List.of(new MaintenanceWindow("nightly",
                T0, T0.plusSeconds(3600), MaintenanceWindow.Kind.MAINTENANCE)));
        orchestrator.setDefaultActionFactory(k -> okAction);
        IngestOutcome o1 = orchestrator.ingestAndDrive(alert("a1", "evt-ver"));
        assertEquals(RemediationOutcome.Kind.DELAYED, o1.remediation().kind());
        String snapshotV1 = event("evt-ver").getPolicySnapshot();
        log.info("input: event ingested under policy v1 (maxRetries=2), delayed by window; "
                + "snapshot={}", snapshotV1);

        // 策略调整为 v2：抑制 + 高风险 + maxRetries=0 —— 不应影响已命中 v1 的事件
        usePolicy("p-v2", true, true, 0);
        orchestrator.setWindows(List.of());

        // 用可观测行为验证：始终可重试失败的动作，若走 v1 应执行 1+2=3 次；
        // 若被 v2 带偏，则会被抑制 / 要求审批 / 只执行 1 次
        AtomicInteger runs = new AtomicInteger();
        RemediationAction alwaysRetryableFail = (key, attempt) -> {
            runs.incrementAndGet();
            return ExecutionResult.fail(FailureType.RETRYABLE, "flaky");
        };
        RemediationOutcome o2 = orchestrator.remediate("evt-ver", alwaysRetryableFail);
        log.info("input: policy replaced by v2 (suppressed+highRisk+maxRetries=0), then remediate; "
                        + "outcome={} runs={} status={} approvalRecord={}",
                o2, runs.get(), event("evt-ver").getStatus(),
                approvalService.getRecord("evt-ver"));
        log.info("decision basis: runs==3 proves v1.maxRetries=2 was used; no approval record "
                + "proves v1.highRisk=false; execution happened at all proves v1.suppressed=false");
        assertEquals(3, runs.get(), "must retry per v1 maxRetries=2, not v2 maxRetries=0");
        assertNull(approvalService.getRecord("evt-ver"),
                "v2 highRisk must not trigger approval for v1 event");
        assertEquals(EventStatus.FAILED_FINAL, event("evt-ver").getStatus());
        assertEquals(snapshotV1, event("evt-ver").getPolicySnapshot());

        // 终态结论不被后续投递改变
        IngestOutcome o3 = orchestrator.ingestAndDrive(alert("a1", "evt-ver"));
        log.info("input: redeliver after FAILED_FINAL; ingest={} runs={}",
                o3.ingest().rejection(), runs.get());
        assertEquals(RejectionReason.ALREADY_CLOSED, o3.ingest().rejection());
        assertEquals(3, runs.get());
        assertEquals(EventStatus.FAILED_FINAL, event("evt-ver").getStatus());
    }

    @Test
    void resolvedEventImmuneToLaterPolicyChange() {
        usePolicy("p-v1", false, false, 2);
        orchestrator.setDefaultActionFactory(k -> okAction);
        IngestOutcome o1 = orchestrator.ingestAndDrive(alert("a1", "evt-imm"));
        assertEquals(RemediationOutcome.Kind.EXECUTED, o1.remediation().kind());
        assertEquals(EventStatus.RESOLVED, event("evt-imm").getStatus());

        // 策略改为抑制：历史结论不受影响
        usePolicy("p-v2", true, false, 0);
        IngestOutcome o2 = orchestrator.ingestAndDrive(alert("a1", "evt-imm"));
        log.info("input: policy switched to suppressed after RESOLVED, redeliver; "
                        + "ingest={} status={} executionCount={} snapshot={}",
                o2.ingest().rejection(), event("evt-imm").getStatus(),
                event("evt-imm").getExecutionCount(), event("evt-imm").getPolicySnapshot());
        log.info("decision basis: terminal conclusion is reproducible; suppressed v2 must not "
                + "rewrite history nor trigger new execution");
        assertEquals(RejectionReason.ALREADY_CLOSED, o2.ingest().rejection());
        assertEquals(EventStatus.RESOLVED, event("evt-imm").getStatus());
        assertEquals(1, event("evt-imm").getExecutionCount());
        assertEquals(1, actionRuns.get());
        assertTrue(event("evt-imm").getPolicySnapshot().contains("p-v1"));
    }
}
