package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.Alert;
import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.domain.IngestResult;
import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;
import com.github.lisama569.alertremediationorchestratorjava.domain.RemediationOutcome;
import com.github.lisama569.alertremediationorchestratorjava.execution.ExecutionResult;
import com.github.lisama569.alertremediationorchestratorjava.execution.FailureType;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 自动化链路测试（本次新增能力的端到端覆盖）：
 * A 接入即自动一路处置到结论，无需手动触发；
 * B 同事件重复/并发投递只真正执行一次，计数与终态稳定，其余给出明确归并/去重结果；
 * C 自动链路保持原有门控语义（抑制/冻结/高风险审批/终态）；
 * D 维护窗口延迟靠时钟推进自愈，半开区间，每次延迟/恢复判定留痕，恢复后只执行一次；
 * E 重复投递结论一致（拒绝原因不改口）；未匹配规则不被钉死，规则补齐后可继续；
 * F 历史结论可复现：用可观测的实际执行行为证明走的是事件受理当时那一版规则。
 * 所有时间用例仅推进 MutableClock，不做真实等待。
 */
class AutomatedPipelineTest {

    private static final Logger log = LoggerFactory.getLogger(AutomatedPipelineTest.class);

    private static final Instant T0 = Instant.parse("2026-10-05T00:00:00Z");

    private MutableClock clock;
    private EventService events;
    private ApprovalService approvals;
    private OrchestratorService orchestrator;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(T0);
        events = new EventService(clock);
        approvals = new ApprovalService(clock);
        orchestrator = new OrchestratorService(events, approvals,
                new RemediationExecutor(clock), clock);
    }

    private RemediationPolicy policy(String id, String app, boolean suppressed, boolean highRisk,
                                     int maxRetries) {
        return new RemediationPolicy(id, Map.of("app", app), suppressed, highRisk, maxRetries,
                Duration.ofSeconds(30));
    }

    private Alert alert(String id, String key, String app) {
        return new Alert(id, key, "critical", "prometheus",
                Map.of("app", app), clock.now());
    }

    // ---------- A：接入即自动处置，无需手动触发 ----------

    @Test
    void a_ingestDrivesAutomaticallyToConclusion() {
        events.setPolicies(List.of(policy("p-web", "web", false, false, 0)));
        IngestResult r = events.ingest(alert("a1", "evt-auto", "web"));
        log.info("[A 接入即处置] input: one ingest, no manual trigger; disposition={} drive={} status={}",
                r.disposition(), r.drive(), r.status());
        assertEquals(IngestResult.Disposition.NEW_ACCEPTED, r.disposition());
        assertEquals(RemediationOutcome.Kind.EXECUTED, r.drive().kind());
        assertEquals(EventStatus.RESOLVED, events.getEvent("evt-auto").getStatus());
        assertEquals(1, events.getEvent("evt-auto").getExecutionCount());
    }

    // ---------- B：同事件重复投递只执行一次，归并/去重结果明确且稳定 ----------

    @Test
    void b_repeatedIngestExecutesOnceAndCountsStably() {
        events.setPolicies(List.of(policy("p-web", "web", false, false, 0)));
        IngestResult r1 = events.ingest(alert("a1", "evt-dup", "web"));
        IngestResult r2 = events.ingest(alert("a2", "evt-dup", "web"));
        IngestResult r3 = events.ingest(alert("a3", "evt-dup", "web"));
        log.info("[B 重复投递去重] input: 3 sequential ingests same eventKey; drives={}/{}/{} statuses={}/{}/{}",
                r1.drive().kind(), r2.drive().kind(), r3.drive().kind(),
                r1.status(), r2.status(), r3.status());
        assertEquals(RemediationOutcome.Kind.EXECUTED, r1.drive().kind());
        // 后两次到达时事件已终态：给出明确去重结果（终态拒绝 ALREADY_CLOSED），不重复计数/执行
        assertEquals(IngestResult.Disposition.REJECTED, r2.disposition());
        assertEquals(IngestResult.Disposition.REJECTED, r3.disposition());
        assertEquals(RemediationOutcome.Kind.REJECTED, r2.drive().kind());
        assertEquals(RejectionReason.ALREADY_CLOSED, r2.drive().rejection());
        AlertEvent e = events.getEvent("evt-dup");
        assertEquals(1, e.getExecutionCount());
        assertEquals(1, e.getAlertCount());
        assertEquals(EventStatus.RESOLVED, e.getStatus());
    }

    @Test
    void b_concurrentIngestFromMultipleThreadsRunsOnce() throws InterruptedException {
        events.setPolicies(List.of(policy("p-web", "web", false, false, 0)));
        int n = 16;
        AtomicInteger actionRuns = new AtomicInteger();
        CountDownLatch allDrove = new CountDownLatch(n);
        // 动作等所有线程都进入驱动后再放行，制造最大重叠；可观测实际执行次数
        orchestrator.setActionResolver(event -> (key, attempt) -> {
            try {
                allDrove.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            actionRuns.incrementAndGet();
            clock.advanceMillis(10);
            return ExecutionResult.ok("done");
        });
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch go = new CountDownLatch(1);
        List<IngestResult> results = new CopyOnWriteArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    go.await();
                    results.add(events.ingest(alert("c" + idx, "evt-conc", "web")));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } finally {
                    allDrove.countDown();
                }
            });
        }
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        long executed = results.stream()
                .filter(r -> r.drive().kind() == RemediationOutcome.Kind.EXECUTED).count();
        long duplicated = results.stream()
                .filter(r -> r.drive().kind() == RemediationOutcome.Kind.DUPLICATE).count();
        log.info("[B 并发同一事件] input: {} concurrent ingests; EXECUTED={} DUPLICATE={} "
                        + "actionRuns={} executionCount={} alertCount={} finalStatus={}",
                n, executed, duplicated, actionRuns.get(),
                events.getEvent("evt-conc").getExecutionCount(),
                events.getEvent("evt-conc").getAlertCount(),
                events.getEvent("evt-conc").getStatus());
        assertEquals(1, actionRuns.get());
        assertEquals(1, executed);
        assertEquals(n - 1, duplicated);
        assertEquals(1, events.getEvent("evt-conc").getExecutionCount());
        assertEquals(n, events.getEvent("evt-conc").getAlertCount());
        assertEquals(EventStatus.RESOLVED, events.getEvent("evt-conc").getStatus());
    }

    // ---------- C：自动链路门控语义与手动一致 ----------

    @Test
    void c_suppressedAlertIsRejectedAndNotExecuted() {
        events.setPolicies(List.of(policy("p-noise", "noise", true, false, 0)));
        IngestResult r = events.ingest(alert("a1", "evt-sup", "noise"));
        log.info("[C 抑制门控] input: suppressed policy matched; accepted={} rejection={} status={}",
                r.accepted(), r.rejection(), r.status());
        assertFalse(r.accepted());
        assertEquals(RejectionReason.SUPPRESSED, r.rejection());
        assertEquals(EventStatus.REJECTED, events.getEvent("evt-sup").getStatus());
        assertEquals(0, events.getEvent("evt-sup").getExecutionCount());
    }

    @Test
    void c_freezeWindowBlocksAutomaticChain() {
        events.setPolicies(List.of(policy("p-web", "web", false, false, 0)));
        orchestrator.setWindows(List.of(new MaintenanceWindow("freeze",
                T0, T0.plusSeconds(3600), MaintenanceWindow.Kind.FREEZE)));
        IngestResult r = events.ingest(alert("a1", "evt-frz", "web"));
        log.info("[C 冻结门控] input: ingest inside FREEZE window; drive={} executionCount={}",
                r.drive(), events.getEvent("evt-frz").getExecutionCount());
        assertEquals(RemediationOutcome.Kind.REJECTED, r.drive().kind());
        assertEquals(RejectionReason.CHANGE_FREEZE, r.drive().rejection());
        assertEquals(0, events.getEvent("evt-frz").getExecutionCount());
    }

    @Test
    void c_highRiskAwaitsApprovalThenAutoContinues() {
        events.setPolicies(List.of(policy("p-risk", "risk", false, true, 0)));
        IngestResult r = events.ingest(alert("a1", "evt-risk", "risk"));
        log.info("[C 高风险审批] input: high-risk ingest; drive={} status={}",
                r.drive(), r.status());
        assertEquals(RejectionReason.APPROVAL_REQUIRED, r.drive().rejection());
        assertEquals(EventStatus.AWAITING_APPROVAL, events.getEvent("evt-risk").getStatus());

        approvals.approve("evt-risk", "lead");
        log.info("[C 高风险审批] basis: approved, pipeline auto-continued; status={} executionCount={}",
                events.getEvent("evt-risk").getStatus(),
                events.getEvent("evt-risk").getExecutionCount());
        assertEquals(EventStatus.RESOLVED, events.getEvent("evt-risk").getStatus());
        assertEquals(1, events.getEvent("evt-risk").getExecutionCount());
    }

    // ---------- D：延迟自愈、半开区间、每次判定留痕、恢复后一次执行 ----------

    @Test
    void d_delayedAutoHealsOnClockAdvanceWithTraceAndRunsOnce() {
        events.setPolicies(List.of(policy("p-web", "web", false, false, 0)));
        orchestrator.setWindows(List.of(new MaintenanceWindow("mw",
                T0, T0.plusSeconds(600), MaintenanceWindow.Kind.MAINTENANCE)));
        IngestResult r = events.ingest(alert("a1", "evt-dly", "web"));
        assertEquals(RemediationOutcome.Kind.DELAYED, r.drive().kind());
        assertEquals(0, events.getEvent("evt-dly").getExecutionCount());

        // 窗口内时钟推进：仍不恢复，但每次判定留痕
        clock.advanceMillis(300_000);
        assertEquals(EventStatus.DELAYED, events.getEvent("evt-dly").getStatus());
        log.info("[D 延迟自愈] input: advance 300s inside window; status={} (still waiting note recorded)",
                events.getEvent("evt-dly").getStatus());
        assertEquals(0, events.getEvent("evt-dly").getExecutionCount());

        // 推进到 end（半开，end 放行）：无需手动唤醒，自动恢复执行一次
        clock.advanceMillis(300_000);
        AlertEvent e = events.getEvent("evt-dly");
        log.info("[D 延迟自愈] basis: advance to window end; status={} executionCount={} history={}",
                e.getStatus(), e.getExecutionCount(), e.getHistory());
        assertEquals(EventStatus.RESOLVED, e.getStatus());
        assertEquals(1, e.getExecutionCount());
        assertTrue(e.getHistory().stream().anyMatch(h -> h.detail().contains("delayed until")),
                "delay decision must be recorded");
        assertTrue(e.getHistory().stream().anyMatch(h -> h.detail().contains("remain DELAYED")),
                "each waiting re-evaluation must be recorded");
        assertTrue(e.getHistory().stream().anyMatch(h -> h.detail().contains("window ended, resuming")),
                "resume decision must be recorded");

        // 恢复后再投递/再推进都不重复执行
        clock.advanceMillis(1000);
        assertEquals(1, e.getExecutionCount());
    }

    // ---------- E：结论一致性：拒绝原因不改口；规则补齐可恢复 ----------

    @Test
    void e_repeatedSuppressedDeliveryKeepsSameReason() {
        events.setPolicies(List.of(policy("p-noise", "noise", true, false, 0)));
        IngestResult r1 = events.ingest(alert("a1", "evt-cons", "noise"));
        IngestResult r2 = events.ingest(alert("a2", "evt-cons", "noise"));
        IngestResult r3 = events.ingest(alert("a3", "evt-cons", "noise"));
        log.info("[E 结论一致性] input: 3 deliveries of suppressed event; reasons={}/{}/{}",
                r1.rejection(), r2.rejection(), r3.rejection());
        assertEquals(RejectionReason.SUPPRESSED, r1.rejection());
        assertEquals(RejectionReason.SUPPRESSED, r2.rejection());
        assertEquals(RejectionReason.SUPPRESSED, r3.rejection());
        assertEquals(EventStatus.REJECTED, events.getEvent("evt-cons").getStatus());
        assertEquals(RejectionReason.SUPPRESSED, events.getEvent("evt-cons").getRejection());
    }

    @Test
    void e_policyMismatchRecoversAfterRuleFixed() {
        events.setPolicies(List.of(policy("p-web", "web", false, false, 0)));
        IngestResult r1 = events.ingest(alert("a1", "evt-fix", "db"));
        log.info("[E 规则补齐可恢复] input: labels app=db with no rule; rejection={} status={}",
                r1.rejection(), r1.status());
        assertEquals(RejectionReason.POLICY_MISMATCH, r1.rejection());
        assertEquals(EventStatus.NEW, events.getEvent("evt-fix").getStatus());

        // 把规则补上，同样的告警再投，自动链路正常走到底
        events.setPolicies(List.of(
                policy("p-web", "web", false, false, 0),
                policy("p-db", "db", false, false, 0)));
        IngestResult r2 = events.ingest(alert("a2", "evt-fix", "db"));
        log.info("[E 规则补齐可恢复] basis: rule added then same alert redelivered; disposition={} drive={} status={}",
                r2.disposition(), r2.drive(), r2.status());
        assertEquals(IngestResult.Disposition.NEW_ACCEPTED, r2.disposition());
        assertEquals(RemediationOutcome.Kind.EXECUTED, r2.drive().kind());
        assertEquals(EventStatus.RESOLVED, events.getEvent("evt-fix").getStatus());
        assertEquals(1, events.getEvent("evt-fix").getExecutionCount());
    }

    // ---------- F：历史结论按当时规则版本真实复现（观测实际执行行为） ----------

    @Test
    void f_finalConclusionReplaysWithHistoricalPolicyVersion() {
        // 初始版本 p-flaky：maxRetries=1，动作前两次可重试失败 -> 旧版应 2 次尝试后失败终态
        AtomicInteger v1Runs = new AtomicInteger();
        Map<String, AtomicInteger> runsByPolicy = new ConcurrentHashMap<>();
        runsByPolicy.put("p-flaky", v1Runs);
        events.setPolicies(List.of(new RemediationPolicy("p-flaky", Map.of("app", "svc"),
                false, false, 1, Duration.ofSeconds(30))));
        orchestrator.setActionResolver(event -> {
            String pid = event.getPolicy().policyId();
            AtomicInteger counter = runsByPolicy.computeIfAbsent(pid, k -> new AtomicInteger());
            return (key, attempt) -> {
                counter.incrementAndGet();
                if ("p-flaky".equals(pid)) {
                    // 旧版规则下动作持续失败：maxRetries=1 -> 2 次尝试后 FAILED_FINAL
                    return ExecutionResult.fail(FailureType.RETRYABLE, "v1 flaky backend");
                }
                // 新版规则动作本可成功：若被新规则带偏执行，就会成功
                return ExecutionResult.ok("v2 healthy backend");
            };
        });
        events.ingest(alert("a1", "evt-hist", "svc"));
        AlertEvent hist = events.getEvent("evt-hist");
        int v1Attempts = v1Runs.get();
        log.info("[F 历史结论复现] input: v1 policy maxRetries=1, always-failing action; "
                        + "runsByV1={} finalStatus={} snapshot={}",
                v1Attempts, hist.getStatus(), hist.getPolicySnapshot());
        assertEquals(2, v1Attempts);
        assertEquals(EventStatus.FAILED_FINAL, hist.getStatus());
        assertTrue(hist.getPolicySnapshot().contains("p-flaky"));

        // 规则升级为新版（maxRetries=5，动作健康），再投同一事件：必须原样复现旧版最终结论
        events.setPolicies(List.of(new RemediationPolicy("p-healthy", Map.of("app", "svc"),
                false, false, 5, Duration.ofSeconds(30))));
        AtomicInteger v2Runs = runsByPolicy.computeIfAbsent("p-healthy", k -> new AtomicInteger());
        IngestResult again = events.ingest(alert("a2", "evt-hist", "svc"));
        log.info("[F 历史结论复现] basis: policy upgraded to p-healthy then redelivered; "
                        + "drive={} status={} v1Runs={} v2Runs={} rejection={}",
                again.drive(), again.status(), v1Runs.get(), v2Runs.get(), again.drive().rejection());
        // 可观测证据：没有按新版规则执行任何一次（v2 动作 0 次），旧版尝试次数不增加，终态不变
        assertEquals(0, v2Runs.get(), "final event must not execute under the new policy version");
        assertEquals(v1Attempts, v1Runs.get(), "historical execution must not be retried/replayed");
        assertEquals(EventStatus.FAILED_FINAL, events.getEvent("evt-hist").getStatus());
        assertEquals(RejectionReason.ALREADY_CLOSED, again.drive().rejection());

        // 另一个新事件则使用新版规则：一次成功，证明新规则对新事件生效，旧结论不是因动作失效
        events.ingest(alert("a3", "evt-fresh", "svc"));
        log.info("[F 历史结论复现] control: fresh event under p-healthy; status={} v2Runs={}",
                events.getEvent("evt-fresh").getStatus(), v2Runs.get());
        assertEquals(EventStatus.RESOLVED, events.getEvent("evt-fresh").getStatus());
        assertEquals(1, v2Runs.get());
    }

    @Test
    void f_suppressedHistoryReplaysSuppressionEvenAfterRuleChanges() {
        events.setPolicies(List.of(policy("p-noise", "noise", true, false, 0)));
        events.ingest(alert("a1", "evt-sh", "noise"));
        // 抑制规则被移除，换成放行规则
        events.setPolicies(List.of(policy("p-noise-v2", "noise", false, false, 0)));
        IngestResult again = events.ingest(alert("a2", "evt-sh", "noise"));
        log.info("[F 历史结论复现] input: suppressed event redelivered after suppression removed; "
                        + "rejection={} status={} executionCount={}",
                again.rejection(), again.status(),
                events.getEvent("evt-sh").getExecutionCount());
        assertEquals(RejectionReason.SUPPRESSED, again.rejection());
        assertEquals(EventStatus.REJECTED, events.getEvent("evt-sh").getStatus());
        assertEquals(0, events.getEvent("evt-sh").getExecutionCount());
    }
}
