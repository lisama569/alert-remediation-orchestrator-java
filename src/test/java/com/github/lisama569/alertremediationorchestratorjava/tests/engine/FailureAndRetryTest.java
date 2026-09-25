package com.github.lisama569.alertremediationorchestratorjava.tests.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import com.github.lisama569.alertremediationorchestratorjava.model.AttemptStatus;
import com.github.lisama569.alertremediationorchestratorjava.model.Conclusion;
import com.github.lisama569.alertremediationorchestratorjava.model.EventState;
import com.github.lisama569.alertremediationorchestratorjava.model.ExecutionRecord;
import com.github.lisama569.alertremediationorchestratorjava.model.ExecutionStatus;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionResult;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import com.github.lisama569.alertremediationorchestratorjava.model.RiskLevel;
import com.github.lisama569.alertremediationorchestratorjava.model.Severity;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.ClockAdvancingAction;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.Harness;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.RecordingAction;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处置失败三分类必须可区分且原因不丢失：
 * - 不可重试失败：仅 1 次尝试，立即 FAILED_NON_RETRYABLE；
 * - 可重试失败：按 1s backoff 由手动时钟推进驱动重试，耗尽后 FAILED_RETRIES_EXHAUSTED；
 * - 可重试 -> 最终成功：RESOLVED_AUTO，executionCount 仍为 1；
 * - 超时：动作推进时钟超过阈值，判 TIMED_OUT / TIMED_OUT 结论。
 */
class FailureAndRetryTest {

    private static final Logger log = LoggerFactory.getLogger(FailureAndRetryTest.class);

    @Test
    void nonRetryableFailsImmediatelyWithoutRetriesAndKeepsReason() {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.nr")
                .nonRetryable("precondition missing: no quorum (simulated)");
        h.register(action);
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.nr", RiskLevel.LOW, true, 3, 5000);

        IngestionResult r = h.ingestionService.ingest(
                h.alert("a1", "fp-nr", null, "disk.full", Severity.WARNING, "h"));
        String eventId = r.eventId();
        log.info("[test-input] 不可重试动作 -> {} reason={}", r.outcome(), r.reason());
        assertEquals(IngestionOutcome.ACCEPTED_NEW, r.outcome());

        IncidentEvent event = h.eventStore.findById(eventId);
        ExecutionRecord rec = h.eventStore.executionsOf(eventId).get(0);
        log.info("[test-judge] state={} conclusion={} execStatus={} attempts={} failureMsg={}",
                event.getState(), event.getConclusion(), rec.getStatus(),
                rec.getAttemptCount(), rec.getFailureMessage());
        assertEquals(EventState.FAILED, event.getState());
        assertEquals(Conclusion.FAILED_NON_RETRYABLE, event.getConclusion());
        assertEquals(ExecutionStatus.FAILED_NON_RETRYABLE, rec.getStatus());
        assertEquals(1, rec.getAttemptCount(), "不可重试失败不得重试");
        assertTrue(rec.getFailureMessage().contains("no quorum"), "失败原因不得被吞掉");
        assertEquals(AttemptStatus.NON_RETRYABLE_FAILED, rec.getAttempts().get(0).status());
    }

    @Test
    void retryableFailuresRetryOnClockAdvanceThenSucceedWithSingleExecution() {
        Harness h = new Harness();
        AtomicInteger calls = new AtomicInteger();
        RemediationAction flaky = new RemediationAction() {
            @Override
            public String actionId() {
                return "act.flaky-custom";
            }

            @Override
            public ActionOutcome execute(String eventId) {
                int n = calls.incrementAndGet();
                return n < 3
                        ? ActionOutcome.retryable("transient #" + n)
                        : ActionOutcome.success();
            }
        };
        h.register(flaky);
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.flaky-custom", RiskLevel.LOW, true, 5, 5000);

        IngestionResult r = h.ingestionService.ingest(
                h.alert("a2", "fp-r", null, "disk.full", Severity.WARNING, "h"));
        String eventId = r.eventId();
        IncidentEvent afterFirst = h.eventStore.findById(eventId);
        log.info("[test-judge] 第1次尝试后 state={} calls={}", afterFirst.getState(), calls.get());
        assertEquals(EventState.OPEN, afterFirst.getState(), "可重试失败回到 OPEN 等待重试");
        assertEquals(1, calls.get());

        // backoff 不足：推进 0.5s 不触发重试（通过 1s 边界验证调度精确，无真实等待）。
        h.clock.advance(Duration.ofMillis(500));
        assertEquals(1, calls.get(), "backoff 未到不得重试");

        h.clock.advance(Duration.ofMillis(500)); // 累计 1s -> 第 2 次
        log.info("[test-judge] 推进到 1s calls={}", calls.get());
        assertEquals(2, calls.get());
        assertEquals(EventState.OPEN, h.eventStore.findById(eventId).getState());

        h.clock.advance(Duration.ofSeconds(1)); // -> 第 3 次，成功
        IncidentEvent resolved = h.eventStore.findById(eventId);
        ExecutionRecord rec = h.eventStore.executionsOf(eventId).get(0);
        log.info("[test-judge] 第3次尝试后 state={} conclusion={} attempts={} executions={} calls={}",
                resolved.getState(), resolved.getConclusion(), rec.getAttemptCount(),
                resolved.getExecutionCount(), calls.get());
        assertEquals(EventState.RESOLVED, resolved.getState());
        assertEquals(Conclusion.RESOLVED_AUTO, resolved.getConclusion());
        assertEquals(3, rec.getAttemptCount());
        assertEquals(ExecutionStatus.SUCCEEDED, rec.getStatus());
        assertEquals(1, resolved.getExecutionCount(), "重试归属同一执行，不新增 executionCount");
        assertEquals(3, calls.get());
    }

    @Test
    void retriesExhaustedLeadsToDistinctFailureConclusion() {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.always")
                .retryable("persistent error (simulated)");
        h.register(action);
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.always", RiskLevel.LOW, true, 3, 5000);

        IngestionResult r = h.ingestionService.ingest(
                h.alert("a3", "fp-e", null, "disk.full", Severity.WARNING, "h"));
        String eventId = r.eventId();
        h.clock.advance(Duration.ofSeconds(1)); // attempt 2
        h.clock.advance(Duration.ofSeconds(1)); // attempt 3 -> exhausted
        IncidentEvent event = h.eventStore.findById(eventId);
        ExecutionRecord rec = h.eventStore.executionsOf(eventId).get(0);
        log.info("[test-judge] state={} conclusion={} execStatus={} attempts={} msg={}",
                event.getState(), event.getConclusion(), rec.getStatus(),
                rec.getAttemptCount(), rec.getFailureMessage());
        assertEquals(EventState.FAILED, event.getState());
        assertEquals(Conclusion.FAILED_RETRIES_EXHAUSTED, event.getConclusion());
        assertEquals(ExecutionStatus.FAILED_RETRYABLE, rec.getStatus());
        assertEquals(3, rec.getAttemptCount());
        assertTrue(rec.getFailureMessage().contains("persistent error"));
        assertEquals(3, action.callsOf(eventId));
        assertEquals(1, event.getExecutionCount());
    }

    @Test
    void actionExceedingTimeoutIsClassifiedAsTimeout() {
        Harness h = new Harness();
        // 阈值 5000ms；动作推进时钟 6s（纯模拟耗时）。
        h.register(new ClockAdvancingAction("act.slow", h.clock, Duration.ofSeconds(6)));
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.slow", RiskLevel.LOW, true, 2, 5000);

        IngestionResult r = h.ingestionService.ingest(
                h.alert("a4", "fp-t", null, "disk.full", Severity.WARNING, "h"));
        String eventId = r.eventId();
        h.clock.advance(Duration.ofSeconds(1)); // 超时同样可重试：第 2 次（又推进 6s）
        IncidentEvent event = h.eventStore.findById(eventId);
        ExecutionRecord rec = h.eventStore.executionsOf(eventId).get(0);
        log.info("[test-judge] state={} conclusion={} execStatus={} attemptStatuses={} msg={}",
                event.getState(), event.getConclusion(), rec.getStatus(),
                rec.getAttempts().stream().map(a -> a.status().name()).toList(),
                rec.getFailureMessage());
        assertEquals(EventState.FAILED, event.getState());
        assertEquals(Conclusion.TIMED_OUT, event.getConclusion());
        assertEquals(ExecutionStatus.TIMED_OUT, rec.getStatus());
        assertEquals(2, rec.getAttemptCount());
        assertTrue(rec.getAttempts().stream().allMatch(a -> a.status() == AttemptStatus.TIMEOUT));
        assertTrue(rec.getFailureMessage().contains("timeout after"));
    }
}
