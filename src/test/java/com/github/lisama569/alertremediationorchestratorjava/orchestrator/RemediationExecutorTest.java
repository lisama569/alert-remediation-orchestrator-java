package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.execution.ExecutionResult;
import com.github.lisama569.alertremediationorchestratorjava.execution.FailureType;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemediationExecutorTest {

    private static final Logger log = LoggerFactory.getLogger(RemediationExecutorTest.class);

    private MutableClock clock;
    private RemediationExecutor executor;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        executor = new RemediationExecutor(clock);
    }

    @Test
    void concurrentTriggersExecuteExactlyOnce() throws InterruptedException {
        AlertEvent event = new AlertEvent("evt-c", clock.now());
        AtomicInteger actionRuns = new AtomicInteger();
        // 动作内部推进可控时钟，模拟执行耗时，让并发触发有重叠窗口
        var action = (com.github.lisama569.alertremediationorchestratorjava.execution.RemediationAction)
                (key, attempt) -> {
                    actionRuns.incrementAndGet();
                    clock.advanceMillis(100);
                    return ExecutionResult.ok("done");
                };
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Boolean> taken = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    taken.add(executor.trigger(event, action, 0, Duration.ofSeconds(30)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        ready.await();
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        long takenCount = taken.stream().filter(Boolean::booleanValue).count();
        log.info("input: {} concurrent triggers on evt-c; taken={} actionRuns={} executionCount={} status={}",
                threads, takenCount, actionRuns.get(), event.getExecutionCount(), event.getStatus());
        assertEquals(1, actionRuns.get(), "action must run exactly once");
        assertEquals(1, event.getExecutionCount(), "execution count must be exactly 1");
        assertEquals(EventStatus.RESOLVED, event.getStatus());
    }

    @Test
    void retryableFailureRetriesUntilSuccess() {
        AlertEvent event = new AlertEvent("evt-r", clock.now());
        AtomicInteger runs = new AtomicInteger();
        var action = (com.github.lisama569.alertremediationorchestratorjava.execution.RemediationAction)
                (key, attempt) -> {
                    runs.incrementAndGet();
                    return attempt < 3
                            ? ExecutionResult.fail(FailureType.RETRYABLE, "flaky dependency")
                            : ExecutionResult.ok("recovered");
                };
        executor.trigger(event, action, 5, Duration.ofSeconds(30));
        log.info("input: fail twice then success, maxRetries=5; runs={} executionCount={} status={}",
                runs.get(), event.getExecutionCount(), event.getStatus());
        assertEquals(3, runs.get());
        assertEquals(3, event.getExecutionCount());
        assertEquals(EventStatus.RESOLVED, event.getStatus());
    }

    @Test
    void nonRetryableFailureStopsImmediately() {
        AlertEvent event = new AlertEvent("evt-n", clock.now());
        AtomicInteger runs = new AtomicInteger();
        var action = (com.github.lisama569.alertremediationorchestratorjava.execution.RemediationAction)
                (key, attempt) -> {
                    runs.incrementAndGet();
                    return ExecutionResult.fail(FailureType.NON_RETRYABLE, "invalid config");
                };
        executor.trigger(event, action, 5, Duration.ofSeconds(30));
        log.info("input: non-retryable failure, maxRetries=5; runs={} status={} history={}",
                runs.get(), event.getStatus(), event.getHistory());
        assertEquals(1, runs.get());
        assertEquals(EventStatus.FAILED_FINAL, event.getStatus());
        assertTrue(event.getHistory().toString().contains("invalid config"),
                "failure reason must be recorded, not swallowed");
    }

    @Test
    void timeoutFailureClassifiedAndRetriedWithinLimit() {
        AlertEvent event = new AlertEvent("evt-t", clock.now());
        AtomicInteger runs = new AtomicInteger();
        var action = (com.github.lisama569.alertremediationorchestratorjava.execution.RemediationAction)
                (key, attempt) -> {
                    runs.incrementAndGet();
                    clock.advanceMillis(2_000); // 超过 1s 超时
                    return ExecutionResult.fail(FailureType.RETRYABLE, "slow");
                };
        executor.trigger(event, action, 2, Duration.ofSeconds(1));
        log.info("input: each attempt exceeds 1s timeout, maxRetries=2; runs={} status={} history={}",
                runs.get(), event.getStatus(), event.getHistory());
        assertEquals(3, runs.get(), "initial + 2 retries");
        assertEquals(EventStatus.FAILED_FINAL, event.getStatus());
        assertTrue(event.getHistory().toString().contains("TIMEOUT"));
    }
}
