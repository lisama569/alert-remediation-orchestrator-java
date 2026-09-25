package com.github.lisama569.alertremediationorchestratorjava.tests.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.alertremediationorchestratorjava.model.Conclusion;
import com.github.lisama569.alertremediationorchestratorjava.model.EventState;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionResult;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import com.github.lisama569.alertremediationorchestratorjava.model.RiskLevel;
import com.github.lisama569.alertremediationorchestratorjava.model.Severity;
import com.github.lisama569.alertremediationorchestratorjava.model.TriggerOutcome;
import com.github.lisama569.alertremediationorchestratorjava.model.TriggerResult;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.Harness;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.RecordingAction;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 并发触发的确定性要求：
 * - 同一事件多个告警并发接入：只建 1 个事件、只执行 1 次；其余拿到“已闭环”拒绝；
 * - 处置执行期间并发人工触发：全部拿到 EXECUTION_IN_PROGRESS，不产生第二个执行。
 */
class ConcurrentTriggerTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentTriggerTest.class);

    @Test
    void concurrentIngestionOfSameFingerprintProducesSingleExecution() throws Exception {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.c");
        h.register(action);
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.c", RiskLevel.LOW, true, 1, 5000);

        int n = 16;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<IngestionOutcome> outcomes = new ConcurrentLinkedQueue<>();
        AtomicInteger started = new AtomicInteger();

        for (int i = 0; i < n; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                    // 相同 fingerprint、不同 alertId：归并到同一事件
                    var alert = h.alert("a-" + idx, "fp-same", "corr-same",
                            "disk.full", Severity.WARNING, "host");
                    IngestionResult r = h.ingestionService.ingest(alert);
                    outcomes.add(r.outcome());
                    // “真正启动了处置”的线程：要么是建事件者(ACCEPTED_NEW)，
                    // 要么是归并后抢到执行权的线程(MERGED 携带已开始执行语义)。
                    if (r.outcome() == IngestionOutcome.ACCEPTED_NEW
                            || r.outcome() == IngestionOutcome.MERGED) {
                        started.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("worker failed", e);
                }
            });
        }
        log.info("[test-input] {} 个线程同刻并发接入相同 fingerprint 的告警", n);
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long rejectedClosed = outcomes.stream()
                .filter(o -> o == IngestionOutcome.REJECT_ALREADY_CLOSED).count();
        long rejectedInProgress = outcomes.stream()
                .filter(o -> o == IngestionOutcome.REJECT_EXECUTION_IN_PROGRESS).count();
        long startCount = started.get();
        log.info("[test-judge] outcomes={} startedExecution={} rejectedClosed={} rejectedInProgress={} events={} executions={} actionCalls={}",
                outcomes, startCount, rejectedClosed, rejectedInProgress,
                h.eventStore.findAll().size(),
                h.eventStore.findAll().stream().mapToInt(IncidentEvent::getExecutionCount).sum(),
                action.globalCallCount());

        assertEquals(1, h.eventStore.findAll().size(), "只允许一个事件");
        IncidentEvent event = h.eventStore.findAll().get(0);
        assertEquals(1, event.getExecutionCount(), "executionCount 必须确定为 1");
        assertEquals(1, action.globalCallCount(), "动作真实执行次数必须确定为 1");
        assertEquals(1, startCount,
                "并发下必须恰好一个线程真正启动处置（建事件者或抢到执行权的归并者）: " + outcomes);
        // 注意：并发下“谁启动处置”由线程调度决定（创建者可能在 createOrGet 后被切走，
        // 由归并者先触发），因此这里不强制 ACCEPTED_NEW 落在哪个线程；
        // “建事件即 ACCEPTED_NEW”的单线程路径由 DedupAndMergeTest 覆盖。
        // 其余触发要么看到“执行中”，要么看到“已闭环”（取决于胜者占位/完成时机），
        // 两类都是可区分的合法拒绝，但绝不允许第二个执行。
        assertEquals(n - 1, rejectedClosed + rejectedInProgress,
                "其余触发必须全部被拒绝（已闭环或执行中）: " + outcomes);
        assertTrue(outcomes.stream().allMatch(o ->
                        o == IngestionOutcome.ACCEPTED_NEW
                                || o == IngestionOutcome.REJECT_ALREADY_CLOSED
                                || o == IngestionOutcome.REJECT_EXECUTION_IN_PROGRESS),
                "只允许接受/已闭环/执行中三类结果: " + outcomes);
        assertEquals(EventState.RESOLVED, event.getState());
        assertEquals(Conclusion.RESOLVED_AUTO, event.getConclusion());
    }

    @Test
    void triggersWhileExecutionInProgressAreRejectedAndNotCounted() throws Exception {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.barrier");
        action.installSingleBarrier();
        h.register(action);
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.barrier", RiskLevel.LOW, true, 1, 5000);

        // 首个接入在独立线程执行；它进入动作后停在屏障，事件稳定处于 EXECUTING。
        ExecutorService ingestor = Executors.newSingleThreadExecutor();
        var ingestFuture = ingestor.submit(() -> h.ingestionService.ingest(
                h.alert("a0", "fp-b", null, "disk.full", Severity.WARNING, "host")));
        // 接入线程在屏障处阻塞，事件此刻必然处于 EXECUTING。
        assertTrue(action.enteredLatch().await(5, TimeUnit.SECONDS));
        String eventId = h.eventStore.findByFingerprint("fp-b").getEventId();
        log.info("[test-input] 首个执行已进入动作屏障，事件处于 EXECUTING，并发触发 8 次");

        int n = 8;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<TriggerOutcome> results = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < n; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    TriggerResult r = h.remediationService.trigger(eventId, "concurrent-manual");
                    results.add(r.outcome());
                } catch (Exception e) {
                    log.error("trigger worker failed", e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        log.info("[test-judge] 执行中并发触发结果={}，释放屏障", results);
        action.releaseBarrier();
        var created = ingestFuture.get(5, TimeUnit.SECONDS);
        ingestor.shutdown();
        log.info("[test-judge] 首个接入结果={}", created.outcome());

        IncidentEvent event = h.eventStore.findById(eventId);
        log.info("[test-judge] 最终 state={} executionCount={} actionCalls={}",
                event.getState(), event.getExecutionCount(), action.callsOf(eventId));
        assertEquals(n, results.size());
        assertTrue(results.stream().allMatch(o -> o == TriggerOutcome.EXECUTION_IN_PROGRESS),
                "执行中的并发触发必须全部被拒绝: " + results);
        assertEquals(IngestionOutcome.ACCEPTED_NEW, created.outcome());
        assertEquals(1, event.getExecutionCount(), "不得产生第二个执行");
        assertEquals(1, action.callsOf(eventId), "动作不得重复执行");
        assertEquals(EventState.RESOLVED, event.getState());
    }
}
