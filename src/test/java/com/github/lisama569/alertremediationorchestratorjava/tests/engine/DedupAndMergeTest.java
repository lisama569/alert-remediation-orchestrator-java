package com.github.lisama569.alertremediationorchestratorjava.tests.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.github.lisama569.alertremediationorchestratorjava.model.Conclusion;
import com.github.lisama569.alertremediationorchestratorjava.model.EventState;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionResult;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import com.github.lisama569.alertremediationorchestratorjava.model.RiskLevel;
import com.github.lisama569.alertremediationorchestratorjava.model.Severity;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.Harness;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.RecordingAction;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 重复投递、fingerprint 去重、correlationKey 关联归并，以及各类拒绝原因必须可区分。 */
class DedupAndMergeTest {

    private static final Logger log = LoggerFactory.getLogger(DedupAndMergeTest.class);

    @Test
    void duplicateAndCorrelatedAlertsMergeIntoOneEventWithSingleExecution() {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.restart");
        h.register(action);
        h.putPolicy("p.restart", "disk.full", Severity.WARNING,
                "act.restart", RiskLevel.LOW, true, 3, 5000);

        var a1 = h.alert("a1", "fp-1", "corr-1", "disk.full", Severity.WARNING, "host-1");
        log.info("[test-input] 首次告警 a1 fingerprint=fp-1，期望 ACCEPTED_NEW");
        IngestionResult r1 = h.ingestionService.ingest(a1);
        log.info("[test-judge] a1 -> {} reason={}", r1.outcome(), r1.reason());
        assertEquals(IngestionOutcome.ACCEPTED_NEW, r1.outcome());

        String eventId = r1.eventId();

        var a1Replay = h.alert("a1", "fp-1", "corr-1", "disk.full", Severity.WARNING, "host-1");
        log.info("[test-input] 同 alertId 重放 a1，期望 MERGED 且不重复计数/执行");
        IngestionResult r1b = h.ingestionService.ingest(a1Replay);
        log.info("[test-judge] a1 replay -> {} reason={}", r1b.outcome(), r1b.reason());
        assertEquals(IngestionOutcome.MERGED, r1b.outcome());

        var a2 = h.alert("a2", "fp-1", "corr-9", "disk.full", Severity.CRITICAL, "host-1");
        log.info("[test-input] 同 fingerprint 不同告警 a2，期望归并但事件已闭环 -> REJECT_ALREADY_CLOSED");
        IngestionResult r2 = h.ingestionService.ingest(a2);
        log.info("[test-judge] a2 -> {} reason={}", r2.outcome(), r2.reason());
        assertEquals(IngestionOutcome.REJECT_ALREADY_CLOSED, r2.outcome());
        assertEquals(eventId, r2.eventId());

        var a3 = h.alert("a3", "fp-other", "corr-1", "disk.full", Severity.WARNING, "host-1");
        log.info("[test-input] 不同 fingerprint 但同 correlationKey 的关联告警 a3");
        IngestionResult r3 = h.ingestionService.ingest(a3);
        log.info("[test-judge] a3 -> {} reason={}", r3.outcome(), r3.reason());
        assertEquals(IngestionOutcome.REJECT_ALREADY_CLOSED, r3.outcome());
        assertEquals(eventId, r3.eventId());

        IncidentEvent event = h.eventStore.findById(eventId);
        log.info("[test-judge] 最终 event: state={} conclusion={} alertCount={} executionCount={} actionCalls={}",
                event.getState(), event.getConclusion(), event.getAlertCount(),
                event.getExecutionCount(), action.callsOf(eventId));
        assertEquals(EventState.RESOLVED, event.getState());
        assertEquals(Conclusion.RESOLVED_AUTO, event.getConclusion());
        // a1 + a2 + a3 三条不同告警归并；a1 重放不计入。
        assertEquals(3, event.getAlertCount());
        assertEquals(List.of("a1", "a2", "a3"), event.getAlertIds());
        // 只允许一次真实执行。
        assertEquals(1, event.getExecutionCount());
        assertEquals(1, action.callsOf(eventId));
    }

    @Test
    void suppressionReasonsAreDistinguishable() {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.x");
        h.register(action);
        h.putPolicy("p.cpu", "cpu.hot", Severity.WARNING,
                "act.x", RiskLevel.LOW, true, 2, 5000);
        h.putPolicy("p.disabled", "mem.leak", Severity.WARNING,
                "act.x", RiskLevel.LOW, false, 2, 5000);

        log.info("[test-input] 无匹配类型的告警，期望 REJECT_NO_POLICY");
        var noPolicy = h.ingestionService.ingest(
                h.alert("np1", "fp-np", null, "network.down", Severity.CRITICAL, "host-x"));
        log.info("[test-judge] -> {} reason={}", noPolicy.outcome(), noPolicy.reason());
        assertEquals(IngestionOutcome.REJECT_NO_POLICY, noPolicy.outcome());
        assertNotNull(noPolicy.reason());

        log.info("[test-input] 级别不足（INFO 策略要求 WARNING），期望 REJECT_NO_POLICY");
        var lowSev = h.ingestionService.ingest(
                h.alert("ls1", "fp-ls", null, "cpu.hot", Severity.INFO, "host-x"));
        log.info("[test-judge] -> {} reason={}", lowSev.outcome(), lowSev.reason());
        assertEquals(IngestionOutcome.REJECT_NO_POLICY, lowSev.outcome());

        log.info("[test-input] 策略停用类型，期望 REJECT_POLICY_DISABLED");
        var disabled = h.ingestionService.ingest(
                h.alert("d1", "fp-d", null, "mem.leak", Severity.CRITICAL, "host-x"));
        log.info("[test-judge] -> {} reason={}", disabled.outcome(), disabled.reason());
        assertEquals(IngestionOutcome.REJECT_POLICY_DISABLED, disabled.outcome());

        // 被抑制的告警不得创建事件。
        assertEquals(0, h.eventStore.findAll().size());
        log.info("[test-judge] 事件数=0，确认被抑制告警未建事件、未执行，actionCalls={}",
                action.globalCallCount());
        assertEquals(0, action.globalCallCount());
    }
}
