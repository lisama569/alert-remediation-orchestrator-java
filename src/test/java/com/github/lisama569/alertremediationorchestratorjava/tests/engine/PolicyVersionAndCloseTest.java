package com.github.lisama569.alertremediationorchestratorjava.tests.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.github.lisama569.alertremediationorchestratorjava.model.Conclusion;
import com.github.lisama569.alertremediationorchestratorjava.model.EventState;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionResult;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import com.github.lisama569.alertremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.model.RiskLevel;
import com.github.lisama569.alertremediationorchestratorjava.model.Severity;
import com.github.lisama569.alertremediationorchestratorjava.model.TriggerOutcome;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.Harness;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.RecordingAction;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 策略调整后历史事件结论可复现：
 * 事件固化命中时的 policyVersion；即使策略后来把 maxAttempts/风险等级/动作改掉，
 * 该事件的重试仍按固化版本执行，结论保持不变。新事件才使用新版本。
 * 另覆盖人工闭环对后续触发/重试的抑制。
 */
class PolicyVersionAndCloseTest {

    private static final Logger log = LoggerFactory.getLogger(PolicyVersionAndCloseTest.class);

    @Test
    void historicalEventKeepsPinnedPolicyVersionAfterPolicyChange() {
        Harness h = new Harness();
        AtomicInteger v1ActionCalls = new AtomicInteger();
        RecordingAction actionV1 = new RecordingAction("act.v1");
        actionV1.retryable("v1 transient");
        h.register(actionV1);
        RecordingAction actionV2 = new RecordingAction("act.v2");
        h.register(actionV2);

        // v1: maxAttempts=4, action=act.v1, LOW
        RemediationPolicy v1 = h.putPolicy("p.evolve", "disk.full", Severity.WARNING,
                "act.v1", RiskLevel.LOW, true, 4, 5000);
        log.info("[test-input] 写入策略 v1: action={} maxAttempts={}",
                v1.actionId(), v1.maxAttempts());

        // 制造一个处于重试中的历史事件（固化 v1）。
        IngestionResult r = h.ingestionService.ingest(
                h.alert("a-old", "fp-old", null, "disk.full", Severity.WARNING, "h"));
        String oldEventId = r.eventId();
        IncidentEvent pinned = h.eventStore.findById(oldEventId);
        assertEquals(1, pinned.getPolicyVersion());
        log.info("[test-judge] 历史事件 {} 固化 policy={}@v{} state={}",
                oldEventId, pinned.getMatchedPolicyId(), pinned.getPolicyVersion(), pinned.getState());

        // 策略调整：动作换成 act.v2、maxAttempts 改为 1、风险改为 HIGH（v2）。
        RemediationPolicy v2 = h.policyStore.put(new RemediationPolicy(
                "p.evolve", 0, "disk.full", Severity.WARNING,
                "act.v2", RiskLevel.HIGH, true, 1, 5000));
        log.info("[test-input] 策略调整 -> v2: action={} maxAttempts={} risk={}",
                v2.actionId(), v2.maxAttempts(), v2.riskLevel());
        assertNotEquals(v1.version(), v2.version());
        assertEquals(2, v2.version());

        // 推进时钟驱动历史事件重试：必须继续用 act.v1（不是 v2），也不受 v2 高风险审批约束。
        for (int i = 0; i < 3; i++) {
            h.clock.advance(java.time.Duration.ofSeconds(1));
        }
        IncidentEvent oldAfter = h.eventStore.findById(oldEventId);
        log.info("[test-judge] 历史事件重试后 state={} conclusion={} attempts(v1calls={},v2calls={}) execCount={}",
                oldAfter.getState(), oldAfter.getConclusion(),
                actionV1.callsOf(oldEventId), actionV2.callsOf(oldEventId),
                oldAfter.getExecutionCount());
        // v1 maxAttempts=4 -> 共 4 次尝试全部走 act.v1，最终重试耗尽。
        assertEquals(4, actionV1.callsOf(oldEventId), "历史事件必须按固化版本 v1 重试");
        assertEquals(0, actionV2.callsOf(oldEventId), "新策略动作不得作用于历史事件");
        assertEquals(EventState.FAILED, oldAfter.getState());
        assertEquals(Conclusion.FAILED_RETRIES_EXHAUSTED, oldAfter.getConclusion());
        assertEquals(1, oldAfter.getExecutionCount());

        // 全新事件命中 v2：高风险，需审批，且动作是 act.v2。
        IngestionResult fresh = h.ingestionService.ingest(
                h.alert("a-new", "fp-new", null, "disk.full", Severity.CRITICAL, "h"));
        log.info("[test-judge] 新事件在 v2 下接入 -> {}", fresh.outcome());
        assertEquals(IngestionOutcome.REJECT_APPROVAL_REQUIRED, fresh.outcome());
        IncidentEvent freshEvent = h.eventStore.findById(fresh.eventId());
        assertEquals(2, freshEvent.getPolicyVersion(), "新事件必须使用新版本 v2");
    }

    @Test
    void manualCloseBlocksLaterTriggersAndPendingRetries() {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.mc").retryable("still failing");
        h.register(action);
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.mc", RiskLevel.LOW, true, 5, 5000);

        IngestionResult r = h.ingestionService.ingest(
                h.alert("a1", "fp-mc", null, "disk.full", Severity.WARNING, "h"));
        String eventId = r.eventId();
        assertEquals(1, action.callsOf(eventId));

        var close = h.remediationService.manualClose(eventId, "sre-oncall", "escalated to ticket");
        log.info("[test-input] 人工闭环 -> {}", close.outcome());
        IncidentEvent closed = h.eventStore.findById(eventId);
        assertEquals(EventState.CLOSED, closed.getState());
        assertEquals(Conclusion.CLOSED_MANUALLY, closed.getConclusion());

        var later = h.remediationService.trigger(eventId, "after-close");
        log.info("[test-judge] 闭环后触发 -> {} reason={}", later.outcome(), later.reason());
        assertEquals(TriggerOutcome.ALREADY_CLOSED, later.outcome());

        var repeatedAlert = h.ingestionService.ingest(
                h.alert("a2", "fp-mc", null, "disk.full", Severity.WARNING, "h"));
        log.info("[test-judge] 闭环后重复告警 -> {}", repeatedAlert.outcome());
        assertEquals(IngestionOutcome.REJECT_ALREADY_CLOSED, repeatedAlert.outcome());

        // 已调度的重试唤醒在时间推进后必须被丢弃。
        int callsBefore = action.callsOf(eventId);
        h.clock.advance(java.time.Duration.ofSeconds(60));
        log.info("[test-judge] 推进 60s 后动作调用 {} -> {}（不得增加）",
                callsBefore, action.callsOf(eventId));
        assertEquals(callsBefore, action.callsOf(eventId), "闭环后待执行重试不得再运行");
        assertEquals(EventState.CLOSED, h.eventStore.findById(eventId).getState());
    }
}
