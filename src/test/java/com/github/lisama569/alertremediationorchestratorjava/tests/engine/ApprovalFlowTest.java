package com.github.lisama569.alertremediationorchestratorjava.tests.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.alertremediationorchestratorjava.clock.ManualClockService;
import com.github.lisama569.alertremediationorchestratorjava.model.Conclusion;
import com.github.lisama569.alertremediationorchestratorjava.model.EventState;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionResult;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import com.github.lisama569.alertremediationorchestratorjava.model.RiskLevel;
import com.github.lisama569.alertremediationorchestratorjava.model.Severity;
import com.github.lisama569.alertremediationorchestratorjava.model.TriggerOutcome;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.Harness;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.RecordingAction;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 高风险操作未审批不得执行；审批通过后可执行；审批拒绝则事件终结、后续触发被拒。 */
class ApprovalFlowTest {

    private static final Logger log = LoggerFactory.getLogger(ApprovalFlowTest.class);

    @Test
    void highRiskRequiresApprovalThenRunsAfterApprove() {
        Harness h = new Harness(new ManualClockService());
        RecordingAction action = new RecordingAction("act.high");
        h.register(action);
        h.putPolicy("p.high", "data.wipe", Severity.WARNING,
                "act.high", RiskLevel.HIGH, true, 1, 5000);

        IngestionResult r = h.ingestionService.ingest(
                h.alert("a1", "fp-h", null, "data.wipe", Severity.CRITICAL, "db-1"));
        String eventId = r.eventId();
        log.info("[test-input] 高风险告警接入 -> {} reason={}", r.outcome(), r.reason());
        assertEquals(IngestionOutcome.REJECT_APPROVAL_REQUIRED, r.outcome());
        IncidentEvent waiting = h.eventStore.findById(eventId);
        assertEquals(EventState.WAITING_APPROVAL, waiting.getState());
        assertFalse(waiting.isApproved());
        assertEquals(0, action.callsOf(eventId), "未审批不得执行");

        // 审批前再次触发，仍被拒绝。
        var again = h.remediationService.trigger(eventId, "impatient-operator");
        log.info("[test-judge] 审批前再触发 -> {}", again.outcome());
        assertEquals(TriggerOutcome.REJECT_APPROVAL_REQUIRED, again.outcome());
        assertEquals(0, action.callsOf(eventId));

        var approval = h.remediationService.approve(eventId, "ops-lead", true, "change ticket CHG-42");
        log.info("[test-input] 人工批准 -> approved={} reason={}", approval.approved(), approval.reason());
        assertTrue(approval.approved());

        var triggered = h.remediationService.trigger(eventId, "post-approval");
        log.info("[test-judge] 批准后触发 -> {}", triggered.outcome());
        assertEquals(TriggerOutcome.EXECUTION_STARTED, triggered.outcome());
        IncidentEvent done = h.eventStore.findById(eventId);
        assertEquals(EventState.RESOLVED, done.getState());
        assertEquals(Conclusion.RESOLVED_AUTO, done.getConclusion());
        assertTrue(done.isApproved());
        assertEquals("ops-lead", done.getApprover());
        assertEquals(1, action.callsOf(eventId));
        assertEquals(1, done.getExecutionCount());
    }

    @Test
    void rejectedApprovalTerminatesEventAndBlocksFurtherTriggers() {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.high2");
        h.register(action);
        h.putPolicy("p.high2", "data.wipe", Severity.WARNING,
                "act.high2", RiskLevel.HIGH, true, 1, 5000);

        IngestionResult r = h.ingestionService.ingest(
                h.alert("a2", "fp-h2", null, "data.wipe", Severity.CRITICAL, "db-2"));
        String eventId = r.eventId();
        assertEquals(IngestionOutcome.REJECT_APPROVAL_REQUIRED, r.outcome());

        var denied = h.remediationService.approve(eventId, "ops-lead", false, "too risky");
        log.info("[test-input] 人工拒绝审批 -> approved={} reason={}", denied.approved(), denied.reason());
        assertFalse(denied.approved());

        IncidentEvent terminated = h.eventStore.findById(eventId);
        assertEquals(EventState.FAILED, terminated.getState());
        assertEquals(Conclusion.CLOSED_MANUALLY, terminated.getConclusion());
        assertEquals(0, action.callsOf(eventId));

        var afterDeny = h.remediationService.trigger(eventId, "retry-after-deny");
        log.info("[test-judge] 拒绝后再触发 -> {} reason={}", afterDeny.outcome(), afterDeny.reason());
        assertEquals(TriggerOutcome.ALREADY_CLOSED, afterDeny.outcome(),
                "审批拒绝是终态，后续触发必须被已闭环原因拒绝");
        assertEquals(0, action.callsOf(eventId));
    }
}
