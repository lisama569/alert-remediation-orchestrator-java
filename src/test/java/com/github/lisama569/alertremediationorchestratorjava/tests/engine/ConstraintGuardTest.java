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
import com.github.lisama569.alertremediationorchestratorjava.model.TriggerOutcome;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.Harness;
import com.github.lisama569.alertremediationorchestratorjava.tests.support.RecordingAction;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 维护窗口（延迟语义）与变更冻结（拒绝语义）。
 * 全部依赖手动时钟推进：不做任何真实等待；窗口为半开区间 [start,end)。
 */
class ConstraintGuardTest {

    private static final Logger log = LoggerFactory.getLogger(ConstraintGuardTest.class);

    @Test
    void maintenanceWindowDefersThenAutoRunsAtWindowEndBoundary() {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.mw");
        h.register(action);
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.mw", RiskLevel.LOW, true, 2, 5000);
        // 窗口 [t+10s, t+60s)：接入发生在 t0（窗口前），用于验证触发延迟；另做边界用例验证端点。
        h.constraintStore.addWindow(h.window("mw1", "host-1", 10, 60));

        // 情形 A：窗口开始前 1 秒接入 -> 立即执行成功（窗口不含当前时刻）。
        var before = h.ingestionService.ingest(
                h.alert("a-before", "fp-before", null, "disk.full", Severity.WARNING, "host-1"));
        log.info("[test-input] t={} 窗口前接入 -> {} reason={}",
                h.clock.now(), before.outcome(), before.reason());
        assertEquals(IngestionOutcome.ACCEPTED_NEW, before.outcome());
        assertEquals(1, action.callsOf(before.eventId()));

        // 推进到 t+10（窗口起点，闭端）：触发必须被延迟。
        h.clock.advance(Duration.ofSeconds(10));
        var inWindow = h.ingestionService.ingest(
                h.alert("a-in", "fp-in", null, "disk.full", Severity.WARNING, "host-1"));
        String eventId = inWindow.eventId();
        log.info("[test-input] t={} 窗口起点接入 -> {} reason={}",
                h.clock.now(), inWindow.outcome(), inWindow.reason());
        assertEquals(IngestionOutcome.DEFERRED_MAINTENANCE_WINDOW, inWindow.outcome());
        assertEquals(EventState.DEFERRED, h.eventStore.findById(eventId).getState());
        assertNotNull(h.eventStore.findById(eventId).getDeferredUntil());
        assertEquals(0, action.callsOf(eventId), "窗口内不得执行");

        // 推进到 t+59 仍在窗口内：不自动执行。
        h.clock.advance(Duration.ofSeconds(49));
        log.info("[test-input] t={} 窗口内（end-1s），期望仍未执行 calls={}",
                h.clock.now(), action.callsOf(eventId));
        assertEquals(0, action.callsOf(eventId));
        assertEquals(EventState.DEFERRED, h.eventStore.findById(eventId).getState());

        // 推进到 t+60（窗口末端，开端）：到期唤醒自动触发并成功。
        h.clock.advance(Duration.ofSeconds(1));
        IncidentEvent event = h.eventStore.findById(eventId);
        log.info("[test-judge] t={} 越过窗口末端 -> state={} conclusion={} calls={}",
                h.clock.now(), event.getState(), event.getConclusion(), action.callsOf(eventId));
        assertEquals(EventState.RESOLVED, event.getState());
        assertEquals(Conclusion.RESOLVED_AUTO, event.getConclusion());
        assertEquals(1, action.callsOf(eventId), "延迟唤醒只执行一次");
        assertEquals(1, event.getExecutionCount());
    }

    @Test
    void changeFreezeRejectsAndDoesNotSchedule() {
        Harness h = new Harness();
        RecordingAction action = new RecordingAction("act.fz");
        h.register(action);
        h.putPolicy("p", "disk.full", Severity.WARNING,
                "act.fz", RiskLevel.LOW, true, 2, 5000);
        // 冻结 [t0, t+30s)，此刻立即命中。
        h.constraintStore.addFreeze(h.freeze("fz1", 0, 30));

        var rejected = h.ingestionService.ingest(
                h.alert("a1", "fp-fz", null, "disk.full", Severity.WARNING, "host-1"));
        String eventId = rejected.eventId();
        log.info("[test-input] t={} 冻结期接入 -> {} reason={}",
                h.clock.now(), rejected.outcome(), rejected.reason());
        assertEquals(IngestionOutcome.REJECT_CHANGE_FREEZE, rejected.outcome());
        assertEquals(EventState.OPEN, h.eventStore.findById(eventId).getState(),
                "冻结是拒绝而非延迟，事件保持 OPEN 等待后续显式触发");
        assertEquals(0, action.callsOf(eventId));

        // 冻结结束前推进时间：没有任何调度任务，动作仍不执行。
        h.clock.advance(Duration.ofSeconds(29));
        assertEquals(0, action.callsOf(eventId), "冻结拒绝不得被时间推进自动解除");

        // 越过末端 t+30 后显式触发：放行。
        h.clock.advance(Duration.ofSeconds(1));
        var manual = h.remediationService.trigger(eventId, "after-freeze");
        log.info("[test-input] t={} 冻结结束后人工触发 -> {} reason={}",
                h.clock.now(), manual.outcome(), manual.reason());
        assertEquals(TriggerOutcome.EXECUTION_STARTED, manual.outcome());
        assertEquals(1, action.callsOf(eventId));
        assertEquals(EventState.RESOLVED, h.eventStore.findById(eventId).getState());
    }
}
