package com.github.lisama569.alertremediationorchestratorjava.tests.support;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import com.github.lisama569.alertremediationorchestratorjava.clock.TimeService;
import java.time.Duration;

/**
 * 执行时推进手动时钟来模拟“执行耗时超过超时阈值”——无真实等待。
 * 推进后返回成功；编排层应依据时钟差值判为 TIMEOUT 而非 SUCCESS。
 */
public class ClockAdvancingAction implements RemediationAction {

    private final String actionId;
    private final TimeService clock;
    private final Duration simulatedDuration;

    public ClockAdvancingAction(String actionId, TimeService clock, Duration simulatedDuration) {
        this.actionId = actionId;
        this.clock = clock;
        this.simulatedDuration = simulatedDuration;
    }

    @Override
    public String actionId() {
        return actionId;
    }

    @Override
    public ActionOutcome execute(String eventId) {
        clock.advance(simulatedDuration);
        return ActionOutcome.success();
    }
}
