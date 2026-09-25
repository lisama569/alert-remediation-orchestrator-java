package com.github.lisama569.alertremediationorchestratorjava.action.builtin;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import org.springframework.stereotype.Component;

/** 始终成功的模拟动作。 */
@Component
public class SuccessAction implements RemediationAction {

    @Override
    public String actionId() {
        return "builtin.success";
    }

    @Override
    public ActionOutcome execute(String eventId) {
        return ActionOutcome.success();
    }
}
