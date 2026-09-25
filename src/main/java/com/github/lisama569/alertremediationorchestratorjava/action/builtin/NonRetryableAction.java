package com.github.lisama569.alertremediationorchestratorjava.action.builtin;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import org.springframework.stereotype.Component;

/** 立即返回不可重试失败，用于验证 FAILED_NON_RETRYABLE 不做任何重试。 */
@Component
public class NonRetryableAction implements RemediationAction {

    @Override
    public String actionId() {
        return "builtin.non-retryable";
    }

    @Override
    public ActionOutcome execute(String eventId) {
        return ActionOutcome.nonRetryable("precondition missing: disk cannot be remounted (simulated)");
    }
}
