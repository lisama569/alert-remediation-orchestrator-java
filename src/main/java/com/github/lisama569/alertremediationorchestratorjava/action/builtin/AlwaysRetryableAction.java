package com.github.lisama569.alertremediationorchestratorjava.action.builtin;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import org.springframework.stereotype.Component;

/** 始终可重试失败，用于验证重试耗尽结论（FAILED_RETRIES_EXHAUSTED）。 */
@Component
public class AlwaysRetryableAction implements RemediationAction {

    @Override
    public String actionId() {
        return "builtin.always-retryable";
    }

    @Override
    public ActionOutcome execute(String eventId) {
        return ActionOutcome.retryable("persistent transient error (simulated)");
    }
}
