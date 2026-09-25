package com.github.lisama569.alertremediationorchestratorjava.model;

/** 触发处置的判定结果。 */
public record TriggerResult(
        String eventId,
        TriggerOutcome outcome,
        String reason,
        String executionId) {
}
