package com.github.lisama569.alertremediationorchestratorjava.model;

/** 人工/重复触发处置的结论，与接入拒绝原因保持同一套可区分语义。 */
public enum TriggerOutcome {
    EXECUTION_STARTED,
    ALREADY_CLOSED,
    EXECUTION_IN_PROGRESS,
    DEFERRED_MAINTENANCE_WINDOW,
    REJECT_CHANGE_FREEZE,
    REJECT_APPROVAL_REQUIRED,
    NO_POLICY,
    EVENT_NOT_FOUND
}
