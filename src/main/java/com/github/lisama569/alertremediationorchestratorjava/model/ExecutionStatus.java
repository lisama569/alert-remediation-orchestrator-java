package com.github.lisama569.alertremediationorchestratorjava.model;

/** 处置执行状态。 */
public enum ExecutionStatus {
    RUNNING,
    SUCCEEDED,
    RETRYING,
    FAILED_RETRYABLE,
    FAILED_NON_RETRYABLE,
    TIMED_OUT
}
