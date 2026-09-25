package com.github.lisama569.alertremediationorchestratorjava.model;

/** 单次处置尝试结果，三类失败必须可区分。 */
public enum AttemptStatus {
    SUCCESS,
    RETRYABLE_FAILED,
    NON_RETRYABLE_FAILED,
    TIMEOUT
}
