package com.github.lisama569.alertremediationorchestratorjava.model;

/** 事件闭环结论，用于可解释地复现历史结果。 */
public enum Conclusion {
    RESOLVED_AUTO,
    FAILED_RETRIES_EXHAUSTED,
    FAILED_NON_RETRYABLE,
    TIMED_OUT,
    CLOSED_MANUALLY
}
