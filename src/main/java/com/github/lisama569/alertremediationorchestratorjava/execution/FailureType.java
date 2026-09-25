package com.github.lisama569.alertremediationorchestratorjava.execution;

/**
 * 执行失败分类。
 */
public enum FailureType {
    NONE,
    TIMEOUT,
    RETRYABLE,
    NON_RETRYABLE
}
