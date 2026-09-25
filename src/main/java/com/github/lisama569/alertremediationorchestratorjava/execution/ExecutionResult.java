package com.github.lisama569.alertremediationorchestratorjava.execution;

/**
 * 一次处置执行的结果。失败原因显式携带，不得静默吞掉。
 */
public record ExecutionResult(boolean success, FailureType failureType, String detail) {

    public static ExecutionResult ok(String detail) {
        return new ExecutionResult(true, FailureType.NONE, detail);
    }

    public static ExecutionResult fail(FailureType type, String detail) {
        return new ExecutionResult(false, type, detail);
    }
}
