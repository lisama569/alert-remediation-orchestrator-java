package com.github.lisama569.alertremediationorchestratorjava.execution;

/**
 * 处置动作。实现需线程安全；执行次数通过 attempt 计数可观测。
 */
@FunctionalInterface
public interface RemediationAction {
    ExecutionResult run(String eventKey, int attempt);
}
