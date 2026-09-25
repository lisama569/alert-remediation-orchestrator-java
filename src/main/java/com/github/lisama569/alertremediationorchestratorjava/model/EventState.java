package com.github.lisama569.alertremediationorchestratorjava.model;

/**
 * 事件生命周期状态：
 * OPEN -> EXECUTING -> RESOLVED/FAILED/PARTIAL
 * OPEN -> WAITING_APPROVAL -> EXECUTING ...
 * EXECUTING -> DEFERRED(维护窗口) -> EXECUTING ...
 */
public enum EventState {
    OPEN,
    DEFERRED,
    WAITING_APPROVAL,
    EXECUTING,
    RESOLVED,
    FAILED,
    PARTIAL,
    CLOSED;

    /** 终态事件拒绝一切新的处置触发，重复告警只归并不执行。 */
    public boolean isTerminal() {
        return this == RESOLVED || this == FAILED || this == PARTIAL || this == CLOSED;
    }
}
