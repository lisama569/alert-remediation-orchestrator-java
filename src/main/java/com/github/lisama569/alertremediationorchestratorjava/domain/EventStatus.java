package com.github.lisama569.alertremediationorchestratorjava.domain;

/**
 * 事件状态机：
 * NEW -> REMEDIATING -> RESOLVED / FAILED_RETRYABLE / FAILED_FINAL
 * NEW -> AWAITING_APPROVAL -> (批准后) REMEDIATING / (拒绝后) REJECTED
 * NEW -> DELAYED -> (窗口结束后) REMEDIATING
 * 任意 -> CLOSED（人工闭环，终态）/ REJECTED（终态）
 */
public enum EventStatus {
    NEW,
    AWAITING_APPROVAL,
    DELAYED,
    REMEDIATING,
    RESOLVED,
    FAILED_RETRYABLE,
    FAILED_FINAL,
    CLOSED,
    REJECTED
}
