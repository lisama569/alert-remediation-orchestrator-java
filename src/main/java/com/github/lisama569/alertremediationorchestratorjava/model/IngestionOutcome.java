package com.github.lisama569.alertremediationorchestratorjava.model;

/** 告警接入判定结论。除 ACCEPTED 外均为可区分的拒绝原因。 */
public enum IngestionOutcome {
    /** 新事件已创建，等待处置。 */
    ACCEPTED_NEW,
    /** 归并到既有事件（重复或关联告警）。 */
    MERGED,
    /** 事件已闭环，重复告警被拒绝。 */
    REJECT_ALREADY_CLOSED,
    /** 事件存在正在执行的处置，本次触发被拒绝。 */
    REJECT_EXECUTION_IN_PROGRESS,
    /** 命中活动维护窗口，处置延迟（告警仍归并到事件）。 */
    DEFERRED_MAINTENANCE_WINDOW,
    /** 命中变更冻结期，处置被拒绝。 */
    REJECT_CHANGE_FREEZE,
    /** 高风险操作缺少人工审批，处置被拒绝。 */
    REJECT_APPROVAL_REQUIRED,
    /** 无匹配策略，不触发处置。 */
    REJECT_NO_POLICY,
    /** 策略匹配但未启用自动处置。 */
    REJECT_POLICY_DISABLED
}
