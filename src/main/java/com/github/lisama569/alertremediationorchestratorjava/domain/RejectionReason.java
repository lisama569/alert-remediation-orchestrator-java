package com.github.lisama569.alertremediationorchestratorjava.domain;

/**
 * 可区分的拒绝原因。
 */
public enum RejectionReason {
    /** 被抑制规则命中 */
    SUPPRESSED,
    /** 事件已闭环，不再处置 */
    ALREADY_CLOSED,
    /** 无任何策略匹配该告警 */
    POLICY_MISMATCH,
    /** 命中维护窗口，按语义延迟 */
    MAINTENANCE_WINDOW,
    /** 命中变更冻结期，拒绝执行 */
    CHANGE_FREEZE,
    /** 高风险操作未获审批 */
    APPROVAL_REQUIRED,
    /** 审批被拒绝 / 未授权操作 */
    UNAUTHORIZED
}
