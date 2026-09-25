package com.github.lisama569.alertremediationorchestratorjava.model;

/** 执行前置约束判定结论。 */
public enum GuardVerdict {
    PROCEED,
    /** 维护窗口：延迟。 */
    DEFER,
    /** 变更冻结：拒绝。 */
    REJECT_FREEZE,
    /** 高风险未审批：拒绝。 */
    REJECT_UNAPPROVED
}
