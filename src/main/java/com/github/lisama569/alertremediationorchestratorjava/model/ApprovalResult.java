package com.github.lisama569.alertremediationorchestratorjava.model;

/** 人工审批结果。 */
public record ApprovalResult(
        String eventId,
        boolean approved,
        String reason) {
}
