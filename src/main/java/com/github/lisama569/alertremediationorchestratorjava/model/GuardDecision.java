package com.github.lisama569.alertremediationorchestratorjava.model;

import java.time.Instant;

/** 前置约束判定详情（可解释、可审计）。 */
public record GuardDecision(
        GuardVerdict verdict,
        String constraintId,
        String reason,
        Instant deferredUntil) {

    public static GuardDecision proceed() {
        return new GuardDecision(GuardVerdict.PROCEED, null, "no active constraint", null);
    }

    public static GuardDecision defer(String constraintId, String reason, Instant deferredUntil) {
        return new GuardDecision(GuardVerdict.DEFER, constraintId, reason, deferredUntil);
    }

    public static GuardDecision rejectFreeze(String constraintId, String reason) {
        return new GuardDecision(GuardVerdict.REJECT_FREEZE, constraintId, reason, null);
    }

    public static GuardDecision rejectUnapproved() {
        return new GuardDecision(
                GuardVerdict.REJECT_UNAPPROVED,
                null,
                "high risk action requires manual approval",
                null);
    }
}
