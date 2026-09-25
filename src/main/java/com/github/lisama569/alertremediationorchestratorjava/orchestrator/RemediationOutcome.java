package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.RejectionReason;

/**
 * 一次处置请求的结果：执行 / 延迟 / 拒绝 / 去重，原因可区分。
 */
public record RemediationOutcome(Kind kind, RejectionReason rejection, String detail) {

    public enum Kind { EXECUTED, DELAYED, REJECTED, DUPLICATE }

    public static RemediationOutcome executed(String detail) {
        return new RemediationOutcome(Kind.EXECUTED, null, detail);
    }

    public static RemediationOutcome delayed(String detail) {
        return new RemediationOutcome(Kind.DELAYED, null, detail);
    }

    public static RemediationOutcome rejected(RejectionReason reason, String detail) {
        return new RemediationOutcome(Kind.REJECTED, reason, detail);
    }

    public static RemediationOutcome duplicate(String detail) {
        return new RemediationOutcome(Kind.DUPLICATE, null, detail);
    }
}
