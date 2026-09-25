package com.github.lisama569.alertremediationorchestratorjava.action;

import java.util.Objects;

/**
 * 处置动作的执行结果：成功 / 可重试失败（带原因）/ 不可重试失败（带原因）。
 * 失败结果强制携带非空 message，编排层原样记录到审计与执行记录中，不得静默吞掉。
 */
public class ActionOutcome {

    public enum Kind {
        SUCCESS,
        RETRYABLE_FAILURE,
        NON_RETRYABLE_FAILURE
    }

    private final Kind kind;
    private final String message;

    private ActionOutcome(Kind kind, String message) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.message = message;
    }

    public static ActionOutcome success() {
        return new ActionOutcome(Kind.SUCCESS, "ok");
    }

    public static ActionOutcome retryable(String message) {
        Objects.requireNonNull(message, "retryable failure must carry a reason");
        if (message.isBlank()) {
            throw new IllegalArgumentException("retryable failure reason must not be blank");
        }
        return new ActionOutcome(Kind.RETRYABLE_FAILURE, message);
    }

    public static ActionOutcome nonRetryable(String message) {
        Objects.requireNonNull(message, "non-retryable failure must carry a reason");
        if (message.isBlank()) {
            throw new IllegalArgumentException("non-retryable failure reason must not be blank");
        }
        return new ActionOutcome(Kind.NON_RETRYABLE_FAILURE, message);
    }

    public Kind getKind() {
        return kind;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return kind + "(" + message + ")";
    }
}
