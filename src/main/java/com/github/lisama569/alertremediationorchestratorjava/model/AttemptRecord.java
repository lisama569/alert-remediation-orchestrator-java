package com.github.lisama569.alertremediationorchestratorjava.model;

import java.time.Instant;

/** 一次执行尝试的不可变记录。 */
public record AttemptRecord(
        int attemptNo,
        AttemptStatus status,
        String message,
        Instant startedAt,
        Instant finishedAt) {
}
