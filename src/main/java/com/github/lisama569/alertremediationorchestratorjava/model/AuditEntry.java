package com.github.lisama569.alertremediationorchestratorjava.model;

import java.time.Instant;

/** 不可变审计条目：记录输入、判定依据与状态流转。 */
public record AuditEntry(
        long seq,
        Instant time,
        String eventId,
        String stage,
        String detail) {
}
