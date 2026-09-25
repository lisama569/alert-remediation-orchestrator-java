package com.github.lisama569.alertremediationorchestratorjava.model;

import java.time.Instant;

/**
 * 接入的一条告警。
 * fingerprint 为去重键；correlationKey 为关联归并键（不同 fingerprint 的关联告警归并到同一事件）。
 */
public record Alert(
        String alertId,
        String source,
        String alertType,
        Severity severity,
        String fingerprint,
        String correlationKey,
        String target,
        String summary,
        Instant occurredAt) {
}
