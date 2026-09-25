package com.github.lisama569.alertremediationorchestratorjava.domain;

import java.time.Instant;
import java.util.Map;

/**
 * 一条入站告警。eventKey 用于事件归并：同一 eventKey 的告警归入同一事件。
 */
public record Alert(
        String alertId,
        String eventKey,
        String severity,
        String source,
        Map<String, String> labels,
        Instant occurredAt) {
}
