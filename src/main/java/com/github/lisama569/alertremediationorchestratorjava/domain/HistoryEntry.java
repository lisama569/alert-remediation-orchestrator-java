package com.github.lisama569.alertremediationorchestratorjava.domain;

import java.time.Instant;

/**
 * 事件历史条目：记录状态流转与判定依据，保证可追溯、可复现。
 */
public record HistoryEntry(Instant at, EventStatus from, EventStatus to, String detail) {
}
