package com.github.lisama569.alertremediationorchestratorjava.model;

import java.time.Instant;

/**
 * 维护窗口：命中时间区间且 target 匹配（"*" 表示全部目标）时，
 * 处置语义为延迟到窗口结束后执行。
 */
public record MaintenanceWindow(
        String windowId,
        String target,
        Instant startAt,
        Instant endAt,
        String reason) {

    /** 半开区间 [startAt, endAt)：恰好在 endAt 触发不再命中，保证边界判定确定。 */
    public boolean contains(Instant time) {
        return !time.isBefore(startAt) && time.isBefore(endAt);
    }
}
