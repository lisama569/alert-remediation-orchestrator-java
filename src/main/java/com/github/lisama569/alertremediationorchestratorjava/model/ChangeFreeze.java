package com.github.lisama569.alertremediationorchestratorjava.model;

import java.time.Instant;

/** 变更冻结期：命中即拒绝执行（不延迟）。 */
public record ChangeFreeze(
        String freezeId,
        Instant startAt,
        Instant endAt,
        String reason) {

    /** 半开区间 [startAt, endAt)：恰好在 endAt 触发不再命中冻结。 */
    public boolean contains(Instant time) {
        return !time.isBefore(startAt) && time.isBefore(endAt);
    }
}
