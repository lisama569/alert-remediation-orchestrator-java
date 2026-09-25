package com.github.lisama569.alertremediationorchestratorjava.policy;

import java.time.Instant;

/**
 * 维护窗口 / 变更冻结期。[start, end) 区间内生效。
 */
public record MaintenanceWindow(String name, Instant start, Instant end, Kind kind) {

    public enum Kind {
        /** 维护窗口：操作延迟到窗口结束 */
        MAINTENANCE,
        /** 变更冻结：操作直接拒绝 */
        FREEZE
    }

    /** 半开区间 [start, end)：start 命中、end 不命中，保证边界语义确定。 */
    public boolean contains(Instant t) {
        return !t.isBefore(start) && t.isBefore(end);
    }
}
