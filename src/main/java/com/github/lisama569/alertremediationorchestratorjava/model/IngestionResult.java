package com.github.lisama569.alertremediationorchestratorjava.model;

/** 一次告警接入/触发的判定结果（骨架）。 */
public record IngestionResult(
        String eventId,
        IngestionOutcome outcome,
        String reason,
        String matchedPolicyId) {

    /** 仅当新事件创建且处置已启动时视为真正接受；其余（含延迟、拒绝、归并）均给出独立原因。 */
    public boolean accepted() {
        return outcome == IngestionOutcome.ACCEPTED_NEW;
    }
}
