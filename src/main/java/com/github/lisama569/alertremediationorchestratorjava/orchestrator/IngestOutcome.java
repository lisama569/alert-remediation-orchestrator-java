package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.IngestResult;

/**
 * 告警接入并自动驱动的组合结果。
 * ingest 为接入判定（接受归并 / 可区分拒绝）；
 * remediation 为自动处置结果（执行 / 延迟 / 拒绝 / 去重），接入未受理时为 null。
 */
public record IngestOutcome(IngestResult ingest, RemediationOutcome remediation) {

    public boolean accepted() {
        return ingest.accepted();
    }
}
