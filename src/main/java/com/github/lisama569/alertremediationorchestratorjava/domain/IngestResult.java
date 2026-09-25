package com.github.lisama569.alertremediationorchestratorjava.domain;

/**
 * 告警接入结果：要么被接受并归并到事件，要么以可区分原因被拒绝。
 */
public record IngestResult(boolean accepted, String eventKey, RejectionReason rejection, String detail) {

    public static IngestResult accepted(String eventKey, String detail) {
        return new IngestResult(true, eventKey, null, detail);
    }

    public static IngestResult rejected(String eventKey, RejectionReason reason, String detail) {
        return new IngestResult(false, eventKey, reason, detail);
    }
}
