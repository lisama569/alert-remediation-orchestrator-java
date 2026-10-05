package com.github.lisama569.alertremediationorchestratorjava.domain;

/**
 * 告警接入结果。
 * disposition：本次投递的去重/受理判定（首次受理 / 归并到既有事件 / 拒绝）；
 * drive：受理后自动驱动链路（门控+处置）给出的结论，未驱动时为 null；
 * status/alertCount：返回时事件的状态与归并计数，便于投递方直接核对最终可查结果。
 */
public record IngestResult(boolean accepted, Disposition disposition, String eventKey,
                           RejectionReason rejection, String detail,
                           RemediationOutcome drive, EventStatus status, int alertCount) {

    public enum Disposition {
        /** 首次受理：新建事件并接受 */
        NEW_ACCEPTED,
        /** 重复投递：归并到既有事件，不重复计数为一次新处置 */
        MERGED,
        /** 拒绝：原因见 rejection */
        REJECTED
    }

    public static IngestResult accepted(String eventKey, Disposition disposition, String detail,
                                        RemediationOutcome drive, EventStatus status, int alertCount) {
        return new IngestResult(true, disposition, eventKey, null, detail, drive, status, alertCount);
    }

    public static IngestResult rejected(String eventKey, RejectionReason reason, String detail,
                                        EventStatus status, int alertCount) {
        return new IngestResult(false, Disposition.REJECTED, eventKey, reason, detail, null, status,
                alertCount);
    }
}
