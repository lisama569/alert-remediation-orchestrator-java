package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 人工审批：高风险操作需先获批。审批状态按事件记录，可追溯。
 */
public class ApprovalService {

    private static final Logger log = LoggerFactory.getLogger(ApprovalService.class);

    public enum ApprovalState { PENDING, APPROVED, REJECTED }

    public record ApprovalRecord(ApprovalState state, String approver, Instant at) {
    }

    private final Map<String, ApprovalRecord> records = new ConcurrentHashMap<>();
    private final MutableClock clock;

    public ApprovalService(MutableClock clock) {
        this.clock = clock;
    }

    public void requestApproval(String eventKey) {
        records.merge(eventKey, new ApprovalRecord(ApprovalState.PENDING, null, clock.now()),
                (old, pending) -> old.state() == ApprovalState.APPROVED ? old : pending);
        log.info("approval requested: eventKey={}", eventKey);
    }

    public boolean approve(String eventKey, String approver) {
        ApprovalRecord rec = new ApprovalRecord(ApprovalState.APPROVED, approver, clock.now());
        records.put(eventKey, rec);
        log.info("approval granted: eventKey={} approver={}", eventKey, approver);
        return true;
    }

    public boolean reject(String eventKey, String approver) {
        ApprovalRecord rec = new ApprovalRecord(ApprovalState.REJECTED, approver, clock.now());
        records.put(eventKey, rec);
        log.info("approval rejected: eventKey={} approver={}", eventKey, approver);
        return true;
    }

    public boolean isApproved(String eventKey) {
        ApprovalRecord rec = records.get(eventKey);
        return rec != null && rec.state() == ApprovalState.APPROVED;
    }

    public ApprovalRecord getRecord(String eventKey) {
        return records.get(eventKey);
    }
}
