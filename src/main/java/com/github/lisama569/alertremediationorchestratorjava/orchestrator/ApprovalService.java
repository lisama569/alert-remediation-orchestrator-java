package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 人工审批：高风险操作需先获批。审批状态按事件记录，可追溯。
 * 批准/拒绝后通知监听器，由编排层自动继续（批准->执行，拒绝->终态拒绝），无需再手动触发。
 */
public class ApprovalService {

    private static final Logger log = LoggerFactory.getLogger(ApprovalService.class);

    public enum ApprovalState { PENDING, APPROVED, REJECTED }

    public record ApprovalRecord(ApprovalState state, String approver, Instant at) {
    }

    /** 审批确定（批准/拒绝）后的回调：参数为 eventKey。 */
    public interface ApprovalListener {
        void onApprovalDecided(String eventKey, ApprovalState state);
    }

    private final Map<String, ApprovalRecord> records = new ConcurrentHashMap<>();
    private final List<ApprovalListener> listeners = new CopyOnWriteArrayList<>();
    private final MutableClock clock;

    public ApprovalService(MutableClock clock) {
        this.clock = clock;
    }

    public void addListener(ApprovalListener listener) {
        listeners.add(listener);
    }

    private void fireDecided(String eventKey, ApprovalState state) {
        for (ApprovalListener l : listeners) {
            l.onApprovalDecided(eventKey, state);
        }
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
        fireDecided(eventKey, ApprovalState.APPROVED);
        return true;
    }

    public boolean reject(String eventKey, String approver) {
        records.put(eventKey, new ApprovalRecord(ApprovalState.REJECTED, approver, clock.now()));
        log.info("approval rejected: eventKey={} approver={}", eventKey, approver);
        fireDecided(eventKey, ApprovalState.REJECTED);
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
