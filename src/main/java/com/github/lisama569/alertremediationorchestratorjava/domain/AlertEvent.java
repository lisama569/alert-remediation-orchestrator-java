package com.github.lisama569.alertremediationorchestratorjava.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 归并后的事件。同一 eventKey 只存在一个实例；
 * 所有状态流转在本对象监视器上串行化，保证并发下结论一致。
 */
public class AlertEvent {

    private final String eventKey;
    private final List<HistoryEntry> history = new ArrayList<>();
    private EventStatus status;
    private int alertCount;
    private int executionCount;
    private RejectionReason rejection;
    /** 判定时匹配到的策略快照（策略调整后历史结论仍可复现） */
    private String policySnapshot;
    /** 判定时命中的策略对象（不可变 record，引用即快照） */
    private com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy policy;

    public AlertEvent(String eventKey, Instant createdAt) {
        this.eventKey = eventKey;
        this.status = EventStatus.NEW;
        this.history.add(new HistoryEntry(createdAt, null, EventStatus.NEW, "event created"));
    }

    public synchronized String getEventKey() {
        return eventKey;
    }

    public synchronized EventStatus getStatus() {
        return status;
    }

    public synchronized int getAlertCount() {
        return alertCount;
    }

    public synchronized int getExecutionCount() {
        return executionCount;
    }

    public synchronized RejectionReason getRejection() {
        return rejection;
    }

    public synchronized String getPolicySnapshot() {
        return policySnapshot;
    }

    public synchronized List<HistoryEntry> getHistory() {
        return List.copyOf(history);
    }

    /** 归并一条告警：计数 +1，返回归并后的总数。 */
    public synchronized int mergeAlert(Instant at, String alertId) {
        alertCount++;
        history.add(new HistoryEntry(at, status, status,
                "alert merged: alertId=" + alertId + ", alertCount=" + alertCount));
        return alertCount;
    }

    /** 记录判定时命中的策略快照。 */
    public synchronized void setPolicySnapshot(String snapshot) {
        this.policySnapshot = snapshot;
    }

    public synchronized com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy getPolicy() {
        return policy;
    }

    public synchronized void setPolicy(
            com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy p) {
        this.policy = p;
    }

    /**
     * 状态流转并记录历史。返回是否成功（非法的并发流转返回 false）。
     */
    public synchronized boolean transition(Instant at, EventStatus to, String detail) {
        EventStatus from = this.status;
        this.status = to;
        history.add(new HistoryEntry(at, from, to, detail));
        return true;
    }

    /** 标记拒绝（终态）并记录可区分原因。 */
    public synchronized void reject(Instant at, RejectionReason reason, String detail) {
        this.rejection = reason;
        transition(at, EventStatus.REJECTED, "rejected: " + reason + " - " + detail);
    }

    /** 追加一条不改变状态的判定备注（如未受理投递、终态重复投递），保证每次判定可查。 */
    public synchronized void recordNote(Instant at, String detail) {
        history.add(new HistoryEntry(at, status, status, detail));
    }

    /** 执行计数 +1（仅在真正开始一次执行时调用，保证不重复计数）。 */
    public synchronized int incrementExecution(Instant at, int attempt) {
        executionCount++;
        history.add(new HistoryEntry(at, status, status,
                "execution started: attempt=" + attempt + ", executionCount=" + executionCount));
        return executionCount;
    }
}
