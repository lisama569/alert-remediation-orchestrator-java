package com.github.lisama569.alertremediationorchestratorjava.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 归并后的单一事件（进程内可变聚合根）。
 * 并发安全由 EventStore 以每事件锁保证，本类字段方法骨架后续填充。
 */
public class IncidentEvent {

    private String eventId;
    private String fingerprint;
    private String correlationKey;
    private String alertType;
    private Severity maxSeverity;
    private String target;
    private EventState state;
    private Conclusion conclusion;
    private String matchedPolicyId;
    private Integer policyVersion;
    private int alertCount;
    private final List<String> alertIds = new ArrayList<>();
    private Instant createdAt;
    private Instant updatedAt;
    private Instant deferredUntil;
    private Instant deadline;
    private boolean approved;
    private String approver;
    private Instant approvedAt;
    private int executionCount;

    public IncidentEvent() {
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getCorrelationKey() {
        return correlationKey;
    }

    public void setCorrelationKey(String correlationKey) {
        this.correlationKey = correlationKey;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public Severity getMaxSeverity() {
        return maxSeverity;
    }

    public void setMaxSeverity(Severity maxSeverity) {
        this.maxSeverity = maxSeverity;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public EventState getState() {
        return state;
    }

    public void setState(EventState state) {
        this.state = state;
    }

    public Conclusion getConclusion() {
        return conclusion;
    }

    public void setConclusion(Conclusion conclusion) {
        this.conclusion = conclusion;
    }

    public String getMatchedPolicyId() {
        return matchedPolicyId;
    }

    public void setMatchedPolicyId(String matchedPolicyId) {
        this.matchedPolicyId = matchedPolicyId;
    }

    public Integer getPolicyVersion() {
        return policyVersion;
    }

    public void setPolicyVersion(Integer policyVersion) {
        this.policyVersion = policyVersion;
    }

    public int getAlertCount() {
        return alertCount;
    }

    public void setAlertCount(int alertCount) {
        this.alertCount = alertCount;
    }

    public List<String> getAlertIds() {
        return alertIds;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getDeferredUntil() {
        return deferredUntil;
    }

    public void setDeferredUntil(Instant deferredUntil) {
        this.deferredUntil = deferredUntil;
    }

    public Instant getDeadline() {
        return deadline;
    }

    public void setDeadline(Instant deadline) {
        this.deadline = deadline;
    }

    public boolean isApproved() {
        return approved;
    }

    public void setApproved(boolean approved) {
        this.approved = approved;
    }

    public String getApprover() {
        return approver;
    }

    public void setApprover(String approver) {
        this.approver = approver;
    }

    public Instant getApprovedAt() {
        return approvedAt;
    }

    public void setApprovedAt(Instant approvedAt) {
        this.approvedAt = approvedAt;
    }

    public int getExecutionCount() {
        return executionCount;
    }

    public void setExecutionCount(int executionCount) {
        this.executionCount = executionCount;
    }
}
