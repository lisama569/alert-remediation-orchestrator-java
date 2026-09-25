package com.github.lisama569.alertremediationorchestratorjava.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 一次处置执行的生命周期记录（每事件当前只允许存在一个进行中的执行）。
 */
public class ExecutionRecord {

    private String executionId;
    private String eventId;
    private String actionId;
    private ExecutionStatus status;
    private int attemptCount;
    private Instant startedAt;
    private Instant finishedAt;
    private String failureMessage;
    private final List<AttemptRecord> attempts = new ArrayList<>();

    public ExecutionRecord() {
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getActionId() {
        return actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(ExecutionStatus status) {
        this.status = status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }

    public List<AttemptRecord> getAttempts() {
        return attempts;
    }
}
