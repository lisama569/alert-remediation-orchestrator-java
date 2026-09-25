package com.github.lisama569.alertremediationorchestratorjava.engine;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.action.ActionRegistry;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import com.github.lisama569.alertremediationorchestratorjava.clock.TimeService;
import com.github.lisama569.alertremediationorchestratorjava.model.ApprovalResult;
import com.github.lisama569.alertremediationorchestratorjava.model.AttemptRecord;
import com.github.lisama569.alertremediationorchestratorjava.model.AttemptStatus;
import com.github.lisama569.alertremediationorchestratorjava.model.Conclusion;
import com.github.lisama569.alertremediationorchestratorjava.model.EventState;
import com.github.lisama569.alertremediationorchestratorjava.model.ExecutionRecord;
import com.github.lisama569.alertremediationorchestratorjava.model.ExecutionStatus;
import com.github.lisama569.alertremediationorchestratorjava.model.GuardDecision;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import com.github.lisama569.alertremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.model.TriggerOutcome;
import com.github.lisama569.alertremediationorchestratorjava.model.TriggerResult;
import com.github.lisama569.alertremediationorchestratorjava.store.AuditLog;
import com.github.lisama569.alertremediationorchestratorjava.store.EventStore;
import com.github.lisama569.alertremediationorchestratorjava.store.PolicyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 处置编排核心。
 *
 * 并发安全（重复/并发触发不产生重复执行、不重复计数）：
 * - 每次 trigger 在该事件分段锁（EventStore#mutateWithLock）内完成
 *   “状态检查 -> guard 判定 -> 置 EXECUTING/建 ExecutionRecord/计数”，临界区外并发的
 *   第二个触发只能观察到 EXECUTING 并拿到 EXECUTION_IN_PROGRESS；
 * - 动作本体在锁外执行（纯内存模拟、无真实等待），结果回写与重试调度再次进入同一把锁；
 * - 维护窗口延迟、重试延迟均通过 {@link TimeService#schedule} 唤醒，期间状态为
 *   DEFERRED/OPEN（非 EXECUTING），唤醒走同一条 trigger 路径，语义与人工触发一致。
 *
 * 失败三分类不被吞掉：SUCCESS / RETRYABLE_FAILED / NON_RETRYABLE_FAILED / TIMEOUT
 * 分别落 AttemptRecord，失败原因原文写入 ExecutionRecord.failureMessage 与审计日志。
 */
@Service
public class RemediationService {

    private static final Logger log = LoggerFactory.getLogger(RemediationService.class);

    /** 可重试失败后的重试间隔，由手动时钟推进驱动（测试不做任何真实等待）。 */
    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(1);

    private final EventStore eventStore;
    private final PolicyStore policyStore;
    private final AuditLog auditLog;
    private final TimeService clock;
    private final GuardService guardService;
    private final ActionRegistry actionRegistry;
    private final AtomicLong executionSeq = new AtomicLong();

    public RemediationService(
            EventStore eventStore,
            PolicyStore policyStore,
            AuditLog auditLog,
            TimeService clock,
            GuardService guardService,
            ActionRegistry actionRegistry) {
        this.eventStore = eventStore;
        this.policyStore = policyStore;
        this.auditLog = auditLog;
        this.clock = clock;
        this.guardService = guardService;
        this.actionRegistry = actionRegistry;
    }

    public TriggerResult trigger(String eventId, String triggerSource) {
        if (eventStore.findById(eventId) == null) {
            auditLog.append(eventId, "TRIGGER", "source=" + triggerSource + " -> event not found");
            return new TriggerResult(eventId, TriggerOutcome.EVENT_NOT_FOUND, "事件不存在", null);
        }

        // ---- 临界区 1：门禁判定 + 占位执行权（互斥核心）----
        TriggerStart start = eventStore.mutateWithLock(eventId, event -> {
            Instant now = clock.now();
            event.setUpdatedAt(now);

            if (event.getState().isTerminal()) {
                return TriggerStart.reject(new TriggerResult(
                        eventId,
                        TriggerOutcome.ALREADY_CLOSED,
                        "事件已闭环 state=" + event.getState() + " conclusion=" + event.getConclusion(),
                        null));
            }
            if (event.getState() == EventState.EXECUTING) {
                return TriggerStart.reject(new TriggerResult(
                        eventId,
                        TriggerOutcome.EXECUTION_IN_PROGRESS,
                        "已有处置正在执行（同一时刻只允许一个）",
                        null));
            }

            RemediationPolicy policy = resolvePolicy(event);
            if (policy == null) {
                return TriggerStart.reject(new TriggerResult(
                        eventId, TriggerOutcome.NO_POLICY,
                        "无匹配策略 policy=" + event.getMatchedPolicyId()
                                + "@v" + event.getPolicyVersion(),
                        null));
            }

            GuardDecision guard = guardService.evaluate(event, policy);
            switch (guard.verdict()) {
                case REJECT_FREEZE -> {
                    auditLog.append(eventId, "GUARD",
                            "input(source=" + triggerSource + ") verdict=REJECT_FREEZE by "
                                    + guard.constraintId() + " reason=" + guard.reason());
                    return TriggerStart.reject(new TriggerResult(
                            eventId, TriggerOutcome.REJECT_CHANGE_FREEZE, guard.reason(), null));
                }
                case DEFER -> {
                    event.setState(EventState.DEFERRED);
                    event.setDeferredUntil(guard.deferredUntil());
                    auditLog.append(eventId, "GUARD",
                            "input(source=" + triggerSource + ") verdict=DEFER by "
                                    + guard.constraintId() + " until=" + guard.deferredUntil()
                                    + " reason=" + guard.reason());
                    clock.schedule(guard.deferredUntil(),
                            () -> trigger(eventId, "maintenance-window-unblock@" + guard.constraintId()));
                    return TriggerStart.reject(new TriggerResult(
                            eventId, TriggerOutcome.DEFERRED_MAINTENANCE_WINDOW,
                            guard.reason() + "；已调度 " + guard.deferredUntil() + " 自动重试",
                            null));
                }
                case REJECT_UNAPPROVED -> {
                    event.setState(EventState.WAITING_APPROVAL);
                    auditLog.append(eventId, "GUARD",
                            "input(source=" + triggerSource + ") verdict=REJECT_UNAPPROVED"
                                    + " risk=" + policy.riskLevel() + " action=" + policy.actionId());
                    return TriggerStart.reject(new TriggerResult(
                            eventId, TriggerOutcome.REJECT_APPROVAL_REQUIRED, guard.reason(), null));
                }
                case PROCEED -> {
                    /* 继续执行 */
                }
            }

            RemediationAction action = actionRegistry.get(policy.actionId());
            if (action == null) {
                // 动作缺失视为不可重试失败：不允许静默吞掉，也不允许无限重试一个不存在的动作。
                ExecutionRecord rec = beginExecution(event, policy, now);
                finishNonRetryable(event, rec, "处置动作未注册 actionId=" + policy.actionId(), now);
                auditLog.append(eventId, "EXECUTE",
                        "action=" + policy.actionId() + " -> NON_RETRYABLE: action not registered");
                return TriggerStart.proceed(rec.getExecutionId());
            }

            ExecutionRecord rec = beginExecution(event, policy, now);
            auditLog.append(eventId, "TRIGGER",
                    "input(source=" + triggerSource + ") policy=" + policy.policyId()
                            + "@v" + policy.version() + " action=" + policy.actionId()
                            + " -> execution " + rec.getExecutionId() + " started");
            return TriggerStart.proceed(rec.getExecutionId(), rec, policy, action);
        });

        if (start.rejected != null) {
            return start.rejected;
        }
        ExecutionRecord rec = start.execution;

        // ---- 锁外：执行尝试循环（动作纯内存、无真实等待；重试由时钟调度）----
        runAttemptLoop(eventId, rec.getExecutionId(), start.policy, start.action);
        return new TriggerResult(eventId, TriggerOutcome.EXECUTION_STARTED,
                "处置执行结束，见执行记录与审计", rec.getExecutionId());
    }

    /**
     * 单次执行的尝试循环。除动作本体外，所有状态读写都在事件锁内。
     * 可重试失败不就地循环，而是 schedule 一个 RETRY_BACKOFF 后的唤醒（由时钟推进触发），
     * 因此本方法在“当前时刻”同步跑完前置尝试后即返回，绝不真实等待。
     */
    private void runAttemptLoop(
            String eventId, String executionId, RemediationPolicy policy, RemediationAction action) {
        Instant attemptStart = clock.now();

        // ---- 临界区 2：动作执行前的状态闸门 + 读取下一次 attempt 序号 ----
        // 人工闭环/新执行取代等情况下，已排队的重试唤醒必须在真正执行动作前被拦截，
        // 否则会出现“事件已 CLOSED 却又跑了一次处置”的不一致。
        int attemptNo = eventStore.mutateWithLock(eventId, event -> {
            ExecutionRecord rec = latestExecution(event, executionId);
            if (event.getState().isTerminal()
                    || event.getState() != EventState.EXECUTING
                    || rec.getStatus() != ExecutionStatus.RUNNING) {
                auditLog.append(eventId, "ATTEMPT",
                        "execution=" + executionId
                                + " attempt skipped before action: state=" + event.getState()
                                + " execStatus=" + rec.getStatus());
                return -1;
            }
            return rec.getAttemptCount() + 1;
        });
        if (attemptNo < 0) {
            return;
        }

        ActionOutcome outcome;
        try {
            log.info("[execute] event={} execution={} attempt={} action={} input-policy={}@v{}",
                    eventId, executionId, attemptNo, action.actionId(),
                    policy.policyId(), policy.version());
            outcome = action.execute(eventId);
        } catch (RuntimeException e) {
            // 动作抛异常等价于可重试失败，异常信息不得丢失。
            outcome = ActionOutcome.retryable("action threw " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        }
        if (outcome == null) {
            outcome = ActionOutcome.nonRetryable("action returned null outcome");
        }
        final ActionOutcome finalOutcome = outcome;

        Instant attemptEnd = clock.now();
        boolean clockAdvanced = attemptEnd.isAfter(attemptStart);
        long elapsedMillis = Duration.between(attemptStart, attemptEnd).toMillis();
        boolean timedOut = clockAdvanced && elapsedMillis > policy.timeoutMillis();
        final long elapsedSnapshot = elapsedMillis;
        final boolean timedOutSnapshot = timedOut;

        // ---- 临界区 3：记录尝试结果并决定终态/重试 ----
        RetryDecision decision = eventStore.mutateWithLock(eventId, event -> {
            ExecutionRecord recNow = latestExecution(event, executionId);
            if (recNow.getStatus() != ExecutionStatus.RUNNING) {
                // 执行已被人工闭环等方式终止，忽略迟到结果。
                return RetryDecision.stop();
            }

            AttemptStatus status;
            String message = finalOutcome.getMessage();
            if (timedOutSnapshot) {
                status = AttemptStatus.TIMEOUT;
                message = "timeout after " + elapsedSnapshot + "ms (limit "
                        + policy.timeoutMillis() + "ms): " + message;
            } else {
                status = switch (finalOutcome.getKind()) {
                    case SUCCESS -> AttemptStatus.SUCCESS;
                    case RETRYABLE_FAILURE -> AttemptStatus.RETRYABLE_FAILED;
                    case NON_RETRYABLE_FAILURE -> AttemptStatus.NON_RETRYABLE_FAILED;
                };
            }
            addAttempt(recNow, new AttemptRecord(attemptNo, status, message, attemptStart, attemptEnd));
            auditLog.append(eventId, "ATTEMPT",
                    "execution=" + executionId + " attempt=" + attemptNo + " -> " + status
                            + " reason=" + message);

            switch (status) {
                case SUCCESS -> {
                    finishSuccess(event, recNow, attemptEnd);
                    return RetryDecision.stop();
                }
                case NON_RETRYABLE_FAILED -> {
                    finishNonRetryable(event, recNow, message, attemptEnd);
                    return RetryDecision.stop();
                }
                case TIMEOUT, RETRYABLE_FAILED -> {
                    if (attemptNo >= policy.maxAttempts()) {
                        finishRetriesExhausted(event, recNow, status, message, attemptEnd);
                        return RetryDecision.stop();
                    }
                    recNow.setStatus(ExecutionStatus.RETRYING);
                    event.setState(EventState.OPEN);
                    Instant retryAt = attemptEnd.plus(RETRY_BACKOFF);
                    auditLog.append(eventId, "RETRY",
                            "execution=" + executionId + " attempt=" + attemptNo
                                    + " exhausted，schedule retry #" + (attemptNo + 1)
                                    + " at " + retryAt + " (backoff=" + RETRY_BACKOFF + ")");
                    return RetryDecision.retry(retryAt);
                }
                default -> {
                    return RetryDecision.stop();
                }
            }
        });

        if (decision.retryAt != null) {
            // 唤醒后重新走 trigger：会重新过冻结/维护窗口/审批门禁，再重新占位执行权，
            // 因此与人工触发、窗口解除在并发下仍然互斥、不重复计数。
            clock.schedule(decision.retryAt,
                    () -> continueExecution(eventId, executionId, policy, action));
        }
    }

    /** 重试唤醒：单阶段在锁内确认仍属于同一次执行且事件未终结，再回收执行权继续尝试。 */
    private void continueExecution(
            String eventId, String executionId, RemediationPolicy policy, RemediationAction action) {
        boolean reclaimed = eventStore.mutateWithLock(eventId, event -> {
            ExecutionRecord last = latestExecutionOrNull(event);
            boolean ours = last != null
                    && last.getExecutionId().equals(executionId)
                    && last.getStatus() == ExecutionStatus.RETRYING;
            // 人工闭环/审批拒绝等终结、或被更新的执行取代时，唤醒直接丢弃，绝不覆盖回 OPEN。
            if (!ours || event.getState().isTerminal()) {
                if (event.getState().isTerminal()) {
                    auditLog.append(eventId, "RETRY",
                            "execution=" + executionId
                                    + " retry dropped: event terminal " + event.getState());
                }
                return false;
            }
            // 重试归属同一个 ExecutionRecord，不新增 executionCount。
            last.setStatus(ExecutionStatus.RUNNING);
            event.setState(EventState.EXECUTING);
            return true;
        });
        if (reclaimed) {
            runAttemptLoop(eventId, executionId, policy, action);
        }
    }

    public ApprovalResult approve(String eventId, String approver, boolean approved, String comment) {
        IncidentEvent event = eventStore.findById(eventId);
        if (event == null) {
            return new ApprovalResult(eventId, false, "事件不存在");
        }
        return eventStore.mutateWithLock(eventId, e -> {
            if (e.getState().isTerminal()) {
                auditLog.append(eventId, "APPROVAL",
                        "input(approver=" + approver + ",approved=" + approved
                                + ") -> reject: event terminal " + e.getState());
                return new ApprovalResult(eventId, false, "事件已闭环，不可审批");
            }
            if (!approved) {
                e.setState(EventState.FAILED);
                e.setConclusion(Conclusion.CLOSED_MANUALLY);
                e.setUpdatedAt(clock.now());
                auditLog.append(eventId, "APPROVAL",
                        "input(approver=" + approver + ",approved=false) comment=" + comment
                                + " -> state=FAILED（审批拒绝，处置永久禁止）");
                return new ApprovalResult(eventId, false, "审批已拒绝，事件终结");
            }
            e.setApproved(true);
            e.setApprover(approver);
            e.setApprovedAt(clock.now());
            if (e.getState() == EventState.WAITING_APPROVAL) {
                e.setState(EventState.OPEN);
            }
            e.setUpdatedAt(clock.now());
            auditLog.append(eventId, "APPROVAL",
                    "input(approver=" + approver + ",approved=true) comment=" + comment
                            + " -> 高风险操作获准，可触发处置");
            return new ApprovalResult(eventId, true, "审批通过");
        });
    }

    public TriggerResult manualClose(String eventId, String operator, String note) {
        IncidentEvent event = eventStore.findById(eventId);
        if (event == null) {
            return new TriggerResult(eventId, TriggerOutcome.EVENT_NOT_FOUND, "事件不存在", null);
        }
        eventStore.mutateWithLock(eventId, e -> {
            e.setState(EventState.CLOSED);
            e.setConclusion(Conclusion.CLOSED_MANUALLY);
            e.setUpdatedAt(clock.now());
            auditLog.append(eventId, "MANUAL_CLOSE",
                    "input(operator=" + operator + ") note=" + note
                            + " -> state=CLOSED（调度中的重试/延迟唤醒将被丢弃）");
            return null;
        });
        return new TriggerResult(eventId, TriggerOutcome.ALREADY_CLOSED,
                "人工闭环 by " + operator + ": " + note, null);
    }

    // ------------------------------------------------------------------
    // 锁内辅助方法（只能在 mutateWithLock 回调中调用）
    // ------------------------------------------------------------------

    private RemediationPolicy resolvePolicy(IncidentEvent event) {
        if (event.getMatchedPolicyId() == null) {
            return null;
        }
        if (event.getPolicyVersion() != null) {
            // 固化版本优先：策略后来调整不改变该事件的处置结论。
            return policyStore.getVersion(event.getMatchedPolicyId(), event.getPolicyVersion());
        }
        return policyStore.latest(event.getMatchedPolicyId());
    }

    private ExecutionRecord beginExecution(
            IncidentEvent event, RemediationPolicy policy, Instant now) {
        ExecutionRecord rec = new ExecutionRecord();
        rec.setExecutionId("exec-" + executionSeq.incrementAndGet());
        rec.setEventId(event.getEventId());
        rec.setActionId(policy.actionId());
        rec.setStatus(ExecutionStatus.RUNNING);
        rec.setAttemptCount(0);
        rec.setStartedAt(now);
        eventStore.addExecution(event.getEventId(), rec);
        event.setState(EventState.EXECUTING);
        event.setExecutionCount(event.getExecutionCount() + 1);
        event.setDeferredUntil(null);
        event.setUpdatedAt(now);
        return rec;
    }

    private void addAttempt(ExecutionRecord rec, AttemptRecord attempt) {
        rec.getAttempts().add(attempt);
        rec.setAttemptCount(attempt.attemptNo());
    }

    private void finishSuccess(IncidentEvent event, ExecutionRecord rec, Instant now) {
        rec.setStatus(ExecutionStatus.SUCCEEDED);
        rec.setFinishedAt(now);
        event.setState(EventState.RESOLVED);
        event.setConclusion(Conclusion.RESOLVED_AUTO);
        event.setUpdatedAt(now);
        auditLog.append(event.getEventId(), "FINISH",
                "execution=" + rec.getExecutionId() + " -> SUCCEEDED, state=RESOLVED");
    }

    private void finishNonRetryable(
            IncidentEvent event, ExecutionRecord rec, String message, Instant now) {
        rec.setStatus(ExecutionStatus.FAILED_NON_RETRYABLE);
        rec.setFailureMessage(message);
        rec.setFinishedAt(now);
        event.setState(EventState.FAILED);
        event.setConclusion(Conclusion.FAILED_NON_RETRYABLE);
        event.setUpdatedAt(now);
        auditLog.append(event.getEventId(), "FINISH",
                "execution=" + rec.getExecutionId()
                        + " -> FAILED_NON_RETRYABLE reason=" + message + ", state=FAILED");
    }

    private void finishRetriesExhausted(
            IncidentEvent event, ExecutionRecord rec, AttemptStatus lastStatus,
            String message, Instant now) {
        ExecutionStatus execStatus = lastStatus == AttemptStatus.TIMEOUT
                ? ExecutionStatus.TIMED_OUT
                : ExecutionStatus.FAILED_RETRYABLE;
        Conclusion conclusion = lastStatus == AttemptStatus.TIMEOUT
                ? Conclusion.TIMED_OUT
                : Conclusion.FAILED_RETRIES_EXHAUSTED;
        rec.setStatus(execStatus);
        rec.setFailureMessage("retries exhausted (last=" + lastStatus + "): " + message);
        rec.setFinishedAt(now);
        event.setState(EventState.FAILED);
        event.setConclusion(conclusion);
        event.setUpdatedAt(now);
        auditLog.append(event.getEventId(), "FINISH",
                "execution=" + rec.getExecutionId() + " -> " + execStatus
                        + " reason=" + rec.getFailureMessage() + ", state=FAILED");
    }

    private ExecutionRecord latestExecution(IncidentEvent event, String executionId) {
        return eventStore.executionsOf(event.getEventId()).stream()
                .filter(r -> r.getExecutionId().equals(executionId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "execution record vanished: " + executionId));
    }

    private ExecutionRecord latestExecutionOrNull(IncidentEvent event) {
        var list = eventStore.executionsOf(event.getEventId());
        return list.isEmpty() ? null : list.get(list.size() - 1);
    }

    /** 触发临界区的返回载体：要么是拒绝结果，要么是已占位的执行上下文。 */
    private static final class TriggerStart {
        private TriggerResult rejected;
        private ExecutionRecord execution;
        private RemediationPolicy policy;
        private RemediationAction action;

        static TriggerStart reject(TriggerResult result) {
            TriggerStart s = new TriggerStart();
            s.rejected = result;
            return s;
        }

        static TriggerStart proceed(
                String executionId, ExecutionRecord rec, RemediationPolicy policy,
                RemediationAction action) {
            TriggerStart s = new TriggerStart();
            s.execution = rec;
            s.policy = policy;
            s.action = action;
            return s;
        }

        /** 动作未注册时已在锁内同步终结执行，无锁外循环要跑。 */
        static TriggerStart proceed(String ignoredExecutionId) {
            return new TriggerStart();
        }
    }

    private static final class RetryDecision {
        private Instant retryAt;

        static RetryDecision stop() {
            return new RetryDecision();
        }

        static RetryDecision retry(Instant at) {
            RetryDecision d = new RetryDecision();
            d.retryAt = at;
            return d;
        }
    }
}
