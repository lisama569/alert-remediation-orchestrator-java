package com.github.lisama569.alertremediationorchestratorjava.engine;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionRegistry;
import com.github.lisama569.alertremediationorchestratorjava.clock.TimeService;
import com.github.lisama569.alertremediationorchestratorjava.model.Alert;
import com.github.lisama569.alertremediationorchestratorjava.model.EventState;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.model.IngestionResult;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import com.github.lisama569.alertremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.model.TriggerResult;
import com.github.lisama569.alertremediationorchestratorjava.store.AuditLog;
import com.github.lisama569.alertremediationorchestratorjava.store.EventStore;
import com.github.lisama569.alertremediationorchestratorjava.store.PolicyStore;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 告警接入：
 * 1) 按 alertId 拦截完全重复投递（同一条告警重放不重复计数）；
 * 2) 按 fingerprint 去重、correlationKey 关联，归并为单一事件（EventStore 串行保证并发唯一）；
 * 3) 抑制判定：已闭环 / 执行中 / 维护窗口延迟 / 冻结拒绝 / 待审批 / 无策略 / 策略停用，
 *    每种情况给出可区分的 {@link IngestionOutcome} 与原因；
 * 4) 归并后在同一把事件锁内走 RemediationService.trigger，保证“接入触发”与“人工/定时触发”
 *    共用同一套互斥与门禁语义。
 *
 * 策略快照：新事件首次命中策略时固化 policyId+version，之后策略调整只影响新事件，
 * 历史事件重放其既有结论（见 PolicyStore / README）。
 */
@Service
public class AlertIngestionService {

    private static final Logger log = LoggerFactory.getLogger(AlertIngestionService.class);

    private final EventStore eventStore;
    private final PolicyStore policyStore;
    private final AuditLog auditLog;
    private final TimeService clock;
    private final RemediationService remediationService;
    private final ActionRegistry actionRegistry;
    private final ConcurrentMap<String, Boolean> seenAlertIds = new ConcurrentHashMap<>();

    public AlertIngestionService(
            EventStore eventStore,
            PolicyStore policyStore,
            AuditLog auditLog,
            TimeService clock,
            RemediationService remediationService,
            ActionRegistry actionRegistry) {
        this.eventStore = eventStore;
        this.policyStore = policyStore;
        this.auditLog = auditLog;
        this.clock = clock;
        this.remediationService = remediationService;
        this.actionRegistry = actionRegistry;
    }

    public IngestionResult ingest(Alert alert) {
        Instant now = clock.now();
        log.info("[ingest] input alertId={} type={} severity={} fingerprint={} correlation={} target={}",
                alert.alertId(), alert.alertType(), alert.severity(),
                alert.fingerprint(), alert.correlationKey(), alert.target());

        if (seenAlertIds.putIfAbsent(alert.alertId(), Boolean.TRUE) != null) {
            IncidentEvent maybe = eventStore.findByFingerprint(alert.fingerprint());
            String eventId = maybe == null ? null : maybe.getEventId();
            auditLog.append(eventId, "INGEST",
                    "input(alertId=" + alert.alertId() + ") -> duplicate delivery, ignored (no count)");
            return new IngestionResult(eventId, IngestionOutcome.MERGED,
                    "告警重复投递，已忽略且不重复计数", null);
        }

        // 先在“当前最新”策略上做匹配（历史事件则使用其固化版本，与本次匹配无关）。
        RemediationPolicy matched = policyStore.match(alert.alertType(), alert.severity());
        if (matched == null) {
            // 无匹配策略：按需求不建事件、不触发处置，仅留可区分拒绝审计。
            auditLog.append(null, "INGEST",
                    "input(alertId=" + alert.alertId() + ", type=" + alert.alertType()
                            + ", severity=" + alert.severity()
                            + ") -> NO_POLICY: no enabled matching policy, suppressed") ;
            return new IngestionResult(null, IngestionOutcome.REJECT_NO_POLICY,
                    "无匹配处置策略，告警被抑制不触发处置", null);
        }
        if (!matched.autoRemediationEnabled()) {
            auditLog.append(null, "INGEST",
                    "input(alertId=" + alert.alertId() + ") matched " + matched.policyId()
                            + "@v" + matched.version() + " -> POLICY_DISABLED");
            return new IngestionResult(null, IngestionOutcome.REJECT_POLICY_DISABLED,
                    "匹配策略 " + matched.policyId() + " 未启用自动处置", matched.policyId());
        }

        IncidentEvent prototype = newEventFrom(alert, now);
        // 事件创建与策略版本固化必须在同一临界区：并发归并者读到的一定是已固化策略的完整事件。
        IncidentEvent event = eventStore.createOrGet(prototype, e -> {
            e.setMatchedPolicyId(matched.policyId());
            e.setPolicyVersion(matched.version());
            auditLog.append(e.getEventId(), "INGEST",
                    "input(alertId=" + alert.alertId() + ", fingerprint=" + alert.fingerprint()
                            + ") -> NEW event, matched policy " + matched.policyId()
                            + "@v" + matched.version() + " (pinned)");
        });
        boolean newlyCreated = event == prototype;

        String eventId = event.getEventId();
        // 临界区：只做归并记账并快照当前状态；不在锁内触发处置，
        // 避免“持事件锁 -> 动作执行（可能被测试屏障阻塞）-> 其它线程永远拿不到锁”。
        EventState stateAtMerge = eventStore.mutateWithLock(eventId, e -> {
            boolean firstAlert = e.getAlertIds().isEmpty();
            if (!firstAlert && !e.getAlertIds().contains(alert.alertId())) {
                e.getAlertIds().add(alert.alertId());
                e.setAlertCount(e.getAlertCount() + 1);
                if (alert.severity().ordinal() > e.getMaxSeverity().ordinal()) {
                    e.setMaxSeverity(alert.severity());
                }
                e.setUpdatedAt(now);
                auditLog.append(eventId, "MERGE",
                        "input(alertId=" + alert.alertId() + ", correlation="
                                + alert.correlationKey() + ") merged into event; alertCount="
                                + e.getAlertCount());
            } else if (firstAlert) {
                e.getAlertIds().add(alert.alertId());
                e.setAlertCount(1);
                e.setUpdatedAt(now);
            }
            return e.getState();
        });

        String policyId = event.getMatchedPolicyId();
        if (stateAtMerge.isTerminal()) {
            return new IngestionResult(eventId, IngestionOutcome.REJECT_ALREADY_CLOSED,
                    "事件已闭环 state=" + stateAtMerge + "，告警归并但不触发处置", policyId);
        }

        // 锁外触发处置；RemediationService.trigger 内部以同一把事件锁做互斥占位，
        // 因此并发接入/人工触发/定时唤醒之间仍然不会重复执行、重复计数。
        TriggerResult trigger =
                remediationService.trigger(eventId, "alert:" + alert.alertId());
        return mapTrigger(trigger, newlyCreated, policyId);
    }

    private IngestionResult mapTrigger(
            TriggerResult trigger, boolean newlyCreated, String policyId) {
        return switch (trigger.outcome()) {
            case EXECUTION_STARTED -> newlyCreated
                    ? new IngestionResult(trigger.eventId(), IngestionOutcome.ACCEPTED_NEW,
                            "新事件已创建，处置已开始执行 execution=" + trigger.executionId(), policyId)
                    : new IngestionResult(trigger.eventId(), IngestionOutcome.MERGED,
                            "关联/重复告警已归并到既有事件并（重新）触发处置 execution="
                                    + trigger.executionId(), policyId);
            case DEFERRED_MAINTENANCE_WINDOW -> new IngestionResult(
                    trigger.eventId(), IngestionOutcome.DEFERRED_MAINTENANCE_WINDOW,
                    trigger.reason(), policyId);
            case REJECT_CHANGE_FREEZE -> new IngestionResult(
                    trigger.eventId(), IngestionOutcome.REJECT_CHANGE_FREEZE,
                    trigger.reason(), policyId);
            case REJECT_APPROVAL_REQUIRED -> new IngestionResult(
                    trigger.eventId(), IngestionOutcome.REJECT_APPROVAL_REQUIRED,
                    trigger.reason(), policyId);
            case EXECUTION_IN_PROGRESS -> new IngestionResult(
                    trigger.eventId(), IngestionOutcome.REJECT_EXECUTION_IN_PROGRESS,
                    trigger.reason(), policyId);
            case ALREADY_CLOSED -> new IngestionResult(
                    trigger.eventId(), IngestionOutcome.REJECT_ALREADY_CLOSED,
                    trigger.reason(), policyId);
            case NO_POLICY -> new IngestionResult(
                    trigger.eventId(), IngestionOutcome.REJECT_NO_POLICY,
                    trigger.reason(), policyId);
            case EVENT_NOT_FOUND -> throw new IllegalStateException("event vanished after merge");
        };
    }

    private IncidentEvent newEventFrom(Alert alert, Instant now) {
        IncidentEvent event = new IncidentEvent();
        event.setEventId("evt-" + UUID.randomUUID());
        event.setFingerprint(alert.fingerprint());
        event.setCorrelationKey(alert.correlationKey());
        event.setAlertType(alert.alertType());
        event.setMaxSeverity(alert.severity());
        event.setTarget(alert.target());
        event.setState(EventState.OPEN);
        event.setAlertCount(0);
        event.setCreatedAt(now);
        event.setUpdatedAt(now);
        return event;
    }

    /** 测试重置。 */
    public void clearSeenAlerts() {
        seenAlertIds.clear();
    }
}
