package com.github.lisama569.alertremediationorchestratorjava.tests.support;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionRegistry;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import com.github.lisama569.alertremediationorchestratorjava.clock.ManualClockService;
import com.github.lisama569.alertremediationorchestratorjava.clock.TimeService;
import com.github.lisama569.alertremediationorchestratorjava.engine.AlertIngestionService;
import com.github.lisama569.alertremediationorchestratorjava.engine.GuardService;
import com.github.lisama569.alertremediationorchestratorjava.engine.RemediationService;
import com.github.lisama569.alertremediationorchestratorjava.model.Alert;
import com.github.lisama569.alertremediationorchestratorjava.model.ChangeFreeze;
import com.github.lisama569.alertremediationorchestratorjava.model.MaintenanceWindow;
import com.github.lisama569.alertremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.model.RiskLevel;
import com.github.lisama569.alertremediationorchestratorjava.model.Severity;
import com.github.lisama569.alertremediationorchestratorjava.store.AuditLog;
import com.github.lisama569.alertremediationorchestratorjava.store.ConstraintStore;
import com.github.lisama569.alertremediationorchestratorjava.store.EventStore;
import com.github.lisama569.alertremediationorchestratorjava.store.PolicyStore;
import java.time.Instant;
import java.util.UUID;

/**
 * 纯手工装配的测试容器：不启动 Spring，全部组件进程内、内存态，
 * 时间由 {@link ManualClockService} 完全控制。
 */
public class Harness {

    public final ManualClockService clock;
    public final EventStore eventStore;
    public final PolicyStore policyStore;
    public final ConstraintStore constraintStore;
    public final AuditLog auditLog;
    public final ActionRegistry actionRegistry;
    public final GuardService guardService;
    public final RemediationService remediationService;
    public final AlertIngestionService ingestionService;

    public Harness() {
        this(new ManualClockService());
    }

    public Harness(ManualClockService clock) {
        this.clock = clock;
        this.eventStore = new EventStore();
        this.policyStore = new PolicyStore();
        this.constraintStore = new ConstraintStore();
        this.auditLog = new AuditLog(clock);
        this.actionRegistry = new ActionRegistry();
        this.guardService = new GuardService(clock, constraintStore);
        this.remediationService = new RemediationService(
                eventStore, policyStore, auditLog, clock, guardService, actionRegistry);
        this.ingestionService = new AlertIngestionService(
                eventStore, policyStore, auditLog, clock, remediationService, actionRegistry);
    }

    public void register(RemediationAction action) {
        actionRegistry.register(action);
    }

    public RemediationPolicy putPolicy(
            String policyId,
            String alertType,
            Severity minSeverity,
            String actionId,
            RiskLevel risk,
            boolean enabled,
            int maxAttempts,
            long timeoutMillis) {
        // version 由 PolicyStore 分配，入参版本位填 0 仅为满足 record 形态。
        RemediationPolicy stored = policyStore.put(new RemediationPolicy(
                policyId, 0, alertType, minSeverity, actionId, risk,
                enabled, maxAttempts, timeoutMillis));
        return stored;
    }

    public MaintenanceWindow window(String id, String target, long startOffsetSec, long endOffsetSec) {
        Instant base = clock.now();
        return new MaintenanceWindow(
                id, target,
                base.plusSeconds(startOffsetSec), base.plusSeconds(endOffsetSec),
                "test window " + id);
    }

    public ChangeFreeze freeze(String id, long startOffsetSec, long endOffsetSec) {
        Instant base = clock.now();
        return new ChangeFreeze(
                id, base.plusSeconds(startOffsetSec), base.plusSeconds(endOffsetSec),
                "test freeze " + id);
    }

    public Alert alert(String alertId, String fingerprint, String correlation,
            String type, Severity severity, String target) {
        return new Alert(alertId, "monitor-" + alertId, type, severity,
                fingerprint, correlation, target, "simulated " + type, clock.now());
    }

    public Alert uniqueAlert(String type, Severity severity, String target) {
        String id = UUID.randomUUID().toString();
        return alert(id, "fp-" + id, "corr-" + id, type, severity, target);
    }
}
