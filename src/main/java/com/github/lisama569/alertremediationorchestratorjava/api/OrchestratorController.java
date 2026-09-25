package com.github.lisama569.alertremediationorchestratorjava.api;

import com.github.lisama569.alertremediationorchestratorjava.clock.TimeService;
import com.github.lisama569.alertremediationorchestratorjava.engine.AlertIngestionService;
import com.github.lisama569.alertremediationorchestratorjava.engine.RemediationService;
import com.github.lisama569.alertremediationorchestratorjava.model.Alert;
import com.github.lisama569.alertremediationorchestratorjava.model.ChangeFreeze;
import com.github.lisama569.alertremediationorchestratorjava.model.MaintenanceWindow;
import com.github.lisama569.alertremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.store.AuditLog;
import com.github.lisama569.alertremediationorchestratorjava.store.ConstraintStore;
import com.github.lisama569.alertremediationorchestratorjava.store.EventStore;
import com.github.lisama569.alertremediationorchestratorjava.store.PolicyStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 本地手工验证入口：告警、事件查询/触发/审批/闭环、策略与约束配置、时钟推进。 */
@RestController
@RequestMapping("/api")
public class OrchestratorController {

    private final AlertIngestionService ingestionService;
    private final RemediationService remediationService;
    private final EventStore eventStore;
    private final PolicyStore policyStore;
    private final ConstraintStore constraintStore;
    private final AuditLog auditLog;
    private final TimeService clock;

    public OrchestratorController(
            AlertIngestionService ingestionService,
            RemediationService remediationService,
            EventStore eventStore,
            PolicyStore policyStore,
            ConstraintStore constraintStore,
            AuditLog auditLog,
            TimeService clock) {
        this.ingestionService = ingestionService;
        this.remediationService = remediationService;
        this.eventStore = eventStore;
        this.policyStore = policyStore;
        this.constraintStore = constraintStore;
        this.auditLog = auditLog;
        this.clock = clock;
    }

    @PostMapping("/alerts")
    public Object ingest(@RequestBody Alert alert) {
        return ingestionService.ingest(alert);
    }

    @PostMapping("/events/{eventId}/trigger")
    public Object trigger(
            @PathVariable String eventId,
            @RequestParam(defaultValue = "manual") String source) {
        return remediationService.trigger(eventId, source);
    }

    @PostMapping("/events/{eventId}/approval")
    public Object approve(
            @PathVariable String eventId,
            @RequestParam String approver,
            @RequestParam boolean approved,
            @RequestParam(required = false) String comment) {
        return remediationService.approve(eventId, approver, approved, comment);
    }

    @PostMapping("/events/{eventId}/close")
    public Object close(
            @PathVariable String eventId,
            @RequestParam String operator,
            @RequestParam(required = false) String note) {
        return remediationService.manualClose(eventId, operator, note);
    }

    @GetMapping("/events/{eventId}")
    public Object getEvent(@PathVariable String eventId) {
        var event = eventStore.findById(eventId);
        if (event == null) {
            return Map.of("error", "event not found", "eventId", eventId);
        }
        return eventView(event);
    }

    @GetMapping("/events")
    public Object listEvents() {
        return eventStore.findAll().stream().map(this::eventView).toList();
    }

    @GetMapping("/events/{eventId}/executions")
    public Object executions(@PathVariable String eventId) {
        return eventStore.executionsOf(eventId);
    }

    @GetMapping("/events/{eventId}/audit")
    public Object audit(@PathVariable String eventId) {
        return auditLog.entriesOf(eventId);
    }

    @PostMapping("/policies")
    public Object putPolicy(@RequestBody RemediationPolicy policy) {
        return Map.of("stored", policyStore.put(policy));
    }

    @GetMapping("/policies")
    public Object listPolicies() {
        return policyStore.allLatest();
    }

    @PostMapping("/constraints/windows")
    public Object addWindow(@RequestBody MaintenanceWindow window) {
        constraintStore.addWindow(window);
        return Map.of("added", window);
    }

    @PostMapping("/constraints/freezes")
    public Object addFreeze(@RequestBody ChangeFreeze freeze) {
        constraintStore.addFreeze(freeze);
        return Map.of("added", freeze);
    }

    @PostMapping("/clock/advance")
    public Object advanceClock(@RequestParam long seconds) {
        clock.advance(Duration.ofSeconds(seconds));
        return Map.of("now", clock.now().toString());
    }

    private Map<String, Object> eventView(com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", e.getEventId());
        m.put("fingerprint", e.getFingerprint());
        m.put("correlationKey", e.getCorrelationKey());
        m.put("alertType", e.getAlertType());
        m.put("maxSeverity", e.getMaxSeverity());
        m.put("target", e.getTarget());
        m.put("state", e.getState());
        m.put("conclusion", e.getConclusion());
        m.put("matchedPolicyId", e.getMatchedPolicyId());
        m.put("policyVersion", e.getPolicyVersion());
        m.put("alertCount", e.getAlertCount());
        m.put("alertIds", e.getAlertIds());
        m.put("executionCount", e.getExecutionCount());
        m.put("approved", e.isApproved());
        m.put("approver", e.getApprover());
        m.put("deferredUntil", e.getDeferredUntil());
        m.put("createdAt", e.getCreatedAt());
        m.put("updatedAt", e.getUpdatedAt());
        return m;
    }
}
