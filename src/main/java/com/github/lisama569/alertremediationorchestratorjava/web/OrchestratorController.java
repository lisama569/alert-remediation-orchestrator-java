package com.github.lisama569.alertremediationorchestratorjava.web;

import com.github.lisama569.alertremediationorchestratorjava.domain.Alert;
import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.IngestResult;
import com.github.lisama569.alertremediationorchestratorjava.execution.ExecutionResult;
import com.github.lisama569.alertremediationorchestratorjava.orchestrator.ApprovalService;
import com.github.lisama569.alertremediationorchestratorjava.orchestrator.EventService;
import com.github.lisama569.alertremediationorchestratorjava.orchestrator.OrchestratorService;
import com.github.lisama569.alertremediationorchestratorjava.orchestrator.RemediationOutcome;
import com.github.lisama569.alertremediationorchestratorjava.policy.MaintenanceWindow;
import com.github.lisama569.alertremediationorchestratorjava.policy.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 本地验证用 REST 入口。
 */
@RestController
@RequestMapping("/api")
public class OrchestratorController {

    private final EventService eventService;
    private final OrchestratorService orchestrator;
    private final ApprovalService approvalService;
    private final MutableClock clock;

    public OrchestratorController(EventService eventService, OrchestratorService orchestrator,
                                  ApprovalService approvalService, MutableClock clock) {
        this.eventService = eventService;
        this.orchestrator = orchestrator;
        this.approvalService = approvalService;
        this.clock = clock;
    }

    public record AlertRequest(String alertId, String eventKey, String severity, String source,
                               Map<String, String> labels) {
    }

    public record PolicyRequest(String policyId, Map<String, String> matchLabels,
                                boolean suppressed, boolean highRisk,
                                int maxRetries, long timeoutSeconds) {
    }

    public record WindowRequest(String name, String start, String end, String kind) {
    }

    @PostMapping("/alerts")
    public IngestResult ingest(@RequestBody AlertRequest req) {
        return eventService.ingest(new Alert(req.alertId(), req.eventKey(), req.severity(),
                req.source(), req.labels() == null ? Map.of() : req.labels(), clock.now()));
    }

    @PostMapping("/events/{eventKey}/remediate")
    public RemediationOutcome remediate(@PathVariable String eventKey) {
        return orchestrator.remediate(eventKey,
                (key, attempt) -> ExecutionResult.ok("local no-op remediation"));
    }

    @PostMapping("/events/{eventKey}/approve")
    public boolean approve(@PathVariable String eventKey, @RequestParam String approver) {
        return approvalService.approve(eventKey, approver);
    }

    @PostMapping("/events/{eventKey}/reject-approval")
    public boolean rejectApproval(@PathVariable String eventKey, @RequestParam String approver) {
        return approvalService.reject(eventKey, approver);
    }

    @PostMapping("/events/{eventKey}/close")
    public boolean close(@PathVariable String eventKey, @RequestParam String operator) {
        return orchestrator.close(eventKey, operator);
    }

    @PostMapping("/delayed/process")
    public int processDelayed() {
        return orchestrator.processDelayed(
                key -> (k, attempt) -> ExecutionResult.ok("local no-op remediation"));
    }

    @GetMapping("/events")
    public List<AlertEvent> listEvents() {
        return eventService.listEvents();
    }

    @GetMapping("/events/{eventKey}")
    public AlertEvent getEvent(@PathVariable String eventKey) {
        return eventService.getEvent(eventKey);
    }

    @PostMapping("/policies")
    public void setPolicies(@RequestBody List<PolicyRequest> reqs) {
        eventService.setPolicies(reqs.stream()
                .map(r -> new RemediationPolicy(r.policyId(), r.matchLabels(), r.suppressed(),
                        r.highRisk(), r.maxRetries(), Duration.ofSeconds(r.timeoutSeconds())))
                .toList());
    }

    @PostMapping("/windows")
    public void setWindows(@RequestBody List<WindowRequest> reqs) {
        orchestrator.setWindows(reqs.stream()
                .map(r -> new MaintenanceWindow(r.name(), Instant.parse(r.start()),
                        Instant.parse(r.end()), MaintenanceWindow.Kind.valueOf(r.kind())))
                .toList());
    }

    @PostMapping("/clock/advance")
    public Instant advanceClock(@RequestParam long millis) {
        clock.advanceMillis(millis);
        return clock.now();
    }

    @GetMapping("/clock")
    public Instant now() {
        return clock.now();
    }
}
