package com.github.lisama569.alertremediationorchestratorjava;

import com.github.lisama569.alertremediationorchestratorjava.orchestrator.ApprovalService;
import com.github.lisama569.alertremediationorchestratorjava.orchestrator.EventService;
import com.github.lisama569.alertremediationorchestratorjava.orchestrator.OrchestratorService;
import com.github.lisama569.alertremediationorchestratorjava.orchestrator.RemediationExecutor;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;

/**
 * 编排服务 Bean 装配。时钟起点可通过 orchestrator.clock.start 配置。
 */
@Configuration
public class OrchestratorConfig {

    @Bean
    public MutableClock mutableClock(
            @Value("${orchestrator.clock.start:2026-01-01T00:00:00Z}") String start) {
        return new MutableClock(Instant.parse(start));
    }

    @Bean
    public EventService eventService(MutableClock clock) {
        return new EventService(clock);
    }

    @Bean
    public ApprovalService approvalService(MutableClock clock) {
        return new ApprovalService(clock);
    }

    @Bean
    public RemediationExecutor remediationExecutor(MutableClock clock) {
        return new RemediationExecutor(clock);
    }

    @Bean
    public OrchestratorService orchestratorService(EventService eventService,
                                                   ApprovalService approvalService,
                                                   RemediationExecutor executor,
                                                   MutableClock clock) {
        OrchestratorService service = new OrchestratorService(eventService, approvalService,
                executor, clock);
        // 本地验证默认动作为 no-op 成功；自动驱动与延迟自愈共用
        service.setDefaultActionFactory(
                key -> (k, attempt) -> com.github.lisama569.alertremediationorchestratorjava
                        .execution.ExecutionResult.ok("local no-op remediation"));
        // 时钟推进过维护窗口结束点时，延迟事件自动恢复，无需手动唤醒
        service.enableAutoResume();
        return service;
    }
}
