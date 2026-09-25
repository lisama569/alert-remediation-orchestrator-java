package com.github.lisama569.alertremediationorchestratorjava.model;

/**
 * 处置策略（不可变值对象，带版本号）。
 * 策略每次调整生成新版本；事件固化其判定时使用的 policyVersion，保证历史结论可复现。
 */
public record RemediationPolicy(
        String policyId,
        int version,
        String alertType,
        Severity minSeverity,
        String actionId,
        RiskLevel riskLevel,
        boolean autoRemediationEnabled,
        int maxAttempts,
        long timeoutMillis) {
}
