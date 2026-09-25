package com.github.lisama569.alertremediationorchestratorjava.policy;

import java.time.Duration;
import java.util.Map;

/**
 * 处置策略：按标签匹配告警，决定是否抑制、是否高风险（需审批）、重试与超时配置。
 */
public record RemediationPolicy(
        String policyId,
        Map<String, String> matchLabels,
        boolean suppressed,
        boolean highRisk,
        int maxRetries,
        Duration timeout) {

    /** 全量标签匹配：matchLabels 中每个键值都需出现在告警标签中。 */
    public boolean matches(Map<String, String> labels) {
        if (labels == null) {
            return matchLabels.isEmpty();
        }
        return matchLabels.entrySet().stream()
                .allMatch(e -> e.getValue().equals(labels.get(e.getKey())));
    }
}
