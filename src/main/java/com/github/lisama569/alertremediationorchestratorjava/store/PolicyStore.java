package com.github.lisama569.alertremediationorchestratorjava.store;

import com.github.lisama569.alertremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.model.Severity;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/**
 * 进程内策略存储，保留每个 policyId 的全部历史版本。
 *
 * 历史可复现语义：事件在首次命中时固化 (matchedPolicyId, policyVersion)；
 * 之后策略被调整（新版本）不影响该事件后续重试用的策略——编排层始终用
 * getVersion(事件固化的版本) 取快照，因此同一历史事件的结论与判定依据保持不变。
 */
@Component
public class PolicyStore {

    private final ConcurrentMap<String, List<RemediationPolicy>> byId = new ConcurrentHashMap<>();

    /**
     * 写入策略：version 由 store 统一分配（同 policyId 递增），
     * 调用方传入的 version 字段被忽略，避免乱序/覆盖。
     */
    public synchronized RemediationPolicy put(RemediationPolicy input) {
        List<RemediationPolicy> versions =
                byId.computeIfAbsent(input.policyId(), k -> new java.util.ArrayList<>());
        int version = versions.isEmpty() ? 1 : versions.get(versions.size() - 1).version() + 1;
        RemediationPolicy stored = new RemediationPolicy(
                input.policyId(),
                version,
                input.alertType(),
                input.minSeverity(),
                input.actionId(),
                input.riskLevel(),
                input.autoRemediationEnabled(),
                input.maxAttempts(),
                input.timeoutMillis());
        versions.add(stored);
        return stored;
    }

    public RemediationPolicy latest(String policyId) {
        List<RemediationPolicy> versions = byId.get(policyId);
        if (versions == null || versions.isEmpty()) {
            return null;
        }
        synchronized (versions) {
            return versions.get(versions.size() - 1);
        }
    }

    public RemediationPolicy getVersion(String policyId, int version) {
        List<RemediationPolicy> versions = byId.get(policyId);
        if (versions == null) {
            throw new NoSuchElementException("policy not found: " + policyId);
        }
        synchronized (versions) {
            return versions.stream()
                    .filter(p -> p.version() == version)
                    .findFirst()
                    .orElseThrow(() -> new NoSuchElementException(
                            "policy version not found: " + policyId + "@v" + version));
        }
    }

    public List<RemediationPolicy> versions(String policyId) {
        List<RemediationPolicy> versions = byId.get(policyId);
        return versions == null ? List.of() : List.copyOf(versions);
    }

    /** 取所有最新版本策略（按 policyId 排序，匹配顺序确定）。 */
    public List<RemediationPolicy> allLatest() {
        return byId.keySet().stream()
                .sorted()
                .map(this::latest)
                .toList();
    }

    /**
     * 匹配规则（取“当前最新”版本，仅供新接入事件使用）：
     * alertType 完全相等且告警严重级别 >= 策略最低级别；
     * 多个匹配时取 minSeverity 最高（更具体）、再按 policyId 字典序，保证确定性。
     */
    public RemediationPolicy match(String alertType, Severity severity) {
        return byId.values().stream()
                .map(list -> {
                    synchronized (list) {
                        return list.get(list.size() - 1);
                    }
                })
                .filter(p -> p.alertType().equals(alertType)
                        && severity.ordinal() >= p.minSeverity().ordinal())
                .min(Comparator.comparingInt((RemediationPolicy p) -> p.minSeverity().ordinal())
                        .reversed()
                        .thenComparing(RemediationPolicy::policyId))
                .orElse(null);
    }

    /** 仅供测试/重置使用。 */
    public void clear() {
        byId.clear();
    }

    Map<String, List<RemediationPolicy>> rawView() {
        return byId;
    }
}
