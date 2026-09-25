package com.github.lisama569.alertremediationorchestratorjava.engine;

import com.github.lisama569.alertremediationorchestratorjava.clock.TimeService;
import com.github.lisama569.alertremediationorchestratorjava.model.ChangeFreeze;
import com.github.lisama569.alertremediationorchestratorjava.model.GuardDecision;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import com.github.lisama569.alertremediationorchestratorjava.model.MaintenanceWindow;
import com.github.lisama569.alertremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.alertremediationorchestratorjava.model.RiskLevel;
import com.github.lisama569.alertremediationorchestratorjava.store.ConstraintStore;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 执行前置约束判定，固定优先级（保证结论确定、可解释）：
 * 1. 变更冻结：命中即拒绝（不延迟），多个命中取 freezeId 最小；
 * 2. 维护窗口：命中则延迟到“覆盖当前时刻的窗口中最晚的 endAt”之后；
 *    target 需匹配（"*" 或与事件 target 相等），多个命中取 endAt 最晚者；
 * 3. 高风险审批：riskLevel=HIGH 且事件未审批 -> 拒绝（等待人工 approve 后再触发）；
 * 4. 其余放行。
 *
 * 边界语义：窗口/冻结均为半开区间 [start, end)，恰好在 end 触发视为已结束。
 */
@Service
public class GuardService {

    private final TimeService clock;
    private final ConstraintStore constraintStore;

    public GuardService(TimeService clock, ConstraintStore constraintStore) {
        this.clock = clock;
        this.constraintStore = constraintStore;
    }

    public GuardDecision evaluate(IncidentEvent event, RemediationPolicy policy) {
        Instant now = clock.now();

        List<ChangeFreeze> hitFreezes = constraintStore.freezes().stream()
                .filter(f -> f.contains(now))
                .sorted(Comparator.comparing(ChangeFreeze::freezeId))
                .toList();
        if (!hitFreezes.isEmpty()) {
            ChangeFreeze freeze = hitFreezes.get(0);
            return GuardDecision.rejectFreeze(
                    freeze.freezeId(),
                    "change freeze active until " + freeze.endAt() + ": " + freeze.reason());
        }

        List<MaintenanceWindow> hitWindows = constraintStore.windows().stream()
                .filter(w -> targetMatches(w.target(), event.getTarget()))
                .filter(w -> w.contains(now))
                .sorted(Comparator.comparing(MaintenanceWindow::endAt)
                        .thenComparing(MaintenanceWindow::windowId))
                .toList();
        if (!hitWindows.isEmpty()) {
            MaintenanceWindow latest = hitWindows.get(hitWindows.size() - 1);
            return GuardDecision.defer(
                    latest.windowId(),
                    "maintenance window " + latest.windowId()
                            + " active until " + latest.endAt() + ": " + latest.reason(),
                    latest.endAt());
        }

        if (policy.riskLevel() == RiskLevel.HIGH && !event.isApproved()) {
            return GuardDecision.rejectUnapproved();
        }

        return GuardDecision.proceed();
    }

    private boolean targetMatches(String windowTarget, String eventTarget) {
        return "*".equals(windowTarget)
                || (windowTarget != null && windowTarget.equals(eventTarget));
    }
}
