package com.github.lisama569.alertremediationorchestratorjava.execution;

import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;

/**
 * 为事件解析处置动作。自动驱动链路（接入即处置、维护窗口恢复、审批后继续）
 * 通过它取得动作；默认/本地实现可以返回固定 no-op，生产实现可按事件上的策略快照选择动作，
 * 从而保证执行走的是事件受理当时那一版规则。
 */
@FunctionalInterface
public interface ActionResolver {
    RemediationAction resolve(AlertEvent event);
}
