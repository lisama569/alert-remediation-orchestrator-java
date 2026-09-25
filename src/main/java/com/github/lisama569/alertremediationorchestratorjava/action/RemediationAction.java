package com.github.lisama569.alertremediationorchestratorjava.action;

/**
 * 具体处置动作（如重启服务、清理磁盘）。
 * 实现须为纯模拟、无真实等待、线程安全；失败必须携带可解释原因。
 */
public interface RemediationAction {

    String actionId();

    ActionOutcome execute(String eventId);
}
