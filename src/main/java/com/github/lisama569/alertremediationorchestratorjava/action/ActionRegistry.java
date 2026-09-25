package com.github.lisama569.alertremediationorchestratorjava.action;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** 按 actionId 注册与查找处置动作；注册可来自 Spring 注入或测试动态挂载（如计数/屏障动作）。 */
@Component
public class ActionRegistry {

    private final Map<String, RemediationAction> actions = new ConcurrentHashMap<>();

    public ActionRegistry() {
    }

    /** Spring 自动收集所有动作 Bean（没有动作 Bean 时也可启动）。 */
    @Autowired(required = false)
    public ActionRegistry(Collection<RemediationAction> actionBeans) {
        actionBeans.forEach(this::register);
    }

    public void register(RemediationAction action) {
        RemediationAction previous = actions.put(action.actionId(), action);
        if (previous != null) {
            throw new IllegalStateException("duplicate actionId: " + action.actionId());
        }
    }

    public RemediationAction get(String actionId) {
        return actions.get(actionId);
    }

    public Collection<RemediationAction> all() {
        return List.copyOf(actions.values());
    }

    public void clear() {
        actions.clear();
    }
}
