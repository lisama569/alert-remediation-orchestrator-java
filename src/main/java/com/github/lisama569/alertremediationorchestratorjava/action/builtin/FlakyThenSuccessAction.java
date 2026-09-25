package com.github.lisama569.alertremediationorchestratorjava.action.builtin;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 同一事件前 failTimes 次返回可重试失败（携带原因），之后成功。
 * 每事件独立计数，纯内存、无真实等待。
 *
 * <p>Spring 只暴露无参构造（默认前 2 次失败）；测试可通过带参构造自定义失败次数后，
 * 手动注册到 ActionRegistry（actionId 不变）。
 */
@Component
public class FlakyThenSuccessAction implements RemediationAction {

    private final int failTimes;
    private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

    @Autowired
    public FlakyThenSuccessAction() {
        this(2);
    }

    public FlakyThenSuccessAction(int failTimes) {
        this.failTimes = failTimes;
    }

    @Override
    public String actionId() {
        return "builtin.flaky";
    }

    @Override
    public ActionOutcome execute(String eventId) {
        int n = counters.computeIfAbsent(eventId, k -> new AtomicInteger()).incrementAndGet();
        if (n <= failTimes) {
            return ActionOutcome.retryable("transient failure #" + n + " (simulated), will retry");
        }
        return ActionOutcome.success();
    }
}
