package com.github.lisama569.alertremediationorchestratorjava.tests.support;

import com.github.lisama569.alertremediationorchestratorjava.action.ActionOutcome;
import com.github.lisama569.alertremediationorchestratorjava.action.RemediationAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并发测试用动作：
 * - 统计每个事件 / 全局的真实执行次数（并发安全）；
 * - 可选屏障：第一个进入的线程停在屏障前，其他线程尝试进入即可验证“同一事件互斥”；
 * - 结果可预设（成功 / 可重试 / 不可重试）。
 */
public class RecordingAction implements RemediationAction {

    private final String actionId;
    private final AtomicInteger globalCalls = new AtomicInteger();
    private final Map<String, AtomicInteger> perEvent = new ConcurrentHashMap<>();
    private final List<String> callOrder = new CopyOnWriteArrayList<>();
    private volatile ActionOutcome fixedOutcome = ActionOutcome.success();
    private volatile CountDownLatch entered;
    private volatile CountDownLatch release;

    public RecordingAction(String actionId) {
        this.actionId = actionId;
    }

    public RecordingAction succeed() {
        this.fixedOutcome = ActionOutcome.success();
        return this;
    }

    public RecordingAction retryable(String message) {
        this.fixedOutcome = ActionOutcome.retryable(message);
        return this;
    }

    public RecordingAction nonRetryable(String message) {
        this.fixedOutcome = ActionOutcome.nonRetryable(message);
        return this;
    }

    /** 安装单槽屏障：首个调用者 await release，用于制造“执行中”的真实并发窗口。 */
    public void installSingleBarrier() {
        this.entered = new CountDownLatch(1);
        this.release = new CountDownLatch(1);
    }

    public CountDownLatch enteredLatch() {
        return entered;
    }

    public void releaseBarrier() {
        if (release != null) {
            release.countDown();
        }
    }

    @Override
    public String actionId() {
        return actionId;
    }

    @Override
    public ActionOutcome execute(String eventId) {
        globalCalls.incrementAndGet();
        perEvent.computeIfAbsent(eventId, k -> new AtomicInteger()).incrementAndGet();
        callOrder.add(eventId + "#" + Thread.currentThread().getName());
        if (release != null) {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return fixedOutcome;
    }

    public int globalCallCount() {
        return globalCalls.get();
    }

    public int callsOf(String eventId) {
        AtomicInteger c = perEvent.get(eventId);
        return c == null ? 0 : c.get();
    }

    public List<String> callOrder() {
        return List.copyOf(callOrder);
    }
}
