package com.github.lisama569.alertremediationorchestratorjava.orchestrator;

import com.github.lisama569.alertremediationorchestratorjava.domain.AlertEvent;
import com.github.lisama569.alertremediationorchestratorjava.domain.EventStatus;
import com.github.lisama569.alertremediationorchestratorjava.execution.ExecutionResult;
import com.github.lisama569.alertremediationorchestratorjava.execution.FailureType;
import com.github.lisama569.alertremediationorchestratorjava.execution.RemediationAction;
import com.github.lisama569.alertremediationorchestratorjava.time.MutableClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 处置执行器。
 * 并发安全：running 集合保证同一事件任一时刻只有一个执行，
 * 重复/并发触发直接返回 false，不重复执行、不重复计数。
 * 失败分类：TIMEOUT / RETRYABLE / NON_RETRYABLE，原因写入事件历史，不静默吞掉。
 * 时间测量基于 MutableClock，测试不依赖真实等待。
 */
public class RemediationExecutor {

    private static final Logger log = LoggerFactory.getLogger(RemediationExecutor.class);

    private final Set<String> running = ConcurrentHashMap.newKeySet();
    private final MutableClock clock;

    public RemediationExecutor(MutableClock clock) {
        this.clock = clock;
    }

    /**
     * 触发处置。返回 true 表示本次触发接管了执行（含重试）；
     * 返回 false 表示已有执行在进行中（重复/并发触发被去重）。
     */
    public boolean trigger(AlertEvent event, RemediationAction action,
                           int maxRetries, Duration timeout) {
        String key = event.getEventKey();
        synchronized (event) {
            EventStatus status = event.getStatus();
            if (status == EventStatus.RESOLVED || status == EventStatus.FAILED_FINAL
                    || status == EventStatus.CLOSED || status == EventStatus.REJECTED) {
                log.info("trigger skipped: eventKey={} already terminal status={}", key, status);
                return false;
            }
            if (!running.add(key)) {
                log.info("trigger deduplicated: eventKey={} already running", key);
                return false;
            }
        }
        try {
            event.transition(clock.now(), EventStatus.REMEDIATING, "remediation started");
            int attempt = 0;
            while (true) {
                attempt++;
                event.incrementExecution(clock.now(), attempt);
                Instant start = clock.now();
                ExecutionResult result = invoke(action, key, attempt);
                Duration elapsed = Duration.between(start, clock.now());
                if (result.success()) {
                    event.transition(clock.now(), EventStatus.RESOLVED,
                            "remediation succeeded: attempt=" + attempt + " - " + result.detail());
                    log.info("remediation succeeded: eventKey={} attempt={}", key, attempt);
                    return true;
                }
                FailureType type = classify(result, elapsed, timeout);
                String detail = "attempt=" + attempt + " failureType=" + type
                        + " detail=" + result.detail();
                log.info("remediation failed: eventKey={} {}", key, detail);
                if (type == FailureType.NON_RETRYABLE) {
                    event.transition(clock.now(), EventStatus.FAILED_FINAL,
                            "non-retryable failure: " + detail);
                    return true;
                }
                if (attempt > maxRetries) {
                    event.transition(clock.now(), EventStatus.FAILED_FINAL,
                            "retries exhausted (maxRetries=" + maxRetries + "): " + detail);
                    return true;
                }
                // 可重试/超时失败：记录后继续重试，状态保持 REMEDIATING
                event.transition(clock.now(), EventStatus.FAILED_RETRYABLE, detail);
                event.transition(clock.now(), EventStatus.REMEDIATING,
                        "retrying: nextAttempt=" + (attempt + 1));
            }
        } finally {
            running.remove(key);
        }
    }

    private ExecutionResult invoke(RemediationAction action, String key, int attempt) {
        try {
            ExecutionResult r = action.run(key, attempt);
            return r != null ? r : ExecutionResult.fail(FailureType.NON_RETRYABLE,
                    "action returned null result");
        } catch (Exception e) {
            // 异常不静默吞掉：归类为不可重试失败并携带原因
            return ExecutionResult.fail(FailureType.NON_RETRYABLE,
                    "action threw " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private FailureType classify(ExecutionResult result, Duration elapsed, Duration timeout) {
        if (elapsed.compareTo(timeout) > 0) {
            return FailureType.TIMEOUT;
        }
        return result.failureType() == FailureType.NONE ? FailureType.RETRYABLE : result.failureType();
    }
}
