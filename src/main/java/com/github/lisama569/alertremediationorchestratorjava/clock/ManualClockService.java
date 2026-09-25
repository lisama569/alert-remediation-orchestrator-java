package com.github.lisama569.alertremediationorchestratorjava.clock;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 进程内手动推进的时钟：
 * - now() 仅随 advance() 变化，默认起点固定，测试可构造时指定；
 * - schedule() 注册的到期任务只在 advance() 推进后、且不持时钟锁的情况下执行，
 *   避免与事件锁形成反向加环，同时保证维护窗口到期唤醒、超时判定、重试延迟全部可控、无真实等待；
 * - 同一时刻多个任务按注册顺序执行，保证结论确定可复现。
 */
@Component
public class ManualClockService implements TimeService {

    private static final Logger log = LoggerFactory.getLogger(ManualClockService.class);

    private static final Instant DEFAULT_START = Instant.parse("2026-09-25T00:00:00Z");

    private final Object lock = new Object();
    private final PriorityQueue<ScheduledTask> queue = new PriorityQueue<>();
    private long sequence;
    private Instant now;

    public ManualClockService() {
        this(DEFAULT_START);
    }

    public ManualClockService(Instant start) {
        this.now = start;
    }

    @Override
    public Instant now() {
        synchronized (lock) {
            return now;
        }
    }

    @Override
    public void advance(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("advance duration must be positive: " + duration);
        }
        Instant target;
        synchronized (lock) {
            target = now.plus(duration);
            now = target;
        }
        log.info("[clock] advance by {} -> {}", duration, target);
        // 反复抽干到期任务：任务执行中可能又注册了同一时刻/更早时刻的新任务（例如零延迟重试）。
        while (true) {
            List<ScheduledTask> due = new ArrayList<>();
            synchronized (lock) {
                while (!queue.isEmpty() && !queue.peek().due.isAfter(target)) {
                    due.add(queue.poll());
                }
            }
            if (due.isEmpty()) {
                return;
            }
            // 不持时钟锁执行任务，避免 eventLock -> clockLock 与 clockLock -> eventLock 的加锁环。
            for (ScheduledTask task : due) {
                try {
                    log.debug("[clock] run scheduled task due={} seq={}", task.due, task.seq);
                    task.action.run();
                } catch (RuntimeException e) {
                    log.error("[clock] scheduled task failed at {}: {}", target, e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public void schedule(Instant due, Runnable task) {
        if (due == null || task == null) {
            throw new IllegalArgumentException("due and task must not be null");
        }
        synchronized (lock) {
            long seq = sequence++;
            queue.add(new ScheduledTask(due, seq, task));
            log.debug("[clock] scheduled task due={} seq={}", due, seq);
        }
    }

    private static final class ScheduledTask implements Comparable<ScheduledTask> {
        private final Instant due;
        private final long seq;
        private final Runnable action;

        private ScheduledTask(Instant due, long seq, Runnable action) {
            this.due = due;
            this.seq = seq;
            this.action = action;
        }

        @Override
        public int compareTo(ScheduledTask o) {
            int c = this.due.compareTo(o.due);
            return c != 0 ? c : Long.compare(this.seq, o.seq);
        }
    }
}
