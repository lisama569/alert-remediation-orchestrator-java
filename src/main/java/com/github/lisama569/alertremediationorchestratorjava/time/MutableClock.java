package com.github.lisama569.alertremediationorchestratorjava.time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 可控时钟：测试通过 advance 推进时间，不依赖真实等待。
 * 支持时间推进监听器：监听回调在时钟锁之外触发，避免与事件锁形成反向加锁顺序。
 */
public class MutableClock {

    private static final Logger log = LoggerFactory.getLogger(MutableClock.class);

    private final List<Consumer<Instant>> listeners = new CopyOnWriteArrayList<>();
    private Instant now;

    public MutableClock(Instant start) {
        this.now = start;
    }

    public synchronized Instant now() {
        return now;
    }

    /** 注册时间推进监听器（set / advanceMillis 时触发，回调在锁外执行）。 */
    public void addListener(Consumer<Instant> listener) {
        listeners.add(listener);
    }

    public void set(Instant instant) {
        synchronized (this) {
            this.now = instant;
        }
        notifyListeners();
    }

    public void advanceMillis(long millis) {
        synchronized (this) {
            this.now = this.now.plusMillis(millis);
        }
        notifyListeners();
    }

    private void notifyListeners() {
        Instant t = now();
        for (Consumer<Instant> l : listeners) {
            try {
                l.accept(t);
            } catch (Exception e) {
                // 监听器异常不破坏时钟推进本身，记录后继续
                log.warn("clock listener failed: {}", e.toString());
            }
        }
    }
}
