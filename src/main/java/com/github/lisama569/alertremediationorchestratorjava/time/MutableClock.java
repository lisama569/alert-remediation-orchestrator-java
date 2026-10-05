package com.github.lisama569.alertremediationorchestratorjava.time;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 可控时钟：测试通过 advance/set 推进时间，不依赖真实等待。
 * 时间推进后通知 tick 监听器（在时钟监视器之外回调，避免监听器重入造成死锁），
 * 用于维护窗口延迟事件的自动恢复。
 */
public class MutableClock {

    private Instant now;
    private final List<Consumer<Instant>> tickListeners = new CopyOnWriteArrayList<>();

    public MutableClock(Instant start) {
        this.now = start;
    }

    /** 注册时间推进监听器；每次 set/advance 后以新时间回调一次。 */
    public void onTick(Consumer<Instant> listener) {
        tickListeners.add(listener);
    }

    public synchronized Instant now() {
        return now;
    }

    public void set(Instant instant) {
        Instant n;
        synchronized (this) {
            this.now = instant;
            n = instant;
        }
        fireTick(n);
    }

    public void advanceMillis(long millis) {
        Instant n;
        synchronized (this) {
            this.now = this.now.plusMillis(millis);
            n = this.now;
        }
        fireTick(n);
    }

    private void fireTick(Instant at) {
        for (Consumer<Instant> l : tickListeners) {
            l.accept(at);
        }
    }
}
