package com.github.lisama569.alertremediationorchestratorjava.time;

import java.time.Instant;

/**
 * 可控时钟：测试通过 advance 推进时间，不依赖真实等待。
 */
public class MutableClock {

    private Instant now;

    public MutableClock(Instant start) {
        this.now = start;
    }

    public synchronized Instant now() {
        return now;
    }

    public synchronized void set(Instant instant) {
        this.now = instant;
    }

    public synchronized void advanceMillis(long millis) {
        this.now = this.now.plusMillis(millis);
    }
}
