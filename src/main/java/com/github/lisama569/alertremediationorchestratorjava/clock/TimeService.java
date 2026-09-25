package com.github.lisama569.alertremediationorchestratorjava.clock;

import java.time.Duration;
import java.time.Instant;

/**
 * 可被测试控制的时间源：业务代码只能通过该接口读取时间，
 * 时间推进时触发已到期的调度任务，避免任何真实等待。
 */
public interface TimeService {

    Instant now();

    void advance(Duration duration);

    /** 在指定时刻执行任务（由时间推进驱动，不依赖真实线程调度）。 */
    void schedule(Instant due, Runnable task);
}
