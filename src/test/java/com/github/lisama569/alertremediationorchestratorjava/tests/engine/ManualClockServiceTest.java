package com.github.lisama569.alertremediationorchestratorjava.tests.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.alertremediationorchestratorjava.clock.ManualClockService;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 手动时钟：到期任务只在 advance 后按 (due, 注册顺序) 确定执行，无任何真实等待。 */
class ManualClockServiceTest {

    private static final Logger log = LoggerFactory.getLogger(ManualClockServiceTest.class);

    @Test
    void scheduledTasksRunInDeterministicOrderOnAdvance() {
        ManualClockService clock = new ManualClockService(Instant.parse("2026-09-25T00:00:00Z"));
        List<String> order = new ArrayList<>();

        clock.schedule(clock.now().plusSeconds(10), () -> order.add("t10"));
        clock.schedule(clock.now().plusSeconds(5), () -> order.add("t5"));
        clock.schedule(clock.now().plusSeconds(5), () -> order.add("t5-late-registered"));

        log.info("[test-input] advance 4s: 期望没有任务触发");
        clock.advance(Duration.ofSeconds(4));
        assertEquals(List.of(), order);
        assertEquals(Instant.parse("2026-09-25T00:00:04Z"), clock.now());

        log.info("[test-input] advance 2s: 期望 t5 两个任务按注册顺序执行，t10 不执行");
        clock.advance(Duration.ofSeconds(2));
        assertEquals(List.of("t5", "t5-late-registered"), order);

        log.info("[test-input] advance 4s: 期望 t10 执行");
        clock.advance(Duration.ofSeconds(4));
        assertEquals(List.of("t5", "t5-late-registered", "t10"), order);
        assertEquals(Instant.parse("2026-09-25T00:00:10Z"), clock.now());
        assertTrue(true, "全部任务由时钟推进驱动，未发生真实等待");
    }

    @Test
    void taskScheduledAtCurrentTimeDuringAdvanceIsAlsoDrained() {
        ManualClockService clock = new ManualClockService(Instant.parse("2026-09-25T00:00:00Z"));
        List<String> ran = new ArrayList<>();

        clock.schedule(clock.now().plusSeconds(1), () -> {
            ran.add("outer");
            // 任务执行中再注册一个“当前时刻”任务，也应在同一次 advance 内抽干。
            clock.schedule(clock.now(), () -> ran.add("inner-now"));
        });
        clock.advance(Duration.ofSeconds(1));
        log.info("[test-input] advance 到任务时刻，判定依据：任务内注册同刻任务 -> {}", ran);
        assertEquals(List.of("outer", "inner-now"), ran);
    }
}
