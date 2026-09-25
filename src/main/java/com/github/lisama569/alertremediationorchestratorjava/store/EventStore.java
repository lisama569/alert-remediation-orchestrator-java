package com.github.lisama569.alertremediationorchestratorjava.store;

import com.github.lisama569.alertremediationorchestratorjava.model.ExecutionRecord;
import com.github.lisama569.alertremediationorchestratorjava.model.IncidentEvent;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 进程内事件存储，并发模型分两层：
 *
 * 1) createOrGet 用 this 串行化，内部完成 fingerprint -> correlationKey 的查重与创建，
 *    保证同一事件的重复/关联告警在并发接入时只产生一个 IncidentEvent；
 * 2) mutateWithLock 使用每事件分段锁（eventId -> lock），把同一事件的
 *    “读取状态 -> 判定 -> 状态流转/计数”做成临界区，保证任一时刻只有一个处置能进入执行、
 *    executionCount 不会重复计数，同时不同事件仍可完全并行。
 *
 * 注意：临界区内不得执行真实阻塞动作；动作执行放在编排层锁外进行，
 * 结果回写时再进入同一把锁，避免长持锁。
 */
@Component
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private final Map<String, IncidentEvent> byId = new ConcurrentHashMap<>();
    private final Map<String, String> fingerprintIndex = new ConcurrentHashMap<>();
    private final Map<String, String> correlationIndex = new ConcurrentHashMap<>();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    private final Map<String, List<ExecutionRecord>> executions = new ConcurrentHashMap<>();

    /**
     * 按 fingerprint 去重、再按 correlationKey 关联归并；都不存在则用传入的原型创建。
     * 整个判定-插入过程串行，杜绝并发下重复建事件。
     *
     * 新建事件的初始化（如固化命中策略版本）必须通过 initializer 在本临界区内完成，
     * 否则并发线程可能在“事件已建、策略尚未固化”的窗口里读到半成品事件。
     */
    public synchronized IncidentEvent createOrGet(
            IncidentEvent prototype, java.util.function.Consumer<IncidentEvent> initializer) {
        String existingId = fingerprintIndex.get(prototype.getFingerprint());
        if (existingId == null && prototype.getCorrelationKey() != null) {
            existingId = correlationIndex.get(prototype.getCorrelationKey());
        }
        if (existingId != null) {
            IncidentEvent existing = byId.get(existingId);
            if (existing != null) {
                return existing;
            }
        }
        initializer.accept(prototype);
        byId.put(prototype.getEventId(), prototype);
        fingerprintIndex.put(prototype.getFingerprint(), prototype.getEventId());
        if (prototype.getCorrelationKey() != null) {
            correlationIndex.putIfAbsent(prototype.getCorrelationKey(), prototype.getEventId());
        }
        executions.put(prototype.getEventId(), new java.util.concurrent.CopyOnWriteArrayList<>());
        log.debug("[event-store] created event {} fingerprint={} correlation={}",
                prototype.getEventId(), prototype.getFingerprint(), prototype.getCorrelationKey());
        return prototype;
    }

    public IncidentEvent findById(String eventId) {
        return byId.get(eventId);
    }

    public IncidentEvent findByFingerprint(String fingerprint) {
        String id = fingerprintIndex.get(fingerprint);
        return id == null ? null : byId.get(id);
    }

    public IncidentEvent findByCorrelationKey(String correlationKey) {
        if (correlationKey == null) {
            return null;
        }
        String id = correlationIndex.get(correlationKey);
        return id == null ? null : byId.get(id);
    }

    public List<IncidentEvent> findAll() {
        return byId.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toList();
    }

    /** 在该事件的分段锁内执行原子“读-判定-写”，返回回调结果。 */
    public <T> T mutateWithLock(String eventId, Function<IncidentEvent, T> mutation) {
        IncidentEvent event = byId.get(eventId);
        if (event == null) {
            throw new NoSuchElementException("event not found: " + eventId);
        }
        Object lock = locks.computeIfAbsent(eventId, k -> new Object());
        synchronized (lock) {
            return mutation.apply(event);
        }
    }

    /** 无额外初始化的便捷重载。 */
    public synchronized IncidentEvent createOrGet(IncidentEvent prototype) {
        return createOrGet(prototype, e -> { });
    }

    public void addExecution(String eventId, ExecutionRecord execution) {
        List<ExecutionRecord> list = executions.get(eventId);
        if (list == null) {
            throw new NoSuchElementException("event not found: " + eventId);
        }
        list.add(execution);
    }

    public List<ExecutionRecord> executionsOf(String eventId) {
        List<ExecutionRecord> list = executions.get(eventId);
        return list == null ? List.of() : List.copyOf(list);
    }

    public void clear() {
        byId.clear();
        fingerprintIndex.clear();
        correlationIndex.clear();
        locks.clear();
        executions.clear();
    }
}
