package com.github.lisama569.alertremediationorchestratorjava.store;

import com.github.lisama569.alertremediationorchestratorjava.clock.TimeService;
import com.github.lisama569.alertremediationorchestratorjava.model.AuditEntry;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 顺序追加、不可变的审计日志：
 * 全局单调递增 seq 给出确定性顺序；每条记录包含阶段与判定依据（输入/规则/结果），
 * 是“一致且可解释结论”的追溯来源。
 */
@Component
public class AuditLog {

    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);

    private final CopyOnWriteArrayList<AuditEntry> entries = new CopyOnWriteArrayList<>();
    private final AtomicLong sequence = new AtomicLong();
    private final TimeService clock;

    public AuditLog(TimeService clock) {
        this.clock = clock;
    }

    public AuditEntry append(String eventId, String stage, String detail) {
        AuditEntry entry = new AuditEntry(sequence.incrementAndGet(), clock.now(), eventId, stage, detail);
        entries.add(entry);
        log.info("[audit] seq={} time={} event={} stage={} | {}",
                entry.seq(), entry.time(), eventId, stage, detail);
        return entry;
    }

    public List<AuditEntry> entriesOf(String eventId) {
        return entries.stream().filter(e -> eventId.equals(e.eventId())).toList();
    }

    public List<AuditEntry> all() {
        return List.copyOf(entries);
    }
}
