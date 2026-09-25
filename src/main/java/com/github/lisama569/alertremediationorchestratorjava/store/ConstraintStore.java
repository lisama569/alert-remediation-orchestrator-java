package com.github.lisama569.alertremediationorchestratorjava.store;

import com.github.lisama569.alertremediationorchestratorjava.model.ChangeFreeze;
import com.github.lisama569.alertremediationorchestratorjava.model.MaintenanceWindow;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;

/** 进程内维护窗口与变更冻结存储（追加式，本地模拟）。 */
@Component
public class ConstraintStore {

    private final CopyOnWriteArrayList<MaintenanceWindow> windows = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ChangeFreeze> freezes = new CopyOnWriteArrayList<>();

    public void addWindow(MaintenanceWindow window) {
        windows.add(window);
    }

    public void addFreeze(ChangeFreeze freeze) {
        freezes.add(freeze);
    }

    public List<MaintenanceWindow> windows() {
        return List.copyOf(windows);
    }

    public List<ChangeFreeze> freezes() {
        return List.copyOf(freezes);
    }

    public void clear() {
        windows.clear();
        freezes.clear();
    }
}
