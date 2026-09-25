# 告警自动处置编排服务（本地可运行模拟版）

一套**进程内状态 + 可控时间推进**的告警自动处置编排服务，用于演示与验证：
同一事件在**重复投递、并发触发、人工干预**下，仍然得到**一致、确定、可解释、可复现**的结论。
不依赖数据库 / 消息队列 / 外部线程调度，全部状态在内存中，时间由手动时钟推进，**时间相关用例零真实等待**。

## 1. 快速开始

```bash
# 运行全部测试（18 个，含重复投递/并发/窗口边界/未授权/失败分类/策略版本）
mvn clean test

# 启动服务（默认随机端口；固定端口：SERVER_PORT=8080 mvn spring-boot:run）
mvn spring-boot:run
```

内置模拟处置动作（Spring 自动注册）：

| actionId | 行为 |
| --- | --- |
| `builtin.success` | 总是成功 |
| `builtin.flaky` | 同事件前 2 次可重试失败，之后成功 |
| `builtin.always-retryable` | 总是可重试失败（用于验证重试耗尽） |
| `builtin.non-retryable` | 立即不可重试失败（用于验证不重试） |

## 2. 本地验证步骤（curl）

```bash
BASE=http://localhost:8080

# 1) 写策略（version 由服务端分配，写入返回 1）
curl -s -XPOST $BASE/api/policies -H 'Content-Type: application/json' -d '{
  "policyId":"p.disk","version":0,"alertType":"disk.full","minSeverity":"WARNING",
  "actionId":"builtin.flaky","riskLevel":"LOW","autoRemediationEnabled":true,
  "maxAttempts":3,"timeoutMillis":5000}'

# 2) 接入告警 -> ACCEPTED_NEW，第 1 次可重试失败，事件回到 OPEN
curl -s -XPOST $BASE/api/alerts -H 'Content-Type: application/json' -d '{
  "alertId":"a1","source":"zabbix","alertType":"disk.full","severity":"CRITICAL",
  "fingerprint":"fp-1","correlationKey":"corr-1","target":"host-1",
  "summary":"disk full","occurredAt":"2026-09-25T00:00:00Z"}'

# 3) 推进时钟 1s 触发一次重试（再推进一次 -> 第 3 次成功），没有任何真实等待
curl -s -XPOST "$BASE/api/clock/advance?seconds=1"
curl -s -XPOST "$BASE/api/clock/advance?seconds=1"

# 4) 查事件 / 执行记录 / 审计（输入与判定依据均在审计中）
curl -s $BASE/api/events | head
curl -s $BASE/api/events/<eventId>/executions
curl -s $BASE/api/events/<eventId>/audit
```

其它入口：

- 维护窗口：`POST /api/constraints/windows`
- 变更冻结：`POST /api/constraints/freezes`
- 人工触发：`POST /api/events/{id}/trigger?source=manual`
- 人工审批：`POST /api/events/{id}/approval?approver=lead&approved=true&comment=CHG-1`
- 人工闭环：`POST /api/events/{id}/close?operator=sre&note=escalated`
- 推进时间：`POST /api/clock/advance?seconds=10`

## 3. 事件状态机与判定规则

### 3.1 事件状态（EventState）

```
OPEN ──触发(放行)──────────────► EXECUTING ──成功──────────────► RESOLVED（终态）
  ▲                                │
  │                                ├── 不可重试失败 ───────────► FAILED / FAILED_NON_RETRYABLE（终态）
  │                                ├── 超时或可重试失败(有余量)─► OPEN（调度 1s 后重试，仍属同一执行）
  │                                └── 重试/超时耗尽 ───────────► FAILED / FAILED_RETRIES_EXHAUSTED | TIMED_OUT（终态）
  │
  ├── 命中维护窗口 ──► DEFERRED ──(时钟到窗口 end 自动唤醒, 重走门禁)──► OPEN/EXECUTING
  ├── 高风险未审批 ──► WAITING_APPROVAL ──批准──► OPEN；拒绝──► FAILED / CLOSED_MANUALLY（终态）
  └── 人工闭环 ─────► CLOSED / CLOSED_MANUALLY（终态；排队中的重试/延迟唤醒一律丢弃）
```

终态：`RESOLVED / FAILED / PARTIAL / CLOSED`。终态事件拒绝一切新的处置触发，重复告警只归并、不执行。

### 3.2 接入归并与抑制（IngestionOutcome，拒绝原因全部可区分）

1. `alertId` 相同：**重复投递**，直接忽略，不计数、不执行（`MERGED`）。
2. `fingerprint` 相同：去重归并到同一事件；`fingerprint` 不同但 `correlationKey` 相同：关联归并。
3. 归并后按以下顺序抑制 / 触发，每种都有独立结论码：
   - `REJECT_NO_POLICY`：无匹配策略（类型不等或级别低于 `minSeverity`），**不建事件**；
   - `REJECT_POLICY_DISABLED`：命中策略但 `autoRemediationEnabled=false`；
   - `REJECT_ALREADY_CLOSED`：事件已闭环（终态）；
   - `REJECT_EXECUTION_IN_PROGRESS`：已有处置在执行（同一时刻只允许一个）；
   - `DEFERRED_MAINTENANCE_WINDOW`：命中维护窗口，**延迟**到窗口结束自动重试；
   - `REJECT_CHANGE_FREEZE`：命中变更冻结，**直接拒绝**（不延迟、不调度）；
   - `REJECT_APPROVAL_REQUIRED`：高风险动作未审批；
   - `ACCEPTED_NEW`：新事件创建且处置已开始。

### 3.3 前置约束门禁（GuardService，固定优先级，保证确定性）

1. **变更冻结优先**：命中任一冻结区间即拒绝（多个命中取 `freezeId` 最小者）。
2. **维护窗口次之**：`target` 为 `*` 或与事件 target 相等且命中区间，则延迟到
   “覆盖当前时刻的窗口中最晚的 `endAt`”，并向时钟注册该时刻的自动唤醒。
3. **高风险审批**：`riskLevel=HIGH` 且事件未批准则拒绝，事件进入 `WAITING_APPROVAL`。
4. 其余放行。

边界统一为**半开区间 `[start, end)`**：恰好在 `end` 触发视为已结束（见 `ConstraintGuardTest`）。

### 3.4 处置执行与失败三分类

- 单次尝试结论 `AttemptStatus`：`SUCCESS / RETRYABLE_FAILED / NON_RETRYABLE_FAILED / TIMEOUT`。
- 可重试失败与超时：在策略 `maxAttempts` 内按固定 1s backoff 重试；重试由手动时钟唤醒，
  **归属同一个 `ExecutionRecord`，不新增 `executionCount`**。
- 不可重试失败：立即终结，不重试。
- 重试 / 超时耗尽：事件 `FAILED`，结论分别为 `FAILED_RETRIES_EXHAUSTED` / `TIMED_OUT`。
- 超时判定基于**可控时钟的时间差**（`clock.now()` 在动作前后的差值 > `timeoutMillis`），
  不使用真实线程计时；所有失败原因原文写入 `AttemptRecord.message`、
  `ExecutionRecord.failureMessage` 和审计日志，**不允许静默吞掉**（动作抛异常也折算为可重试失败并保留信息）。

## 4. 并发安全模型（为什么重复/并发触发不产生重复执行）

- `EventStore#createOrGet` 以 `synchronized` 串行化“查重(fingerprint→correlationKey)→建事件→固化策略版本”，
  并发接入相同事件只会得到同一个聚合根，且读到的一定是已固化策略的完整事件。
- `EventStore#mutateWithLock` 使用**每事件分段锁**（不同事件仍并行）。
  `RemediationService.trigger` 在锁内完成“状态检查 → 门禁 → 置 EXECUTING + 建执行记录 + executionCount+1”的占位，
  因此并发的第二个触发只能观察到 `EXECUTING` 并被拒绝。
- 动作本体在**锁外**执行（纯内存、无真实等待），结果回写再次进入同一把锁；
  动作执行前还有一道状态闸门，确保人工闭环后排队的重试唤醒不会再执行动作。
- 加锁顺序固定为“事件锁 →（需要时）时钟锁”，时钟到期任务在**不持时钟锁**时回调，避免加锁环。
- 确定性结果：并发 N 个相同告警 ⇒ 恰好 1 个 `ACCEPTED_NEW`、恰好 1 个事件、`executionCount==1`、动作真实调用 1 次
  （见 `ConcurrentTriggerTest`）。

## 5. 策略版本固化（历史结论可复现）

- 策略每次写入都生成**递增新版本**，旧版本全部保留（`PolicyStore`）。
- 事件在首次命中时固化 `(matchedPolicyId, policyVersion)`；后续重试 / 重放始终用
  `getVersion(policyId, 事件固化版本)` 取快照。
- 因此策略后来调整（改动作、`maxAttempts`、风险等级、停用）**只影响新事件**，
  历史事件仍按旧版本跑出相同结论（见 `PolicyVersionAndCloseTest`）。

## 6. 可追溯性

- `AuditLog` 为全局单调 `seq` 的追加式日志，记录每个阶段的**输入与判定依据**：
  `INGEST / MERGE / GUARD / TRIGGER / ATTEMPT / RETRY / APPROVAL / FINISH / MANUAL_CLOSE`，
  例：`verdict=DEFER by mw1 until=... reason=...`。
- 测试通过 SLF4J 打印 `[test-input]` 与 `[test-judge]`，单测日志可直接看到输入与判定理由。

## 7. 关键代码结构

```
clock/      TimeService / ManualClockService      可控时间 + 到期任务队列
model/      事件/策略/窗口/冻结/执行/审计等值对象与枚举
store/      EventStore(分段锁) / PolicyStore(版本) / ConstraintStore / AuditLog
action/     RemediationAction SPI、ActionRegistry、ActionOutcome（失败必带原因）
engine/     GuardService(门禁) / AlertIngestionService(归并抑制) / RemediationService(触发/重试/审批/闭环)
api/        OrchestratorController（本地 REST 验证入口）
```

## 8. 测试清单

| 测试类 | 覆盖点 |
| --- | --- |
| `ManualClockServiceTest` | 时钟推进、到期顺序、执行中注册同刻任务，零真实等待 |
| `DedupAndMergeTest` | 重复投递、fingerprint/correlation 归并、各类抑制原因可区分 |
| `ConcurrentTriggerTest` | 16 线程并发接入单事件、执行中 8 并发触发：执行次数/最终状态确定 |
| `ConstraintGuardTest` | 维护窗口 `[start,end)` 边界、延迟自动唤醒、冻结拒绝不调度 |
| `ApprovalFlowTest` | 未授权高风险不执行、批准后执行、拒绝审批终结 |
| `FailureAndRetryTest` | 不可重试 / 可重试耗尽 / 重试成功 / 超时四分类，原因不丢失 |
| `PolicyVersionAndCloseTest` | 策略调整后历史事件按固化版本复现；人工闭环丢弃排队重试 |
| `OrchestratorApiIntegrationTest` | REST 端到端：策略、接入、归并、状态/执行/审计、时钟推进 |
