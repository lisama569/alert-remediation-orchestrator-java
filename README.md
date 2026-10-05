# 告警自动处置编排服务（本地可运行）

基于 Spring Boot 的进程内告警处置编排服务：告警**接入即自动一路驱动到处置结论**，
命中维护窗口的操作在窗口结束后**随时间推进自动恢复**，无需任何手动二次触发/唤醒。
用可控时钟（`MutableClock`）模拟时间推进，在重复投递、并发触发与人工干预下保证同一事件
得到一致、可解释、可复现的结论。

## 事件状态机

```
NEW ──► REMEDIATING ──► RESOLVED（终态）
 │           │
 │           ├──► FAILED_RETRYABLE ──► REMEDIATING（重试，状态保持 REMEDIATING）
 │           └──► FAILED_FINAL（终态：不可重试失败或重试耗尽）
 ├──► DELAYED ──► NEW ──► ...（维护窗口结束后自动恢复，半开区间）
 ├──► AWAITING_APPROVAL ──►（批准）自动 REMEDIATING /（拒绝）自动 REJECTED
 └──► REJECTED（终态：抑制 / 未授权）/ CLOSED（终态：人工闭环）
```

> 说明：命中**冻结期**不终结事件（事件仍为 NEW，仅拒绝本次操作并留痕），非冻结时段可继续；
> 命中**抑制策略**与**审批被拒**为终态 REJECTED。POLICY_MISMATCH 只是「本次未受理」，事件保持
> NEW，不被钉死。

## 自动化能力（本次新增）

### 1. 接入即自动处置（无需手动触发）

`EventService.ingest` 受理后通过自动驱动钩子（`OrchestratorService` 在装配时注册）立即走一遍
门控 + 执行，自动链路与原手动 `remediate` 是**同一段门控代码**，语义完全一致：

- 命中抑制策略 → 终态 `SUPPRESSED`；
- 命中冻结窗口 → 拒绝 `CHANGE_FREEZE`（留痕，不终结事件）；
- 命中维护窗口 → 转 DELAYED（见下条自愈）；
- 高风险未批 → AWAITING_APPROVAL；批准后**自动继续执行**，拒绝后**自动转终态 UNAUTHORIZED**；
- 已得出最终结论（RESOLVED / FAILED_FINAL / REJECTED / CLOSED）→ 复现终态结论，不再执行。

### 2. 同事件重复 / 并发投递：只真正执行一次，结论稳定可查

同一 `eventKey` 归并为单一事件，所有状态流转在事件监视器上串行化，执行层另有 `running`
集合兜底：

- 首次受理：`disposition=NEW_ACCEPTED`，自动驱动到结论；
- 处理途中重复投递：归并计数 `disposition=MERGED`，自动驱动返回 `DUPLICATE`（已在执行中）或
  终态拒绝，**执行只发生一次、executionCount 恒为 1**；
- 终态后重复投递：`disposition=REJECTED`，给出明确去重结果，不重复计数、不重复执行。

`IngestResult` 同时回传 `disposition`、自动驱动结论 `drive`、事件最终 `status` 与 `alertCount`，
投递方可直接核对稳定结果。

### 3. 维护窗口延迟自愈（无需手动唤醒）

- `MutableClock` 在 `advance/set` 后通知 tick 监听器；`OrchestratorService` 收到后自动重新评估
  所有 DELAYED 事件；窗口设置（`setWindows`）变更时也会立即重评。
- 仍在窗口内：继续等待，并追加一条 `remain DELAYED` 判定记录；
- 到达/越过窗口结束：追加 `window ended, resuming` 记录后自动续跑。
- 边界仍为**半开区间 `[start, end)`**：start 那一刻命中（延迟/拒绝），end 那一刻放行。
- 每次被推迟（`delayed until ...`）、等待重评（`remain DELAYED`）、被恢复
  （`window ended, resuming`）都写事件历史；恢复后由执行层保证只真正执行一次。

### 4. 结论一致性与规则版本

- **拒绝原因不改口**：终态事件的重复投递原样复现事件上保存的拒绝原因（如始终 SUPPRESSED /
  UNAUTHORIZED），不会第一次一个原因、再投变成另一个；非拒绝类终态统一为 ALREADY_CLOSED。
- **未匹配规则不钉死**：POLICY_MISMATCH 时事件保持 NEW、不计数、仅留痕；规则补齐/修好后，
  同样的告警再投即可正常受理并自动走到底。
- **历史结论按当时规则版本复现**：事件首次受理时把命中的策略（id / suppressed / highRisk /
  maxRetries / timeout）作为不可变快照**粘在事件上**，之后 `setPolicies` 不影响该事件的门控与
  执行参数。真正的终态事件再投递时，执行的仍是当时那一版规则（自动化测试用「按策略 id 计数的
  实际执行行为」证明，而非只比对历史文字）。

## 判定规则

### 告警接入（EventService.ingest），按序判定，原因可区分

| 顺序 | 条件 | 结果（RejectionReason / disposition） |
|---|---|---|
| 1 | 事件已处于 CLOSED / RESOLVED / REJECTED / FAILED_FINAL | 复现原拒绝原因（默认 `ALREADY_CLOSED`），`REJECTED` |
| 2 | 无任何策略的标签匹配 | `POLICY_MISMATCH`（不终结事件，保持 NEW，留痕） |
| 3 | 命中策略 `suppressed=true` | 终态 `SUPPRESSED` |
| 4 | 否则 | 受理：首次 `NEW_ACCEPTED` / 重复 `MERGED`，`alertCount+1`，随后自动驱动 |

### 处置门控（OrchestratorService.remediate，自动与手动共用）

| 条件 | 语义 |
|---|---|
| 事件终态 | 复现原拒绝原因（默认 `ALREADY_CLOSED`），不执行 |
| 命中 FREEZE 窗口 | 拒绝 `CHANGE_FREEZE`，写入历史，不终结事件 |
| 命中 MAINTENANCE 窗口 | 转 DELAYED，窗口结束随时间推进自动恢复执行 |
| 高风险策略且未获批 | 转 AWAITING_APPROVAL，拒绝 `APPROVAL_REQUIRED` |
| 审批被拒绝 | 终态 REJECTED，原因 `UNAUTHORIZED`（审批决定后自动到达） |
| 通过全部门控 | 执行 |

维护窗口为半开区间 `[start, end)`：start 命中（延迟/拒绝），end 不命中（放行）。

### 执行语义（RemediationExecutor）

- 同一事件任一时刻只允许一个执行：并发/重复触发返回去重结果，不重复执行、不重复计数；
  事件到达终态后的再次触发直接跳过。
- 失败分类：`TIMEOUT`（单次尝试耗时超过策略 timeout）、`RETRYABLE`、`NON_RETRYABLE`。
  可重试与超时失败在 `maxRetries` 内重试；不可重试失败立即转 FAILED_FINAL。
- 失败原因与每次尝试均写入事件历史，不静默吞掉。

## 配置项

| 配置 | 说明 | 默认 |
|---|---|---|
| `orchestrator.clock.start` | 可控时钟起点（ISO-8601） | `2026-01-01T00:00:00Z` |
| `server.port` | HTTP 端口（0 为随机） | `0` |

策略与维护窗口通过 REST 动态设置。策略字段：`policyId`、`matchLabels`、`suppressed`、
`highRisk`、`maxRetries`、`timeoutSeconds`。

生产接入时用 `OrchestratorService.setActionResolver(ActionResolver)` 提供按事件策略快照解析的
处置动作（默认是本地 no-op 成功动作）；`resolver.resolve(event)` 可读取
`event.getPolicy()`（受理当时粘住的快照），从而保证走的是事件受理那一版规则。

## 兼容范围

- 原手动 REST 入口保留可用：`POST /api/events/{key}/remediate`、`POST /api/delayed/process`
  仍可调用（内部与自动链路共用门控/ActionResolver），但正常自动化流程下不再需要它们。
- 单元测试中直接 `new EventService(clock)`（不构造 `OrchestratorService`）时无自动驱动钩子，
  `IngestResult.drive` 为 `null`，保持纯接入语义可独立测试。
- `IngestResult` 在原 `accepted/rejection/detail` 基础上扩展了 `disposition/drive/status/
  alertCount` 字段；旧字段含义不变。

## 本地验证

### 单元测试（不依赖真实等待，时间由 MutableClock 推进）

```bash
mvn test
```

覆盖（共 27 个用例，测试日志以 `[业务名] input/basis: ...` 打印输入与判定依据）：

- 接入即自动处置到结论；
- 顺序重复投递与 16 线程并发投递同一事件：执行 1 次、计数/终态稳定，其余明确归并/去重；
- 自动链路门控：抑制、冻结、高风险审批（批准自动执行 / 拒绝自动终态）；
- 维护窗口：半开边界、窗口内推进保持 DELAYED 并留痕、到 end 自动恢复且只执行一次；
- 拒绝原因多次投递一致；POLICY_MISMATCH 在规则补齐后可恢复；
- 历史结论复现：换规则后终态事件按旧版 maxRetries 实际执行（按策略 id 的动作调用计数为证），
  新事件才走新规则；抑制规则删除后旧事件仍复现 SUPPRESSED；
- 原有：重复归并、并发执行、重试/超时分类、失败不吞因等用例不回归。

### 手动验证（REST）

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080

# 1. 先布维护窗口（可选）与策略
curl -X POST localhost:8080/api/windows -H 'Content-Type: application/json' -d \
  '[{"name":"nightly","start":"2026-01-01T00:00:00Z","end":"2026-01-01T01:00:00Z","kind":"MAINTENANCE"}]'
curl -X POST localhost:8080/api/policies -H 'Content-Type: application/json' -d \
  '[{"policyId":"p1","matchLabels":{"app":"web"},"suppressed":false,"highRisk":false,"maxRetries":2,"timeoutSeconds":30}]'

# 2. 接入告警：自动驱动（窗口内 -> DELAYED，窗口外 -> RESOLVED），无需再调 remediate
curl -X POST localhost:8080/api/alerts -H 'Content-Type: application/json' -d \
  '{"alertId":"a1","eventKey":"evt-1","severity":"critical","source":"demo","labels":{"app":"web"}}'

# 3. 推进可控时钟：窗口结束后延迟事件自动恢复执行（无需 /api/delayed/process）
curl -X POST 'localhost:8080/api/clock/advance?millis=3600000'
curl localhost:8080/api/events/evt-1

# 4. 高风险：接入后 AWAITING_APPROVAL，批准即自动执行；拒绝即自动终态 UNAUTHORIZED
curl -X POST 'localhost:8080/api/events/evt-1/approve?approver=team-lead'

# 5. 人工闭环
curl -X POST 'localhost:8080/api/events/evt-1/close?operator=oncall'
```

## 代码结构

```
domain/        Alert、AlertEvent（状态机+历史）、EventStatus、RejectionReason、IngestResult、
               RemediationOutcome（处置结论）、HistoryEntry
time/          MutableClock（可控时钟 + tick 监听，驱动延迟自愈）
policy/        RemediationPolicy（标签匹配）、MaintenanceWindow（半开区间）
execution/     RemediationAction、ActionResolver（按事件快照解析动作）、ExecutionResult、FailureType
orchestrator/  EventService（接入归并抑制 + 自动驱动钩子）、RemediationExecutor（并发安全执行）、
               ApprovalService（审批 + 决定监听）、OrchestratorService（自动装配：接入即处置、
               时钟自愈、审批续跑；窗口/冻结/审批门控）
web/           OrchestratorController（本地验证 REST）
```
