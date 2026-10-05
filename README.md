# 告警自动处置编排服务（本地可运行）

基于 Spring Boot 的进程内告警处置编排服务：用可控时钟（`MutableClock`）模拟时间推进，
在重复投递、并发触发与人工干预下保证同一事件得到一致且可解释的结论。

## 事件状态机

```
NEW ──► REMEDIATING ──► RESOLVED（终态）
 │           │
 │           ├──► FAILED_RETRYABLE ──► REMEDIATING（重试，状态保持 REMEDIATING）
 │           └──► FAILED_FINAL（终态：不可重试失败或重试耗尽）
 ├──► DELAYED ──► NEW（维护窗口结束后自动恢复）
 ├──► AWAITING_APPROVAL ──► REMEDIATING（获批）/ REJECTED（审批被拒，终态）
 ├──► REJECTED（终态：抑制 / 未授权；策略不匹配只是非终态暂时结论，不进入此态）
 └──► CLOSED（终态：人工闭环）
```

## 判定规则

### 告警接入（EventService.ingest），按序判定，原因可区分且前后一致

| 顺序 | 条件 | 结果（RejectionReason） |
|---|---|---|
| 1 | 事件已处于 CLOSED / RESOLVED / FAILED_FINAL | `ALREADY_CLOSED` |
| 2 | 事件已终态拒绝（REJECTED） | 原样返回当时记录的原因（如 `SUPPRESSED` / `UNAUTHORIZED`），不随重投改变 |
| 3 | 事件尚未命中过策略且当前无策略匹配 | `POLICY_MISMATCH`（非终态，见下） |
| 4 | 首次命中策略且 `suppressed=true` | `SUPPRESSED`（终态） |
| 5 | 否则 | 接受并归并，`alertCount+1` |

同一 `eventKey` 的告警归并为单一事件。判定时命中的策略（id、suppressed、highRisk、
maxRetries、timeout）以快照形式写入事件，且事件一旦命中策略就不再重新匹配，
策略调整后历史结论仍可复现、不被改判。

### 处置门控（OrchestratorService.remediate）

| 条件 | 语义 |
|---|---|
| 事件终态 | 拒绝 `ALREADY_CLOSED` |
| 命中 FREEZE 窗口 | 拒绝 `CHANGE_FREEZE`，写入历史 |
| 命中 MAINTENANCE 窗口 | 转 DELAYED，窗口结束后自动恢复执行（见"延迟自愈"） |
| 高风险策略且未获批 | 转 AWAITING_APPROVAL，拒绝 `APPROVAL_REQUIRED` |
| 审批被拒绝 | 终态 REJECTED，原因 `UNAUTHORIZED` |
| 通过全部门控 | 执行 |

维护窗口为半开区间 `[start, end)`：start 命中（延迟/拒绝），end 不命中（放行）。

### 执行语义（RemediationExecutor）

- 同一事件任一时刻只允许一个执行：并发/重复触发返回去重结果，不重复执行、不重复计数；
  事件到达终态后的再次触发直接跳过。
- 失败分类：`TIMEOUT`（单次尝试耗时超过策略 timeout）、`RETRYABLE`、`NON_RETRYABLE`。
  可重试与超时失败在 `maxRetries` 内重试；不可重试失败立即转 FAILED_FINAL。
- 失败原因与每次尝试均写入事件历史，不静默吞掉。

## 自动驱动、延迟自愈与结论一致性

### 自动驱动（ingestAndDrive）

`OrchestratorService.ingestAndDrive(alert)` 在告警接入后自动推进到处置结论，
无需再手动调用 remediate；REST 入口 `POST /api/alerts` 已切换为该路径，
返回 `IngestOutcome{ingest, remediation}`（接入判定 + 自动处置结果，
接入未受理时 remediation 为 null）。

- 同一告警重复投递、或多线程同时投递同一 eventKey：全部归并到同一事件，
  真正执行只有一次（执行次数与最终状态可通过 `GET /api/events/{eventKey}` 查询），
  其余并发驱动得到明确的 `DUPLICATE` 去重结果，不重复计数、不重复执行。
- 自动链路沿用与手动触发完全一致的门控：抑制、冻结期、高风险审批、终态事件
  照样拦截，语义不变。

### 延迟自愈（enableAutoResume）

`OrchestratorService.enableAutoResume()` 在时钟上注册监听器（生产装配已默认开启）：
时钟每次推进后自动检查 DELAYED 事件，窗口结束即恢复执行，无需手动调用
`processDelayed`。窗口边界仍为半开区间 `[start, end)`；每次推迟与恢复的判定
都写入事件历史（"delayed until ..." / "resuming"），恢复后同样只真正执行一次。
手动入口 `POST /api/delayed/process` 保留，重复调用幂等（已恢复的事件不会再次执行）。

### 结论一致性

- 同一告警反复投递结论一致：终态拒绝（如 SUPPRESSED）再次投递时原样返回当时
  记录的原因，不会变成其他说法。
- `POLICY_MISMATCH` 只是"投递当时没匹配到规则"的暂时结论（非终态）：补齐或修正
  策略后，同一告警再投即可正常受理并自动走到结论。
- 已得出最终结论的事件（RESOLVED / FAILED_FINAL / CLOSED）：事件持有判定当时
  命中的策略对象（不可变快照），后续策略调整不会改判；重投只会得到
  `ALREADY_CLOSED`，历史结论原样复现。单测用可观测行为证明这一点：策略被换成
  "抑制+高风险+maxRetries=0" 后，旧事件仍按当时版本的 maxRetries=2 重试 3 次、
  且不触发审批。

## 配置项

| 配置 | 说明 | 默认 |
|---|---|---|
| `orchestrator.clock.start` | 可控时钟起点（ISO-8601） | `2026-01-01T00:00:00Z` |
| `server.port` | HTTP 端口（0 为随机） | `0` |

策略与维护窗口通过 REST 动态设置（见下）。策略字段：`policyId`、`matchLabels`、
`suppressed`、`highRisk`、`maxRetries`、`timeoutSeconds`。

## 本地验证

### 单元测试（不依赖真实等待，时间由 MutableClock 推进）

```bash
mvn test
```

覆盖：重复投递归并、并发触发（执行次数与终态确定）、维护窗口边界（半开区间）、
冻结拒绝、未授权/审批流、重试与超时分类；以及本次新增的——接入即自动驱动、
并发投递去重（仅一次执行，其余 DUPLICATE）、延迟事件随时钟推进自动恢复、
POLICY_MISMATCH 补策略后可恢复、终态拒绝原因跨投递稳定、终态结论按当时策略
版本复现（以实际执行次数/审批记录等可观测行为验证）。测试日志打印每类用例的
输入与判定依据。

### 手动验证（REST）

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080

# 1. 设置策略
curl -X POST localhost:8080/api/policies -H 'Content-Type: application/json' -d \
  '[{"policyId":"p1","matchLabels":{"app":"web"},"suppressed":false,"highRisk":false,"maxRetries":2,"timeoutSeconds":30}]'

# 2. 接入告警：自动驱动到处置结论，无需再手动触发（重复投递同一 eventKey 会归并/去重）
curl -X POST localhost:8080/api/alerts -H 'Content-Type: application/json' -d \
  '{"alertId":"a1","eventKey":"evt-1","severity":"critical","source":"demo","labels":{"app":"web"}}'

# 3. 查询事件（含历史、执行计数与最终状态）
curl localhost:8080/api/events/evt-1

# 4. 维护窗口与冻结
curl -X POST localhost:8080/api/windows -H 'Content-Type: application/json' -d \
  '[{"name":"nightly","start":"2026-01-01T00:00:00Z","end":"2026-01-01T01:00:00Z","kind":"MAINTENANCE"}]'

# 5. 推进可控时钟（毫秒）：越过窗口结束点时延迟事件自动恢复，无需手动唤醒
curl -X POST 'localhost:8080/api/clock/advance?millis=3600000'
# （手动入口仍保留，幂等：curl -X POST localhost:8080/api/delayed/process）

# 6. 高风险审批与人工闭环
curl -X POST 'localhost:8080/api/events/evt-1/approve?approver=team-lead'
curl -X POST 'localhost:8080/api/events/evt-1/close?operator=oncall'
```

## 兼容范围

- `EventService.ingest`、`OrchestratorService.remediate`、`processDelayed(factory)`、
  审批与闭环等既有接口语义不变，既有用例不回归；
  唯一的行为收紧：POLICY_MISMATCH 不再把事件置为终态（改为可恢复的暂时结论），
  终态拒绝重投时返回当时记录的原因而非笼统的 ALREADY_CLOSED。
- 自动恢复为显式开启（`enableAutoResume`），生产装配（OrchestratorConfig）默认开启；
  手动 `POST /api/delayed/process` 保留且幂等。
- `POST /api/alerts` 的响应由 `IngestResult` 扩展为 `IngestOutcome{ingest, remediation}`，
  原接入判定字段在 `ingest` 内原样保留。

## 代码结构

```
domain/        Alert、AlertEvent（状态机+历史）、EventStatus、RejectionReason、IngestResult
time/          MutableClock（可控时钟，支持推进监听器）
policy/        RemediationPolicy（标签匹配）、MaintenanceWindow（半开区间）
execution/     RemediationAction、ExecutionResult、FailureType
orchestrator/  EventService（接入归并抑制）、RemediationExecutor（并发安全执行）、
               ApprovalService（审批）、OrchestratorService（窗口/冻结/审批门控、
               自动驱动 ingestAndDrive、延迟自愈 enableAutoResume）、IngestOutcome
web/           OrchestratorController（本地验证 REST）
```
