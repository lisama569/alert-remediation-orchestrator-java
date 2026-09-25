# 告警自动处置编排服务（本地可运行）

基于 Spring Boot 的进程内告警处置编排服务：用可控时钟（`MutableClock`）模拟时间推进，
在重复投递、并发触发与人工干预下保证同一事件得到一致且可解释的结论。

## 事件状态机

```
NEW ──► REMEDIATING ──► RESOLVED（终态）
 │           │
 │           ├──► FAILED_RETRYABLE ──► REMEDIATING（重试，状态保持 REMEDIATING）
 │           └──► FAILED_FINAL（终态：不可重试失败或重试耗尽）
 ├──► DELAYED ──► NEW（维护窗口结束后恢复）
 ├──► AWAITING_APPROVAL ──► REMEDIATING（获批）/ REJECTED（审批被拒，终态）
 ├──► REJECTED（终态：抑制 / 策略不匹配 / 未授权）
 └──► CLOSED（终态：人工闭环）
```

## 判定规则

### 告警接入（EventService.ingest），按序判定，原因可区分

| 顺序 | 条件 | 结果（RejectionReason） |
|---|---|---|
| 1 | 事件已处于 CLOSED / RESOLVED / REJECTED | `ALREADY_CLOSED` |
| 2 | 无任何策略的标签匹配 | `POLICY_MISMATCH` |
| 3 | 命中策略 `suppressed=true` | `SUPPRESSED` |
| 4 | 否则 | 接受并归并，`alertCount+1` |

同一 `eventKey` 的告警归并为单一事件。判定时命中的策略（id、suppressed、highRisk、
maxRetries、timeout）以快照形式写入事件，策略调整后历史结论仍可复现。

### 处置门控（OrchestratorService.remediate）

| 条件 | 语义 |
|---|---|
| 事件终态 | 拒绝 `ALREADY_CLOSED` |
| 命中 FREEZE 窗口 | 拒绝 `CHANGE_FREEZE`，写入历史 |
| 命中 MAINTENANCE 窗口 | 转 DELAYED，窗口结束后由 `processDelayed` 恢复执行 |
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
冻结拒绝、未授权/审批流、重试与超时分类。测试日志打印输入与判定依据。

### 手动验证（REST）

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080

# 1. 设置策略
curl -X POST localhost:8080/api/policies -H 'Content-Type: application/json' -d \
  '[{"policyId":"p1","matchLabels":{"app":"web"},"suppressed":false,"highRisk":false,"maxRetries":2,"timeoutSeconds":30}]'

# 2. 接入告警（重复投递同一 eventKey 会归并）
curl -X POST localhost:8080/api/alerts -H 'Content-Type: application/json' -d \
  '{"alertId":"a1","eventKey":"evt-1","severity":"critical","source":"demo","labels":{"app":"web"}}'

# 3. 触发处置 / 查询事件（含历史与执行计数）
curl -X POST localhost:8080/api/events/evt-1/remediate
curl localhost:8080/api/events/evt-1

# 4. 维护窗口与冻结
curl -X POST localhost:8080/api/windows -H 'Content-Type: application/json' -d \
  '[{"name":"nightly","start":"2026-01-01T00:00:00Z","end":"2026-01-01T01:00:00Z","kind":"MAINTENANCE"}]'

# 5. 推进可控时钟（毫秒），再恢复延迟事件
curl -X POST 'localhost:8080/api/clock/advance?millis=3600000'
curl -X POST localhost:8080/api/delayed/process

# 6. 高风险审批与人工闭环
curl -X POST 'localhost:8080/api/events/evt-1/approve?approver=team-lead'
curl -X POST 'localhost:8080/api/events/evt-1/close?operator=oncall'
```

## 代码结构

```
domain/        Alert、AlertEvent（状态机+历史）、EventStatus、RejectionReason、IngestResult
time/          MutableClock（可控时钟）
policy/        RemediationPolicy（标签匹配）、MaintenanceWindow（半开区间）
execution/     RemediationAction、ExecutionResult、FailureType
orchestrator/  EventService（接入归并抑制）、RemediationExecutor（并发安全执行）、
               ApprovalService（审批）、OrchestratorService（窗口/冻结/审批门控）
web/           OrchestratorController（本地验证 REST）
```
