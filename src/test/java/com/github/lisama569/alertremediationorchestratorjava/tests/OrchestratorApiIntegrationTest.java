package com.github.lisama569.alertremediationorchestratorjava.tests;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * 端到端：经 REST 接入策略与告警，验证归并、自动处置、状态/审计可查。
 * 复用进程内状态与手动时钟；时间推进走 /api/clock/advance，无真实等待。
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrchestratorApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorApiIntegrationTest.class);

    @Autowired
    private MockMvc mvc;

    @Test
    void endToEndPolicyAlertMergeResolveAndAudit() throws Exception {
        log.info("[test-input] 通过 REST 写入策略 builtin.success");
        mvc.perform(post("/api/policies")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"policyId":"p.api","version":0,"alertType":"disk.full",
                                 "minSeverity":"WARNING","actionId":"builtin.success",
                                 "riskLevel":"LOW","autoRemediationEnabled":true,
                                 "maxAttempts":2,"timeoutMillis":5000}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.stored.version").value(1));

        String alert1 = """
                {"alertId":"api-a1","source":"zabbix","alertType":"disk.full",
                 "severity":"WARNING","fingerprint":"fp-api","correlationKey":"corr-api",
                 "target":"host-9","summary":"disk almost full",
                 "occurredAt":"2026-09-25T00:00:00Z"}
                """;
        MvcResult r1 = mvc.perform(post("/api/alerts")
                        .contentType(MediaType.APPLICATION_JSON).content(alert1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("ACCEPTED_NEW"))
                .andReturn();
        String body = r1.getResponse().getContentAsString();
        // 用简单正则取 eventId，避免引入额外 JSON 依赖。
        String eventId = body.replaceAll(".*\"eventId\"\\s*:\\s*\"([^\"]+)\".*", "$1");
        log.info("[test-judge] 新事件 eventId={}", eventId);

        // 相同 fingerprint 重放 -> 已闭环拒绝（第一次执行已成功 RESOLVED）。
        mvc.perform(post("/api/alerts")
                        .contentType(MediaType.APPLICATION_JSON).content(alert1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("MERGED"));

        mvc.perform(get("/api/events/{id}", eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RESOLVED"))
                .andExpect(jsonPath("$.conclusion").value("RESOLVED_AUTO"))
                .andExpect(jsonPath("$.executionCount").value(1));

        mvc.perform(get("/api/events/{id}/executions", eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].status").value("SUCCEEDED"))
                .andExpect(jsonPath("$[0].attemptCount").value(1));

        MvcResult audit = mvc.perform(get("/api/events/{id}/audit", eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(org.hamcrest.Matchers.greaterThan(2)))
                .andReturn();
        log.info("[test-judge] 审计条目数>=3，审计可追溯: {}",
                audit.getResponse().getContentAsString().length() > 0);

        mvc.perform(post("/api/clock/advance").param("seconds", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.now").value("2026-09-25T00:00:05Z"));
    }
}
