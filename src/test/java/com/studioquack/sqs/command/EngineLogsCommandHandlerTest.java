package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.quota.NodeExecutionMeter;
import com.studioquack.sqs.status.log.SandboxLogEntity;
import com.studioquack.sqs.status.log.SandboxLogRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.domain.PageRequest;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class EngineLogsCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void pullsByWorkflowExecutionId_andIncludesPendingNodeCount() throws Exception {
        SandboxLogRepository repo = mock(SandboxLogRepository.class);
        OffsetDateTime t1 = OffsetDateTime.of(2026, 11, 22, 10, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime t2 = t1.plusSeconds(1);
        when(repo.findByWorkflowExecutionIdOrderByCdateAsc("run-probe-1")).thenReturn(List.of(
                SandboxLogEntity.builder()
                        .id("slog_1").workflowExecutionId("run-probe-1").nodeId("start-1")
                        .nodeName("Start").status("COMPLETED").language("bpmn").cdate(t1).build(),
                SandboxLogEntity.builder()
                        .id("slog_2").workflowExecutionId("run-probe-1").nodeId("end-1")
                        .nodeName("End").status("COMPLETED").language("bpmn").cdate(t2).build()
        ));
        NodeExecutionMeter meter = new NodeExecutionMeter();
        meter.recordActivation();
        meter.recordActivation();
        @SuppressWarnings("unchecked")
        ObjectProvider<NodeExecutionMeter> meterProvider = mock(ObjectProvider.class);
        when(meterProvider.getIfAvailable()).thenReturn(meter);

        EngineLogsCommandHandler handler = new EngineLogsCommandHandler(mapper, repo, meterProvider);
        DaemonCommand cmd = new DaemonCommand("cmd-1", "engine-logs", "2026-11-22T10:00:05Z",
                "{\"workflowExecutionId\":\"run-probe-1\",\"limit\":50}");

        DaemonCommandOutcome outcome = handler.handle(cmd);
        assertTrue(outcome.succeeded());
        JsonNode json = mapper.readTree(outcome.output());
        assertEquals("run-probe-1", json.path("workflowExecutionId").asText());
        assertEquals(2L, json.path("pendingNodeExecutions").asLong());
        assertEquals(2, json.path("rows").size());
        assertEquals("slog_1", json.path("rows").get(0).path("logId").asText());
        assertEquals(t2.toString(), json.path("nextCursor").asText());
        // PHI minimization: no input/output fields present.
        assertTrue(json.path("rows").get(0).path("input").isMissingNode());
        assertTrue(json.path("rows").get(0).path("output").isMissingNode());
    }

    @Test
    void pullsIncrementallySinceCursor_andClampsLimit() throws Exception {
        SandboxLogRepository repo = mock(SandboxLogRepository.class);
        OffsetDateTime since = OffsetDateTime.of(2026, 11, 22, 9, 0, 0, 0, ZoneOffset.UTC);
        when(repo.findAfter(eq(since), eq(PageRequest.of(0, EngineLogsCommandHandler.MAX_LIMIT))))
                .thenReturn(List.of());

        EngineLogsCommandHandler handler = new EngineLogsCommandHandler(mapper, repo, null);
        DaemonCommand cmd = new DaemonCommand("cmd-2", "engine-logs", "2026-11-22T10:00:05Z",
                "{\"since\":\"2026-11-22T09:00:00Z\",\"limit\":9999}");

        DaemonCommandOutcome outcome = handler.handle(cmd);
        assertTrue(outcome.succeeded());
        verify(repo).findAfter(eq(since), eq(PageRequest.of(0, EngineLogsCommandHandler.MAX_LIMIT)));
    }

    @Test
    void rejectsMalformedSinceCursor() {
        SandboxLogRepository repo = mock(SandboxLogRepository.class);
        EngineLogsCommandHandler handler = new EngineLogsCommandHandler(mapper, repo, null);
        DaemonCommand cmd = new DaemonCommand("cmd-3", "engine-logs", "2026-11-22T10:00:05Z",
                "{\"since\":\"not-a-timestamp\"}");

        DaemonCommandOutcome outcome = handler.handle(cmd);
        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("invalid ISO-8601"));
        verifyNoInteractions(repo);
    }
}
