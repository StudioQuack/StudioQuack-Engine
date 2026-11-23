package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.status.log.SandboxLogEntity;
import com.studioquack.sqs.status.log.SandboxLogRepository;
import com.studioquack.sqs.status.log.SandboxLogRowMapper;
import com.studioquack.sqs.status.log.SandboxLogWireRow;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code run-logs} is read-only, payload-minimal, and never throws out of {@code handle}.
 */
class RunLogsCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void missingWorkflowExecutionId_fails() throws Exception {
        RunLogsCommandHandler handler = new RunLogsCommandHandler(
                mapper, mock(SandboxLogRepository.class), mock(SandboxLogRowMapper.class));
        DaemonCommandOutcome outcome = handler.handle(
                new DaemonCommand("c1", RunLogsCommandHandler.TYPE, null, "{}"));
        assertThat(outcome.succeeded()).isFalse();
        assertThat(outcome.error()).contains("workflowExecutionId");
    }

    @Test
    void malformedPayload_fails() {
        RunLogsCommandHandler handler = new RunLogsCommandHandler(
                mapper, mock(SandboxLogRepository.class), mock(SandboxLogRowMapper.class));
        DaemonCommandOutcome outcome = handler.handle(
                new DaemonCommand("c1", RunLogsCommandHandler.TYPE, null, "not-json"));
        assertThat(outcome.succeeded()).isFalse();
        assertThat(outcome.error()).contains("malformed");
    }

    @Test
    void returnsMappedRows() throws Exception {
        SandboxLogRepository repository = mock(SandboxLogRepository.class);
        SandboxLogRowMapper rowMapper = mock(SandboxLogRowMapper.class);
        SandboxLogEntity entity = new SandboxLogEntity();
        entity.setNodeId("n1");
        List<SandboxLogEntity> entities = List.of(entity);
        when(repository.findByWorkflowExecutionIdOrderByCdateAsc("run-1")).thenReturn(entities);
        // REF_ONLY is the contract: the backend never persists a run-logs answer, so file bytes
        // must not be hydrated on this polled lane (see SandboxLogRowMapper.FileMode).
        when(rowMapper.toWireRows(entities, SandboxLogRowMapper.FileMode.REF_ONLY)).thenReturn(List.of(
                new SandboxLogWireRow("n1", "Step", "COMPLETED", 0, 5L, "python", null,
                        "ok", null, null, null, null, null, null, "NONE", null, null)));

        RunLogsCommandHandler handler = new RunLogsCommandHandler(mapper, repository, rowMapper);
        DaemonCommandOutcome outcome = handler.handle(new DaemonCommand(
                "c1", RunLogsCommandHandler.TYPE, null, "{\"workflowExecutionId\":\"run-1\"}"));

        assertThat(outcome.succeeded()).isTrue();
        assertThat(outcome.output()).contains("\"workflowExecutionId\":\"run-1\"");
        assertThat(outcome.output()).contains("\"nodeId\":\"n1\"");
        verify(rowMapper, never()).toWireRows(entities, SandboxLogRowMapper.FileMode.HYDRATE);
    }
}
