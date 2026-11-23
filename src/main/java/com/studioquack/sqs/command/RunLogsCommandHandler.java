package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.status.log.SandboxLogEntity;
import com.studioquack.sqs.status.log.SandboxLogRepository;
import com.studioquack.sqs.status.log.SandboxLogRowMapper;
import com.studioquack.sqs.status.log.SandboxLogWireRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Serves the on-demand {@code run-logs} command: returns the daemon's current per-node {@code sandbox_log}
 * rows for one {@code workflowExecutionId}. This is how the control plane answers a Monitoring "details"
 * request for a BATCH run that is still RUNNING — the per-node rows have not yet been flushed (that happens
 * once, at the terminal boundary), so the backend pulls the live buffer on demand and merges it into the
 * details response.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RunLogsCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "run-logs";

    private final ObjectMapper objectMapper;
    private final SandboxLogRepository repository;
    private final SandboxLogRowMapper rowMapper;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        RunLogsPayload payload;
        try {
            payload = objectMapper.readValue(command.payload(), RunLogsPayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("run-logs: malformed payload: " + message(parse));
        }
        if (payload == null || StringUtils.isBlank(payload.workflowExecutionId())) {
            return DaemonCommandOutcome.failed("run-logs: payload needs workflowExecutionId");
        }
        try {
            List<SandboxLogEntity> entities =
                    repository.findByWorkflowExecutionIdOrderByCdateAsc(payload.workflowExecutionId());
            List<SandboxLogWireRow> rows =
                    rowMapper.toWireRows(entities, SandboxLogRowMapper.FileMode.REF_ONLY);
            RunLogsResult result = new RunLogsResult(payload.workflowExecutionId(), rows);
            log.debug("run-logs.done run={} rows={}", payload.workflowExecutionId(), rows.size());
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("run-logs.failed run={}: {}", payload.workflowExecutionId(), ex.toString());
            return DaemonCommandOutcome.failed("run-logs failed: " + message(ex));
        }
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Request payload: the single run to read the local buffer for.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record RunLogsPayload(String workflowExecutionId) {
    }

    /**
     * Command output: the run id echoed back plus its ordered per-node rows.
     */
    record RunLogsResult(String workflowExecutionId, List<SandboxLogWireRow> rows) {
    }
}
