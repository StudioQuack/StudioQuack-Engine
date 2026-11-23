package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.quota.NodeExecutionMeter;
import com.studioquack.sqs.status.log.SandboxLogEntity;
import com.studioquack.sqs.status.log.SandboxLogRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Serves the reverse-channel {@code engine-logs} pull command: returns a bounded, PHI-minimal view of the
 * daemon's local {@code sandbox_log} records — either for a single {@code workflowExecutionId} or
 * incrementally since an ISO-8601 {@code since} cursor — along with the daemon's current unflushed
 * {@code pendingNodeExecutions} count. Exists so the control plane does not have to rely solely on
 * daemon-initiated log/quota pushes: the control plane can actively pull execution logs over the reverse
 * channel (WireGuard overlay, with HMAC-authenticated HTTPS heartbeat fallback when the overlay is
 * unreachable) to reconcile node executions and verify system-managed integrity checks.
 */
@Slf4j
@Component
public class EngineLogsCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "engine-logs";

    static final int DEFAULT_LIMIT = 100;
    static final int MAX_LIMIT = 200;

    private final ObjectMapper objectMapper;
    private final SandboxLogRepository repository;
    private final ObjectProvider<NodeExecutionMeter> nodeExecutionMeter;

    public EngineLogsCommandHandler(ObjectMapper objectMapper,
                                    SandboxLogRepository repository,
                                    ObjectProvider<NodeExecutionMeter> nodeExecutionMeter) {
        this.objectMapper = objectMapper;
        this.repository = repository;
        this.nodeExecutionMeter = nodeExecutionMeter;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        EngineLogsPayload payload = null;
        String rawPayload = command == null ? null : command.payload();
        if (StringUtils.isNotBlank(rawPayload)) {
            try {
                payload = objectMapper.readValue(rawPayload, EngineLogsPayload.class);
            } catch (Exception parse) {
                return DaemonCommandOutcome.failed("engine-logs: malformed payload: " + message(parse));
            }
        }
        int limit = clampLimit(payload == null ? null : payload.limit());
        String runId = payload == null ? null : StringUtils.trimToNull(payload.workflowExecutionId());
        String sinceRaw = payload == null ? null : StringUtils.trimToNull(payload.since());

        try {
            List<SandboxLogEntity> entities;
            if (runId != null) {
                List<SandboxLogEntity> forRun = repository.findByWorkflowExecutionIdOrderByCdateAsc(runId);
                entities = forRun.size() > limit ? forRun.subList(0, limit) : forRun;
            } else if (sinceRaw != null) {
                OffsetDateTime since;
                try {
                    since = OffsetDateTime.parse(sinceRaw);
                } catch (DateTimeParseException badDate) {
                    return DaemonCommandOutcome.failed("engine-logs: invalid ISO-8601 'since': " + sinceRaw);
                }
                entities = repository.findAfter(since, PageRequest.of(0, limit));
            } else {
                List<SandboxLogEntity> recent = new ArrayList<>(repository.findRecent(PageRequest.of(0, limit)));
                Collections.reverse(recent);
                entities = recent;
            }

            List<EngineLogEntry> rows = new ArrayList<>(entities.size());
            OffsetDateTime latestCdate = null;
            for (SandboxLogEntity entity : entities) {
                if (entity == null) {
                    continue;
                }
                rows.add(toEntry(entity));
                if (entity.getCdate() != null && (latestCdate == null || entity.getCdate().isAfter(latestCdate))) {
                    latestCdate = entity.getCdate();
                }
            }
            NodeExecutionMeter meter = nodeExecutionMeter == null ? null : nodeExecutionMeter.getIfAvailable();
            long pending = meter == null ? 0L : meter.pendingCount();
            String nextCursor = latestCdate != null ? latestCdate.toString() : sinceRaw;

            EngineLogsResult result = new EngineLogsResult(runId, pending, nextCursor, rows);
            log.debug("engine-logs.done run={} since={} rows={} pending={}",
                    runId, sinceRaw, rows.size(), pending);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("engine-logs.failed run={} since={}: {}", runId, sinceRaw, ex.toString());
            return DaemonCommandOutcome.failed("engine-logs failed: " + message(ex));
        }
    }

    private static EngineLogEntry toEntry(SandboxLogEntity e) {
        return new EngineLogEntry(
                e.getId(),
                e.getWorkflowExecutionId(),
                e.getNodeId(),
                e.getNodeName(),
                e.getStatus(),
                e.getLanguage(),
                e.getDurationMs(),
                e.getExitCode(),
                e.getFailureKind(),
                e.getErrorMessage(),
                e.getCdate() == null ? null : e.getCdate().toString(),
                Boolean.TRUE.equals(e.getRetry()) ? Boolean.TRUE : null);
    }

    private static int clampLimit(Integer raw) {
        if (raw == null || raw <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(raw, MAX_LIMIT);
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Optional pull selector: specific run, incremental cursor, or most recent rows.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record EngineLogsPayload(String workflowExecutionId, String since, Integer limit) {
    }

    /**
     * Compact, PHI-free node execution log row for reverse-channel verification and reconciliation.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record EngineLogEntry(
            String logId,
            String workflowExecutionId,
            String nodeId,
            String nodeName,
            String status,
            String language,
            Long durationMs,
            Integer exitCode,
            String failureKind,
            String errorMessage,
            String cdate,
            Boolean retry) {
    }

    /**
     * Command output returned over the reverse channel.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record EngineLogsResult(
            String workflowExecutionId,
            long pendingNodeExecutions,
            String nextCursor,
            List<EngineLogEntry> rows) {
    }
}
