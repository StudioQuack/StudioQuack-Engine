package com.studioquack.sqs.status.log;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.status.SandboxLogCallback;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;

/**
 * Writes each per-node sandbox log to {@code sandbox_log}. A write failure is logged and swallowed
 * so it cannot roll back the engine transaction.
 */
@Slf4j
@Component
public class SandboxLogStore {

    /** DoS guard only — not a shipping truncation cap (aligns with the 2 GiB safety ceiling). */
    public static final int MAX_TEXT_CEILING_CHARS = Integer.MAX_VALUE;

    private final SandboxLogRepository repository;
    private final ObjectMapper objectMapper;

    public SandboxLogStore(SandboxLogRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    /**
     * @return The generated row id ({@code slog_...}) when the write landed, so the fan-out can attach
     * the same per-attempt identity to every backend delivery of this row.
     */
    public String save(SandboxLogCallback.SandboxLogRecord record) {
        if (record == null || record.workflowExecutionId() == null || record.workflowExecutionId().isBlank()
                || record.nodeId() == null || record.nodeId().isBlank()) {
            return null;
        }
        try {
            String rowId = "slog_" + NanoIdUtils.randomNanoId();
            repository.save(SandboxLogEntity.builder()
                    .id(rowId)
                    .workflowExecutionId(record.workflowExecutionId())
                    .nodeId(record.nodeId())
                    .nodeName(record.nodeName())
                    .status(record.status())
                    .exitCode(record.exitCode())
                    .durationMs(record.durationMs())
                    .language(record.language())
                    .containerId(record.containerId())
                    .stdout(record.stdout())
                    .stderr(record.stderr())
                    .errorMessage(record.errorMessage())
                    .inputJson(toJson(record.input()))
                    .outputJson(toJson(record.output()))
                    .appliedInputsJson(toJson(record.appliedInputs()))
                    .unknownInputParametersJson(toJson(record.unknownInputParameters()))
                    .failureKind(record.failureKind())
                    .postureNote(record.postureNote())
                    .retry(Boolean.TRUE.equals(record.retry()) ? Boolean.TRUE : null)
                    .cdate(OffsetDateTime.now())
                    .build());
            return rowId;
        } catch (Exception e) {
            // Local custody must never break a dispatch; the BE mirror is the independent copy.
            log.warn("sandbox-log[store]: local write failed for run={} node={}: {}",
                    record.workflowExecutionId(), record.nodeId(), e.toString());
            return null;
        }
    }

    /**
     * Persist the "sandbox started" marker (the STARTED half of the two-step live signal). Returns the
     * generated row id like {@link #save} ({@code null} on skip/failure).
     */
    public String saveStarted(SandboxLogCallback.SandboxStartRecord record) {
        if (record == null || record.workflowExecutionId() == null || record.workflowExecutionId().isBlank()
                || record.nodeId() == null || record.nodeId().isBlank()) {
            return null;
        }
        try {
            String rowId = "slog_" + NanoIdUtils.randomNanoId();
            repository.save(SandboxLogEntity.builder()
                    .id(rowId)
                    .workflowExecutionId(record.workflowExecutionId())
                    .nodeId(record.nodeId())
                    .nodeName(record.nodeName())
                    .status("STARTED")
                    .language(record.language())
                    .cdate(OffsetDateTime.now())
                    .build());
            return rowId;
        } catch (Exception e) {
            log.warn("sandbox-log[store]: local STARTED write failed for run={} node={}: {}",
                    record.workflowExecutionId(), record.nodeId(), e.toString());
            return null;
        }
    }

    /** JSON-serialise an IO payload; null stays null and a serialisation failure degrades to null. */
    private String toJson(Object payload) {
        if (payload == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.debug("sandbox-log[store]: IO payload not serialisable ({}); storing null", e.toString());
            return null;
        }
    }
}
