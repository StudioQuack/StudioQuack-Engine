package com.studioquack.sqs.status.log;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * One per-node sandbox-log row as shipped to the control plane in a BATCH flush or returned by the
 * on-demand {@code run-logs} pull. Field names match the control plane the control plane ingest
 * body so the same listener consumes a batched row and a live-streamed one identically.
 *
 * @param cdate ISO-8601 daemon-local creation time of the row
 * @param logId Per-attempt identity: the daemon-local {@code sandbox_log} row id ({@code slog_...}
 * ).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SandboxLogWireRow(
        String nodeId,
        String nodeName,
        String status,
        Integer exitCode,
        Long durationMs,
        String language,
        String containerId,
        String stdout,
        String stderr,
        String errorMessage,
        Object input,
        Object output,
        Object appliedInputs,
        java.util.List<String> unknownInputParameters,
        String failureKind,
        String postureNote,
        String cdate,
        String logId,
        Boolean retry) {

    /** Pre-identity shape: no per-attempt id ({@code logId} stays null). */
    public SandboxLogWireRow(String nodeId, String nodeName, String status, Integer exitCode,
                             Long durationMs, String language, String containerId, String stdout,
                             String stderr, String errorMessage, Object input, Object output,
                             Object appliedInputs, java.util.List<String> unknownInputParameters,
                             String failureKind, String postureNote, String cdate) {
        this(nodeId, nodeName, status, exitCode, durationMs, language, containerId, stdout, stderr,
                errorMessage, input, output, appliedInputs, unknownInputParameters, failureKind,
                postureNote, cdate, null, null);
    }

    /** Pre-retry shape: {@code retry} stays null. */
    public SandboxLogWireRow(String nodeId, String nodeName, String status, Integer exitCode,
                             Long durationMs, String language, String containerId, String stdout,
                             String stderr, String errorMessage, Object input, Object output,
                             Object appliedInputs, java.util.List<String> unknownInputParameters,
                             String failureKind, String postureNote, String cdate, String logId) {
        this(nodeId, nodeName, status, exitCode, durationMs, language, containerId, stdout, stderr,
                errorMessage, input, output, appliedInputs, unknownInputParameters, failureKind,
                postureNote, cdate, logId, null);
    }
}
