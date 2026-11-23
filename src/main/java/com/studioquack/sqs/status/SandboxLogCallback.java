package com.studioquack.sqs.status;


public interface SandboxLogCallback {

    /** Reports one node's sandbox result. Never throws. */
    void report(SandboxLogRecord record);

    /**
     * Reports that a node's sandbox is ABOUT TO RUN — the "started" half of the two-step live signal.
     * The live-execution animation needs a RUNNING marker before the terminal result arrives; without
     * it a node jumps straight to success.
     */
    void reportStarted(SandboxStartRecord record);

    /**
     * Minimal "sandbox started" signal — identifiers only, no streams/results yet.
     *
     * @param logId Per-attempt identity: the daemon-local {@code sandbox_log} row id ({@code slog_...}
     * ), attached by the fan-out AFTER the local write lands.
     */
    record SandboxStartRecord(
            String workflowExecutionId,
            String nodeId,
            String nodeName,
            String language,
            String logId) {

        /** Producer-side constructor: no local row id yet — the fan-out attaches it post-write. */
        public SandboxStartRecord(String workflowExecutionId, String nodeId, String nodeName,
                                  String language) {
            this(workflowExecutionId, nodeId, nodeName, language, null);
        }

        /** Copy carrying the local row id, made by the fan-out once the store write landed. */
        public SandboxStartRecord withLogId(String logId) {
            return new SandboxStartRecord(workflowExecutionId, nodeId, nodeName, language, logId);
        }
    }

    /**
     * One node's sandbox result. All fields optional except {@code workflowExecutionId} +
     * {@code nodeId}; {@code input}/{@code output} feed the Monitoring "echoes" tab, {@code stdout}/
     * {@code stderr} the per-node log panel.
     *
     * @param status
     * @param appliedInputs Inputs actually applied inside the sandbox (when known)
     * @param unknownInputParameters Dotted {@code portShortCode.paramName} entries dropped because
     * they were outside the target port's declared inbound schema; null / empty when nothing was
     * dropped
     * @param failureKind Coarse failure classification mirroring
     * {@code com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind} — one of {@code NONE}/
     * {@code SCRIPT}/{@code CONTAINER}/{@code INFRA}/{@code ENGINE}.
     * @param postureNote Non-null only when the node's container ran while the host's overlay-egress
     * enforcement was {@code DEGRADED}.
     * @param logId Per-attempt identity: the daemon-local {@code sandbox_log} row id ({@code slog_...}
     * ), attached by the fan-out AFTER the local write lands.
     */
    record SandboxLogRecord(
            String workflowExecutionId,
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
            String logId,
            Boolean retry) {

        /** Producer-side constructor: no local row id yet — the fan-out attaches it post-write. */
        public SandboxLogRecord(String workflowExecutionId, String nodeId, String nodeName,
                                String status, Integer exitCode, Long durationMs, String language,
                                String containerId, String stdout, String stderr, String errorMessage,
                                Object input, Object output, Object appliedInputs,
                                java.util.List<String> unknownInputParameters, String failureKind,
                                String postureNote) {
            this(workflowExecutionId, nodeId, nodeName, status, exitCode, durationMs, language,
                    containerId, stdout, stderr, errorMessage, input, output, appliedInputs,
                    unknownInputParameters, failureKind, postureNote, null, null);
        }

        /** Back-compat 18-arg constructor (no retry flag). */
        public SandboxLogRecord(String workflowExecutionId, String nodeId, String nodeName,
                                String status, Integer exitCode, Long durationMs, String language,
                                String containerId, String stdout, String stderr, String errorMessage,
                                Object input, Object output, Object appliedInputs,
                                java.util.List<String> unknownInputParameters, String failureKind,
                                String postureNote, String logId) {
            this(workflowExecutionId, nodeId, nodeName, status, exitCode, durationMs, language,
                    containerId, stdout, stderr, errorMessage, input, output, appliedInputs,
                    unknownInputParameters, failureKind, postureNote, logId, null);
        }

        /** Copy carrying the local row id, made by the fan-out once the store write landed. */
        public SandboxLogRecord withLogId(String logId) {
            return new SandboxLogRecord(workflowExecutionId, nodeId, nodeName, status, exitCode,
                    durationMs, language, containerId, stdout, stderr, errorMessage, input, output,
                    appliedInputs, unknownInputParameters, failureKind, postureNote, logId, retry);
        }

        /** Copy marking whether this attempt was triggered by an operator {@code retry-incident}. */
        public SandboxLogRecord withRetry(Boolean retry) {
            return new SandboxLogRecord(workflowExecutionId, nodeId, nodeName, status, exitCode,
                    durationMs, language, containerId, stdout, stderr, errorMessage, input, output,
                    appliedInputs, unknownInputParameters, failureKind, postureNote, logId, retry);
        }
    }
}
