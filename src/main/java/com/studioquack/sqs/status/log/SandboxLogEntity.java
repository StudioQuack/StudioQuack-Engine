package com.studioquack.sqs.status.log;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * One per-node sandbox log row stored in {@code sandbox_log}. Stream text is already redacted
 * before it is saved.
 */
@Entity
@Table(name = "sandbox_log", indexes = {
        // Retention candidate selection: distinct runs with any row older than the cutoff.
        @Index(name = "idx_sandbox_log_run_cdate", columnList = "workflowExecutionId, cdate"),
        // Per-node read (local diagnostics / archive export ordering).
        @Index(name = "idx_sandbox_log_run_node", columnList = "workflowExecutionId, nodeId")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SandboxLogEntity {

    /** Stable row id ({@code slog_<NanoId>}); generated at write time, never reused. */
    @Id
    @Column(name = "id", length = 64, nullable = false)
    private String id;

    /** The run this node belongs to (Activiti business key; a function-port sub-PI keeps its child id). */
    @Column(name = "workflow_execution_id", length = 128, nullable = false)
    private String workflowExecutionId;

    @Column(name = "node_id", length = 128, nullable = false)
    private String nodeId;

    @Column(name = "node_name", length = 512)
    private String nodeName;

    /** {@code STARTED} / {@code COMPLETED} / {@code FAILED}. */
    @Column(name = "status", length = 32, nullable = false)
    private String status;

    @Column(name = "exit_code")
    private Integer exitCode;

    @Column(name = "duration_ms")
    private Long durationMs;

    @Column(name = "language", length = 64)
    private String language;

    @Column(name = "container_id", length = 128)
    private String containerId;

    /** Redacted, stream-capped node stdout. Null for boundary/system records. */
    @Column(name = "stdout", columnDefinition = "text")
    private String stdout;

    /** Redacted, stream-capped node stderr. */
    @Column(name = "stderr", columnDefinition = "text")
    private String stderr;

    /** Redacted, coarse failure diagnostic (never a raw stack trace). */
    @Column(name = "error_message", columnDefinition = "text")
    private String errorMessage;

    /** Node input list serialised as JSON ({@code sq-file:} handles, never hydrated file bytes). */
    @Column(name = "input_json", columnDefinition = "text")
    private String inputJson;

    /** Node output list serialised as JSON (handles, not bytes — same rule as {@link #inputJson}). */
    @Column(name = "output_json", columnDefinition = "text")
    private String outputJson;

    /** The inputs actually applied to the sandbox, serialised as JSON. */
    @Column(name = "applied_inputs_json", columnDefinition = "text")
    private String appliedInputsJson;

    /**
     * Dotted {@code portShortCode.paramName} entries dropped by inbound port-schema filtering
     * because they were outside the target port's declared schema — serialised as JSON array.
     */
    @Column(name = "unknown_input_parameters_json", columnDefinition = "text")
    private String unknownInputParametersJson;

    /** Coarse failure classification mirroring {@code SandboxJobResponse.FailureKind}; {@code NONE} on success. */
    @Column(name = "failure_kind", length = 32)
    private String failureKind;

    /** Set only when the node ran while host overlay-egress enforcement was
{@code DEGRADED}. */
    @Column(name = "posture_note", length = 1024)
    private String postureNote;

    /** True only when this attempt was dispatched by an operator {@code retry-incident} repair. */
    @Column(name = "is_retry")
    private Boolean retry;

    @Column(name = "cdate", nullable = false)
    private OffsetDateTime cdate;
}
