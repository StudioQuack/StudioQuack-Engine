package com.studioquack.sqs.execution.store;

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
 * JPA home of one execution-context <b>attempt</b>, keyed by {@link #nodeRefId}
 * ({@code node_<NanoId>}). One row per dispatch, addressed by {@code (executionId, nodeRefId)}.
 *
 * <p><b>REF-ONLY (Mapping Invariant 15).</b> The full node payload lives here (sealed);
 * only the tiny {@code studioquackRef} handle addressing this row ever enters the engine's
 * {@code ACT_*} variable tables.
 *
 * <p><b>At-rest sealing.</b> {@link #inputsJson}, {@link #outputsJson}, and
 * {@link #contextBlobJson} hold the AES-GCM-sealed envelope serialised as JSON text
 * ({@code {"$sqenc":"…","v":1}} for the blob, {@code [{"$sqenc":"…","v":1}]} for the
 * lists). The store is the single owner that seals on write / opens on read — no caller
 * ever sees ciphertext, and legacy/disabled plaintext rows round-trip unchanged.
 *
 * <p>DB-agnostic mappings ({@code text} columns, {@code OffsetDateTime}) so the same DDL is
 * generated on H2 — {@code ddl-auto: update} auto-creates the
 * table on either. Large text columns pin {@code columnDefinition = "text"}: H2 accepts
 * TEXT as a CHARACTER LARGE OBJECT alias and Postgres has it natively. Plain {@code @Lob}
 * is NOT used: on Postgres it maps to {@code oid} and every read/write then goes through
 * the Large Object API, which fails outside an explicit transaction
 * ("Unable to access lob stream" / "Large Objects may not be used in auto-commit mode").
 */
@Entity
@Table(name = "execution_context", indexes = {
        // Repair / latest-attempt lookup: "given an executionId, the most recent attempt at this flow node".
        @Index(name = "idx_exec_flow_revision", columnList = "executionId, bpmnFlowNodeId, revision"),
        // Live-row lookup: "given the studioquackRef read off the engine, load the source-of-truth blob".
        @Index(name = "idx_exec_node_ref", columnList = "executionId, nodeRefId"),
        @Index(name = "idx_exec_ctx_execution", columnList = "executionId"),
        // Retention / log-archive / log-delete: indexed per-run batch delete and time-window scans.
        @Index(name = "idx_exec_ctx_wf_exec_cdate", columnList = "workflowExecutionId, cdate"),
        @Index(name = "idx_exec_ctx_cdate", columnList = "cdate")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExecutionContextEntity {

    /** Stable per-attempt ref ({@code node_<NanoId>}); the value the {@code ProcessContextRef} carries. */
    @Id
    @Column(name = "node_ref_id", length = 64, nullable = false)
    private String nodeRefId;

    /** Identifies the overall execution (the run). */
    @Column(name = "execution_id", length = 128)
    private String executionId;

    /** Stable across retries / replays of the same logical execution. */
    @Column(name = "workflow_execution_id", length = 128)
    private String workflowExecutionId;

    /** BPMN flow node id this attempt is bound to (e.g. {@code node_abc123}). */
    @Column(name = "bpmn_flow_node_id", length = 128)
    private String bpmnFlowNodeId;

    /** Target env id; mirrors {@code ProcessContextRef.envId}. */
    @Column(name = "env_id", length = 128)
    private String envId;

    /**
     * Principal that owns this attempt. On this daemon the value is informational.
     */
    @Column(name = "owner_id", length = 128)
    private String ownerId;

    /** Workflow id this attempt belongs to. */
    @Column(name = "workflow_id", length = 128)
    private String workflowId;

    /** Attempt counter at this flow node; a retry mints a fresh row with revision+1. */
    @Column(name = "revision")
    private long revision;

    /** Per-attempt status name ({@code RUNNING} / {@code COMPLETED} / {@code FAILED}). */
    @Column(name = "status", length = 32)
    private String status;

    /**
     * Sealed inputs list, serialised as JSON text. Must be a large-object column (not the default
     * VARCHAR(32600) that {@code LONGVARCHAR} produced): the AES-GCM-sealed, base64 envelope of a
     * real node payload — or the full graph carried in a start-run seedContext — routinely exceeds
     * 32 KB. {@code columnDefinition = "text"} achieves that on both tiers (TEXT is a
     * CHARACTER LARGE OBJECT alias on H2, native on Postgres); plain {@code @Lob} is NOT
     * used because Postgres maps it to {@code oid} and every access then requires the
     * Large Object API inside an explicit transaction ("Unable to access lob stream").
     */
    @Column(name = "inputs_json", columnDefinition = "text")
    private String inputsJson;

    /** Sealed outputs list, serialised as JSON text. {@code text} for the same reason as inputs. */
    @Column(name = "outputs_json", columnDefinition = "text")
    private String outputsJson;

    /**
     * Sealed full-context snapshot blob, serialised as JSON text. {@code text}: the seedContext
     * blob (full graph + inbound values, sealed) is hundreds of KB, which overflowed the prior
     * VARCHAR(32600) with {@code Value too long for column "CONTEXT_BLOB_JSON"}.
     */
    @Column(name = "context_blob_json", columnDefinition = "text")
    private String contextBlobJson;

    /** Captured engine job key, for cross-referencing monitoring. */
    @Column(name = "job_key")
    private Long jobKey;

    @Column(name = "cdate")
    private OffsetDateTime cdate;

    @Column(name = "udate")
    private OffsetDateTime udate;
}
