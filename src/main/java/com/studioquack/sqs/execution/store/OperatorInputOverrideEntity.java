package com.studioquack.sqs.execution.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * JPA home of one <b>operator input override</b> — the sealed inbound list an operator set via
 * DuckTales "Update parameters" / "Add parameter" ({@code apply-inputs}) for one failed node of
 * one run. Keyed by the natural pair ({@code workflow_execution_id}, {@code node_id}) so the
 * override outlives the per-attempt {@code execution_context} rows: a Retry re-dispatches the
 * node in the same run, and a Replay mints a NEW process instance under the SAME
 * {@code workflowExecutionId} business key — both find this row. This replaces the old
 * {@code execution_context.operator_inputs_applied} row-flag, which died with its row on replay.
 *
 * <p><b>One row, two lanes.</b> Besides the inputs the row also carries the operator's OUTPUT
 * override ({@link #outputsJson}) for Skip &amp; Continue; the class/table names keep the
 * original "input override" wording — renaming the table would forfeit the auto-generated DDL
 * continuity on existing installs.
 *
 * <p><b>PHI at rest.</b> {@link #inputsJson} holds the AES-GCM-sealed envelope serialised as JSON
 * text ({@code [{"$sqenc":"…","v":1}]}), exactly like {@code execution_context.inputs_json} — the
 * {@code OperatorInputOverrideStore} seals on write / opens on read with the same
 * {@code ExecutionContextCrypto}, and plaintext legacy rows (encryption disabled) round-trip
 * unchanged. Values never enter engine variables and are never logged.
 *
 * <p>DB-agnostic mappings ({@code text} column, {@code OffsetDateTime}, no {@code @Lob}) so the
 * the same DDL is generated on H2 — {@code ddl-auto: update} auto-creates
 * the table on either. See {@link ExecutionContextEntity} for why {@code text} and not
 * {@code @Lob} (Postgres oid / Large Object API pitfall).
 */
@Entity
@Table(name = "operator_input_override", indexes = {
        // Retention sweep: "delete every override of this batch of runs" joins on the run id.
        @Index(name = "idx_op_override_run", columnList = "workflow_execution_id")
})
@IdClass(OperatorInputOverrideKey.class)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OperatorInputOverrideEntity {

    /** The run this override belongs to (business key; stable across retry AND replay). */
    @Id
    @Column(name = "workflow_execution_id", length = 128, nullable = false)
    private String workflowExecutionId;

    /** BPMN flow node id the edited inputs bind to (e.g. {@code node_abc123}). */
    @Id
    @Column(name = "node_id", length = 128, nullable = false)
    private String nodeId;

    /**
     * Sealed override inputs list, serialised as JSON text. {@code text} for the same reason as
     * {@code execution_context.inputs_json}: a sealed real node payload routinely exceeds the
     * default VARCHAR. An empty sealed list is a VALID row — the operator cleared every input.
     */
    @Column(name = "inputs_json", columnDefinition = "text")
    private String inputsJson;

    /**
     * Sealed override OUTPUTS list — the DuckTales "Skip &amp; Continue" output edit. Same
     * envelope shape as {@link #inputsJson} but sealed under its OWN AAD ({@code run|node|out})
     * so an outputs blob can never be spliced into the inputs lane. NULLABLE: a row may carry
     * inputs only, outputs only, or both (latest-wins merges at the ROW level — writing one lane
     * never clears the other). Output overrides are consumed by the skip short-circuit ONLY; a
     * node that actually runs always produces its own outputs.
     */
    @Column(name = "outputs_json", columnDefinition = "text")
    private String outputsJson;

    @Column(name = "cdate")
    private OffsetDateTime cdate;

    @Column(name = "udate")
    private OffsetDateTime udate;
}
