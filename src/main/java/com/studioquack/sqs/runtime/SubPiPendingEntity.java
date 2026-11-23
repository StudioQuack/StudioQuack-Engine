package com.studioquack.sqs.runtime;

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
 * Durable row for a function-port sub-process that is still in flight.
 * One row per {@code studioquack.execute} child sub-PI: it records the parent↔child link so that
 * if the daemon restarts while the parent is parked awaiting the child, the parent is never left
 * blocked forever. The in-memory {@code CompletableFuture} the caller awaits evaporates on restart;
 * this row lets boot recovery deterministically <b>recover-by-fail</b> the parent.
 *
 * Boot recovery uses the row to fail a parent that was still waiting when the daemon restarted.
 * {@link #clientEpoch} is the boot UUID. Terminal rows are dropped after 7 days.
 *
 * <p><b>REF-ONLY.</b> {@link #resultVariables} stores ONLY whitelisted, non-PHI routing/ref tokens
 * ({@code RefOnlyVariableGuard.filterToWhitelist}) as JSON text — never the child's raw PHI payload.
 */
@Entity
@Table(name = "sub_pi_pending", indexes = {
        @Index(name = "idx_subpi_status_epoch", columnList = "status, client_epoch"),
        @Index(name = "idx_subpi_updated_at", columnList = "updated_at")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubPiPendingEntity {

    /** The child workflow-execution id (== child PI business key). Primary key. */
    @Id
    @Column(name = "child_workflow_execution_id", length = 64, nullable = false)
    private String childWorkflowExecutionId;

    /** The parent workflow-execution id (the run the function-port belongs to). */
    @Column(name = "parent_workflow_execution_id", length = 64)
    private String parentWorkflowExecutionId;

    /** The Activiti execution id of the parent, parked awaiting the child (recover-by-fail target). */
    @Column(name = "parent_execution_id", length = 64)
    private String parentExecutionId;

    /** Informational target environment id (single-tenant: replaces the control plane {@code tenant_id}). */
    @Column(name = "environment_id", length = 64)
    private String environmentId;

    /** The child sub-process definition key ({@code Process_SubProc_<root>}). */
    @Column(name = "bpmn_process_id", length = 191)
    private String bpmnProcessId;

    /** The daemon boot epoch stamped at register time (UUID text — see class javadoc). */
    @Column(name = "client_epoch", length = 64, nullable = false)
    private String clientEpoch;

    /** PENDING | COMPLETED | FAILED | CANCELED. */
    @Column(name = "status", length = 16, nullable = false)
    private String status;

    /** Whitelisted (non-PHI) result variables as JSON text — REF-ONLY, may be null/empty. */
    @Column(name = "result_variables", columnDefinition = "text")
    private String resultVariables;

    /** Short, non-PHI failure / recovery-reason string, or null. */
    @Column(name = "error_message", length = 1024)
    private String errorMessage;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;
}
