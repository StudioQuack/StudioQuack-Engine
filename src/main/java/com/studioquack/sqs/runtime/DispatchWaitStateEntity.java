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
 * Durable row for one in-flight sandbox dispatch.
 * It records which ReceiveTask a dispatch id must resume after a restart.
 *
 * {@link #clientEpoch} is the boot UUID. Columns are routing ids, status, and timestamps — no payload.
 *
 * <p>{@link #processInstanceId}, {@link #businessKey}, {@link #nodeId} and {@link #waitActivityId}
 * extend the minimal plan schema: they are the routing ids the boot resumer needs to re-resolve the
 * still-parked execution (its id can shift across a restart, so we re-query by
 * {@code businessKey + waitActivityId}). They carry no PHI.
 */
@Entity
@Table(name = "dispatch_wait_state", indexes = {
        @Index(name = "idx_dws_status_epoch", columnList = "status, client_epoch"),
        @Index(name = "idx_dws_registered_at", columnList = "registered_at")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DispatchWaitStateEntity {

    /** Unique per dispatch (also the sandbox jobId + helper-channel bearer suffix). */
    @Id
    @Column(name = "dispatch_id", length = 64, nullable = false)
    private String dispatchId;

    /**
     * The candidate parked ReceiveTask execution id captured at register time. For a linear
     * single-token flow the ServiceTask execution IS the execution that parks at {@code wait_<node>},
     * so this is a strong hint the resumer verifies (and re-resolves by business key if it shifted).
     */
    @Column(name = "execution_id", length = 64, nullable = false)
    private String executionId;

    /** The owning process instance id (diagnostics + sweeper reconciliation). */
    @Column(name = "process_instance_id", length = 64)
    private String processInstanceId;

    /** The workflow-execution id (== PI business key) used to re-resolve the wait after restart. */
    @Column(name = "business_key", length = 64)
    private String businessKey;

    /** The flow node id being executed (diagnostics). */
    @Column(name = "node_id", length = 128)
    private String nodeId;

    /** The ReceiveTask activity id ({@code wait_<nodeId>}) the resumer triggers. */
    @Column(name = "wait_activity_id", length = 160)
    private String waitActivityId;

    /** Informational target environment id (single-tenant: replaces the control plane {@code tenant_id}). */
    @Column(name = "environment_id", length = 64)
    private String environmentId;

    /** The daemon boot epoch stamped at register time (UUID text — see class javadoc). */
    @Column(name = "client_epoch", length = 64, nullable = false)
    private String clientEpoch;

    /** PENDING | RESOLVED | DISCARDED. */
    @Column(name = "status", length = 16, nullable = false)
    private String status;

    @Column(name = "registered_at", nullable = false)
    private OffsetDateTime registeredAt;

    @Column(name = "resolved_at")
    private OffsetDateTime resolvedAt;

    /** Short, non-PHI failure / orphan-reason string (e.g. {@code orphaned across restart}). */
    @Column(name = "error_message", length = 1024)
    private String errorMessage;
}
