package com.studioquack.sqs.execution.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
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
 * JPA home of one function-port {@code studioquack.execute} invocation under a parent run.
 * Auto-increment {@link #id} is the invocation order (monotonic). {@code ddl-auto: update}
 * creates {@code sub_pi_invocation} on H2.
 *
 * <p><b>REF-ONLY.</b> Columns are identifiers only — never payloads. The sealed
 * {@code execution_context} store remains the I/O carrier.
 *
 * <p>DB-agnostic mappings ({@code OffsetDateTime}, explicit {@code @Column} names, no {@code @Lob})
 * so the same DDL is generated on either tier.
 */
@Entity
@Table(name = "sub_pi_invocation", indexes = {
        @Index(name = "idx_sub_pi_inv_parent", columnList = "parentExecutionId")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubPiInvocationEntity {

    /** Monotonic invocation order; IDENTITY so persist order == query order. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    /** Parent run (workflow execution id) that issued {@code studioquack.execute}. */
    @Column(name = "parent_execution_id", length = 128)
    private String parentExecutionId;

    /** Child run minted for the function-port sub-PI. */
    @Column(name = "child_execution_id", length = 128)
    private String childExecutionId;

    /** Caller node on the parent graph (the function-port owner). */
    @Column(name = "caller_node_id", length = 128)
    private String callerNodeId;

    /** Root node of the invoked sub-PI ({@code Process_SubProc_<this>}). */
    @Column(name = "sub_pi_root_node_id", length = 128)
    private String subPiRootNodeId;

    @Column(name = "cdate")
    private OffsetDateTime cdate;
}
