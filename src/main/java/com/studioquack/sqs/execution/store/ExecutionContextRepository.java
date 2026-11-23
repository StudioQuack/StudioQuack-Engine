package com.studioquack.sqs.execution.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for the encrypted {@code execution_context} table.
 * Lookups cover the live ref, retry history, the run seed, and the latest attempt at a node.
 */
@Repository
public interface ExecutionContextRepository extends JpaRepository<ExecutionContextEntity, String> {

    Optional<ExecutionContextEntity> findByNodeRefId(String nodeRefId);

    List<ExecutionContextEntity> findByExecutionIdOrderByRevisionAsc(String executionId);

    /** Replay seed lookup: the earliest attempt of the whole run (cdate, then revision). */
    Optional<ExecutionContextEntity> findFirstByExecutionIdOrderByCdateAscRevisionAsc(String executionId);

    Optional<ExecutionContextEntity> findFirstByExecutionIdAndBpmnFlowNodeIdOrderByRevisionDesc(
            String executionId, String bpmnFlowNodeId);

    long countByExecutionIdAndBpmnFlowNodeId(String executionId, String bpmnFlowNodeId);
}
