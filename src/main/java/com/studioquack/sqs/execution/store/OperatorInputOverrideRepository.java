package com.studioquack.sqs.execution.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data JPA repository for the {@code operator_input_override} table (encrypted-file H2 on
 * H2. The mapping is database-agnostic, like the
 * {@code execution_context} store).
 */
@Repository
public interface OperatorInputOverrideRepository
        extends JpaRepository<OperatorInputOverrideEntity, OperatorInputOverrideKey> {

    /** Retention sweep: drops every override of the given runs (same guarded batch as execution_context). */
    long deleteByWorkflowExecutionIdIn(List<String> workflowExecutionIds);
}
