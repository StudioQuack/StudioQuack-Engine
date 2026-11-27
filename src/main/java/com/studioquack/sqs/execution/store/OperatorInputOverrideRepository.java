package com.studioquack.sqs.execution.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data JPA repository for {@code operator_input_override}. Bulk JPQL delete avoids loading
 * sealed JSON blobs into JVM heap during log purges.
 */
@Repository
public interface OperatorInputOverrideRepository
        extends JpaRepository<OperatorInputOverrideEntity, OperatorInputOverrideKey> {

    /** Retention sweep: bulk-deletes every override of the given runs without loading blobs into heap. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("delete from OperatorInputOverrideEntity o where o.workflowExecutionId in :ids")
    int deleteByWorkflowExecutionIdIn(@Param("ids") List<String> workflowExecutionIds);
}
