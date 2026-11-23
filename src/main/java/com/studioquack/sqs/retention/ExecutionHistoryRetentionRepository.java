package com.studioquack.sqs.retention;

import com.studioquack.sqs.execution.store.ExecutionContextEntity;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

/**
 * DB-agnostic queries for on-client execution-history retention. Retention is enforced per
 * workflow run (a terminated instance's finished history), NEVER blindly by row age.
 */
public interface ExecutionHistoryRetentionRepository extends Repository<ExecutionContextEntity, String> {

    /**
     * Distinct {@code workflowExecutionId}s (= Activiti business keys) that have at least one
     * {@code execution_context} row created strictly before {@code cutoff}.
     *
     * @param cutoff
     * @return Candidate run ids (never null; may be empty)
     */
    @Query("select distinct e.workflowExecutionId from ExecutionContextEntity e "
            + "where e.workflowExecutionId is not null and e.cdate < :cutoff")
    List<String> findDistinctWorkflowExecutionIdsCreatedBefore(@Param("cutoff") OffsetDateTime cutoff);

    /**
     * Hard-delete every {@code execution_context} row belonging to the given runs. Callers pass ONLY
     * run ids the engine has confirmed terminated-and-aged, so this never touches a live run's rows.
     *
     * @param workflowExecutionIds The terminated-and-aged run ids to purge
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("delete from ExecutionContextEntity e where e.workflowExecutionId in :workflowExecutionIds")
    int deleteByWorkflowExecutionIdIn(@Param("workflowExecutionIds") Collection<String> workflowExecutionIds);
}
