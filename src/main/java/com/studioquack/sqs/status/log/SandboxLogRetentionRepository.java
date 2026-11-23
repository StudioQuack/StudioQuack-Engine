package com.studioquack.sqs.status.log;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

/**
 * The same per-run safety contract applies: callers pass ONLY run ids the engine has confirmed
 * terminated-and-aged ({@code SqsEngine.allInstancesTerminatedBefore}), so a live/parked run's
 * per-node logs are never purged no matter how old the. Queries are JPQL so the database dialect
 * stays outside this interface.
 */
public interface SandboxLogRetentionRepository extends Repository<SandboxLogEntity, String> {

    /**
     * Distinct {@code workflowExecutionId}s with at least one {@code sandbox_log} row created
     * strictly before {@code cutoff}. Candidate selection only — the terminated-and-aged engine
     * guard decides what is actually purged.
     */
    @Query("select distinct e.workflowExecutionId from SandboxLogEntity e "
            + "where e.workflowExecutionId is not null and e.cdate < :cutoff")
    List<String> findDistinctWorkflowExecutionIdsCreatedBefore(@Param("cutoff") OffsetDateTime cutoff);

    /**
     * Hard-delete every {@code sandbox_log} row belonging to the given (terminated-and-aged,
     * engine-confirmed) runs. LOCAL ONLY by construction: the control-plane copy in was written at
     * ship time and is the continuation of the record — pruning here can never touch it.
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("delete from SandboxLogEntity e where e.workflowExecutionId in :workflowExecutionIds")
    int deleteByWorkflowExecutionIdIn(@Param("workflowExecutionIds") Collection<String> workflowExecutionIds);
}
