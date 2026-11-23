package com.studioquack.sqs.status.log;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Data access for the local {@code sandbox_log} store. Queries are JPQL so the database dialect
 * stays outside this interface.
 */
public interface SandboxLogRepository extends Repository<SandboxLogEntity, String> {

    /** Append one node record. Insert-only in practice — retention deletes by run, never updates. */
    SandboxLogEntity save(SandboxLogEntity entity);

    /** Test/diagnostic hook: rows held locally for one run. */
    long countByWorkflowExecutionId(String workflowExecutionId);

    /** Test/diagnostic hook: every local row for one run, oldest first. */
    List<SandboxLogEntity> findByWorkflowExecutionIdOrderByCdateAsc(String workflowExecutionId);

    /** Bounded reverse-channel pull: rows strictly after {@code since}, oldest first. */
    @Query("select e from SandboxLogEntity e where e.cdate > :since order by e.cdate asc")
    List<SandboxLogEntity> findAfter(@Param("since") OffsetDateTime since,
                                     org.springframework.data.domain.Pageable pageable);

    /** Bounded reverse-channel pull when no cursor is supplied: newest rows first. */
    @Query("select e from SandboxLogEntity e order by e.cdate desc")
    List<SandboxLogEntity> findRecent(org.springframework.data.domain.Pageable pageable);
}
