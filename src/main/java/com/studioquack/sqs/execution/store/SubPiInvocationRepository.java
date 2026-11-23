package com.studioquack.sqs.execution.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;

/**
 * Spring Data JPA repository for {@code sub_pi_invocation}. Query methods stay DB-agnostic
 * (JPQL) so the query does not depend on a database dialect.
 *
 * <p>Retention deletes rows that reference a purged run as <b>parent or child</b> so the
 * index never outlives the {@code execution_context} rows it points at.
 */
@Repository
public interface SubPiInvocationRepository extends JpaRepository<SubPiInvocationEntity, Long> {

    List<SubPiInvocationEntity> findByParentExecutionIdOrderByIdAsc(String parentExecutionId);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("delete from SubPiInvocationEntity e where e.parentExecutionId in :ids or e.childExecutionId in :ids")
    int deleteByParentOrChildExecutionIdIn(@Param("ids") Collection<String> ids);
}
