package com.studioquack.sqs.files;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

/** JPA access to the daemon-held files. Identical on both tiers; only the datasource differs. */
@Repository
public interface ClientFileRepository extends JpaRepository<ClientFileEntity, String> {

    List<ClientFileEntity> findByScopeAndScopeKey(String scope, String scopeKey);

    /** Reclaims everything for one run, or one workflow's superseded deployment. */
    long deleteByScopeAndScopeKey(String scope, String scopeKey);

    /** Batch-reclaims RUN-scoped files alongside terminated-run log purges. */
    long deleteByScopeAndScopeKeyIn(String scope, Collection<String> scopeKeys);

    /** Backstop sweep for rows whose owning run never reported a terminal state. */
    long deleteByCreatedAtBefore(OffsetDateTime cutoff);
}
