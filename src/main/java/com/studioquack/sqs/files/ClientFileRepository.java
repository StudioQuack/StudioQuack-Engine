package com.studioquack.sqs.files;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
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

    /**
     * Bulk SQL delete for RUN-scoped files alongside terminated-run log purges.
     * Deliberately JPQL {@code @Modifying} (never a derived delete) so multi-GB
     * {@code content_envelope} blobs are NEVER loaded into JVM heap just to delete them.
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("delete from ClientFileEntity f where f.scope = :scope and f.scopeKey in :scopeKeys")
    int deleteByScopeAndScopeKeyIn(@Param("scope") String scope, @Param("scopeKeys") Collection<String> scopeKeys);

    /** Backstop sweep for rows whose owning run never reported a terminal state. */
    long deleteByCreatedAtBefore(OffsetDateTime cutoff);
}
