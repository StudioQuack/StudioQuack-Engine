package com.studioquack.sqs.runtime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Spring Data JPA repository for the durable {@code dispatch_wait_state} table (Layer 2).
 * Supplies the boot-hydration and orphan-sweeper queries: pending rows from a previous daemon boot
 * (epoch mismatch) and pending rows that have been parked longer than the stale threshold.
 */
@Repository
public interface DispatchWaitStateRepository extends JpaRepository<DispatchWaitStateEntity, String> {

    /** All rows in a given status (boot hydration scans PENDING). */
    List<DispatchWaitStateEntity> findByStatus(String status);

    /** PENDING rows NOT stamped with the current boot epoch — the orphan-carry-over candidates. */
    List<DispatchWaitStateEntity> findByStatusAndClientEpochNot(String status, String clientEpoch);

    /** PENDING rows registered before {@code cutoff} — the periodic stale-sweep candidates. */
    List<DispatchWaitStateEntity> findByStatusAndRegisteredAtBefore(String status, OffsetDateTime cutoff);
}
