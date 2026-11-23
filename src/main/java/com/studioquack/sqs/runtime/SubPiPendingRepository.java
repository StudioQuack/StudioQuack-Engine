package com.studioquack.sqs.runtime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Spring Data JPA repository for the durable {@code sub_pi_pending} table (Layer 3). Supplies
 * the boot recover-by-fail query (pending children from a previous boot) and the terminal-row
 * retention sweep.
 */
@Repository
public interface SubPiPendingRepository extends JpaRepository<SubPiPendingEntity, String> {

    /** PENDING children NOT stamped with the current boot epoch — the recover-by-fail candidates. */
    List<SubPiPendingEntity> findByStatusAndClientEpochNot(String status, String clientEpoch);

    /** Terminal rows last touched before {@code cutoff} — the 7-day retention sweep candidates. */
    List<SubPiPendingEntity> findByStatusInAndUpdatedAtBefore(List<String> statuses, OffsetDateTime cutoff);
}
