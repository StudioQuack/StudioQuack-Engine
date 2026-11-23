package com.studioquack.sqs.runtime;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Optional;

/**
 * Write-through helper for {@code dispatch_wait_state} whose mutations run in their OWN transaction
 * ({@link Propagation#REQUIRES_NEW}). This is the client port of the control plane
 * {@code daemon} pattern: the resume ({@code resolve}) and orphan
 * ({@code discard}) writes must survive even if the surrounding Activiti engine transaction that
 * triggered the ReceiveTask later rolls back — otherwise a rolled-back resume would silently lose
 * the "already delivered" fact and the token could be resumed twice.
 *
 * <p>It is a separate Spring bean (never self-invoked) so the {@code @Transactional} proxy actually
 * suspends the caller's transaction and opens a fresh one. {@code register} deliberately runs in the
 * CALLER's transaction (default propagation) so the row commits atomically with the parked token.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchWaitStateTxService {

    private final DispatchWaitStateRepository repository;

    /**
     * Inserts the PENDING wait-state row in the <b>caller's</b> transaction, so it commits atomically
     * with the engine transaction that advances the token to {@code wait_<nodeId>} — the row and the
     * parked token are always consistent on disk.
     */
    @Transactional
    public void insertPending(DispatchWaitStateEntity entity) {
        repository.save(entity);
    }

    /** Marks a row RESOLVED in a NEW transaction (resume delivered). Idempotent + rollback-proof. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markResolved(String dispatchId) {
        update(dispatchId, DispatchStatus.RESOLVED, null);
    }

    /** Marks a row DISCARDED in a NEW transaction (audited orphan). Idempotent. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markDiscarded(String dispatchId, String reason) {
        update(dispatchId, DispatchStatus.DISCARDED, reason);
    }

    private void update(String dispatchId, DispatchStatus status, String reason) {
        Optional<DispatchWaitStateEntity> found = repository.findById(dispatchId);
        if (found.isEmpty()) {
            log.debug("dispatch_wait_state.{} miss dispatchId={} (already terminal or never persisted)",
                    status, dispatchId);
            return;
        }
        DispatchWaitStateEntity row = found.get();
        if (!DispatchStatus.PENDING.name().equals(row.getStatus())) {
            return; // idempotent: only a PENDING row transitions to a terminal status
        }
        row.setStatus(status.name());
        row.setResolvedAt(OffsetDateTime.now());
        if (reason != null) {
            row.setErrorMessage(reason);
        }
        repository.save(row);
    }
}
