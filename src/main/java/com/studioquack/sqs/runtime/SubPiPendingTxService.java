package com.studioquack.sqs.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

/**
 * Write-through helper for {@code sub_pi_pending} whose completion mutations run in their OWN
 * transaction ({@link Propagation#REQUIRES_NEW}) — the client port of the control plane
 * {@code daemon} pattern. When {@code SubProcessCompletionListener} fires
 * inside the engine callback, the {@code COMPLETED}/{@code FAILED} write must survive even if that
 * engine transaction later rolls back; otherwise the parent could be resumed while the store still
 * says PENDING (or vice-versa). {@code register} runs in the caller's transaction so the row is
 * present before the child sub-PI starts ("register before child start").
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SubPiPendingTxService {

    private final SubPiPendingRepository repository;
    private final ObjectMapper objectMapper;

    /** Inserts the PENDING child link in the caller's transaction (before the child sub-PI starts). */
    @Transactional
    public void insertPending(SubPiPendingEntity entity) {
        repository.save(entity);
    }

    /** Marks a child COMPLETED with WHITELISTED result vars, in a NEW transaction. Idempotent. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markCompleted(String childWorkflowExecutionId, Map<String, Object> resultVariables) {
        transition(childWorkflowExecutionId, SubPiStatus.COMPLETED, null,
                RefOnlyVariableGuard.filterToWhitelist(resultVariables));
    }

    /** Marks a child FAILED with a non-PHI reason, in a NEW transaction. Idempotent. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(String childWorkflowExecutionId, String errorMessage) {
        transition(childWorkflowExecutionId, SubPiStatus.FAILED, errorMessage, null);
    }

    /** Marks a child CANCELED, in a NEW transaction. Idempotent. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markCanceled(String childWorkflowExecutionId, String reason) {
        transition(childWorkflowExecutionId, SubPiStatus.CANCELED, reason, null);
    }

    private void transition(String childId, SubPiStatus status, String errorMessage,
                            Map<String, Object> whitelistedVars) {
        Optional<SubPiPendingEntity> found = repository.findById(childId);
        if (found.isEmpty()) {
            log.debug("sub_pi_pending.{} miss child={} (never registered or already swept)", status, childId);
            return;
        }
        SubPiPendingEntity row = found.get();
        if (!SubPiStatus.PENDING.name().equals(row.getStatus())) {
            return; // idempotent: only PENDING transitions to a terminal status
        }
        row.setStatus(status.name());
        row.setUpdatedAt(OffsetDateTime.now());
        if (errorMessage != null) {
            row.setErrorMessage(errorMessage);
        }
        if (whitelistedVars != null && !whitelistedVars.isEmpty()) {
            row.setResultVariables(writeJson(whitelistedVars));
        }
        repository.save(row);
    }

    private String writeJson(Map<String, Object> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            log.warn("sub_pi_pending result_variables serialise failed: {}", e.getMessage());
            return null;
        }
    }
}
