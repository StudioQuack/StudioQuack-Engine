package com.studioquack.sqs.runtime;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.BeRuntimeEpoch;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable function-port sub-process completion registry (, Layer 3) — the client port
 * of the control plane {@code ActivitiSubProcessCompletionRegistry}. A {@code studioquack.execute} call
 * starts a child sub-PI ({@code Process_SubProc_<root>}) and the caller parks awaiting its result;
 * this registry holds the caller's {@link CompletableFuture} in memory <b>and</b> a durable
 * {@code sub_pi_pending} row so a restart mid-wait never blocks the parent forever.
 *
 * <p><b>register before child start.</b> {@link #register} persists PENDING in the caller's
 * transaction and creates the future <em>before</em> the child sub-PI is started, so the completion
 * listener can never fire before the row/future exist.
 *
 * <p><b>Recover-by-fail (honour {@code BeRuntimeEpoch}).</b> On boot, PENDING rows from a previous
 * daemon epoch are NEVER resumed (the orphaned child PI is rejected by the epoch guard); instead
 * {@link #hydrateOnBoot} marks the row FAILED ({@code ORPHANED_SUBPI_RECOVERY}) and, when the
 * parent is still parked, fails the parent execution via {@link EngineTriggerService#fail} so the
 * run reaches its error end deterministically — the parent future is never left hanging.
 *
 * <p>Feature flag {@code sqs.sub-pi.durable} (default {@code true}); REF-ONLY result vars only.
 */
@Slf4j
@Component
public class SubProcessCompletionRegistry {

    /** Error code raised on the parent when an orphaned child is recovered-by-fail. */
    public static final String ORPHAN_ERROR_CODE = "SUB_PI_ORPHANED";
    /** Audited outcome/reason recorded on the {@code sub_pi_pending} row for an orphan recovery. */
    public static final String ORPHAN_RECOVERY_REASON = "ORPHANED_SUBPI_RECOVERY";

    /** One parked function-port call. All fields are non-PHI routing ids. */
    public record SubPiLink(String childWorkflowExecutionId,
                            String parentWorkflowExecutionId,
                            String parentExecutionId,
                            String environmentId,
                            String bpmnProcessId,
                            String clientEpoch) {
    }

    private final Map<String, CompletableFuture<Map<String, Object>>> futures = new ConcurrentHashMap<>();

    /**
 * Reporting-only child → parent run-id map so the daemon can attribute a function-port sub-PI's
 * per-node sandbox logs to the top-level run the control plane actually registered, and recognise (then
 * suppress) a sub-PI's own run-status callbacks. A sub-PI runs under a fresh {@code childWfeId}
 * that the control plane never created an {@code ExecutionHistory} for, so posting its node logs / status
 * under that id 404s ({@code Error_RunNotFound}) and the provider node's sandbox log is dropped.
 * Populated in {@link #register} BEFORE the child starts.
 *
 * <p>Deliberately NOT cleared on {@link #complete}/{@link #fail}/{@link #cancel}: the child's
 * terminal run-status callback fires just AFTER completion, so the entry must outlive it. Bounded
 * by insertion-order eviction ({@link #REPORTABLE_MAP_CAP}) so it can never grow without limit.
 */
    private static final int REPORTABLE_MAP_CAP = 20_000;
    private final Map<String, String> childToParentRunId = Collections.synchronizedMap(
            new LinkedHashMap<>(1024, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > REPORTABLE_MAP_CAP;
                }
            });

    private final SubPiPendingRepository repository;
    private final SubPiPendingTxService txService;
    private final BeRuntimeEpoch beRuntimeEpoch;
    private final EngineTriggerService engineTriggerService;
    private final SqsEngine sqsEngine;
    private final MeterRegistry meterRegistry;
    private final boolean durable;

    @Autowired
    public SubProcessCompletionRegistry(SubPiPendingRepository repository,
                                        SubPiPendingTxService txService,
                                        BeRuntimeEpoch beRuntimeEpoch,
                                        EngineTriggerService engineTriggerService,
                                        SqsEngine sqsEngine,
                                        MeterRegistry meterRegistry,
                                        @Value("${sqs.sub-pi.durable:true}") boolean durable) {
        this.repository = repository;
        this.txService = txService;
        this.beRuntimeEpoch = beRuntimeEpoch;
        this.engineTriggerService = engineTriggerService;
        this.sqsEngine = sqsEngine;
        this.meterRegistry = meterRegistry;
        this.durable = durable;
    }

    private boolean durablyEnabled() {
        return durable && repository != null && txService != null;
    }

    private String currentEpoch() {
        return beRuntimeEpoch == null ? "" : beRuntimeEpoch.current();
    }

    // ---------------------------------------------------------------------
    // Register + resolve lifecycle.
    // ---------------------------------------------------------------------

    /**
     * Registers a child sub-PI BEFORE it is started: persists PENDING (caller transaction) and
     * creates the caller's completion future. Returns that future for the caller to await.
     */
    public CompletableFuture<Map<String, Object>> register(SubPiLink link) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();
        futures.put(link.childWorkflowExecutionId(), future);
        if (link.childWorkflowExecutionId() != null && link.parentWorkflowExecutionId() != null) {
            childToParentRunId.put(link.childWorkflowExecutionId(), link.parentWorkflowExecutionId());
        }
        if (durablyEnabled()) {
            try {
                txService.insertPending(toEntity(link));
            } catch (RuntimeException e) {
                log.warn("sub_pi_pending.insert failed child={}: {}",
                        link.childWorkflowExecutionId(), e.toString());
            }
        }
        log.debug("sub_pi.register child={} parent={} parentExec={} durable={}",
                link.childWorkflowExecutionId(), link.parentWorkflowExecutionId(),
                link.parentExecutionId(), durablyEnabled());
        return future;
    }

    /** @return the caller's completion future for {@code childWorkflowExecutionId}, if registered. */
    public Optional<CompletableFuture<Map<String, Object>>> future(String childWorkflowExecutionId) {
        return Optional.ofNullable(futures.get(childWorkflowExecutionId));
    }

    /** @return {@code true} when {@code workflowExecutionId} is a known function-port sub-PI child. */
    public boolean isSubPiChild(String workflowExecutionId) {
        return workflowExecutionId != null && childToParentRunId.containsKey(workflowExecutionId);
    }

    /**
 * Resolves the top-level run id the control plane actually registered for {@code workflowExecutionId}: walks
 * the sub-PI child → parent chain (handles nested function-port sub-PIs) and returns the outermost
 * run. Returns the input unchanged when it is already a top-level run (the common case) or when the
 * id is unknown. Bounded hop count guards against any accidental cycle.
 */
    public String resolveReportableRunId(String workflowExecutionId) {
        if (workflowExecutionId == null) {
            return null;
        }
        String current = workflowExecutionId;
        for (int hops = 0; hops < 32; hops++) {
            String parent = childToParentRunId.get(current);
            if (parent == null || parent.equals(current)) {
                return current;
            }
            current = parent;
        }
        return current;
    }

    /**
     * Re-entrancy probe for the stateful FIFO: {@code true} when {@code workflowExecutionId} is a
     * function-port sub-PI child whose child → parent chain passes through ANY run id in
     * {@code ancestorRunIds} (e.g. the run ids currently holding a permit on a stateful instance).
     * A top-level run (no link) never matches — sibling dispatches of one run are not re-entrant.
     * Same bounded hop count as {@link #resolveReportableRunId}.
     */
    public boolean hasAncestorIn(String workflowExecutionId, java.util.Set<String> ancestorRunIds) {
        if (workflowExecutionId == null || ancestorRunIds == null || ancestorRunIds.isEmpty()) {
            return false;
        }
        String current = workflowExecutionId;
        for (int hops = 0; hops < 32; hops++) {
            String parent = childToParentRunId.get(current);
            if (parent == null || parent.equals(current)) {
                return false;
            }
            if (ancestorRunIds.contains(parent)) {
                return true;
            }
            current = parent;
        }
        return false;
    }

    /**
     * Resolves the child successfully: records WHITELISTED result vars (durable, REQUIRES_NEW) and
     * completes the caller future. Idempotent.
     */
    public void complete(String childWorkflowExecutionId, Map<String, Object> resultVariables) {
        if (durablyEnabled()) {
            try {
                txService.markCompleted(childWorkflowExecutionId, resultVariables);
            } catch (RuntimeException e) {
                log.warn("sub_pi_pending.complete failed child={}: {}", childWorkflowExecutionId, e.toString());
            }
        }
        CompletableFuture<Map<String, Object>> f = futures.remove(childWorkflowExecutionId);
        if (f != null) {
            f.complete(resultVariables == null ? Map.of() : resultVariables);
        }
        log.debug("sub_pi.complete child={}", childWorkflowExecutionId);
    }

    /** Resolves the child as a failure: durable FAILED + fails the caller future. Idempotent. */
    public void fail(String childWorkflowExecutionId, String errorMessage) {
        if (durablyEnabled()) {
            try {
                txService.markFailed(childWorkflowExecutionId, errorMessage);
            } catch (RuntimeException e) {
                log.warn("sub_pi_pending.fail failed child={}: {}", childWorkflowExecutionId, e.toString());
            }
        }
        CompletableFuture<Map<String, Object>> f = futures.remove(childWorkflowExecutionId);
        if (f != null) {
            f.completeExceptionally(new SubProcessFailedException(
                    errorMessage == null ? "sub-process failed" : errorMessage));
        }
        log.debug("sub_pi.fail child={} reason={}", childWorkflowExecutionId, errorMessage);
    }

    /** Marks the child CANCELED (run cancelled) + cancels the caller future. Idempotent. */
    public void cancel(String childWorkflowExecutionId, String reason) {
        if (durablyEnabled()) {
            try {
                txService.markCanceled(childWorkflowExecutionId, reason);
            } catch (RuntimeException e) {
                log.warn("sub_pi_pending.cancel failed child={}: {}", childWorkflowExecutionId, e.toString());
            }
        }
        CompletableFuture<Map<String, Object>> f = futures.remove(childWorkflowExecutionId);
        if (f != null) {
            f.cancel(true);
        }
    }

    /** @return number of sub-process futures currently awaiting resolution in memory. */
    public int pending() {
        return futures.size();
    }

    // ---------------------------------------------------------------------
    // Boot recover-by-fail (crash carry-over).
    // ---------------------------------------------------------------------

    /**
     * Recovers PENDING children left by a previous daemon boot (epoch mismatch): each is marked
     * FAILED ({@code ORPHANED_SUBPI_RECOVERY}) and — when the parent execution is still parked — the
     * parent is failed via {@link EngineTriggerService#fail} so the run reaches its error end. The
     * orphaned child sub-PI is <b>never</b> resumed (the {@code BeRuntimeEpoch} guard rejects it).
     *
     * @return the number of children recovered-by-fail.
     */
    public int hydrateOnBoot() {
        if (!durablyEnabled()) {
            return 0;
        }
        String epoch = currentEpoch();
        List<SubPiPendingEntity> carried;
        try {
            carried = repository.findByStatusAndClientEpochNot(SubPiStatus.PENDING.name(), epoch);
        } catch (RuntimeException e) {
            log.warn("sub_pi_pending hydrate query failed: {}", e.toString());
            return 0;
        }
        int recovered = 0;
        for (SubPiPendingEntity row : carried) {
            txService.markFailed(row.getChildWorkflowExecutionId(), ORPHAN_RECOVERY_REASON);
            // Never resume the orphaned child. Fail the parent (recover-by-fail) if it is still parked.
            String parentExec = row.getParentExecutionId();
            if (parentExec != null && sqsEngine != null && sqsEngine.isExecutionActive(parentExec)) {
                try {
                    engineTriggerService.fail(parentExec, ORPHAN_ERROR_CODE,
                            "function sub-process orphaned across daemon restart");
                } catch (RuntimeException e) {
                    log.warn("sub_pi recover-by-fail: parent trigger failed parentExec={}: {}",
                            parentExec, e.toString());
                }
            }
            recovered++;
            log.warn("sub_pi.recover-by-fail child={} parent={} parentExec={} (epoch carry-over)",
                    row.getChildWorkflowExecutionId(), row.getParentWorkflowExecutionId(), parentExec);
        }
        increment("sq.sub_pi.orphan_recovered", recovered);
        if (recovered > 0) {
            log.info("sub_pi_pending recover-by-fail complete: recovered={} (epoch={})", recovered, epoch);
        }
        return recovered;
    }

    private SubPiPendingEntity toEntity(SubPiLink link) {
        OffsetDateTime now = OffsetDateTime.now();
        return SubPiPendingEntity.builder()
                .childWorkflowExecutionId(link.childWorkflowExecutionId())
                .parentWorkflowExecutionId(link.parentWorkflowExecutionId())
                .parentExecutionId(link.parentExecutionId())
                .environmentId(link.environmentId())
                .bpmnProcessId(link.bpmnProcessId())
                .clientEpoch(link.clientEpoch() == null ? currentEpoch() : link.clientEpoch())
                .status(SubPiStatus.PENDING.name())
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private void increment(String metric, int amount) {
        if (meterRegistry != null && amount > 0) {
            Counter.builder(metric).register(meterRegistry).increment(amount);
        }
    }

    /** Raised into the caller future when a sub-process resolves as a failure. */
    public static final class SubProcessFailedException extends RuntimeException {
        public SubProcessFailedException(String message) {
            super(message);
        }
    }
}
