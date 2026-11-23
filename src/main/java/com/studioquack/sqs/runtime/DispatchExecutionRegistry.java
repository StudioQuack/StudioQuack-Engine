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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps a dispatch id to the ReceiveTask it must resume.
 * Every register, complete, and discard is
 * write-through to {@code dispatch_wait_state} on H2 so a {@code kill -9} between
 * "token parked at {@code wait_<node>}" and "sandbox result delivered" is recoverable on the next
 * boot instead of leaving the token parked forever.
 *
 * <p><b>Two layers.</b> The in-memory {@link #byDispatchId} map is the hot correlation used on the
 * happy path (the resumer already holds the {@link WaitState} in memory from {@code register}). The
 * durable row is the crash backstop: {@link #hydrateOnBoot} re-loads PENDING rows from a previous
 * boot (epoch mismatch) whose Activiti execution is still parked, so a re-delivered result resumes
 * the token; rows whose execution is gone/terminal are DISCARDED as audited orphans.
 *
 * <p><b>Transaction model.</b> {@code register} persists in the CALLER's transaction (so the row
 * commits atomically with the engine transaction that advances the token), while {@code complete}
 * and {@code discard} write in a NEW transaction ({@link DispatchWaitStateTxService}, REQUIRES_NEW)
 * so an engine-transaction rollback of the resume can never lose the "already delivered" fact.
 *
 * <p><b>Feature flag.</b> {@code sqs.dispatch-registry.durable} (default {@code true}). When off (or
 * when constructed via the no-arg constructor for unit tests) the registry is pure in-memory, i.e.
 * the in-memory behaviour. Only non-payload routing ids are stored.
 */
@Slf4j
@Component
public class DispatchExecutionRegistry {

    /**
     * One parked (or soon-to-park) sandbox dispatch. All fields are non-PHI routing tokens.
     *
     * @param dispatchId        unique per dispatch (also the sandbox jobId + helper-channel bearer)
     * @param executionId       the candidate parked ReceiveTask execution id captured at register
     * @param processInstanceId the owning process instance id (diagnostics / re-resolution)
     * @param businessKey       the workflow-execution id (== PI business key) used to re-resolve
     * @param nodeId            the flow node id being executed
     * @param waitActivityId    the ReceiveTask activity id ({@code wait_<nodeId>}) the resumer triggers
     * @param environmentId     informational target environment id (single-tenant)
     * @param clientEpoch       the daemon boot epoch stamped at register time
     * @param registeredAt      registration wall-clock (orphan-age accounting)
     */
    public record WaitState(String dispatchId,
                            String executionId,
                            String processInstanceId,
                            String businessKey,
                            String nodeId,
                            String waitActivityId,
                            String environmentId,
                            String clientEpoch,
                            OffsetDateTime registeredAt) {
    }

    private final Map<String, WaitState> byDispatchId = new ConcurrentHashMap<>();

    private final DispatchWaitStateRepository repository;
    private final DispatchWaitStateTxService txService;
    private final BeRuntimeEpoch beRuntimeEpoch;
    private final SqsEngine sqsEngine;
    private final MeterRegistry meterRegistry;
    private final boolean durable;

    /** Full DI constructor (production). */
    @Autowired
    public DispatchExecutionRegistry(DispatchWaitStateRepository repository,
                                     DispatchWaitStateTxService txService,
                                     BeRuntimeEpoch beRuntimeEpoch,
                                     SqsEngine sqsEngine,
                                     MeterRegistry meterRegistry,
                                     @Value("${sqs.dispatch-registry.durable:true}") boolean durable) {
        this.repository = repository;
        this.txService = txService;
        this.beRuntimeEpoch = beRuntimeEpoch;
        this.sqsEngine = sqsEngine;
        this.meterRegistry = meterRegistry;
        this.durable = durable;
    }

    /** In-memory-only constructor for focused unit tests (no durability, no engine). */
    public DispatchExecutionRegistry() {
        this(null, null, null, null, null, false);
    }

    private boolean durablyEnabled() {
        return durable && repository != null && txService != null;
    }

    private String currentEpoch() {
        return beRuntimeEpoch == null ? "" : beRuntimeEpoch.current();
    }

    // ---------------------------------------------------------------------
    // Registration + resume lifecycle (write-through).
    // ---------------------------------------------------------------------

    /**
     * Registers a dispatch's wait-state BEFORE the delegate returns (and therefore before the token
     * parks and before the sandbox result can resume it). The durable row is written in the caller's
     * transaction so it commits atomically with the token parking at {@code wait_<nodeId>}.
     */
    public WaitState register(String dispatchId, String executionId, String processInstanceId,
                              String businessKey, String nodeId, String waitActivityId,
                              String environmentId) {
        WaitState state = new WaitState(dispatchId, executionId, processInstanceId, businessKey,
                nodeId, waitActivityId, environmentId, currentEpoch(), OffsetDateTime.now());
        byDispatchId.put(dispatchId, state);
        if (durablyEnabled()) {
            try {
                txService.insertPending(toEntity(state));
            } catch (RuntimeException e) {
                // Durability is best-effort at register time — the in-memory entry still resumes the
                // happy path; the orphan sweeper is the backstop. Never fail the dispatch on a write.
                log.warn("dispatch_wait_state.insert failed dispatchId={}: {}", dispatchId, e.toString());
            }
        }
        log.debug("dispatch.register dispatchId={} exec={} pi={} businessKey={} node={} wait={} durable={}",
                dispatchId, executionId, processInstanceId, businessKey, nodeId, waitActivityId,
                durablyEnabled());
        return state;
    }

    /** Convenience overload (in-memory-only paths / tests): executionId defaults to businessKey. */
    public WaitState register(String dispatchId, String processInstanceId, String businessKey,
                              String nodeId, String waitActivityId) {
        return register(dispatchId, businessKey, processInstanceId, businessKey, nodeId, waitActivityId, null);
    }

    /** Looks up a wait-state by its dispatch id (in-memory). */
    public Optional<WaitState> find(String dispatchId) {
        return Optional.ofNullable(byDispatchId.get(dispatchId));
    }

    /**
     * Marks the dispatch RESOLVED (durable, REQUIRES_NEW) and removes the in-memory entry once its
     * ReceiveTask has been resumed. Idempotent: a second call returns empty and is a no-op.
     */
    public Optional<WaitState> complete(String dispatchId) {
        WaitState removed = byDispatchId.remove(dispatchId);
        if (durablyEnabled()) {
            try {
                txService.markResolved(dispatchId);
            } catch (RuntimeException e) {
                log.warn("dispatch_wait_state.resolve failed dispatchId={}: {}", dispatchId, e.toString());
            }
        }
        return Optional.ofNullable(removed);
    }

    /** Marks the dispatch DISCARDED (durable, audited orphan) and removes the in-memory entry. */
    public Optional<WaitState> discard(String dispatchId, String reason) {
        WaitState removed = byDispatchId.remove(dispatchId);
        if (durablyEnabled()) {
            try {
                txService.markDiscarded(dispatchId, reason);
            } catch (RuntimeException e) {
                log.warn("dispatch_wait_state.discard failed dispatchId={}: {}", dispatchId, e.toString());
            }
        }
        return Optional.ofNullable(removed);
    }

    /** @return number of dispatches currently awaiting resume in memory (dashboard / test assertion). */
    public int pending() {
        return byDispatchId.size();
    }

    /**
     * Process instances with a node in flight: a dispatch registered here and not yet resumed or
     * discarded, rows carried over from the previous boot included. Designer Runs count like
     * deployed workflows. This is what an in-place update kills before it hands over.
     */
    public Set<String> processInstancesInFlight() {
        Set<String> out = new LinkedHashSet<>();
        for (WaitState state : byDispatchId.values()) {
            String processInstanceId = state.processInstanceId();
            if (processInstanceId != null && !processInstanceId.isBlank()) {
                out.add(processInstanceId);
            }
        }
        return out;
    }

    /**
     * Discards every wait-state of {@code processInstanceId} — its run was killed, so no result may
     * resume it and the next boot must not hydrate it. Durable rows read {@code reason}.
     *
     * @return how many dispatches were discarded
     */
    public int discardRun(String processInstanceId, String reason) {
        if (processInstanceId == null || processInstanceId.isBlank()) {
            return 0;
        }
        int discarded = 0;
        for (WaitState state : List.copyOf(byDispatchId.values())) {
            if (processInstanceId.equals(state.processInstanceId())) {
                discard(state.dispatchId(), reason);
                discarded++;
            }
        }
        return discarded;
    }

    // ---------------------------------------------------------------------
    // Boot hydration (crash carry-over recovery).
    // ---------------------------------------------------------------------

    /**
     * Re-establishes the in-memory registry from durable rows left PENDING by a previous daemon boot
     * (epoch mismatch). A row is <b>kept</b> (re-loaded, {@code sq.dispatch_registry.hydrated}) only
     * when its Activiti execution is still parked at {@code wait_<node>} — so a re-delivered sandbox
     * result can resume it. A row whose execution is gone/terminal is <b>DISCARDED</b> as an audited
     * orphan ({@code sq.dispatch_registry.orphaned}); it is never resumed (would be a phantom token).
     *
     * <p>Safe to call more than once (tests invoke it explicitly to simulate a restart); rows already
     * loaded into memory are skipped.
     *
     * @return the number of rows re-hydrated (kept) into memory.
     */
    public int hydrateOnBoot() {
        if (!durablyEnabled()) {
            return 0;
        }
        String epoch = currentEpoch();
        List<DispatchWaitStateEntity> carried;
        try {
            carried = repository.findByStatusAndClientEpochNot(DispatchStatus.PENDING.name(), epoch);
        } catch (RuntimeException e) {
            log.warn("dispatch_wait_state hydrate query failed: {}", e.toString());
            return 0;
        }
        int hydrated = 0;
        int orphaned = 0;
        for (DispatchWaitStateEntity row : carried) {
            if (byDispatchId.containsKey(row.getDispatchId())) {
                continue; // already tracked this boot
            }
            boolean stillParked = sqsEngine != null
                    && sqsEngine.findUnfinishedWaitExecution(row.getProcessInstanceId(),
                    row.getWaitActivityId()) != null;
            if (stillParked) {
                byDispatchId.put(row.getDispatchId(), toWaitState(row));
                hydrated++;
                log.info("dispatch_wait_state.hydrate.keep dispatchId={} pi={} wait={} (token still parked)",
                        row.getDispatchId(), row.getProcessInstanceId(), row.getWaitActivityId());
            } else {
                txService.markDiscarded(row.getDispatchId(),
                        "orphaned across restart: ReceiveTask no longer parked (epoch carry-over)");
                orphaned++;
                log.warn("dispatch_wait_state.hydrate.discard dispatchId={} pi={} (execution gone/terminal)",
                        row.getDispatchId(), row.getProcessInstanceId());
            }
        }
        increment("sq.dispatch_registry.hydrated", hydrated);
        increment("sq.dispatch_registry.orphaned", orphaned);
        if (hydrated > 0 || orphaned > 0) {
            log.info("dispatch_wait_state hydration complete: hydrated={} orphaned={} (epoch={})",
                    hydrated, orphaned, epoch);
        }
        return hydrated;
    }

    private DispatchWaitStateEntity toEntity(WaitState s) {
        return DispatchWaitStateEntity.builder()
                .dispatchId(s.dispatchId())
                .executionId(s.executionId() == null ? s.businessKey() : s.executionId())
                .processInstanceId(s.processInstanceId())
                .businessKey(s.businessKey())
                .nodeId(s.nodeId())
                .waitActivityId(s.waitActivityId())
                .environmentId(s.environmentId())
                .clientEpoch(s.clientEpoch())
                .status(DispatchStatus.PENDING.name())
                .registeredAt(s.registeredAt())
                .build();
    }

    private WaitState toWaitState(DispatchWaitStateEntity e) {
        return new WaitState(e.getDispatchId(), e.getExecutionId(), e.getProcessInstanceId(),
                e.getBusinessKey(), e.getNodeId(), e.getWaitActivityId(), e.getEnvironmentId(),
                e.getClientEpoch(), e.getRegisteredAt());
    }

    private void increment(String metric, int amount) {
        if (meterRegistry != null && amount > 0) {
            Counter.builder(metric).register(meterRegistry).increment(amount);
        }
    }
}
