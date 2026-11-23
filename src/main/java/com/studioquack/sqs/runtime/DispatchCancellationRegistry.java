package com.studioquack.sqs.runtime;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Bounded, self-cleaning, per-run registry of in-flight sandbox dispatches: for every dispatch
 * currently submitted to {@code ScriptDispatchDelegate}'s pool it holds the pool {@link Future}
 * (so a cancel can interrupt the blocked dispatch thread <em>only while a container is bound and
 * the dispatch is still in the sandbox-wait phase</em>) and, once known, the live container id
 * backing it plus a teardown callback (so a cancel can kill/remove that container through the SAME
 * teardown path {@code PodmanSandboxExecution} already uses — never by returning it to the warm
 * pool). After {@link #markEnginePhase} the pool thread is in Activiti/H2 code: interrupting it
 * would close the encrypted MVStore for the rest of the process.
 *
 * <p><b>Bounded / self-cleaning.</b> A live {@link Entry} is added when a dispatch is submitted and
 * removed the moment that dispatch's background task finishes (success, failure, or interrupt) —
 * see the {@code finally} the caller wraps around {@code runDispatch}. The live map therefore never
 * holds more entries than there are concurrently in-flight dispatches (bounded by the dispatch
 * pool size). A separate cancelled-id set ({@link #cancelledReasonByDispatchId}) survives
 * {@link #endTracking} so a late helper {@code /result} can still be refused after the live entry
 * is gone; that set is insertion-ordered and evicts the eldest mark when it exceeds
 * {@value #MAX_CANCELLED_MARKS}.
 *
 * <p><b>Cancel is idempotent / never throws.</b> {@link #cancelRun} and {@link #cancelStep}
 * tolerate an unknown process instance (nothing tracked → zero reaped, not an error) and a
 * dispatch with no bound container yet (a "missing container" is treated as a successful no-op,
 * not a failure) — matching the daemon-wide contract that a repeat or late cancel is always safe
 * to retry.
 */
@Slf4j
@Component
public class DispatchCancellationRegistry {

    /** Reason stamped when an interrupting boundary cancels {@code wait_<nodeId>}. */
    public static final String REASON_BOUNDARY_INTERRUPT = "BOUNDARY_INTERRUPT";
    /** Reason stamped when {@link #cancelRun} reaps every dispatch of a process instance. */
    public static final String REASON_RUN_CANCEL = "RUN_CANCEL";

    /** Eldest cancelled marks drop when the set grows past this — late-result refuse stays bounded. */
    static final int MAX_CANCELLED_MARKS = 4096;

    /** One in-flight dispatch's cancellable state. Mutated in place; never shared across dispatches. */
    private static final class Entry {
        final String processInstanceId;
        /** Designer node id when known; {@code null} for the 2-arg {@code beginTracking} overload. */
        final String nodeId;
        volatile Future<?> future;
        volatile String containerId;
        volatile Consumer<String> containerKiller;
        volatile Runnable helperRevoke;
        volatile Runnable statefulAbort;
        /** True only while the pool thread is allowed to be interrupted (blocked in CliRunner). */
        volatile boolean interruptible = true;

        Entry(String processInstanceId, String nodeId) {
            this.processInstanceId = processInstanceId;
            this.nodeId = nodeId;
        }
    }

    private final Map<String, Entry> byDispatchId = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> dispatchIdsByProcessInstance = new ConcurrentHashMap<>();
    /**
     * dispatchId → reason. Survives {@link #endTracking}. Insertion-ordered; eldest evicted above
     * {@link #MAX_CANCELLED_MARKS}. Synchronized — {@link ConcurrentHashMap} has no insertion order.
     */
    private final Map<String, String> cancelledReasonByDispatchId =
            Collections.synchronizedMap(new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > MAX_CANCELLED_MARKS;
                }
            });
    /**
     * Survives {@link #endTracking} so an e2e / listener can ask "was this wait cancelled?"
     * without knowing the dispatch id. Same bound as {@link #cancelledReasonByDispatchId}.
     */
    private final Map<String, String> cancelledReasonByNode =
            Collections.synchronizedMap(new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > MAX_CANCELLED_MARKS;
                }
            });

    /**
     * Begins tracking {@code dispatchId} for {@code processInstanceId}, BEFORE the pool submission
     * that will run it — so a cancel racing the submission itself still finds the entry (the future
     * is attached moments later via {@link #attachFuture}; a cancel in that tiny window simply has no
     * future to interrupt yet, which is safe — the container is not up yet either).
     *
     * <p>Treats {@code nodeId} as {@code null} — prefer {@link #beginTracking(String, String, String)}
     * so {@link #cancelStep} can target this dispatch.
     */
    public void beginTracking(String processInstanceId, String dispatchId) {
        beginTracking(processInstanceId, dispatchId, null);
    }

    /**
     * Preferred tracking start: binds {@code dispatchId} to {@code (processInstanceId, nodeId)} so
     * a boundary cancel of {@code wait_<nodeId>} can reap just that step.
     */
    public void beginTracking(String processInstanceId, String dispatchId, String nodeId) {
        if (dispatchId == null || dispatchId.isBlank()) {
            return;
        }
        String trackedNode = (nodeId == null || nodeId.isBlank()) ? null : nodeId;
        Entry entry = new Entry(processInstanceId, trackedNode);
        byDispatchId.put(dispatchId, entry);
        if (processInstanceId != null && !processInstanceId.isBlank()) {
            dispatchIdsByProcessInstance
                    .computeIfAbsent(processInstanceId, k -> new CopyOnWriteArraySet<>())
                    .add(dispatchId);
        }
    }

    /** Attaches the pool {@link Future} once {@code dispatchPool.submit} has returned it. */
    public void attachFuture(String dispatchId, Future<?> future) {
        Entry entry = dispatchId == null ? null : byDispatchId.get(dispatchId);
        if (entry != null) {
            entry.future = future;
        }
    }

    /**
     * Records the live container backing {@code dispatchId} once it is known, plus the callback that
     * kills/removes it BY ID (never by returning it to a pool). A no-op when the dispatch already
     * finished (self-cleaned) — the container it would have bound is already torn down.
     */
    public void bindContainer(String dispatchId, String containerId, Consumer<String> containerKiller) {
        Entry entry = dispatchId == null ? null : byDispatchId.get(dispatchId);
        if (entry == null || containerId == null || containerId.isBlank()) {
            return;
        }
        entry.containerId = containerId;
        entry.containerKiller = containerKiller;
    }

    /**
     * Binds a helper-channel revoke callback (e.g. drop in-flight helper work for this dispatch).
     * No-op when the live entry is already gone — the cancelled mark still refuses a late result.
     */
    public void bindHelperRevoke(String dispatchId, Runnable revoker) {
        Entry entry = dispatchId == null ? null : byDispatchId.get(dispatchId);
        if (entry == null || revoker == null) {
            return;
        }
        // Compose: stream-through adds a second token without replacing the parent revoke.
        entry.helperRevoke = composeQuiet(entry.helperRevoke, revoker);
    }

    /**
     * Binds a stateful-invoke abort callback. No-op when the live entry is already gone.
     * Multiple binds compose so {@code /invoke/stream} can add its abort without replacing
     * the launcher's.
     */
    public void bindStatefulAbort(String dispatchId, Runnable abort) {
        Entry entry = dispatchId == null ? null : byDispatchId.get(dispatchId);
        if (entry == null || abort == null) {
            return;
        }
        entry.statefulAbort = composeQuiet(entry.statefulAbort, abort);
    }

    /** Clears the container binding early (called from the sandbox port's own teardown). */
    public void unbindContainer(String dispatchId) {
        Entry entry = dispatchId == null ? null : byDispatchId.get(dispatchId);
        if (entry != null) {
            entry.containerId = null;
            entry.containerKiller = null;
        }
    }

    /**
     * Marks {@code dispatchId} past the interruptible window: the sandbox wait has returned and
     * the pool thread is about to touch Activiti/H2. A later {@link #cancelRun} still reaps the
     * container but must NOT {@code Future.cancel(true)} — H2 treats
     * {@code ClosedByInterruptException} as fatal and closes the engine store for the process.
     */
    public void markEnginePhase(String dispatchId) {
        Entry entry = dispatchId == null ? null : byDispatchId.get(dispatchId);
        if (entry != null) {
            entry.interruptible = false;
        }
    }

    /**
     * Ends tracking for {@code dispatchId} — the SELF-CLEANING half of the contract. MUST be called
     * from a {@code finally} covering the whole dispatch task so an entry never survives its dispatch,
     * regardless of how it ended. Does <em>not</em> clear a cancelled mark — late {@code /result}
     * must still see {@link #isCancelled}.
     */
    public void endTracking(String dispatchId) {
        Entry entry = dispatchId == null ? null : byDispatchId.remove(dispatchId);
        if (entry == null || entry.processInstanceId == null) {
            return;
        }
        Set<String> siblings = dispatchIdsByProcessInstance.get(entry.processInstanceId);
        if (siblings != null) {
            siblings.remove(dispatchId);
            dispatchIdsByProcessInstance.computeIfPresent(entry.processInstanceId,
                    (k, v) -> v.isEmpty() ? null : v);
        }
    }

    /**
     * Reaps every dispatch currently in flight for {@code processInstanceId}: interrupts its pool
     * {@link Future} (unblocks a thread parked in {@code CliRunner.run}) and, when a container is
     * bound, kills/removes it through the teardown callback bound at {@link #bindContainer} time.
     * Also marks each reaped dispatch {@link #REASON_RUN_CANCEL} so a late helper call is refused.
     * Idempotent and never throws — an unknown process instance or a dispatch with no container yet
     * both count as a clean (zero-cost) success.
     *
     * @return how many in-flight dispatches were reaped (diagnostics / tests)
     */
    public int cancelRun(String processInstanceId) {
        if (processInstanceId == null || processInstanceId.isBlank()) {
            return 0;
        }
        Set<String> dispatchIds = dispatchIdsByProcessInstance.remove(processInstanceId);
        if (dispatchIds == null || dispatchIds.isEmpty()) {
            return 0;
        }
        int reaped = 0;
        for (String dispatchId : Set.copyOf(dispatchIds)) {
            Entry entry = byDispatchId.remove(dispatchId);
            if (entry == null) {
                continue; // already self-cleaned between the snapshot and here
            }
            markCancelled(dispatchId, REASON_RUN_CANCEL, entry);
            reaped++;
            reapLiveWork(entry, processInstanceId, dispatchId, false);
            log.info("dispatch-cancel.reaped pi={} dispatchId={} container={}", processInstanceId, dispatchId,
                    entry.containerId == null ? "<none>" : shortId(entry.containerId));
        }
        return reaped;
    }

    /**
     * Reaps in-flight dispatch(es) tracked for {@code (processInstanceId, nodeId)} — the interrupting
     * boundary path for {@code wait_<nodeId>}. Marks those dispatch ids {@link #REASON_BOUNDARY_INTERRUPT}
     * (survives {@link #endTracking}), interrupts the pool future when still waiting, and runs any
     * bound container killer / stateful abort / helper revoke. Does not remove the cancelled mark.
     * Blank {@code nodeId} is a no-op. Idempotent and never throws.
     *
     * @return how many matching in-flight dispatches were reaped (diagnostics / tests)
     */
    public int cancelStep(String processInstanceId, String nodeId) {
        if (processInstanceId == null || processInstanceId.isBlank()
                || nodeId == null || nodeId.isBlank()) {
            return 0;
        }
        Set<String> dispatchIds = dispatchIdsByProcessInstance.get(processInstanceId);
        if (dispatchIds == null || dispatchIds.isEmpty()) {
            return 0;
        }
        int reaped = 0;
        for (String dispatchId : Set.copyOf(dispatchIds)) {
            Entry entry = byDispatchId.get(dispatchId);
            if (entry == null || !Objects.equals(nodeId, entry.nodeId)) {
                continue;
            }
            markCancelled(dispatchId, REASON_BOUNDARY_INTERRUPT, entry);
            reaped++;
            // Stateful has no bindContainer — still interrupt the pool future while interruptible.
            reapLiveWork(entry, processInstanceId, dispatchId, true);
            log.info("dispatch-cancel.step pi={} node={} dispatchId={} container={}",
                    processInstanceId, nodeId, dispatchId,
                    entry.containerId == null ? "<none>" : shortId(entry.containerId));
        }
        return reaped;
    }

    /** {@code true} when {@code dispatchId} was cancelled (step or run) — survives {@link #endTracking}. */
    public boolean isCancelled(String dispatchId) {
        return dispatchId != null && cancelledReasonByDispatchId.containsKey(dispatchId);
    }

    /** Cancel reason for {@code dispatchId} (e.g. {@link #REASON_BOUNDARY_INTERRUPT}), or {@code null}. */
    public String cancelReason(String dispatchId) {
        return dispatchId == null ? null : cancelledReasonByDispatchId.get(dispatchId);
    }

    /**
     * Whether {@code cancelStep} (or a run-cancel of that node's dispatch) marked
     * {@code (processInstanceId, nodeId)}. Survives {@link #endTracking}.
     */
    public boolean isStepCancelled(String processInstanceId, String nodeId) {
        if (processInstanceId == null || processInstanceId.isBlank()
                || nodeId == null || nodeId.isBlank()) {
            return false;
        }
        return cancelledReasonByNode.containsKey(nodeKey(processInstanceId, nodeId));
    }

    /** @return how many dispatches are currently tracked in memory (diagnostics / test assertion). */
    public int trackedCount() {
        return byDispatchId.size();
    }

    /**
     * Interrupt (if still waiting) + container kill + stateful abort + helper revoke. Never throws.
     * Live {@link Entry} is left in place for {@link #cancelStep} so {@link #endTracking} owns cleanup;
     * {@link #cancelRun} removes the entry before calling this.
     */
    private void reapLiveWork(Entry entry, String processInstanceId, String dispatchId,
                              boolean interruptWithoutContainer) {
        if (interruptWithoutContainer) {
            interruptIfInterruptible(entry, processInstanceId, dispatchId);
        } else {
            interruptIfStillWaiting(entry, processInstanceId, dispatchId);
        }
        String containerId = entry.containerId;
        Consumer<String> killer = entry.containerKiller;
        if (containerId != null && !containerId.isBlank() && killer != null) {
            try {
                killer.accept(containerId);
            } catch (RuntimeException e) {
                // Never surfaces — a repeat cancel must stay safe to retry.
                log.warn("dispatch-cancel: container teardown failed pi={} dispatchId={} container={}: {}",
                        processInstanceId, dispatchId, shortId(containerId), e.toString());
            }
        }
        runQuietly(entry.statefulAbort, "stateful-abort", processInstanceId, dispatchId);
        runQuietly(entry.helperRevoke, "helper-revoke", processInstanceId, dispatchId);
    }

    private void markCancelled(String dispatchId, String reason, Entry entry) {
        if (dispatchId == null || dispatchId.isBlank() || reason == null) {
            return;
        }
        cancelledReasonByDispatchId.put(dispatchId, reason);
        if (entry != null && entry.processInstanceId != null && entry.nodeId != null) {
            cancelledReasonByNode.put(nodeKey(entry.processInstanceId, entry.nodeId), reason);
        }
    }

    /** Chains {@code first} then {@code second}; each throw is isolated. */
    private static Runnable composeQuiet(Runnable first, Runnable second) {
        if (first == null) {
            return second;
        }
        return () -> {
            try {
                first.run();
            } catch (RuntimeException ignored) {
                // isolated — second still runs
            }
            second.run();
        };
    }

    private static void runQuietly(Runnable action, String what, String processInstanceId, String dispatchId) {
        if (action == null) {
            return;
        }
        try {
            action.run();
        } catch (RuntimeException e) {
            log.warn("dispatch-cancel: {} failed pi={} dispatchId={}: {}",
                    what, processInstanceId, dispatchId, e.toString());
        }
    }

    /**
     * Interrupts the pool thread only while it is still blocked on a live container. After
     * {@link #markEnginePhase} (or once the container is unbound) the thread is in engine/JPA
     * code — interrupting it closes the H2 MVStore.
     */
    private static void interruptIfStillWaiting(Entry entry, String processInstanceId, String dispatchId) {
        if (!entry.interruptible || entry.containerId == null || entry.containerId.isBlank()) {
            return;
        }
        cancelFuture(entry, processInstanceId, dispatchId);
    }

    /** Same as {@link #interruptIfStillWaiting} but does not require a bound container (stateful). */
    private static void interruptIfInterruptible(Entry entry, String processInstanceId, String dispatchId) {
        if (!entry.interruptible) {
            return;
        }
        cancelFuture(entry, processInstanceId, dispatchId);
    }

    private static void cancelFuture(Entry entry, String processInstanceId, String dispatchId) {
        Future<?> future = entry.future;
        if (future == null) {
            return;
        }
        try {
            future.cancel(true);
        } catch (RuntimeException e) {
            log.debug("dispatch-cancel: future.cancel failed pi={} dispatchId={}: {}",
                    processInstanceId, dispatchId, e.toString());
        }
    }

    private static String nodeKey(String processInstanceId, String nodeId) {
        return processInstanceId + '\0' + nodeId;
    }

    private static String shortId(String id) {
        return id == null ? "" : id.substring(0, Math.min(12, id.length()));
    }
}
