package com.studioquack.sqs.status;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records the {@link RunLogMode} chosen for each in-flight run, keyed by its {@code businessKey}
 * (== {@code workflowExecutionId}). The dispatch path ({@link CompositeSandboxLogCallback}) and
 * the terminal bridge ({@link RunStatusEventBridge}) read it to decide whether to mirror per-node
 * logs live or buffer-and-flush.
 */
@Slf4j
@Component
public class RunLogModeRegistry {

    /** Soft cap that triggers a TTL sweep; steady state is far below this (one entry per live run). */
    static final int SWEEP_THRESHOLD = 10_000;

    /** A leaked entry older than this is reclaimed on the next write. Generous: runs are short-lived. */
    static final long ENTRY_TTL_MS = 24L * 60 * 60 * 1000;

    private final Map<String, Entry> modes = new ConcurrentHashMap<>();

    /** Mode + registration time, so the TTL sweep can reclaim leaked entries. */
    private record Entry(RunLogMode mode, long registeredAtMs) {
    }

    /**
     * Registers {@code mode} for {@code businessKey} iff no mode is registered yet (first writer
     * wins).
     */
    public boolean registerIfAbsent(String businessKey, RunLogMode mode) {
        if (businessKey == null || businessKey.isBlank() || mode == null) {
            return false;
        }
        maybeSweep();
        Entry prior = modes.putIfAbsent(businessKey, new Entry(mode, System.currentTimeMillis()));
        boolean created = prior == null;
        if (created) {
            log.debug("run-log-mode: registered {} for run={}", mode, businessKey);
        }
        return created;
    }

    /**
     * The mode registered for {@code businessKey}, or {@link RunLogMode#STREAM} when none is (a
     * fail-open default preserving the historical live-mirror behaviour). Never throws.
     */
    public RunLogMode modeOf(String businessKey) {
        if (businessKey == null || businessKey.isBlank()) {
            return RunLogMode.STREAM;
        }
        Entry entry = modes.get(businessKey);
        return entry == null ? RunLogMode.STREAM : entry.mode();
    }

    /** True when the run is explicitly registered {@link RunLogMode#BATCH}. */
    public boolean isBatch(String businessKey) {
        return modeOf(businessKey) == RunLogMode.BATCH;
    }

    /**
     * Used exactly once: a function-port sub-PI child's PROCESS_STARTED can fire on the engine thread
     * BEFORE {@code HelperExecuteService}'s register call returns (both race), and the default BATCH
     * then buffers the provider node's rows for a.
     */
    public void override(String businessKey, RunLogMode mode) {
        if (businessKey == null || businessKey.isBlank() || mode == null) {
            return;
        }
        maybeSweep();
        modes.put(businessKey, new Entry(mode, System.currentTimeMillis()));
    }

    /** Drops the entry for a run that has reached its terminal boundary. Never throws. */
    public void remove(String businessKey) {
        if (businessKey == null || businessKey.isBlank()) {
            return;
        }
        modes.remove(businessKey);
    }

    /** Test/diagnostic hook: number of live entries. */
    public int size() {
        return modes.size();
    }

    /** Reclaims leaked entries once the map grows past the soft cap. Cheap no-op below it. */
    private void maybeSweep() {
        if (modes.size() < SWEEP_THRESHOLD) {
            return;
        }
        long cutoff = System.currentTimeMillis() - ENTRY_TTL_MS;
        modes.entrySet().removeIf(e -> e.getValue().registeredAtMs() < cutoff);
    }
}
