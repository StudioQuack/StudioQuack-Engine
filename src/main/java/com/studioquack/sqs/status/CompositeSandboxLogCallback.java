package com.studioquack.sqs.status;

import com.studioquack.sqs.status.log.SandboxLogStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * It fans one node record out to two destinations, each independent and best-effort: Local store (
 * {@link SandboxLogStore} → {@code sandbox_log}) — ALWAYS. The daemon keeps its own log record;
 * pruning it later is the owner's call, and the control plane copy is the continuation of whatever
 * was pruned, never a duplicate kept in sync.
 */
@Slf4j
@Primary
@Component
public class CompositeSandboxLogCallback implements SandboxLogCallback {

    private final ObjectProvider<SandboxLogCallback> callbacks;
    private final SandboxLogStore localStore;
    private final RunLogModeRegistry runLogModeRegistry;

    /** The channel/logging impl, resolved once (lazily) so bean construction can never cycle. */
    private volatile SandboxLogCallback backendMirror;

    public CompositeSandboxLogCallback(ObjectProvider<SandboxLogCallback> callbacks,
                                       SandboxLogStore localStore,
                                       RunLogModeRegistry runLogModeRegistry) {
        this.callbacks = callbacks;
        this.localStore = localStore;
        this.runLogModeRegistry = runLogModeRegistry;
    }

    @Override
    public void report(SandboxLogRecord record) {
        if (record == null) {
            return;
        }
        String logId = null;
        try {
            logId = localStore.save(record);
        } catch (Exception e) {
            log.debug("sandbox-log[composite]: local store fault ({})", e.toString());
        }
        if (isBatchRun(record.workflowExecutionId())) {
            // BATCH run (event/webhook/timer/Duckbot/API): the row is kept locally and flushed in one
            // batch at the terminal boundary, so it does NOT ride the per-node control-plane hop here.
            log.debug("sandbox-log[composite]: run={} node={} is BATCH — buffered locally, not mirrored live",
                    record.workflowExecutionId(), record.nodeId());
            return;
        }
        try {
            // The local row id rides the mirror copy as {@code logId}: the same attempt then carries
            // the same identity on every lane (this STREAM post, a later BATCH flush, an on-demand
            // pull), so the backend can recognise a re-delivery of one row versus a new attempt.
            backendMirror().report(logId == null ? record : record.withLogId(logId));
        } catch (Exception e) {
            log.debug("sandbox-log[composite]: BE mirror fault ({})", e.toString());
        }
    }

    @Override
    public void reportStarted(SandboxStartRecord record) {
        if (record == null) {
            return;
        }
        String logId = null;
        try {
            logId = localStore.saveStarted(record);
        } catch (Exception e) {
            log.debug("sandbox-log[composite]: local STARTED store fault ({})", e.toString());
        }
        if (isBatchRun(record.workflowExecutionId())) {
            // BATCH run: STARTED markers are buffered locally too — no live per-node control-plane hop.
            return;
        }
        try {
            // Same identity threading as report(): the STARTED row's local id rides the mirror copy.
            backendMirror().reportStarted(logId == null ? record : record.withLogId(logId));
        } catch (Exception e) {
            log.debug("sandbox-log[composite]: BE STARTED mirror fault ({})", e.toString());
        }
    }

    /**
     * True when the run is registered {@link RunLogMode#BATCH}. A fail-open miss (unknown run, sub-PI
     * child id, daemon restart) resolves to {@link RunLogMode#STREAM}, so the live mirror keeps its
     * historical behaviour.
     */
    private boolean isBatchRun(String workflowExecutionId) {
        try {
            return runLogModeRegistry != null && runLogModeRegistry.isBatch(workflowExecutionId);
        } catch (RuntimeException e) {
            log.debug("sandbox-log[composite]: run-log-mode lookup failed ({}); defaulting to live mirror",
                    e.toString());
            return false;
        }
    }

    /** The one non-composite {@link SandboxLogCallback} bean (channel when on, logging otherwise). */
    private SandboxLogCallback backendMirror() {
        SandboxLogCallback resolved = backendMirror;
        if (resolved == null) {
            synchronized (this) {
                resolved = backendMirror;
                if (resolved == null) {
                    resolved = callbacks.stream()
                            .filter(cb -> cb != this && !(cb instanceof CompositeSandboxLogCallback))
                            .findFirst()
                            .orElseGet(LoggingFallback::new);
                    backendMirror = resolved;
                }
            }
        }
        return resolved;
    }

    /** Last-resort no-op when neither channel impl is wired (a bare slice test). */
    private static final class LoggingFallback implements SandboxLogCallback {
        @Override
        public void report(SandboxLogRecord record) {
            // no-op: local store already ran
        }

        @Override
        public void reportStarted(SandboxStartRecord record) {
            // no-op
        }
    }
}
