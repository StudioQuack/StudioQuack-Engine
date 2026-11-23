package com.studioquack.sqs.runtime;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bounded-TTL memory of recently-cancelled runs, keyed by {@code workflowExecutionId} (== the
 * engine businessKey). A cancel-instance command races the in-flight sandbox dispatch(es) it is
 * reaping: a container that was already mid-run when the cancel landed can still produce a terminal
 * result (a sandbox log push, a run-status event) AFTER the process instance is gone. Consulting this
 * guard before mirroring either one to the control plane stops a cancelled run from being
 * resurrected as COMPLETED/FAILED by that late, now-meaningless result.
 *
 * <p><b>Bounded / self-cleaning.</b> Entries expire after {@link #ttl} — long enough to cover the
 * slowest realistic straggler (a container that was mid-{@code CliRunner.run} when interrupted, or a
 * teardown that took a moment) but short enough that the map can never grow without bound: an entry
 * is dropped lazily the next time it is read past its TTL, and a periodic sweep ({@link #sweep})
 * clears anything nobody reads again. The map's steady-state size is therefore bounded by
 * {@code (cancellation rate) × ttl}, not by process lifetime.
 */
@Slf4j
@Component
public class CancelledRunGuard {

    private final Map<String, Instant> cancelledAt = new ConcurrentHashMap<>();
    private final Duration ttl;

    public CancelledRunGuard(@Value("${sqs.cancel.suppress-ttl-ms:600000}") long ttlMs) {
        this.ttl = Duration.ofMillis(Math.max(1_000L, ttlMs));
    }

    /** Marks {@code workflowExecutionId} as cancelled, starting the suppression TTL now. */
    public void markCancelled(String workflowExecutionId) {
        if (workflowExecutionId == null || workflowExecutionId.isBlank()) {
            return;
        }
        cancelledAt.put(workflowExecutionId, Instant.now());
    }

    /**
 * @return true when {@code workflowExecutionId} was cancelled within the last {@link #ttl} — a
 * late log/status for it should be suppressed rather than mirrored to the control plane.
 */
    public boolean isCancelled(String workflowExecutionId) {
        if (workflowExecutionId == null || workflowExecutionId.isBlank()) {
            return false;
        }
        Instant at = cancelledAt.get(workflowExecutionId);
        if (at == null) {
            return false;
        }
        if (Instant.now().isAfter(at.plus(ttl))) {
            cancelledAt.remove(workflowExecutionId, at); // lazy self-clean past TTL
            return false;
        }
        return true;
    }

    /** Periodic backstop sweep so an id nobody ever re-reads still gets reclaimed. */
    @Scheduled(fixedDelayString = "${sqs.cancel.suppress-sweep-ms:300000}",
            initialDelayString = "${sqs.cancel.suppress-sweep-ms:300000}")
    public void sweep() {
        Instant cutoff = Instant.now().minus(ttl);
        cancelledAt.entrySet().removeIf(e -> e.getValue().isBefore(cutoff));
    }

    /** @return how many cancelled ids are currently remembered (diagnostics / test assertion). */
    public int size() {
        return cancelledAt.size();
    }
}
