package com.studioquack.sqs.engine.update;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Whether this engine takes new work. Closed by any {@link Reason}: the control plane reads
 * {@link #isAccepting()} from every beat ({@code resources.acceptingWork}) and holds new
 * {@code start-run} / {@code correlate-message} on its queue until the engine accepts again.
 * Closing the gate never interrupts work already running; an update kills its in-flight runs
 * itself, once the release is verified ({@link DaemonUpdateService}).
 *
 * <p>Reasons are independent: an aborted update re-opening its own reason never lifts a disk-full
 * stop, and vice versa. Only {@link Reason#UPDATE} parks tokens between nodes
 * ({@link UpdateParkListener}) — the next process resumes them; a disk-full stop must not, because
 * nothing would resume them when the disk is freed without a restart.
 */
@Component
public class WorkIntakeGate {

    public static final String RESOURCE_KEY = "acceptingWork";

    /** Why intake is closed. */
    public enum Reason { UPDATE, DISK_FULL }

    private final Set<Reason> closed = ConcurrentHashMap.newKeySet();

    /** True when no reason holds intake closed. */
    public boolean isAccepting() {
        return closed.isEmpty();
    }

    /** True only while an in-place update is in progress — the one reason that parks tokens. */
    public boolean isUpdateInProgress() {
        return closed.contains(Reason.UPDATE);
    }

    public void close(Reason reason) {
        closed.add(reason);
    }

    public void open(Reason reason) {
        closed.remove(reason);
    }
}
