package com.studioquack.sqs.runtime;

/**
 * Lifecycle of a durable {@code sub_pi_pending} row (Layer 3) — the function-port
 * ({@code studioquack.execute}) sub-process completion registry. Stored as its {@link #name}
 * in the {@code status VARCHAR(16)} column.
 *
 * <ul>
 * <li>{@link #PENDING} — child sub-PI started; the parent is parked awaiting its result.</li>
 * <li>{@link #COMPLETED} — child reached its success end; whitelisted result vars recorded.</li>
 * <li>{@link #FAILED} — child reached the shared error end, or was orphan-recovered by-fail.</li>
 * <li>{@link #CANCELED} — the run (parent) was cancelled before the child completed.</li>
 * </ul>
 */
public enum SubPiStatus {
    PENDING,
    COMPLETED,
    FAILED,
    CANCELED
}
