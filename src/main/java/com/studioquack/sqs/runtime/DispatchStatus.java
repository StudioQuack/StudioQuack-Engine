package com.studioquack.sqs.runtime;

/**
 * Lifecycle of a durable {@code dispatch_wait_state} row. Stored as its {@link #name} in
 * the {@code status VARCHAR(16)} column.
 *
 * <ul>
 * <li>{@link #PENDING} — registered before the token parked; awaiting a sandbox result.</li>
 * <li>{@link #RESOLVED} — the ReceiveTask was successfully resumed (success or routed failure).</li>
 * <li>{@link #DISCARDED} — reconciled as an orphan (the Activiti execution is gone/terminal).</li>
 * </ul>
 */
public enum DispatchStatus {
    PENDING,
    RESOLVED,
    DISCARDED
}
