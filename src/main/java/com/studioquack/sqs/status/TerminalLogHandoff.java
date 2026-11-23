package com.studioquack.sqs.status;

/**
 * Hands a run's per-node sandbox-log rows to the backend before the run is reported FAILED,
 * COMPLETED or CANCELED. The run-status bridge does this at the process lifecycle end.
 */
public interface TerminalLogHandoff {

    /**
     * Never throws.
     *
     * @return {@code true} when the rows are durable at the backend; {@code false} when a BATCH
     *         flush was refused or a STREAM drain timed out / lost a post, so retention must keep
     *         the local copy
     */
    boolean handOffLogs(String workflowExecutionId);
}
