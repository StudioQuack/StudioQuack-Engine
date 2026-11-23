package com.studioquack.sqs.status;

/**
 * Ships a BATCH run's locally-buffered per-node sandbox logs to the control plane in one shot when
 * the run reaches its terminal boundary. Called by {@link RunStatusEventBridge} for a run
 * registered {@link RunLogMode#BATCH}, immediately BEFORE the terminal run-index status is
 * reported.
 */
public interface RunLogBatchFlusher {

    /**
     * Flushes every buffered {@code sandbox_log} row for {@code workflowExecutionId} to the backend,
     * blocking until the rows are acknowledged or a bounded budget elapses.
     *
     * @return {@code true} when the run's rows are durable at the backend (every chunk acknowledged,
     * or nothing was buffered).
     */
    boolean flush(String workflowExecutionId);
}
