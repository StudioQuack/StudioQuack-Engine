package com.studioquack.sqs.status;

/**
 * Seam that reports a run's lifecycle status for a {@code workflowExecutionId} back to the control
 * plane's run index ({@code execution_history}) — the {@code ENG.-> RIX} status callback in the
 * target architecture (§3). RUNNING is reported by the {@code start-run} handler the moment a
 * process instance is created.
 */
public interface RunStatusCallback {

    /** Coarse run-index status the FE "execution history" list renders. */
    enum Status {
        /** A process instance was created for the run (reported at {@code start-run}). */
        RUNNING,
        /** The process instance reached a normal end event. */
        COMPLETED,
        /** The process instance ended via an error end event (sandbox/boundary failure). */
        FAILED,
        /** The process instance was cancelled (operator action / lifecycle command). */
        CANCELED
    }

    /**
     * @param workflowExecutionId The run correlation id (== the engine businessKey)
     * @param status The new run-index status
     * @param detail
     */
    void report(String workflowExecutionId, Status status, String detail);

    /**
     * Same as {@link #report(String, Status, String)} plus an optional {@code workflowId} so a
     * daemon-originated start (timer / message-start, no prior BE row) can create a run-index
     * entry. Default delegates to the 3-arg form so existing implementations stay valid.
     */
    default void report(String workflowExecutionId, Status status, String detail, String workflowId) {
        report(workflowExecutionId, status, detail);
    }

    /**
     * {@code logsDurable=false} tells the channel implementation that this run's BATCH log flush was
     * REFUSED by the backend. Default ignores the flag (durable), so non-channel implementations and
     * the STREAM path stay unchanged.
     */
    default void report(String workflowExecutionId, Status status, String detail, String workflowId,
                        boolean logsDurable) {
        report(workflowExecutionId, status, detail, workflowId);
    }

    /**
     * Daemon-originated-start form carrying the run's correlation key (the caller-addressed or
     * receiver-derived key a message-started run was launched with; {@code null} for any other run).
     * The businessKey of such a run is a minted unique id, so this key is the only caller-meaningful
     * handle — the control plane persists it on the run-index row for display/search.
     */
    default void report(String workflowExecutionId, Status status, String detail, String workflowId,
                        boolean logsDurable, String correlationKey) {
        report(workflowExecutionId, status, detail, workflowId, logsDurable);
    }
}
