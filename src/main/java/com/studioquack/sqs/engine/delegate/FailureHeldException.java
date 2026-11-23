package com.studioquack.sqs.engine.delegate;

/**
 * Thrown by {@link FailureHoldDelegate} to park a failed ROOT run at its {@code hold_<nodeId>}
 * task as a genuine Activiti incident: the exception escapes an async job with
 * {@code failed-job-retries: 0}, so Activiti moves the job straight to the dead-letter table
 * ({@code ACT_RU_DEADLETTER_JOB}) and the token stays put until an operator repairs it
 * ({@code retry-incident} re-queues the job).
 *
 * <p><b>No stack trace.</b> This type is control flow, not a defect — the full failure detail is
 * already on the run's error sentinels, and a dead-letter job message is surfaced (redacted) by
 * {@code IncidentService}. Suppressing the stack keeps the incident row and the logs one line.
 */
public class FailureHeldException extends RuntimeException {

    /**
     * @param message {@code "node <nodeId> failed [<code>]: <message> — held for repair"} —
     *                non-PHI by construction (code + truncated error message only)
     */
    public FailureHeldException(String message) {
        super(message, null, false, false);
    }
}
