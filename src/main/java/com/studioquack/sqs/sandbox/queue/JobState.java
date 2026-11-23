package com.studioquack.sqs.sandbox.queue;

/**
 * Externally-visible state of a durable job. The wire string is what the dashboard + heartbeat
 * surface.
 */
public enum JobState {
    QUEUED("queued"),
    RUNNING("running"),
    SUCCEEDED("succeeded"),
    FAILED("failed"),
    CANCELLED("cancelled");

    private final String wire;

    JobState(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }

    /** True when the job has reached a terminal state (no further transitions). */
    public boolean terminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
