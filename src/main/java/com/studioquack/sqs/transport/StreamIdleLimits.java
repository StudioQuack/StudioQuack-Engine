package com.studioquack.sqs.transport;

/**
 * Shared idle-reaper budget for the stateful launcher and graph-routed stream-through sessions.
 */
public final class StreamIdleLimits {

    /** Default idle window (ms) before a silent stream is reaped. */
    public static final long DEFAULT_STREAM_IDLE_TIMEOUT_MS = 600_000L;

    /** Spring property: {@code sqs.transport.stream-idle-timeout-ms}. */
    public static final String PROPERTY_MS = "sqs.transport.stream-idle-timeout-ms";

    /** Stateful launcher idle budget: {@code sqs.sandbox.oci.stream-idle-timeout-ms}. */
    public static final String PROPERTY_STATEFUL_MS = "sqs.sandbox.oci.stream-idle-timeout-ms";

    /**
     * Curl {@code --max-time} for streaming calls: high enough that an active stream never
     * hits it; the idle reaper is the real bound.
     */
    public static final int STREAM_CURL_MAX_TIME_SEC = 86_400;

    private StreamIdleLimits() {
    }
}
