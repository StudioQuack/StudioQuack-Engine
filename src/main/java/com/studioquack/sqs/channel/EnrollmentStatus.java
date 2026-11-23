package com.studioquack.sqs.channel;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Last enrolment outcome of this daemon, read by the installer through the loopback-only
 * {@code GET /sqs/v1/ops/health} so it can report "enrolled" truthfully instead of assuming it.
 * {@link State#TOKEN_EXPIRED} and {@link State#TOKEN_REJECTED} are terminal for the bearer the daemon holds:
 * only a fresh install command (run on this host) recovers them.
 */
@Component
public class EnrollmentStatus {

    public enum State {
        /**
         * Channel disabled or no enrolment config (local test install).
         */
        NOT_CONFIGURED,
        /**
         * First bootstrap attempt in flight.
         */
        ENROLLING,
        /**
         * Bootstrap completed; the control plane is running.
         */
        ENROLLED,
        /**
         * {@code Error_BearerExpired}: the install command outlived its validity window.
         */
        TOKEN_EXPIRED,
        /**
         * {@code Error_BearerMismatch}: a newer install command replaced this daemon's bearer.
         */
        TOKEN_REJECTED,
        /**
         * Transient failure (network, BE unavailable); retried automatically.
         */
        RETRYING
    }

    private volatile State state = State.NOT_CONFIGURED;
    private volatile String errorCode;
    private volatile Instant updatedAt = Instant.now();

    public void enrolling() {
        set(State.ENROLLING, null);
    }

    public void enrolled() {
        set(State.ENROLLED, null);
    }

    /**
     * Records a failed bootstrap attempt and returns the classified state.
     */
    public State failed(Throwable failure) {
        State classified = classify(failure);
        set(classified, errorCodeOf(classified));
        return classified;
    }

    public State state() {
        return state;
    }

    /**
     * Body for the {@code enrolment} key of {@code /ops/health}; never carries the bearer.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("state", state.name());
        if (errorCode != null) {
            body.put("errorCode", errorCode);
        }
        body.put("updatedAt", updatedAt.toString());
        return body;
    }

    static State classify(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            String message = t.getMessage();
            if (message == null) {
                continue;
            }
            if (message.contains("Error_BearerExpired")) {
                return State.TOKEN_EXPIRED;
            }
            if (message.contains("Error_BearerMismatch")) {
                return State.TOKEN_REJECTED;
            }
        }
        return State.RETRYING;
    }

    private static String errorCodeOf(State state) {
        return switch (state) {
            case TOKEN_EXPIRED -> "Error_BearerExpired";
            case TOKEN_REJECTED -> "Error_BearerMismatch";
            default -> null;
        };
    }

    private void set(State next, String code) {
        this.state = next;
        this.errorCode = code;
        this.updatedAt = Instant.now();
    }
}
