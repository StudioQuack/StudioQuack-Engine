package com.studioquack.sqs.sandbox.egress;

/**
 * Host overlay-egress posture as seen by every sandbox start path. {@code ENFORCED}: the host
 * deny/SNAT rule set is programmed — a networked container cannot reach the host LAN, IMDS,
 * loopback or daemon listeners.
 */
public record EgressReadiness(State state, String reason, String overlayIp, boolean operatorOptOut) {

    public enum State {
        ENFORCED,
        DEGRADED,
        UNAVAILABLE
    }

    public EgressReadiness(State state, String reason, String overlayIp) {
        this(state, reason, overlayIp, false);
    }

    /**
     * {@code true} only when a networked (OPEN-egress) container may start: the host enforces
     * the overlay deny set, or the operator explicitly opted out of enforcement. DEGRADED
     * without the opt-out and UNAVAILABLE both fail closed.
     */
    public boolean internetAllowed() {
        return state == State.ENFORCED || (state == State.DEGRADED && operatorOptOut);
    }

    /** Operator-facing sentence for a refused networked start; never includes payload data. */
    public String refusalMessage() {
        if (state == State.DEGRADED) {
            return "sandbox egress not enforced on this host (" + reason + "): networked nodes are "
                    + "refused. Set sqs.sandbox.overlay-egress=off only if you accept that sandboxes "
                    + "share this host's network.";
        }
        return "sandbox egress unavailable: " + reason;
    }
}
