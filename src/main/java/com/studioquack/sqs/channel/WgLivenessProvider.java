package com.studioquack.sqs.channel;

/**
 * Optional supplier of the daemon's own view of the WireGuard overlay data plane, reported on the heartbeat as
 * {@code wgOverlayUp} + {@code wgLastHandshakeUnix}. The BE uses it to re-arm its overlay reachability gate
 * and render an honest status tile the instant the tunnel is up.
 */
@FunctionalInterface
public interface WgLivenessProvider {

    /**
     * A point-in-time overlay liveness reading.
     */
    record WgLiveness(boolean up, long lastHandshakeUnix) {
        /**
         * The "no overlay" reading (loopback / not brought up).
         */
        public static WgLiveness down() {
            return new WgLiveness(false, 0L);
        }
    }

    /**
     * The current overlay liveness.
     */
    WgLiveness liveness();
}
