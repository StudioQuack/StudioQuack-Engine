package com.studioquack.sqs.channel;

import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the per-environment BE base URL the daemon's OUTBOUND control-plane calls (heartbeat / exec-channel
 * / BeCall) hang off, choosing between: the PUBLIC base (always present — the public
 * {@code sqs.backend.base-url} derived environment base), and. an optional OVERLAY base (a hub HTTPS URL from
 * handshake v2 {@code hubAddresses[]}) that is activated ONLY after a confirmed agent Running + HTTPS hub
 * probe.
 */
@Slf4j
public class BackendBaseResolver {

    /**
     * Default consecutive-failure count on the overlay base before falling back to public.
     */
    public static final int DEFAULT_FAILURE_THRESHOLD = 3;

    /**
     * The always-available public environment base (never null once constructed).
     */
    private final String publicBase;

    /**
     * Consecutive overlay-transport failures that trip the fallback to public.
     */
    private final int failureThreshold;

    /**
     * The overlay environment base; {@code null} whenever the overlay is not active.
     */
    private volatile String overlayBase;

    /**
     * True while outbound calls should target {@link #overlayBase}.
     */
    private volatile boolean overlayActive;

    /**
     * Consecutive overlay failures (only meaningful while {@link #overlayActive}); guarded by {@code this}.
     */
    private int consecutiveOverlayFailures;

    /**
     * Builds a resolver on the public base with the default failure threshold.
     */
    public BackendBaseResolver(String publicBase) {
        this(publicBase, DEFAULT_FAILURE_THRESHOLD);
    }

    /**
     * Builds a resolver on the public base with an explicit consecutive-failure threshold (non-positive values
     * fall back to {@link #DEFAULT_FAILURE_THRESHOLD}).
     */
    public BackendBaseResolver(String publicBase, int failureThreshold) {
        this.publicBase = publicBase == null ? "" : publicBase;
        this.failureThreshold = failureThreshold > 0 ? failureThreshold : DEFAULT_FAILURE_THRESHOLD;
    }

    /**
     * The base outbound calls should use RIGHT NOW: the overlay base when the overlay is active (and set),
     * otherwise the public base. Never null.
     */
    public String current() {
        if (overlayActive) {
            String base = overlayBase;
            if (base != null) {
                return base;
            }
        }
        return publicBase;
    }

    /**
     * The immutable public base (for logging / diagnostics).
     */
    public String publicBase() {
        return publicBase;
    }

    /**
     * True when outbound calls are currently routed over the overlay base.
     */
    public boolean overlayActive() {
        return overlayActive;
    }

    /**
     * Activates the overlay base so {@link #current()} returns it. Idempotent and resets the failure counter, so a
     * re-probe (e.g. after a public recovery) can re-arm the overlay cleanly.
     */
    public synchronized void activateOverlay(String overlayEnvBase) {
        if (overlayEnvBase == null || overlayEnvBase.isBlank()) {
            return;
        }
        this.overlayBase = overlayEnvBase;
        this.overlayActive = true;
        this.consecutiveOverlayFailures = 0;
        log.info("channel/resolver: outbound repointed to overlay base {}", overlayEnvBase);
    }

    /**
     * WG-reconnection watchdog entry point — RE-activates the overlay base after it had fallen back to public
     * (e.g. a host reboot / transient outage healed by {@code WireGuardWatchdog}). Distinct from
     * {@link #activateOverlay(String)} in intent: it is the "return to overlay" transition the watchdog drives
     * once it has re-run the handshake and re-probed the hub, and it is idempotent so a watchdog tick that fires
     * while the overlay is already active is a cheap no-op.
     *
     * @return {@code true} once outbound is routed over {@code overlayBase} (always, on this path)
     */
    public synchronized boolean tryReactivateOverlay(String overlayBase) {
        if (overlayActive) {
            return true; // Already active — nothing to do
        }
        log.info("channel/resolver: attempting to reactivate overlay base {}", overlayBase);
        this.overlayBase = overlayBase;
        this.overlayActive = true;
        this.consecutiveOverlayFailures = 0;
        return true;
    }

    /**
     * Deactivates the overlay and reverts {@link #current()} to the public base. Clears the stored overlay base so
     * a later {@link #activateOverlay(String)} must re-supply it (a fresh probe re-arms the overlay).
     */
    public synchronized void deactivateOverlay() {
        if (!overlayActive) {
            return;
        }
        this.overlayActive = false;
        this.overlayBase = null;
        this.consecutiveOverlayFailures = 0;
        log.info("channel/resolver: outbound reverted to public base {}", publicBase);
    }

    /**
     * Reports a successful transport round-trip on the current base. Resets the consecutive-failure counter (a
     * single good beat clears any accumulated overlay flakiness).
     */
    public synchronized void reportSuccess() {
        consecutiveOverlayFailures = 0;
    }

    /**
     * Classifies a completed HTTP round-trip. A gateway error (502/503/504) means an intermediary answered because
     * the backend could not be reached through it — on the overlay base that is a broken private-network path, so
     * it counts toward the public fallback exactly like a connect failure.
     */
    public void reportResponse(int status) {
        if (status == 502 || status == 503 || status == 504) {
            reportFailure();
        } else {
            reportSuccess();
        }
    }

    /**
     * Reports a transport-level failure (could not connect / timed out) on the current base. While the overlay is
     * active this counts toward the fallback; once {@code failureThreshold} consecutive failures accrue the
     * overlay is auto-deactivated and outbound reverts to the public base.
     */
    public synchronized void reportFailure() {
        if (!overlayActive) {
            return;
        }
        consecutiveOverlayFailures++;
        if (consecutiveOverlayFailures >= failureThreshold) {
            log.warn("channel/resolver: overlay base failed {} consecutive times (threshold={}) — "
                    + "falling back to public base {}", consecutiveOverlayFailures, failureThreshold, publicBase);
            // deactivateOverlay() also resets the counter and flips overlayActive.
            deactivateOverlay();
        }
    }
}
