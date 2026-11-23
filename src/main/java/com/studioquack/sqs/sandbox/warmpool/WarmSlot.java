package com.studioquack.sqs.sandbox.warmpool;

import java.time.Instant;
import java.util.List;

/**
 * One pre-warmed seat handed out by the {@link WarmPool}. The token is unique per slot so
 * helper-channel requests can be correlated back; the container id is the warmer's bookkeeping
 * handle ({@code sleep} container the OCI warmer keeps alive so a dispatch execs into it instead
 * of paying cold-start).
 */
public record WarmSlot(String language,
                       String token,
                       String containerId,
                       String kind,
                       String image,
                       Instant warmedAt,
                       boolean runnerReady,
                       boolean brokerReady,
                       String helperSocketPath,
                       List<String> entrypoint) {

    public WarmSlot {
        entrypoint = entrypoint == null ? List.of() : List.copyOf(entrypoint);
    }

    /** Back-compat constructor: a slot with no persistent runner and no helper broker. */
    public WarmSlot(String language, String token, String containerId, String kind,
                    String image, Instant warmedAt) {
        this(language, token, containerId, kind, image, warmedAt, false, false, "", List.of());
    }

    /** Back-compat constructor: a slot with a runner but no helper broker (older callers). */
    public WarmSlot(String language, String token, String containerId, String kind,
                    String image, Instant warmedAt, boolean runnerReady) {
        this(language, token, containerId, kind, image, warmedAt, runnerReady, false, "", List.of());
    }

    /** Back-compat constructor: the pre-entrypoint full signature (no recorded image argv). */
    public WarmSlot(String language, String token, String containerId, String kind,
                    String image, Instant warmedAt, boolean runnerReady,
                    boolean brokerReady, String helperSocketPath) {
        this(language, token, containerId, kind, image, warmedAt, runnerReady, brokerReady,
                helperSocketPath, List.of());
    }

    /** True when this slot is backed by a live warm container a dispatch can {@code exec} into. */
    public boolean hasContainer() {
        return containerId != null && !containerId.isEmpty();
    }

    /**
     * True when this is a custom (entrypoint) image slot — dispatch execs the image's own recorded
     * entrypoint instead of the language bootstrap; captured via {@code podman inspect} at warm time.
     */
    public boolean isEntrypointImage() {
        return !entrypoint.isEmpty();
    }

    /** A copy that records whether the in-container persistent runner is up and preloaded. */
    public WarmSlot withRunnerReady(boolean ready) {
        return new WarmSlot(language, token, containerId, kind, image, warmedAt,
                ready, brokerReady, helperSocketPath, entrypoint);
    }

    /** A copy that records whether the in-container helper broker is up, and its UDS path. */
    public WarmSlot withBroker(boolean ready, String socketPath) {
        return new WarmSlot(language, token, containerId, kind, image, warmedAt,
                runnerReady, ready, ready && socketPath != null ? socketPath : "", entrypoint);
    }

    /** A copy that records the image's own start argv (Entrypoint+Cmd). */
    public WarmSlot withEntrypoint(List<String> argv) {
        return new WarmSlot(language, token, containerId, kind, image, warmedAt,
                runnerReady, brokerReady, helperSocketPath, argv);
    }
}
