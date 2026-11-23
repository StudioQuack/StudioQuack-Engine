package com.studioquack.sqs.sandbox.stateful;

/**
 * Status literals the daemon uses for stateful instances and OCI image ops. The start/stop/status
 * wire values ({@link #STARTING}, {@link #RUNNING}, {@link #STOPPED}, {@link #FAILED}, {@link
 * #UNKNOWN}) must match {@code com.studioquack.common.enums.deployment.StatefulInstanceStatus} on
 * the control plane side ({@code StatefulInstanceServiceImpl.resolveStatus} upper-cases + maps
 * them).
 */
public final class StatefulStatus {

    private StatefulStatus() {
    }

    /** {@code start-stateful-instance} enqueued, identity not yet reported. */
    public static final String STARTING = "STARTING";
    /** The instance's container is up (start success / status healthy). */
    public static final String RUNNING = "RUNNING";
    /**
     * Daemon-local inventory status while {@link StatefulInstanceManager#replaceWithCurrentImage}
     * waits for in-flight work on the old container. Not a the control plane {@code
     * StatefulInstanceStatus} value ({@code fromCode} returns empty).
     */
    public static final String DRAINING = "DRAINING";
    /** The instance was stopped + removed. */
    public static final String STOPPED = "STOPPED";
    /** A lifecycle op could not complete (podman error). */
    public static final String FAILED = "FAILED";
    /** Status could not be resolved (e.g. inspect returned nothing). */
    public static final String UNKNOWN = "UNKNOWN";

    /** {@code load-oci-image} result — the image loaded + a digest was resolved (legacy tarball flow). */
    public static final String LOADED = "LOADED";

    /** ITEM 5 — {@code pull-oci-image} result — the image was {@code podman pull}ed + a digest resolved. */
    public static final String ACTIVE = "ACTIVE";
}
