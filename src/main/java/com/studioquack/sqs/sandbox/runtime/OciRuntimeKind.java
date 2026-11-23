package com.studioquack.sqs.sandbox.runtime;

/**
 * Which container-runtime CLI the daemon drives for sandbox / warm-pool execution. Podman-only
 * (architecture requirement).
 */
public enum OciRuntimeKind {

    /** {@code podman run …} — the ONLY runtime the client execution layer drives. */
    PODMAN("podman");

    private final String cli;

    OciRuntimeKind(String cli) {
        this.cli = cli;
    }

    /** The CLI binary name for {@code exec.LookPath}. */
    public String cli() {
        return cli;
    }

    /**
     * Autodetection preference order — <b>Podman only</b>. The client must run its sandboxes on
     * Podman and must not fall back to any other runtime.
     */
    public static OciRuntimeKind[] preferenceOrder() {
        return new OciRuntimeKind[]{PODMAN};
    }
}
