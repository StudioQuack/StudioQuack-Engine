package com.studioquack.sqs.sandbox;

/**
 * The 3-tier isolation posture. The strongest tier available on the host is selected at boot; an
 * operator pin is honoured even when its dependency is missing (the capability report surfaces the
 * gap).
 */
public enum IsolationTier {

    /** gVisor / {@code runsc} — strongest sandbox-to-host barrier; production default on Linux. */
    BEST("best"),
    /** Container CLI with the default OCI runtime (typically {@code runc}); standard posture. */
    INTERMEDIATE("intermediate"),
    /** Host-process backend (shared kernel); dev / operator-signed only. */
    TRUSTED("trusted");

    private final String wire;

    IsolationTier(String wire) {
        this.wire = wire;
    }

    /** @return the wire-stable id ({@code best}/{@code intermediate}/{@code trusted}). */
    public String wireId() {
        return wire;
    }

    /**
     * True when this tier is at least as strong as {@code floor} (a null floor means "no floor").
     *
     * <p>Constants are declared strongest-first, so a <em>stronger</em> tier has the <em>lower</em>
     * ordinal. Compare through this method rather than on ordinals directly — the inversion is easy
     * to get backwards, and getting it backwards would silently weaken the isolation gate.
     */
    public boolean atLeastAsStrongAs(IsolationTier floor) {
        return floor == null || this.ordinal() <= floor.ordinal();
    }

    /**
     * Parses a wire id (case-insensitive) to a tier; {@code auto}/null/blank/unknown all map
     * to {@code null} meaning "autodetect the strongest available tier".
     */
    public static IsolationTier fromWire(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.trim().toLowerCase()) {
            case "best" -> BEST;
            case "intermediate" -> INTERMEDIATE;
            case "trusted" -> TRUSTED;
            default -> null; // "auto" / unknown → caller autodetects
        };
    }
}
