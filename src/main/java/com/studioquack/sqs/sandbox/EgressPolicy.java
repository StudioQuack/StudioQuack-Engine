package com.studioquack.sqs.sandbox;

/**
 * Per-job network egress posture the sandbox enforces when wiring a container. The platform
 * default is intentionally {@link #OPEN} because real customer flows call third-party APIs
 * (payment, weather, OAuth, OpenAI); operators tighten this per env.
 */
public enum EgressPolicy {

    /** Segmented jobs network, full outbound + metadata DNS nullification (the default). */
    OPEN,
    /** {@code --network none}: total network containment. */
    NONE;

    /** The platform default when a job does not name a policy (Go {@code DefaultEgressPolicy}). */
    public static final EgressPolicy DEFAULT = OPEN;

    /**
     * Parses a wire string ({@code open}/{@code restricted}/{@code none}, case-insensitive) to a
     * policy, falling back to {@link #DEFAULT} for null/blank/unknown values so a stray value can
     * never silently disable containment by throwing. {@code "restricted"} maps to {@link #OPEN}: the
     * former enum constant was deleted because it never differed from OPEN at the OCI layer; accepting
     * the wire string avoids breaking stored env configs that still say {@code restricted}.
     */
    public static EgressPolicy fromWire(String value) {
        if (value == null) {
            return DEFAULT;
        }
        return switch (value.trim().toLowerCase()) {
            case "none" -> NONE;
            // Legacy synonym — never a real OCI allowlist; treat as OPEN.
            case "restricted" -> OPEN;
            case "open" -> OPEN;
            default -> DEFAULT;
        };
    }
}
