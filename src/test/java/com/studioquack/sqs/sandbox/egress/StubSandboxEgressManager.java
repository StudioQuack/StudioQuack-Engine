package com.studioquack.sqs.sandbox.egress;

/**
 * Test double that pins {@link #readiness()} to a fixed snapshot (no Spring context).
 */
public final class StubSandboxEgressManager extends SandboxEgressManager {

    private final EgressReadiness fixed;

    public StubSandboxEgressManager(EgressReadiness.State state, String reason) {
        this(new EgressReadiness(state, reason, ""));
    }

    public StubSandboxEgressManager(EgressReadiness fixed) {
        this.fixed = fixed;
    }

    public static StubSandboxEgressManager unavailable(String reason) {
        return new StubSandboxEgressManager(EgressReadiness.State.UNAVAILABLE, reason);
    }

    public static StubSandboxEgressManager enforced() {
        return new StubSandboxEgressManager(EgressReadiness.State.ENFORCED, "test-enforced");
    }

    /** DEGRADED without the operator opt-out — fails closed for networked starts. */
    public static StubSandboxEgressManager degraded() {
        return new StubSandboxEgressManager(EgressReadiness.State.DEGRADED, "test-degraded");
    }

    /** DEGRADED produced by {@code overlay-egress=off} — the explicit opt-out that allows starts. */
    public static StubSandboxEgressManager degradedOptOut() {
        return new StubSandboxEgressManager(
                new EgressReadiness(EgressReadiness.State.DEGRADED, "overlay-egress disabled", "", true));
    }

    @Override
    public EgressReadiness readiness() {
        return fixed;
    }
}
