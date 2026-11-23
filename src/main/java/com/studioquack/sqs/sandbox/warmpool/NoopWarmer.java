package com.studioquack.sqs.sandbox.warmpool;

/**
 * No-op {@link Warmer} used only when Podman is unavailable. It deliberately DOES NOT mint fake warm
 * slots: the Runtime UI must not show "pre-built" capacity unless real Podman containers exist, and
 * execution must fail clearly instead of pretending a container pool is ready.
 */
public final class NoopWarmer implements Warmer {

    @Override
    public String kind() {
        return "noop";
    }

    @Override
    public WarmSlot warm(WarmSpec spec) throws WarmException {
        throw new WarmException("Podman runtime unavailable; install/start Podman before setting warm targets");
    }

    @Override
    public void cool(WarmSlot slot) {
        // token-only: nothing to tear down
    }

    @Override
    public boolean alive(WarmSlot slot) {
        return false;
    }
}
