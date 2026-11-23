package com.studioquack.sqs.aimodel;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The daemon's runtime verdict on HOW the configured provider's model lifecycle is controlled —
 * probed, never assumed. It sits next to {@link AiModelLoadState} and is recomputed whenever a
 * {@code set-ai-model} push lands (and on demand for a draft config the {@code test-connection}
 * handler probes before Save).
 */
@Component
public class AiModelCapabilityState {

    /** How the configured provider's model residency is controlled. */
    public enum Lifecycle {
        MANAGED,
        PROVIDER_MANAGED
    }

    /**
     * A probed capability verdict: the {@link Lifecycle} plus an operator-facing
     * {@code reason} the {@code test-connection} result surfaces so the panel can
     * explain WHY a provider is managed-by-provider (no load path, HTTP 404, …).
     */
    public record Capability(Lifecycle lifecycle, String reason) {

        /** True only when the daemon itself can load/unload on this provider. */
        public boolean managed() {
            return lifecycle == Lifecycle.MANAGED;
        }

        public static Capability providerManaged(String reason) {
            return new Capability(Lifecycle.PROVIDER_MANAGED, reason);
        }

        public static Capability managedCapability(String reason) {
            return new Capability(Lifecycle.MANAGED, reason);
        }
    }

    /** Default before any config is applied: provider-managed (never gate on a guess). */
    private static final Capability UNCONFIGURED =
            Capability.providerManaged("No provider configured");

    private final AtomicReference<Capability> current = new AtomicReference<>(UNCONFIGURED);

    /** Whether a real probe has ever written this verdict. */
    private volatile boolean probed = false;

    /** The live capability verdict (never null; provider-managed until a push probes). */
    public Capability current() {
        return current.get();
    }

    /** True when the daemon owns load/unload for the current provider. */
    public boolean managed() {
        return current.get().managed();
    }

    /** True once a probe has authoritatively set the verdict (see {@link #probed}). */
    public boolean isProbed() {
        return probed;
    }

    /** Replaces the verdict after a probe (null ignored, keeping the last verdict). */
    public void set(Capability capability) {
        if (capability != null) {
            current.set(capability);
            probed = true;
        }
    }

    /** Resets to the unconfigured default (provider server cleared / changed away). */
    public void reset() {
        current.set(UNCONFIGURED);
        probed = false;
    }
}
