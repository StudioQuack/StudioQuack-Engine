package com.studioquack.sqs.sandbox.warmpool;

import java.util.List;

/**
 * Backend-aware pre-spawn / kill / health SPI the {@link WarmPool} delegates to. The pool owns the
 * reservoir bookkeeping (image-keyed targets, idle slots, reconcile/reap); the warmer owns the
 * actual container/process lifecycle so the pool logic stays unit-testable against a fake.
 */
public interface Warmer {

    /** Short kind id for dashboards/logs ({@code oci} / {@code noop}). */
    String kind();

    /**
     * Pre-spawns one warm seat for {@code spec} and returns its slot.
     *
     * @throws WarmException when the spawn fails (the pool degrades to a cold dispatch for that job).
     */
    WarmSlot warm(WarmSpec spec) throws WarmException;

    /**
     * What to warm: the resolved OCI image ref (the pool's identity key), an optional language hint
     * (drives the python persistent-runner / helper-broker eligibility and the dep-cache mount; null
     * for a custom entrypoint image), and the per-image caps (<=0 = unset). Adds {@code gpu}: emitted
     * as {@code --gpus} only after {@code GpuCapabilityProbe} confirmed the host can honour it.
     */
    record WarmSpec(String imageRef, String language, double cpus, int ramMb, int diskMb, int gpu) {
        /** Back-compat: older shape (no gpu). */
        public WarmSpec(String imageRef, String language, double cpus, int ramMb, int diskMb) {
            this(imageRef, language, cpus, ramMb, diskMb, 0);
        }
    }

    /** Tears down a slot's underlying container/process (idempotent, best-effort). */
    void cool(WarmSlot slot);

    /** @return true when the slot's container/process is still alive + usable. */
    boolean alive(WarmSlot slot);

    /**
     * One container on the host carrying this daemon's owner label.
     *
     * @param containerId The full (untruncated) id, so it compares equal to the id {@link #warm}.
     * @param role {@code warm} for a parked slot, {@code job} for a single-job container.
     * @param bootId The run of this daemon that created it; a value other than the current one me.
     */
    record OwnedContainer(String containerId, String role, String bootId) {
    }

    /**
     * Host truth: every container this daemon owns, whether or not the reservoir still knows about
     * it. This is what makes the pool converge on what is actually RUNNING rather than on its own
     * bookkeeping — a container the pool lost track of has no other way back.
     *
     * <p>Default is empty: a warmer with no container backend owns nothing, so nothing is reclaimed.
     */
    default List<OwnedContainer> ownedContainers() {
        return List.of();
    }

    /**
     * Force-removes a container by id, with no slot to go by. Used to reclaim an abandoned container
     * the reservoir has no {@link WarmSlot} for.
     */
    default void remove(String containerId) {
        // no container backend: nothing to remove
    }

    /** Thrown by {@link #warm(WarmSpec)} when a seat could not be pre-spawned. */
    class WarmException extends Exception {
        public WarmException(String message) {
            super(message);
        }
    }
}
