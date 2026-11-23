package com.studioquack.sqs.status;

/**
 * Per-run policy that decides how a run's per-node sandbox logs reach the control plane. The
 * distinction exists to cut control-plane traffic: only a Designer "Run" (live execution view)
 * needs a per-step round-trip.
 */
public enum RunLogMode {

    /** Per-node logs are mirrored to the control plane live (Designer Run button). */
    STREAM,

    /** Per-node logs are buffered locally and flushed once at the terminal boundary. */
    BATCH
}
