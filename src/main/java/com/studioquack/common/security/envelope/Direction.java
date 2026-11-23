package com.studioquack.common.security.envelope;

/**
 * Direction bound into the envelope AAD so a payload cannot be replayed the other way.
 * Serialised as the enum name. New values append at the end.
 */
public enum Direction {

    /** Control plane to daemon, or to a sandbox running on the daemon. */
    BE_TO_AGENT,

    /** Daemon or sandbox helper to the control plane. */
    AGENT_TO_BE;
}
