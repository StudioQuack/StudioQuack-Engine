package com.studioquack.sqs.sandbox.reverse;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The per-engine reverse-port claim table. Port space is per ENGINE, not per container (a
 * container does not get its own overlay address — the engine's single {@code /32} is its external
 * identity, and a reverse-channel container is published as a port on it).
 */
public final class ReversePortRegistry {

    /** Ports the daemon itself occupies on the engine's overlay address. */
    public static final Set<Integer> RESERVED_PORTS = Set.of(9876, 8443);

    private final Map<Integer, String> claimantByPort = new ConcurrentHashMap<>();

    /**
     * Claims {@code port} for {@code claimant} (a node label / instance id, for diagnostics).
     * Idempotent for the SAME claimant (a stateful re-start re-publishes its own port).
     *
     * @throws ReversePortException when the port is reserved, out of range, or held by
     *         ANOTHER claimant — the message names the holder.
     */
    public void claim(int port, String claimant) {
        if (port < 1 || port > 65535) {
            throw new ReversePortException("reverse port " + port + " is out of range (1-65535)");
        }
        if (RESERVED_PORTS.contains(port)) {
            throw new ReversePortException("reverse port " + port
                    + " is reserved for the daemon's own overlay listener — pick another port");
        }
        String owner = claimant == null || claimant.isBlank() ? "unknown" : claimant;
        String existing = claimantByPort.putIfAbsent(port, owner);
        if (existing != null && !existing.equals(owner)) {
            throw new ReversePortException("reverse port " + port + " is already published by "
                    + existing + " — stop that node or pick another port");
        }
    }

    /** Releases the claim held by {@code claimant}; a claim held by someone else is left alone. */
    public void release(int port, String claimant) {
        claimantByPort.remove(port, claimant);
    }

    /** @return the claimant holding {@code port}, or empty when it is free. */
    public Optional<String> claimantOf(int port) {
        return Optional.ofNullable(claimantByPort.get(port));
    }

    /** Loud claim failure — carries the reason verbatim to the start result. */
    public static final class ReversePortException extends RuntimeException {
        public ReversePortException(String message) {
            super(message);
        }
    }
}
