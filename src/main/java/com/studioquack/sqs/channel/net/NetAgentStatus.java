package com.studioquack.sqs.channel.net;

import java.util.List;
import java.util.Optional;

/**
 * Parsed {@code sq-net status --json} view the daemon consumes. Field names match the open-source Tailscale
 * client JSON ({@code Self}, {@code Peer}, {@code BackendState}).
 */
public record NetAgentStatus(
        String backendState,
        String hostname,
        String publicKey,
        List<String> selfIps,
        String interfaceName,
        List<Peer> peers) {

    public NetAgentStatus {
        selfIps = selfIps == null ? List.of() : List.copyOf(selfIps);
        peers = peers == null ? List.of() : List.copyOf(peers);
    }

    /**
     * True when the agent reports {@code BackendState=Running}.
     */
    public boolean running() {
        return "Running".equalsIgnoreCase(backendState);
    }

    /**
     * First IPv4, else the first advertised address (listeners never bind {@code 0.0.0.0}).
     */
    public Optional<String> primaryIp() {
        if (selfIps == null || selfIps.isEmpty()) {
            return Optional.empty();
        }
        for (String ip : selfIps) {
            if (ip != null && ip.indexOf(':') < 0) {
                return Optional.of(ip);
            }
        }
        return Optional.ofNullable(selfIps.getFirst());
    }

    /**
     * Peer whose agent hostname is {@code env-<envId>}.
     */
    public Optional<Peer> peerByEnvId(String envId) {
        if (envId == null || envId.isBlank()) {
            return Optional.empty();
        }
        String want = PeerAddressResolver.hostnameFor(envId);
        // A re-registered node (reboot after its key expired, or a re-issued key) can sit next to
        // its stale twin in the netmap until the coordinator drops it — both report env-<id>.
        // Map is ordered by node key, so the first match was often the OFFLINE one and every
        // cross-env call to the returned engine failed as "absent or offline" (2026-11-05).
        Peer offlineMatch = null;
        for (Peer p : peers) {
            if (p != null && want.equals(p.hostname())) {
                if (p.online()) {
                    return Optional.of(p);
                }
                if (offlineMatch == null) {
                    offlineMatch = p;
                }
            }
        }
        return Optional.ofNullable(offlineMatch);
    }

    public record Peer(String hostname, List<String> ips, boolean online) {
        public Peer {
            ips = ips == null ? List.of() : List.copyOf(ips);
        }

        public Optional<String> primaryIp() {
            if (ips == null || ips.isEmpty()) {
                return Optional.empty();
            }
            for (String ip : ips) {
                if (ip != null && ip.indexOf(':') < 0) {
                    return Optional.of(ip);
                }
            }
            return Optional.ofNullable(ips.getFirst());
        }
    }
}
