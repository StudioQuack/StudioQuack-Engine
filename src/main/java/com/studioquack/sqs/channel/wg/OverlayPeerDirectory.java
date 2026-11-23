package com.studioquack.sqs.channel.wg;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Where each same-owner sibling environment lives on the overlay: {@code envId → overlay IP}. Handed over at
 * WireGuard handshake and refreshed on each one.
 */
@Slf4j
@Component
public class OverlayPeerDirectory {

    /**
     * Network key used by the legacy single-network {@link #replace(Map)} form (tests).
     */
    private static final String DEFAULT_KEY = "default";

    /**
     * IP resolution from the local agent map. Null in tests that construct this directory without Spring;
     * {@link #addressOf} then uses the last {@link #replace} map.
     */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.channel.net.PeerAddressResolver peerAddressResolver;

    /**
     * Network key (interface name) → that network's {@code envId → overlay IP} view.
     */
    private final ConcurrentHashMap<String, Map<String, String>> byNetwork = new ConcurrentHashMap<>();

    /**
     * The network the daemon's own control plane currently rides — searched FIRST by {@link #addressOf}. Set by
     * the bootstrap on every primary bring-up and moved to the new network by the drain manager at finalize.
     */
    private final AtomicReference<String> primaryKey = new AtomicReference<>(DEFAULT_KEY);

    /**
     * Single subscriber (the sandbox egress manager) notified on ANY content change — including a peer keeping its
     * id but moving to a new /32, which is exactly the address-churn case per-container egress rules must follow.
     */
    private volatile Runnable changeListener = () -> {
    };

    /**
     * Registers the change subscriber; a null listener restores the no-op.
     */
    public void setChangeListener(Runnable listener) {
        this.changeListener = listener == null ? () -> {
        } : listener;
    }

    /**
     * Marks {@code networkKey} as the network the daemon itself dials from.
     */
    public void setPrimaryKey(String networkKey) {
        if (networkKey != null && !networkKey.isBlank()) {
            primaryKey.set(networkKey);
        }
    }

    /**
     * The current primary network key (diagnostics / tests).
     */
    public String primaryKey() {
        return primaryKey.get();
    }

    /**
     * Legacy single-network form — replaces the view on the current primary network.
     */
    public void replace(Map<String, String> peers) {
        replace(primaryKey.get(), peers);
    }

    /**
     * Replaces ONE network's directory wholesale — a handshake is the authoritative view of that network's mesh,
     * not a delta. Other networks' views are untouched: a drain handshake must never wipe the primary mesh the
     * running containers still resolve through.
     */
    public void replace(String networkKey, Map<String, String> peers) {
        String key = (networkKey == null || networkKey.isBlank()) ? DEFAULT_KEY : networkKey;
        Map<String, String> next = peers == null ? Map.of() : Map.copyOf(peers);
        Map<String, String> previous = byNetwork.put(key, next);
        if (previous == null || !previous.keySet().equals(next.keySet())) {
            log.info("overlay-directory: network {} has {} same-owner peer(s) reachable {}",
                    key, next.size(), next.keySet());
        }
        if (!next.equals(previous)) {
            // Same key set with a moved address matters as much as a join/leave: rules are keyed
            // by engine id but enforced by address, so the subscriber must re-resolve now.
            changeListener.run();
        }
    }

    /**
     * Drops one network's view (its tunnel is gone); other networks keep theirs.
     */
    public void remove(String networkKey) {
        if (networkKey == null) {
            return;
        }
        Map<String, String> removed = byNetwork.remove(networkKey);
        if (removed != null && !removed.isEmpty()) {
            log.info("overlay-directory: network {} dropped ({} peer(s) forgotten)",
                    networkKey, removed.size());
            changeListener.run();
        }
    }

    /**
     * The sibling's overlay address for the DAEMON's own dialing: primary network first (a mid-drain sibling still
     * serves its listener on the old address), then every other network the daemon holds. Empty when it is not a
     * reachable same-owner peer on any live network.
     */
    public Optional<String> addressOf(String envId) {
        if (envId == null || envId.isBlank()) {
            return Optional.empty();
        }
        if (peerAddressResolver != null) {
            Optional<String> fromAgent = peerAddressResolver.resolve(envId);
            if (fromAgent.isPresent()) {
                return fromAgent;
            }
        }
        String primary = primaryKey.get();
        Map<String, String> primaryView = byNetwork.get(primary);
        String hit = primaryView == null ? null : primaryView.get(envId);
        if (hit != null && !hit.isBlank()) {
            return Optional.of(hit);
        }
        for (Map.Entry<String, Map<String, String>> e : byNetwork.entrySet()) {
            if (e.getKey().equals(primary)) {
                continue;
            }
            String addr = e.getValue().get(envId);
            if (addr != null) {
                return Optional.of(addr);
            }
        }
        return Optional.empty();
    }

    /**
     * The sibling's address ON ONE NETWORK — the only correct lookup for a container's egress grant or a
     * reverse-relay ACL, because a container's packets can ride exactly one tunnel. Never falls through to another
     * network: that would resolve an address the caller's packets cannot reach, turning a routing refusal into a
     * silently-wider rule.
     */
    public Optional<String> addressOn(String networkKey, String envId) {
        if (envId == null || envId.isBlank()) {
            return Optional.empty();
        }
        // Single mesh: network key is no longer a routing dimension.
        if (peerAddressResolver != null) {
            Optional<String> fromAgent = peerAddressResolver.resolve(envId);
            if (fromAgent.isPresent()) {
                return fromAgent;
            }
        }
        if (networkKey == null) {
            return Optional.empty();
        }
        Map<String, String> view = byNetwork.get(networkKey);
        String hit = view == null ? null : view.get(envId);
        return (hit == null || hit.isBlank()) ? Optional.empty() : Optional.of(hit);
    }

    /**
     * Handshake / tests: remember granted env ids (addresses come from the agent).
     */
    public void replaceGrantedEnvIds(
            java.util.List<com.studioquack.sqs.channel.wg.GrantedPeerDirectory.GrantedPeer> peers) {
        java.util.Map<String, String> ids = new java.util.LinkedHashMap<>();
        if (peers != null) {
            for (var p : peers) {
                if (p != null && p.envId() != null && !p.envId().isBlank()) {
                    ids.put(p.envId(), "");
                }
            }
        }
        replace(primaryKey.get(), ids);
    }

    /**
     * Test seam — inject the resolver without Spring.
     */
    public void setPeerAddressResolver(com.studioquack.sqs.channel.net.PeerAddressResolver resolver) {
        this.peerAddressResolver = resolver;
    }

    /**
     * Cleared when the overlay goes down, so a stale address is never dialled.
     */
    public void clear() {
        byNetwork.clear();
        changeListener.run();
    }

    /**
     * @return distinct reachable sibling ids across ALL live networks (diagnostics).
     */
    public int size() {
        return byNetwork.values().stream()
                .flatMap(m -> m.keySet().stream())
                .collect(java.util.stream.Collectors.toSet())
                .size();
    }
}
