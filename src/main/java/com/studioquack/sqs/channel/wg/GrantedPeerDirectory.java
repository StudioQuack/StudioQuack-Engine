package com.studioquack.sqs.channel.wg;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The GRANTED-peers directory delivered as {@code meshPeers} on every WireGuard handshake: the sibling
 * environments THIS daemon may call, each with its overlay address and the scopes the backend granted (
 * {@code execute} / {@code llm}). This is the outbound mirror of the inbound grant set — where
 * {@code OverlayPeerDirectory} answers "where does this sibling live", this directory answers "am I allowed to
 * call it, and for what".
 */
@Slf4j
@Component
public class GrantedPeerDirectory {

    /**
     * One granted sibling, exactly as the handshake's {@code meshPeers} array carries it. {@code scopes}
     * normalises to an immutable empty list when absent (grants nothing).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    /**
     * One granted sibling. Address is resolved at call time from the local agent map — this record is grant
     * membership + scopes only.
     */
    public record GrantedPeer(String envId, String name, List<String> scopes) {
        public GrantedPeer {
            scopes = scopes == null ? List.of()
                    : Collections.unmodifiableList(new ArrayList<>(scopes));
        }
    }

    /**
     * Network key used before any primary is designated (tests, pre-drain single network).
     */
    private static final String DEFAULT_KEY = "default";

    /**
     * Network key (interface name) → that network's {@code envId → GrantedPeer} view.
     */
    private final ConcurrentHashMap<String, Map<String, GrantedPeer>> byNetwork = new ConcurrentHashMap<>();

    /**
     * The network the daemon's own dialing resolves on FIRST (set on every primary bring-up).
     */
    private final AtomicReference<String> primaryKey = new AtomicReference<>(DEFAULT_KEY);

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
     * Replaces ONE network's granted view wholesale — a handshake is the authoritative statement of that network's
     * grants, not a delta. Other networks' views are untouched.
     *
     * @return {@code true} when the network's envId set changed (a sibling appeared or disappeared). Scope-only
     * edits return {@code false}.
     */
    public boolean replace(String networkKey, List<GrantedPeer> peers) {
        String key = (networkKey == null || networkKey.isBlank()) ? DEFAULT_KEY : networkKey;
        Map<String, GrantedPeer> next = new LinkedHashMap<>();
        if (peers != null) {
            for (GrantedPeer peer : peers) {
                if (peer == null || peer.envId() == null || peer.envId().isBlank()) {
                    continue;
                }
                next.put(peer.envId(), peer);
            }
        }
        Map<String, GrantedPeer> previous = byNetwork.put(key, Map.copyOf(next));
        boolean idsChanged = !idsOf(previous).equals(idsOf(next));
        log.info("channel/mesh: granted-peer directory for network {} replaced ({} peer(s), "
                + "idSetChanged={})", key, next.size(), idsChanged);
        return idsChanged;
    }

    private static java.util.Set<String> idsOf(Map<String, GrantedPeer> view) {
        return view == null || view.isEmpty() ? java.util.Set.of() : view.keySet();
    }

    /**
     * Drops one network's view (its tunnel is gone); other networks keep theirs.
     */
    public void remove(String networkKey) {
        if (networkKey != null && byNetwork.remove(networkKey) != null) {
            log.info("channel/mesh: granted-peer directory for network {} dropped", networkKey);
        }
    }

    /**
     * Cleared when the overlay goes down, so a stale grant is never resolved.
     */
    public void clear() {
        byNetwork.clear();
    }

    /**
     * Resolves a sibling by environment ID or by environment NAME (case-sensitive exact match — the value an
     * operator copies from Engine Settings). The primary network is searched first, then every other network the
     * daemon holds.
     */
    public Optional<GrantedPeer> find(String envIdOrName) {
        if (envIdOrName == null || envIdOrName.isBlank()) {
            return Optional.empty();
        }
        String primary = primaryKey.get();
        Map<String, GrantedPeer> primaryView = byNetwork.get(primary);
        Optional<GrantedPeer> hit = findIn(primaryView, envIdOrName);
        if (hit.isPresent()) {
            return hit;
        }
        for (Map.Entry<String, Map<String, GrantedPeer>> e : byNetwork.entrySet()) {
            if (e.getKey().equals(primary)) {
                continue;
            }
            hit = findIn(e.getValue(), envIdOrName);
            if (hit.isPresent()) {
                return hit;
            }
        }
        return Optional.empty();
    }

    /**
     * All distinct granted peers across ALL live networks (primary first), de-duplicated by envId.
     */
    public List<GrantedPeer> peers() {
        Map<String, GrantedPeer> merged = new LinkedHashMap<>();
        String primary = primaryKey.get();
        Map<String, GrantedPeer> primaryView = byNetwork.get(primary);
        if (primaryView != null) {
            merged.putAll(primaryView);
        }
        byNetwork.forEach((key, view) -> {
            if (!key.equals(primary)) {
                view.forEach(merged::putIfAbsent);
            }
        });
        return List.copyOf(merged.values());
    }

    /**
     * Distinct granted sibling ids across ALL live networks (diagnostics).
     */
    public int size() {
        return (int) byNetwork.values().stream()
                .flatMap(m -> m.keySet().stream())
                .distinct()
                .count();
    }

    /**
     * EnvId key lookup first, then a case-sensitive exact name match within ONE network's view.
     */
    private static Optional<GrantedPeer> findIn(Map<String, GrantedPeer> view, String envIdOrName) {
        if (view == null) {
            return Optional.empty();
        }
        GrantedPeer byId = view.get(envIdOrName);
        if (byId != null) {
            return Optional.of(byId);
        }
        return view.values().stream()
                .filter(p -> envIdOrName.equals(p.name()))
                .findFirst();
    }
}
