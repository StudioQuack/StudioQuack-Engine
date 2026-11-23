package com.studioquack.sqs.channel.net;

import com.studioquack.sqs.sandbox.egress.OverlayAddresses;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Resolves a sibling's overlay IP from the local agent map at call time. Hostname is {@code env-<envId>}.
 */
@Component
public class PeerAddressResolver {

    /**
     * Upper bound on a cached {@code sq-net status} snapshot.
     */
    public static final long CACHE_TTL_MS = 2_000L;

    private final ObjectProvider<NetAgentClient> agent;
    private final NetAgentClient direct;

    private volatile Cached cached = Cached.EMPTY;

    @org.springframework.beans.factory.annotation.Autowired
    public PeerAddressResolver(ObjectProvider<NetAgentClient> agent) {
        this.agent = agent;
        this.direct = null;
    }

    /**
     * Test / non-Spring constructor.
     */
    public PeerAddressResolver(NetAgentClient agent) {
        this.agent = null;
        this.direct = agent;
    }

    /**
     * Agent hostname for an environment id (never doubles an existing {@code env-} prefix).
     */
    public static String hostnameFor(String envId) {
        if (envId == null || envId.isBlank()) {
            return "";
        }
        String id = envId.trim();
        return id.startsWith("env-") ? id : "env-" + id;
    }

    /**
     * Overlay IP of {@code env-<envId>} when the peer is in the map and online. Empty when the agent is absent,
     * the peer is missing, or the peer is offline.
     */
    public Optional<String> resolve(String envId) {
        NetAgentStatus status = statusCached();
        if (status == null) {
            return Optional.empty();
        }
        return status.peerByEnvId(envId)
                .filter(NetAgentStatus.Peer::online)
                .flatMap(NetAgentStatus.Peer::primaryIp)
                // The address is dialled, pinned into reverse-relay ACLs and written into
                // per-container iptables grants; only an overlay host address may be any of those.
                .filter(OverlayAddresses::isOverlayAddress);
    }

    /**
     * Latest cached (or freshly fetched) status; {@code null} when no data plane.
     */
    public NetAgentStatus statusCached() {
        NetAgentClient client = client();
        if (client == null) {
            return null;
        }
        Cached hit = this.cached;
        long now = System.nanoTime();
        if (hit.status != null && (now - hit.atNanos) < CACHE_TTL_MS * 1_000_000L) {
            return hit.status;
        }
        try {
            NetAgentStatus next = client.status();
            this.cached = new Cached(next, now);
            return next;
        } catch (Exception ignored) {
            return hit.status;
        }
    }

    /**
     * Drops the cache (handshake / reconnect / tests).
     */
    public void invalidate() {
        this.cached = Cached.EMPTY;
    }

    private NetAgentClient client() {
        if (direct != null) {
            return direct;
        }
        return agent == null ? null : agent.getIfAvailable();
    }

    private record Cached(NetAgentStatus status, long atNanos) {
        static final Cached EMPTY = new Cached(null, 0L);
    }
}
