package com.studioquack.sqs.channel.net;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resolve a sibling by {@code env-<envId>} hostname from the local agent map. Offline or unknown
 * peers are empty (never a guessed address).
 */
class PeerAddressResolverTest {

    @Test
    void hostnameFor_prefixesOnce() {
        assertEquals("env-x", PeerAddressResolver.hostnameFor("x"));
        assertEquals("env-x", PeerAddressResolver.hostnameFor("env-x"));
        assertEquals("", PeerAddressResolver.hostnameFor(""));
        assertEquals("", PeerAddressResolver.hostnameFor(null));
    }

    @Test
    void onlinePeer_resolvesPrimaryIp() {
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .addPeer("env-x", "100.64.0.20", true)
                .addPeer("env-offline", "100.64.0.21", false);
        PeerAddressResolver resolver = new PeerAddressResolver(agent);

        assertEquals("100.64.0.20", resolver.resolve("x").orElseThrow());
        assertEquals("100.64.0.20", resolver.resolve("env-x").orElseThrow());
    }

    @Test
    void offlinePeer_isEmpty() {
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .addPeer("env-x", "100.64.0.20", false);
        PeerAddressResolver resolver = new PeerAddressResolver(agent);

        assertTrue(resolver.resolve("x").isEmpty(), "offline must not yield an address");
    }

    // 2026-11-05 — a re-registered node can sit next to its stale twin (same env-<id> hostname)
    // until the coordinator drops it; the map is key-ordered, so the offline twin often came first.
    @Test
    void staleOfflineTwinFirst_stillResolvesTheOnlineNode() {
        PeerAddressResolver resolver = new PeerAddressResolver(new FakeNetAgentClient()
                .withBackendState("Running")
                .addPeer("env-x", "100.64.0.20", false)
                .addPeer("env-x", "100.64.0.33", true));

        assertEquals(java.util.Optional.of("100.64.0.33"), resolver.resolve("x"));
    }

    @Test
    void unknownEnv_isEmpty() {
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .addPeer("env-x", "100.64.0.20", true);
        PeerAddressResolver resolver = new PeerAddressResolver(agent);

        assertTrue(resolver.resolve("zzz").isEmpty());
        assertTrue(resolver.resolve(null).isEmpty());
    }

    @Test
    void noAgent_isEmpty() {
        PeerAddressResolver resolver = new PeerAddressResolver((NetAgentClient) null);
        assertTrue(resolver.resolve("x").isEmpty());
    }
}
