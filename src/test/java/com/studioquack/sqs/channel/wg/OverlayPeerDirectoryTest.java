package com.studioquack.sqs.channel.wg;

import com.studioquack.sqs.channel.net.FakeNetAgentClient;
import com.studioquack.sqs.channel.net.PeerAddressResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OverlayPeerDirectory} resolves by agent hostname {@code env-<id>}. Offline or unknown
 * peers are empty.
 */
class OverlayPeerDirectoryTest {

    private OverlayPeerDirectory directory;
    private FakeNetAgentClient agent;
    private PeerAddressResolver resolver;

    @BeforeEach
    void setUp() {
        directory = new OverlayPeerDirectory();
        agent = new FakeNetAgentClient();
        resolver = new PeerAddressResolver(agent);
        directory.setPeerAddressResolver(resolver);
    }

    @Test
    @DisplayName("HostName env-x online resolves to its overlay IP")
    void onlinePeer_resolvesByHostname() {
        agent.addPeer("env-x", "100.64.0.20", true);
        directory.replaceGrantedEnvIds(List.of(
                new GrantedPeerDirectory.GrantedPeer("x", "box-x", List.of("execute"))));

        assertEquals("100.64.0.20", directory.addressOf("x").orElseThrow());
        assertEquals("100.64.0.20", directory.addressOn("sqnet0", "x").orElseThrow());
    }

    @Test
    @DisplayName("offline HostName env-x → empty")
    void offlinePeer_isEmpty() {
        agent.addPeer("env-x", "100.64.0.20", false);
        directory.replaceGrantedEnvIds(List.of(
                new GrantedPeerDirectory.GrantedPeer("x", "box-x", List.of())));

        assertTrue(directory.addressOf("x").isEmpty());
        assertTrue(directory.addressOn("sqnet0", "x").isEmpty());
    }

    @Test
    @DisplayName("unknown env → empty")
    void unknownEnv_isEmpty() {
        agent.addPeer("env-x", "100.64.0.20", true);
        directory.replaceGrantedEnvIds(List.of(
                new GrantedPeerDirectory.GrantedPeer("x", "box-x", List.of())));

        assertTrue(directory.addressOf("zzz").isEmpty());
        assertTrue(directory.addressOf(null).isEmpty());
    }

    @Test
    @DisplayName("replaceGrantedEnvIds tracks grant membership, not addresses")
    void grantedEnvIds_areAuthoritative() {
        directory.replaceGrantedEnvIds(List.of(
                new GrantedPeerDirectory.GrantedPeer("b", "gpu", List.of()),
                new GrantedPeerDirectory.GrantedPeer("c", "edge", List.of())));
        directory.replaceGrantedEnvIds(List.of(
                new GrantedPeerDirectory.GrantedPeer("c", "edge", List.of())));

        assertEquals(1, directory.size());
    }

    @Test
    @DisplayName("cleared directory is empty")
    void clear_emptiesGrants() {
        directory.replaceGrantedEnvIds(List.of(
                new GrantedPeerDirectory.GrantedPeer("b", "gpu", List.of())));
        directory.clear();
        assertEquals(0, directory.size());
    }
}
