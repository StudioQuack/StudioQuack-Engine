package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.MeshPeerSetChangedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SetMeshPeersCommandHandler}: handshake-v2 payload is {@code {envId,name,scopes}}
 * (no address). A revoked peer disappears; a malformed push fails closed.
 */
class SetMeshPeersCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final GrantedPeerDirectory directory = new GrantedPeerDirectory();
    private final List<Object> published = new ArrayList<>();
    private final ApplicationEventPublisher publisher = published::add;
    private final SetMeshPeersCommandHandler handler =
            new SetMeshPeersCommandHandler(mapper, directory, publisher);

    private DaemonCommand cmd(String payload) {
        return new DaemonCommand("cmd-1", "set-mesh-peers", "2026-08-17T00:00:00Z", payload);
    }

    private static String peerJson(String envId, String name, String scopes) {
        return "{\"meshPeers\":[{\"envId\":\"" + envId + "\",\"name\":\"" + name
                + "\",\"scopes\":" + scopes + "}]}";
    }

    @Test
    void appliesThePushedViewToThePrimaryNetwork() {
        DaemonCommandOutcome outcome = handler.handle(cmd(
                peerJson("env-b", "gpu-box", "[\"llm\"]")));

        assertTrue(outcome.succeeded(), "got: " + outcome.error());
        assertEquals(1, directory.size());
        GrantedPeerDirectory.GrantedPeer peer = directory.find("env-b").orElseThrow();
        assertEquals("gpu-box", peer.name());
        assertEquals(List.of("llm"), peer.scopes());
        assertTrue(directory.find("gpu-box").isPresent());
    }

    @Test
    void wholesaleReplaceDropsRevokedPeers() {
        handler.handle(cmd(peerJson("env-b", "gpu-box", "[\"llm\"]")));
        assertTrue(directory.find("env-b").isPresent());

        DaemonCommandOutcome outcome = handler.handle(cmd("{\"meshPeers\":[]}"));

        assertTrue(outcome.succeeded());
        assertTrue(directory.find("env-b").isEmpty(), "a revoked peer must leave the directory");
        assertEquals(0, directory.size());
    }

    @Test
    void malformedPayloadFailsClosedAndKeepsTheCurrentDirectory() {
        handler.handle(cmd(peerJson("env-b", "gpu-box", "[\"execute\"]")));

        DaemonCommandOutcome outcome = handler.handle(cmd("not-json"));

        assertFalse(outcome.succeeded());
        assertTrue(directory.find("env-b").isPresent(), "a bad push must not wipe the live view");
    }

    @Test
    void missingMeshPeersArrayFails() {
        assertFalse(handler.handle(cmd("{}")).succeeded());
        assertFalse(handler.handle(cmd(null)).succeeded());
    }

    @Test
    void publishesMeshPeerSetChangedWhenAnEnvAppearsOrLeaves() {
        handler.handle(cmd(peerJson("env-b", "gpu-box", "[\"llm\"]")));
        assertEquals(1, published.size());
        assertTrue(published.get(0) instanceof MeshPeerSetChangedEvent);

        handler.handle(cmd("{\"meshPeers\":[]}"));
        assertEquals(2, published.size(), "a revocation must re-fire the event");
    }

    @Test
    void noEventWhenOnlyScopesChangeOrThePushIsIdentical() {
        handler.handle(cmd(peerJson("env-b", "gpu-box", "[\"llm\"]")));
        assertEquals(1, published.size());

        handler.handle(cmd(peerJson("env-b", "gpu-box", "[\"execute\",\"llm\"]")));
        assertEquals(1, published.size(), "a scope-only change must not fire the event");

        handler.handle(cmd(peerJson("env-b", "gpu-box", "[\"execute\",\"llm\"]")));
        assertEquals(1, published.size(), "an identical push must not fire the event");
    }

    @Test
    void malformedPayloadPublishesNothing() {
        handler.handle(cmd("not-json"));
        assertTrue(published.isEmpty());
    }
}
