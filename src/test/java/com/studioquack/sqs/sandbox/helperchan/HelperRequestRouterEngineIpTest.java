package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.net.FakeNetAgentClient;
import com.studioquack.sqs.channel.net.PeerAddressResolver;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code engine-ip} resolves via {@link PeerAddressResolver} ({@code HostName env-<id>}, Online).
 * Empty grants → 503; unknown → 404.
 */
class HelperRequestRouterEngineIpTest {

    private static final String ENGINE_ID = "engine-1";
    private static final String PEER_IP = "100.64.0.5";

    private final ObjectMapper mapper = new ObjectMapper();
    private final OverlayPeerDirectory peerDirectory = new OverlayPeerDirectory();
    private FakeNetAgentClient agent;
    private final HelperRequestRouter router =
            new HelperRequestRouter(mapper, null, null, null, peerDirectory);

    @BeforeEach
    void setUp() {
        agent = new FakeNetAgentClient()
                .addPeer(PeerAddressResolver.hostnameFor(ENGINE_ID), PEER_IP, true);
        peerDirectory.setPeerAddressResolver(new PeerAddressResolver(agent));
    }

    private void grantEngine() {
        peerDirectory.replaceGrantedEnvIds(List.of(
                new GrantedPeerDirectory.GrantedPeer(ENGINE_ID, ENGINE_ID, List.of())));
    }

    private String registerJob() {
        return router.register(HelperJob.builder("job-1", "python").build());
    }

    private HelperRequestRouter.RoutedResponse call(String bearer, String body) {
        return router.route("POST", "engine-ip", bearer,
                body.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void resolvesASameOwnerEngineToItsCurrentAddress() throws Exception {
        String bearer = registerJob();
        grantEngine();

        HelperRequestRouter.RoutedResponse resp = call(bearer, "{\"engineId\":\"" + ENGINE_ID + "\"}");

        assertEquals(200, resp.status());
        Map<?, ?> parsed = mapper.readValue(resp.body(), Map.class);
        assertEquals(PEER_IP, parsed.get("ip"));
    }

    @Test
    void unknownEngineIsA404_neverAGuess() {
        String bearer = registerJob();
        grantEngine();

        HelperRequestRouter.RoutedResponse resp = call(bearer, "{\"engineId\":\"engine-B\"}");

        assertEquals(404, resp.status());
    }

    @Test
    void emptyDirectoryMeansOverlayDown_503() {
        String bearer = registerJob();
        peerDirectory.clear();

        HelperRequestRouter.RoutedResponse resp = call(bearer, "{\"engineId\":\"" + ENGINE_ID + "\"}");

        assertEquals(503, resp.status(),
                "an empty directory must fail closed rather than answer a stale address");
    }

    @Test
    void missingEngineIdIsA400() {
        String bearer = registerJob();
        grantEngine();

        assertEquals(400, call(bearer, "{}").status());
    }

    @Test
    void envIdAliasIsAccepted() throws Exception {
        String bearer = registerJob();
        grantEngine();

        HelperRequestRouter.RoutedResponse resp = call(bearer, "{\"envId\":\"" + ENGINE_ID + "\"}");

        assertEquals(200, resp.status());
        Map<?, ?> parsed = mapper.readValue(resp.body(), Map.class);
        assertEquals(PEER_IP, parsed.get("ip"));
    }

    @Test
    void noBearerIsUnauthorized() {
        grantEngine();

        assertEquals(401, call(null, "{\"engineId\":\"" + ENGINE_ID + "\"}").status());
        assertEquals(401, call("h_bogus", "{\"engineId\":\"" + ENGINE_ID + "\"}").status());
    }

    @Test
    void resolutionTracksTheLatestAgentMap() throws Exception {
        String bearer = registerJob();
        grantEngine();
        assertEquals(200, call(bearer, "{\"engineId\":\"" + ENGINE_ID + "\"}").status());

        agent.clearPeers().addPeer(PeerAddressResolver.hostnameFor(ENGINE_ID), "100.64.0.9", true);
        new PeerAddressResolver(agent); // cache lives on the injected resolver
        peerDirectory.setPeerAddressResolver(new PeerAddressResolver(agent));

        HelperRequestRouter.RoutedResponse resp = call(bearer, "{\"engineId\":\"" + ENGINE_ID + "\"}");

        assertEquals(200, resp.status());
        Map<?, ?> parsed = mapper.readValue(resp.body(), Map.class);
        assertEquals("100.64.0.9", parsed.get("ip"),
                "engineIP must follow the agent map — the id is the stable handle");
        assertTrue(parsed.get("ip").toString().startsWith("100.64."));
    }
}
