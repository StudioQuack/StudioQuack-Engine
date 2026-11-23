package com.studioquack.sqs.channel.net;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.MockBackend;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.MeshIdentity;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Handshake v2: request carries {@code agentVersion}, {@code hostname=env-<id>},
 * {@code listenerCsrPem}; 200 with null authKey skips {@code up} when already Running;
 * 503 is {@code NetworkUnavailable} with Retry-After; 404 is envNotFound.
 */
class NetHandshakeClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "t48-hs";
    private static final String BEARER = "sqvm_bearer_net";

    @Test
    void v2Request_hasAgentVersionHostnameAndCsr_nullAuthKeySkipsUpWhenRunning(@TempDir Path dataDir)
            throws Exception {
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .withVersion("1.102.0-sq-net")
                .withBackendState("Running")
                .withPublicKey("nodekey:already-up")
                .withHostname(PeerAddressResolver.hostnameFor(ENV_ID));
        ListenerKeyStore keys = ListenerKeyStore.loadOrCreate(dataDir, ENV_ID, null);

        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            be.stub(envBase.replace(be.baseUrl(), "") + NetHandshakeClient.PATH, req -> {
                Map<?, ?> body = MAPPER.readValue(req.body(), Map.class);
                assertEquals("1.102.0-sq-net", body.get("agentVersion"));
                assertEquals("env-" + ENV_ID, body.get("hostname"));
                assertEquals(Boolean.TRUE, body.get("agentSessionUp"),
                        "a Running agent must be reported as session-up so the BE may skip the mint");
                assertNotNull(body.get("listenerCsrPem"));
                assertTrue(body.get("listenerCsrPem").toString().contains("BEGIN CERTIFICATE REQUEST"));
                String resp = """
                        {"cellId":"cell-1","controlUrl":"https://hs.example/control","authKey":null,\
                        "hostname":"env-%s","hubAddresses":[{"gatewayId":"g1","address":"203.0.113.10","port":443}],\
                        "keyExpiresAt":"2027-01-01T00:00:00Z","meshToken":"tok","meshVerificationKey":null,\
                        "meshPeers":[{"envId":"env-b","name":"gpu","scopes":["execute"]}],\
                        "envName":"self","networkRules":[],"outboundNetworkRules":[]}
                        """.formatted(ENV_ID);
                return MockBackend.Response.json(200, resp);
            });

            NetHandshakeClient client = newClient(envBase, agent, keys);
            NetHandshakeResult result = client.handshake();

            assertEquals("https://hs.example/control", result.controlUrl());
            assertFalse(result.hasAuthKey());
            assertEquals(0, agent.upCalls(), "null authKey + Running must skip sq-net up");
            MockBackend.Recorded rec = be.firstMatching(NetHandshakeClient.PATH);
            assertNotNull(rec);
            assertEquals("Bearer " + BEARER, rec.headers().get("authorization"));
        }
    }

    // 2026-11-05 — the inbound grant set starts empty at every daemon start and was refilled only by
    // a reload-config push, so a rebooted engine refused its granted callers. Every handshake now
    // restores it from the backend's answer.
    @Test
    void handshake_restoresTheInboundGrantSet(@TempDir Path dataDir) throws Exception {
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .withVersion("1.102.0-sq-net")
                .withBackendState("Running")
                .withPublicKey("nodekey:already-up")
                .withHostname(PeerAddressResolver.hostnameFor(ENV_ID));
        ListenerKeyStore keys = ListenerKeyStore.loadOrCreate(dataDir, ENV_ID, null);
        com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore inbound =
                new com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore();

        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            be.stub(envBase.replace(be.baseUrl(), "") + NetHandshakeClient.PATH, req -> MockBackend.Response.json(200, """
                    {"cellId":"cell-1","controlUrl":"https://hs.example/control","authKey":null,\
                    "hostname":"env-%s","hubAddresses":[],"keyExpiresAt":"2027-01-01T00:00:00Z",\
                    "meshToken":"tok","meshVerificationKey":null,"meshPeers":[],"envName":"self",\
                    "networkRules":[],"outboundNetworkRules":[],\
                    "crossEnvInbound":[{"callerEnvId":"env-runner","scopes":["llm"]}]}
                    """.formatted(ENV_ID)));

            NetHandshakeClient client = newClient(envBase, agent, keys);
            client.setCrossEnvGrantStore(inbound);
            assertFalse(inbound.check("env-runner", "llm"), "a cold daemon starts deny-all");

            client.handshake();

            assertTrue(inbound.check("env-runner", "llm"));
            assertFalse(inbound.check("env-runner", "execute"));
        }
    }

    @Test
    void status503_throwsNetworkUnavailableWithRetryAfter() throws Exception {
        FakeNetAgentClient agent = new FakeNetAgentClient().withVersion("1.0");
        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            be.stub("/be/v1/sqvm/environments/" + ENV_ID + NetHandshakeClient.PATH, req ->
                    MockBackend.Response.json(503,
                            "{\"errorCode\":\"Error_NetworkUnavailable\"}",
                            Map.of("Retry-After", "12")));
            NetHandshakeClient client = newClient(envBase, agent, null);
            NetHandshakeException ex = assertThrows(NetHandshakeException.class, client::handshake);
            assertTrue(ex.networkUnavailable());
            assertEquals(503, ex.status());
            assertEquals(12, ex.retryAfterSeconds());
        }
    }

    @Test
    void status404_throwsEnvNotFound() throws Exception {
        FakeNetAgentClient agent = new FakeNetAgentClient().withVersion("1.0");
        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            be.stub("/be/v1/sqvm/environments/" + ENV_ID + NetHandshakeClient.PATH, req ->
                    MockBackend.Response.json(404, "{\"errorCode\":\"Error_EnvNotFound\"}"));
            NetHandshakeClient client = newClient(envBase, agent, null);
            NetHandshakeException ex = assertThrows(NetHandshakeException.class, client::handshake);
            assertTrue(ex.envNotFound());
            assertEquals(404, ex.status());
        }
    }

    /**
     * The mesh token (minutes-long TTL) must drive the healthy-path re-handshake, not just the
     * days-long node key: a token at 90 % of its signed life flips {@code shouldRefresh()}, a
     * fresh one does not. Regression for the ~35-min cross-env {@code Error_PeerRejectedIdentity}
     * outage on an otherwise healthy overlay.
     */
    @Test
    void shouldRefresh_followsTheMeshTokenLife_notOnlyTheNodeKey(@TempDir Path dataDir)
            throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .withVersion("1.0").withBackendState("Running")
                .withPublicKey("nodekey:up").withHostname(PeerAddressResolver.hostnameFor(ENV_ID));
        ListenerKeyStore keys = ListenerKeyStore.loadOrCreate(dataDir, ENV_ID, null);
        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            java.util.concurrent.atomic.AtomicReference<String> token =
                    new java.util.concurrent.atomic.AtomicReference<>(jws(now, now + 2100));
            be.stub(envBase.replace(be.baseUrl(), "") + NetHandshakeClient.PATH, req ->
                    MockBackend.Response.json(200, """
                            {"cellId":"cell-1","controlUrl":"https://hs.example/control","authKey":null,\
                            "hostname":"env-%s","hubAddresses":[],"keyExpiresAt":"2099-01-01T00:00:00Z",\
                            "meshToken":"%s","meshVerificationKey":null,"meshPeers":[],"envName":"self",\
                            "networkRules":[],"outboundNetworkRules":[]}
                            """.formatted(ENV_ID, token.get())));
            NetHandshakeClient client = newClient(envBase, agent, keys);

            client.handshake();
            assertFalse(client.shouldRefresh(), "a freshly minted token must not trigger a refresh");

            // Re-handshake with a token already at ~90 % of its signed life → refresh due now.
            token.set(jws(now - 1890, now + 210));
            client.handshake();
            assertTrue(client.shouldRefresh(), "a token at 80%%+ of its life must trigger a refresh");
        }
    }

    /** Unsigned-shape compact JWS (the client never verifies its OWN token — the peer does). */
    private static String jws(long iat, long exp) {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        return b64.encodeToString("{\"alg\":\"EdDSA\",\"typ\":\"SQMT\"}".getBytes(StandardCharsets.UTF_8))
                + "." + b64.encodeToString(
                        ("{\"iss\":\"sq-backend\",\"sub\":\"" + ENV_ID + "\",\"iat\":" + iat
                                + ",\"exp\":" + exp + "}").getBytes(StandardCharsets.UTF_8))
                + "." + b64.encodeToString(new byte[64]);
    }

    @Test
    void plainHttpPublicCell_isRefused(@TempDir Path dataDir) throws Exception {
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .withVersion("1.0").withBackendState("Running")
                .withPublicKey("nodekey:up").withHostname(PeerAddressResolver.hostnameFor(ENV_ID));
        ListenerKeyStore keys = ListenerKeyStore.loadOrCreate(dataDir, ENV_ID, null);
        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            be.stub(envBase.replace(be.baseUrl(), "") + NetHandshakeClient.PATH, req ->
                    MockBackend.Response.json(200, """
                            {"cellId":"cell-1","controlUrl":"http://192.0.2.10:8443","authKey":"mesh-auth-test-123-xyz",\
                            "hostname":"env-%s","hubAddresses":[],"keyExpiresAt":"2099-01-01T00:00:00Z",\
                            "meshToken":null,"meshVerificationKey":null,"meshPeers":[],"envName":"self",\
                            "networkRules":[],"outboundNetworkRules":[]}\
                            """.formatted(ENV_ID)));
            NetHandshakeClient client = newClient(envBase, agent, keys);
            NetHandshakeException ex = assertThrows(NetHandshakeException.class, client::handshake);
            assertTrue(ex.getMessage().contains("https-required"), ex.getMessage());
            assertEquals(null, agent.lastControlUrl(), "sq-net up must not run against plain http");
            assertEquals(0, agent.upCalls());
        }
    }

    /**
     * The self-hosted split-brain: the server believes the node is registered (authKey=null) while
     * the local agent has no session. A keyless {@code sq-net up} would park the agent in
     * NeedsLogin forever, so the daemon must NOT attempt it — and must report agentSessionUp=false
     * so a fixed the server mints a fresh key on the next handshake.
     */
    @Test
    void nullAuthKey_needsLoginAgent_skipsKeylessUp_andReportsSessionDown(@TempDir Path dataDir)
            throws Exception {
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .withVersion("1.0").withBackendState("NeedsLogin")
                .withPublicKey("nodekey:lost-session")
                .withHostname(PeerAddressResolver.hostnameFor(ENV_ID));
        ListenerKeyStore keys = ListenerKeyStore.loadOrCreate(dataDir, ENV_ID, null);
        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            be.stub(envBase.replace(be.baseUrl(), "") + NetHandshakeClient.PATH, req -> {
                Map<?, ?> body = MAPPER.readValue(req.body(), Map.class);
                assertEquals(Boolean.FALSE, body.get("agentSessionUp"),
                        "NeedsLogin must be reported as session-down so the BE re-issues");
                return MockBackend.Response.json(200, """
                        {"cellId":"cell-1","controlUrl":"https://hs.example/control","authKey":null,\
                        "hostname":"env-%s","hubAddresses":[],"keyExpiresAt":"2027-01-01T00:00:00Z",\
                        "meshToken":null,"meshVerificationKey":null,"meshPeers":[],"envName":"self",\
                        "networkRules":[],"outboundNetworkRules":[]}
                        """.formatted(ENV_ID));
            });
            NetHandshakeClient client = newClient(envBase, agent, keys);
            NetHandshakeResult result = client.handshake();

            assertFalse(result.hasAuthKey());
            assertEquals(0, agent.upCalls(),
                    "keyless up against a sessionless agent parks it in NeedsLogin — never attempt it");
        }
    }

    /** The legitimate keyless path: a Stopped agent still holds its login session. */
    @Test
    void nullAuthKey_stoppedAgent_resumesWithKeylessUp(@TempDir Path dataDir) throws Exception {
        FakeNetAgentClient agent = new FakeNetAgentClient()
                .withVersion("1.0").withBackendState("Stopped")
                .withPublicKey("nodekey:has-session")
                .withHostname(PeerAddressResolver.hostnameFor(ENV_ID));
        ListenerKeyStore keys = ListenerKeyStore.loadOrCreate(dataDir, ENV_ID, null);
        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            be.stub(envBase.replace(be.baseUrl(), "") + NetHandshakeClient.PATH, req ->
                    MockBackend.Response.json(200, """
                            {"cellId":"cell-1","controlUrl":"https://hs.example/control","authKey":null,\
                            "hostname":"env-%s","hubAddresses":[],"keyExpiresAt":"2027-01-01T00:00:00Z",\
                            "meshToken":null,"meshVerificationKey":null,"meshPeers":[],"envName":"self",\
                            "networkRules":[],"outboundNetworkRules":[]}
                            """.formatted(ENV_ID)));
            NetHandshakeClient client = newClient(envBase, agent, keys);
            client.handshake();

            assertEquals(1, agent.upCalls(), "a Stopped agent still holds its session — keyless up resumes it");
        }
    }

    /**
     * No key AND the agent not answering: nothing in this handshake can succeed, so fail
     * it — the watchdog's fast retry ladder (≤60 s) then drives recovery instead of the
     * ~half-hour mesh-token refresh.
     */
    @Test
    void nullAuthKey_agentUnreachable_failsHandshakeForFastWatchdogRetry(@TempDir Path dataDir)
            throws Exception {
        NetAgentClient deadAgent = new NetAgentClient() {
            @Override public void up(String controlUrl, String authKey, String hostname) { }
            @Override public void down() { }
            @Override public void logout() { }
            @Override public String version() { return "dead"; }
            @Override public NetAgentStatus status() {
                throw new IllegalStateException("LocalAPI unreachable");
            }
            @Override public String backend() { return "dead"; }
        };
        ListenerKeyStore keys = ListenerKeyStore.loadOrCreate(dataDir, ENV_ID, null);
        try (MockBackend be = new MockBackend()) {
            String envBase = be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
            be.stub(envBase.replace(be.baseUrl(), "") + NetHandshakeClient.PATH, req ->
                    MockBackend.Response.json(200, """
                            {"cellId":"cell-1","controlUrl":"https://hs.example/control","authKey":null,\
                            "hostname":"env-%s","hubAddresses":[],"keyExpiresAt":"2027-01-01T00:00:00Z",\
                            "meshToken":null,"meshVerificationKey":null,"meshPeers":[],"envName":"self",\
                            "networkRules":[],"outboundNetworkRules":[]}
                            """.formatted(ENV_ID)));
            NetHandshakeClient client = newClient(envBase, deadAgent, keys);

            NetHandshakeException ex = assertThrows(NetHandshakeException.class, client::handshake);
            assertEquals("Error_NetAgentUnreachable", ex.errorCode());
        }
    }

    private static NetHandshakeClient newClient(String envBase, NetAgentClient agent,
                                                ListenerKeyStore keys) {
        OverlayPeerDirectory overlay = new OverlayPeerDirectory();
        return new NetHandshakeClient(
                envBase, ENV_ID, BEARER,
                new ChannelHttpClients(null), MAPPER,
                agent, keys, new MeshIdentity(), new GrantedPeerDirectory(),
                overlay::replaceGrantedEnvIds, NetworkRuleStore.inMemory());
    }
}
