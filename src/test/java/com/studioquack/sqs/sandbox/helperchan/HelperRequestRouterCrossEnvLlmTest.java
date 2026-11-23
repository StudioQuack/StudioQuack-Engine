package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.channel.net.FakeNetAgentClient;
import com.studioquack.sqs.channel.net.PeerAddressResolver;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient;
import com.studioquack.sqs.channel.wg.mesh.MeshIdentity;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The CALLER side of cross-env {@code studioquack.llm.*} in {@link HelperRequestRouter}. */
class HelperRequestRouterCrossEnvLlmTest {

    private static final String SELF_ENV_ID = "env-self";
    private static final String PEER_ENV_ID = "env-b-id";
    private static final String PEER_NAME = "gpu-box";
    private static final String MESH_TOKEN = "mesh-token";

    private final ObjectMapper mapper = new ObjectMapper();
    private final AiModelProviderHolder providerHolder = new AiModelProviderHolder();
    private final GrantedPeerDirectory granted = new GrantedPeerDirectory();
    private final MeshIdentity meshIdentity = new MeshIdentity();
    private final CrossEnvAuthorizationClient authz = mock(CrossEnvAuthorizationClient.class);

    private HelperRequestRouter router;
    private String token;
    private HttpServer peer;
    private final AtomicReference<String> seenMethod = new AtomicReference<>();
    private final AtomicReference<String> seenPath = new AtomicReference<>();
    private final AtomicReference<String> seenAuth = new AtomicReference<>();
    private final AtomicReference<String> seenAccept = new AtomicReference<>();
    private final AtomicReference<String> seenBody = new AtomicReference<>();
    private volatile int peerStatus = 200;
    private volatile String peerBody = "{\"choices\":[{\"message\":{\"content\":\"pong\"}}]}";
    private volatile String peerContentType = "application/json";
    /** When set, the peer 401s any call presenting exactly this Authorization value. */
    private volatile String rejectAuth;
    private final AtomicInteger peerCalls = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
        peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peer.createContext("/", ex -> {
            seenMethod.set(ex.getRequestMethod());
            seenPath.set(ex.getRequestURI().getPath());
            seenAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
            seenAccept.set(ex.getRequestHeaders().getFirst("Accept"));
            seenBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            peerCalls.incrementAndGet();
            int status = peerStatus;
            String bodyOut = peerBody;
            if (rejectAuth != null && rejectAuth.equals(seenAuth.get())) {
                status = 401;
                bodyOut = "{\"errorCode\":\"Error_PeerRejectedIdentity\",\"message\":\"token expired\"}";
            }
            byte[] out = bodyOut.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", peerContentType);
            ex.sendResponseHeaders(status, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        peer.start();
        meshIdentity.update(MESH_TOKEN, null, "self-name");
        router = newRouter(peer.getAddress().getPort(), meshIdentity);
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("envId", SELF_ENV_ID);
        ec.put("workflowExecutionId", "wfe-1");
        token = router.register(HelperJob.builder("job-1", "python").executionContext(ec).build());
    }

    @AfterEach
    void tearDown() {
        if (peer != null) {
            peer.stop(0);
        }
    }

    private HelperRequestRouter newRouter(int overlayPort, MeshIdentity identity) {
        HelperRequestRouter router = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(providerHolder, mapper),
                null, null, null, granted, identity, authz, overlayPort);
        // The peer is the loopback mock. 's overlay-range filter lives inside PeerAddressResolver
        // and is covered by its own tests — here resolution is stubbed straight to the mock so the
        // test exercises the call path, not address policy.
        router.setPeerAddressResolver(new PeerAddressResolver((com.studioquack.sqs.channel.net.NetAgentClient) null) {
            @Override
            public java.util.Optional<String> resolve(String envId) {
                return java.util.Optional.of("127.0.0.1");
            }
        });
        return router;
    }

    private void grantPeer(String... scopes) {
        granted.replace("default", List.of(new GrantedPeerDirectory.GrantedPeer(
                PEER_ENV_ID, PEER_NAME, List.of(scopes))));
    }

    private HelperRequestRouter.RoutedResponse postLlm(String sub, String json) {
        return router.route("POST", "llm/" + sub, token, json.getBytes(StandardCharsets.UTF_8));
    }

    private static String bodyOf(HelperRequestRouter.RoutedResponse r) {
        return new String(r.body(), StandardCharsets.UTF_8);
    }

    @Test
    void noEnvId_localPathUnchanged_peerNeverDialed() {
        // No provider configured locally: the LOCAL proxy's own 503 envelope comes back, and the
        // cross-env machinery is not even consulted.
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"messages\":[]}");
        assertEquals(503, r.status());
        assertTrue(bodyOf(r).contains("provider_not_configured"), bodyOf(r));
        assertNull(seenPath.get(), "no envId → the local path, byte-for-byte as today");
    }

    @Test
    void envId_isStrippedBeforeForwarding_andThePeerAnswerIsVerbatim() {
        grantPeer("llm");
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}");
        assertEquals(200, r.status());
        assertEquals(peerBody, bodyOf(r), "the peer's body comes back verbatim");
        assertEquals("/sqs/v1/overlay/llm/chat/completions", seenPath.get());
        assertEquals("Bearer " + MESH_TOKEN, seenAuth.get());
        assertFalse(seenBody.get().contains("envId"),
                "envId is routing metadata — it must NEVER reach the peer's provider payload: "
                        + seenBody.get());
        assertTrue(seenBody.get().contains("ping"), "the model payload survives intact: " + seenBody.get());
    }

    @Test
    void envId_namingSelf_staysLocal_andIsStillStripped() throws Exception {
        // A configured LOCAL provider: self-targeting must reach it with envId already removed.
        HttpServer localProvider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicReference<String> localBody = new AtomicReference<>();
        localProvider.createContext("/", ex -> {
            localBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] out = "{\"choices\":[]}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        localProvider.start();
        try {
            providerHolder.apply(new com.studioquack.sqs.aimodel.AiModelProviderConfig(
                    "http://127.0.0.1:" + localProvider.getAddress().getPort(), "prov-key",
                    "/v1/chat/completions", "/v1/models", null, null, null, null, "m"));
            HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                    "{\"model\":\"m\",\"envId\":\"env-self\",\"messages\":[]}");
            assertEquals(200, r.status());
            assertNull(seenPath.get(), "self-targeting never dials a peer");
            assertFalse(localBody.get().contains("envId"),
                    "envId is stripped even on the local path — it never reaches a provider: "
                            + localBody.get());
        } finally {
            localProvider.stop(0);
        }
    }

    @Test
    void unknownEnvironment_502_namesTheValue_neverDialed() {
        grantPeer("llm");
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"no-such-env\"}");
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_UnknownEnvironment"), bodyOf(r));
        assertTrue(bodyOf(r).contains("no-such-env"), "the value passed is named: " + bodyOf(r));
        assertNull(seenPath.get(), "an unknown env is never dialed and never falls back to local");
    }

    @Test
    void scopeMiss_authorizeDenied_502_notGranted() {
        grantPeer("execute"); // granted for execute, NOT for llm
        when(authz.authorize(PEER_ENV_ID, SELF_ENV_ID, "llm"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.DENIED);
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\"}");
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_CrossEnvNotGranted"), bodyOf(r));
        assertNull(seenPath.get());
    }

    @Test
    void scopeMiss_authorizeUnavailable_502_explicitDeny() {
        grantPeer("execute");
        when(authz.authorize(PEER_ENV_ID, SELF_ENV_ID, "llm"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.UNAVAILABLE);
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\"}");
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_AuthorizationUnavailable"), bodyOf(r));
        assertNull(seenPath.get());
    }

    @Test
    void sliceConstructorWithoutCollaborators_failsClosed() {
        HelperRequestRouter sliceRouter = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(providerHolder, mapper), null);
        String sliceToken = sliceRouter.register(HelperJob.builder("job-2", "python")
                .executionContext(new LinkedHashMap<>(Map.of("envId", SELF_ENV_ID))).build());
        HelperRequestRouter.RoutedResponse r = sliceRouter.route("POST", "llm/chat/completions",
                sliceToken, "{\"model\":\"m\",\"envId\":\"gpu-box\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_AuthorizationUnavailable"),
                "no granted directory wired → fail closed, never a silent local run: " + bodyOf(r));
    }

    @Test
    void noMeshToken_failsBeforeDialing_withRejectedIdentitySemantics() {
        MeshIdentity emptyIdentity = new MeshIdentity();
        HelperRequestRouter noTokenRouter = newRouter(peer.getAddress().getPort(), emptyIdentity);
        String t = noTokenRouter.register(HelperJob.builder("job-3", "python")
                .executionContext(new LinkedHashMap<>(Map.of("envId", SELF_ENV_ID))).build());
        grantPeer("llm");
        HelperRequestRouter.RoutedResponse r = noTokenRouter.route("POST", "llm/chat/completions",
                t, "{\"model\":\"m\",\"envId\":\"gpu-box\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_PeerRejectedIdentity"), bodyOf(r));
        assertNull(seenPath.get(), "no identity to present — fail BEFORE dialing");
    }

    @Test
    void peer503ProviderNotConfigured_mapsToPeerProviderNotConfigured_namingThePeer() {
        grantPeer("llm");
        peerStatus = 503;
        peerBody = "{\"error\":\"provider_not_configured\",\"detail\":\"No AI Model provider is "
                + "configured for this environment.\"}";
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\"}");
        assertEquals(503, r.status());
        assertTrue(bodyOf(r).contains("Error_PeerProviderNotConfigured"), bodyOf(r));
        assertTrue(bodyOf(r).contains(PEER_NAME) && bodyOf(r).contains(PEER_ENV_ID),
                "the error names the PEER env (id+name) so operators debug the right machine: "
                        + bodyOf(r));
    }

    @Test
    void peer401_mapsToPeerRejectedIdentity() {
        grantPeer("llm");
        peerStatus = 401;
        peerBody = "{\"errorCode\":\"Error_PeerRejectedIdentity\",\"message\":\"token expired\"}";
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\"}");
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_PeerRejectedIdentity"), bodyOf(r));
    }

    /**
     * The ~35-min regression: the held mesh token expired against the peer's clock. The caller
     * re-handshakes (the installed refresher mints a fresh token, as the bootstrap does) and
     * retries ONCE — the peer authenticated before executing, so no double execution is possible.
     */
    @Test
    void peer401_reHandshakesAndRetriesOnce_withTheFreshToken() {
        grantPeer("llm");
        rejectAuth = "Bearer " + MESH_TOKEN;
        AtomicInteger refreshes = new AtomicInteger();
        router.meshIdentityRefresher = () -> {
            refreshes.incrementAndGet();
            meshIdentity.update("mesh-token-fresh", null, "self-name");
            return true;
        };
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\"}");
        assertEquals(200, r.status(), bodyOf(r));
        assertEquals(peerBody, bodyOf(r));
        assertEquals(1, refreshes.get(), "exactly one re-handshake");
        assertEquals(2, peerCalls.get(), "exactly one retry");
        assertEquals("Bearer mesh-token-fresh", seenAuth.get(), "the retry presents the fresh token");
    }

    /** A 401 that survives the refresh+retry still maps to {@code Error_PeerRejectedIdentity}. */
    @Test
    void peer401_afterRefreshStillRefused_mapsToPeerRejectedIdentity() {
        grantPeer("llm");
        rejectAuth = "Bearer " + MESH_TOKEN;
        peerStatus = 401; // the retry (fresh token) is refused too
        peerBody = "{\"errorCode\":\"Error_PeerRejectedIdentity\",\"message\":\"still expired\"}";
        router.meshIdentityRefresher = () -> {
            meshIdentity.update("mesh-token-fresh", null, "self-name");
            return true;
        };
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\"}");
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_PeerRejectedIdentity"), bodyOf(r));
        assertEquals(2, peerCalls.get(), "one retry, never a loop");
    }

    @Test
    void peer403_passesThePeersErrorCodeThrough() {
        grantPeer("llm");
        peerStatus = 403;
        peerBody = "{\"errorCode\":\"Error_AuthorizationUnavailable\",\"message\":\"backend down\"}";
        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\"}");
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_AuthorizationUnavailable"),
                "the callee's own denial code survives: " + bodyOf(r));
    }

    @Test
    void peerUnreachable_502_withDirectoryContext() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        HelperRequestRouter unreachable = newRouter(closedPort, meshIdentity);
        String t = unreachable.register(HelperJob.builder("job-4", "python")
                .executionContext(new LinkedHashMap<>(Map.of("envId", SELF_ENV_ID))).build());
        grantPeer("llm");
        HelperRequestRouter.RoutedResponse r = unreachable.route("POST", "llm/chat/completions",
                t, "{\"model\":\"m\",\"envId\":\"gpu-box\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_PeerUnreachable"), bodyOf(r));
        assertTrue(bodyOf(r).contains("granted peer(s) known"),
                "tunnel/directory context rides the message: " + bodyOf(r));
    }

    /**
     * The after-idle regression: the first dial's TLS leg dies as {@link SSLException} (stale
     * pooled socket, or the gateway dropping the flow mid-handshake while the overlay re-keys).
     * The caller retries on a fresh connection instead of failing the run with
     * {@code Error_PeerUnreachable}.
     */
    @Test
    @SuppressWarnings("unchecked")
    void sslExceptionOnFirstDial_isRetried_andTheCallSucceeds() throws Exception {
        grantPeer("llm");
        HttpClient real = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpClient flaky = mock(HttpClient.class);
        AtomicInteger sends = new AtomicInteger();
        when(flaky.sendAsync(any(), any())).thenAnswer(inv -> {
            if (sends.incrementAndGet() == 1) {
                return java.util.concurrent.CompletableFuture.failedFuture(new SSLException("Connection reset"));
            }
            return real.sendAsync(inv.getArgument(0), (HttpResponse.BodyHandler<byte[]>) inv.getArgument(1));
        });
        router.setPeerLlmHttp(flaky);

        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\",\"messages\":[]}");
        assertEquals(200, r.status(), bodyOf(r));
        assertEquals(peerBody, bodyOf(r));
        assertEquals(2, sends.get(), "one SSL failure, one successful redial");
        assertEquals(1, peerCalls.get(), "the peer saw exactly one request — no duplicate inference");
    }

    /** A tunnel that is genuinely down exhausts every dial attempt, then fails closed. */
    @Test
    void sslExceptionOnEveryDial_exhaustsRetries_toPeerUnreachable() throws Exception {
        grantPeer("llm");
        HttpClient down = mock(HttpClient.class);
        AtomicInteger sends = new AtomicInteger();
        when(down.sendAsync(any(), any())).thenAnswer(inv -> {
            sends.incrementAndGet();
            return java.util.concurrent.CompletableFuture.failedFuture(new SSLException("Connection reset"));
        });
        router.setPeerLlmHttp(down);

        HelperRequestRouter.RoutedResponse r = postLlm("chat/completions",
                "{\"model\":\"m\",\"envId\":\"gpu-box\",\"messages\":[]}");
        assertEquals(502, r.status());
        assertTrue(bodyOf(r).contains("Error_PeerUnreachable"), bodyOf(r));
        assertTrue(bodyOf(r).contains("SSLException"), bodyOf(r));
        assertEquals(4, sends.get(), "all dial attempts exhausted before failing the run");
        assertEquals(0, peerCalls.get(), "a TLS-dead dial never reached the peer");
    }

    @Test
    void streamEnvId_relaysPeerSseChunks_andKeepsStreamTrue() {
        grantPeer("llm");
        peerContentType = "text/event-stream";
        peerBody = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n";
        CollectingSink sink = new CollectingSink();
        router.routeStream("POST", "llm/chat", token,
                "{\"stream\":true,\"envId\":\"gpu-box\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}"
                        .getBytes(StandardCharsets.UTF_8),
                "text/event-stream", sink);
        assertFalse(sink.oneShot, "a live SSE peer must not collapse to a one-shot pipe frame");
        assertTrue(sink.joined().contains("data:"), sink.joined());
        assertTrue(seenAccept.get() != null && seenAccept.get().contains("text/event-stream"),
                "the overlay hop must ask the peer for SSE: " + seenAccept.get());
        assertFalse(seenBody.get().contains("envId"), "envId stays off the peer payload: " + seenBody.get());
        assertTrue(seenBody.get().contains("\"stream\":true") || seenBody.get().contains("\"stream\": true"),
                "negotiated stream must keep stream=true for the peer: " + seenBody.get());
    }

    @Test
    void streamEnvId_oldPeerJson_isReframedAsSse() {
        grantPeer("llm");
        peerContentType = "application/json";
        peerBody = "{\"choices\":[{\"message\":{\"content\":\"pong\"}}]}";
        CollectingSink sink = new CollectingSink();
        router.routeStream("POST", "llm/chat", token,
                "{\"stream\":true,\"envId\":\"gpu-box\",\"messages\":[]}"
                        .getBytes(StandardCharsets.UTF_8),
                "text/event-stream", sink);
        assertFalse(sink.oneShot, "a buffered peer completion must be reframed as SSE, not one-shot");
        assertTrue(sink.joined().contains("data:"), sink.joined());
        assertTrue(sink.joined().contains("pong"), sink.joined());
        assertTrue(sink.joined().contains("[DONE]"), sink.joined());
    }

    @Test
    void streamEnvId_peer401_isOneShotRejectedIdentity() {
        grantPeer("llm");
        peerStatus = 401;
        peerBody = "{\"errorCode\":\"Error_PeerRejectedIdentity\"}";
        CollectingSink sink = new CollectingSink();
        router.routeStream("POST", "llm/chat", token,
                "{\"stream\":true,\"envId\":\"gpu-box\"}".getBytes(StandardCharsets.UTF_8),
                "text/event-stream", sink);
        assertTrue(sink.oneShot, "authz failures stay one-shot JSON so the SDK reads {error}");
        assertTrue(new String(sink.oneShotBody, StandardCharsets.UTF_8).contains("Error_PeerRejectedIdentity"),
                new String(sink.oneShotBody, StandardCharsets.UTF_8));
    }

    @Test
    void getSub_forwardedAsGet_withNoBody() {
        grantPeer("llm");
        peerStatus = 200;
        peerBody = "{\"data\":[{\"id\":\"m\"}]}";
        HelperRequestRouter.RoutedResponse r = router.route("GET", "llm/models", token,
                "{\"envId\":\"gpu-box\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(200, r.status());
        assertEquals(peerBody, bodyOf(r));
        assertEquals("GET", seenMethod.get(), "models/download-status keep their GET semantics");
        assertEquals("/sqs/v1/overlay/llm/models", seenPath.get());
        assertEquals("", seenBody.get(), "a GET forward carries no body");
    }

    /** Collects routeStream callbacks for the cross-env stream hop assertions. */
    private static final class CollectingSink implements HelperStreamSink {
        private final java.util.List<String> chunks = new java.util.ArrayList<>();
        private boolean oneShot;
        private byte[] oneShotBody = new byte[0];

        @Override
        public void oneShot(int status, String contentType, byte[] body) {
            oneShot = true;
            oneShotBody = body == null ? new byte[0] : body;
        }

        @Override
        public void chunk(int status, String contentType, byte[] chunk, int seq, boolean done) {
            if (chunk != null && chunk.length > 0) {
                chunks.add(new String(chunk, StandardCharsets.UTF_8));
            }
        }

        private String joined() {
            return String.join("", chunks);
        }
    }
}
