package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.net.FakeNetAgentClient;
import com.studioquack.sqs.channel.net.PeerAddressResolver;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient;
import com.studioquack.sqs.channel.wg.mesh.MeshIdentity;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** The daemon-side {@code env=} resolution + caller pre-check in {@link HelperExecuteService}. */
class HelperExecuteServiceCrossEnvTest {

    private static final String SELF_ENV_ID = "env-self";
    private static final String SELF_ENV_NAME = "self-name";
    private static final String PEER_ENV_ID = "env-b-id";
    private static final String PEER_NAME = "prod-eu";
    private static final String MESH_TOKEN = "mesh-token";

    private final ObjectMapper mapper = new ObjectMapper();
    private final SqsEngine sqsEngine = mock(SqsEngine.class);
    private final GrantedPeerDirectory granted = new GrantedPeerDirectory();
    private final MeshIdentity meshIdentity = new MeshIdentity();
    private final CrossEnvAuthorizationClient authz = mock(CrossEnvAuthorizationClient.class);

    private HelperExecuteService service;
    private HttpServer peer;
    private final AtomicReference<String> seenAuth = new AtomicReference<>();
    private final AtomicReference<String> seenPath = new AtomicReference<>();
    private final AtomicReference<String> seenBody = new AtomicReference<>();
    private volatile int peerStatus = 200;
    private volatile String peerBody = "{\"result\":{\"echo\":\"ok\"}}";
    /** When set, the peer 401s any call presenting exactly this Authorization value. */
    private volatile String rejectAuth;
    private final AtomicInteger peerCalls = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
        peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peer.createContext("/", ex -> {
            seenAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
            seenPath.set(ex.getRequestURI().getPath());
            seenBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            peerCalls.incrementAndGet();
            int status = peerStatus;
            String bodyOut = peerBody;
            if (rejectAuth != null && rejectAuth.equals(seenAuth.get())) {
                status = 401;
                bodyOut = "{\"errorCode\":\"Error_PeerRejectedIdentity\",\"message\":\"token expired\"}";
            }
            byte[] out = bodyOut.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(status, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        peer.start();
        meshIdentity.update(MESH_TOKEN, null, SELF_ENV_NAME);
        service = newService(peer.getAddress().getPort());
    }

    @AfterEach
    void tearDown() {
        if (peer != null) {
            peer.stop(0);
        }
    }

    private HelperExecuteService newService(int overlayPort) {
        HelperExecuteService svc = new HelperExecuteService(
                mapper, sqsEngine, mock(SubProcessCompletionRegistry.class),
                mock(RefOnlyVarBuilder.class), mock(TransformDirectiveApplier.class),
                mock(ExecutionContextStore.class),
                new com.studioquack.sqs.channel.wg.OverlayPeerDirectory(),
                meshIdentity, granted, authz);
        ReflectionTestUtils.setField(svc, "overlayExecutePort", overlayPort);
        // The peer is the loopback mock. 's overlay-range filter lives inside PeerAddressResolver
        // and is covered by its own tests — here resolution is stubbed straight to the mock so the
        // test exercises the call path, not address policy.
        svc.setPeerAddressResolver(new PeerAddressResolver((com.studioquack.sqs.channel.net.NetAgentClient) null) {
            @Override
            public java.util.Optional<String> resolve(String envId) {
                return java.util.Optional.of("127.0.0.1");
            }
        });
        return svc;
    }

    private void grantPeer(String... scopes) {
        granted.replace("default", List.of(new GrantedPeerDirectory.GrantedPeer(
                PEER_ENV_ID, PEER_NAME, List.of(scopes))));
    }

    /** A job whose execution context carries one FUNCTION-port connection detail on "model". */
    private HelperJob job() {
        Map<String, Object> portDto = new LinkedHashMap<>();
        portDto.put("value", "model_in");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("nodeId", "node-target");
        detail.put("portDto", portDto);
        Map<String, Object> conn = new LinkedHashMap<>();
        conn.put("portValue", "model");
        conn.put("connectionDetailList", List.of(detail));
        Map<String, Object> currentNode = new LinkedHashMap<>();
        currentNode.put("id", "node-agent");
        currentNode.put("connectionList", List.of(conn));
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("envId", SELF_ENV_ID);
        ec.put("workflowExecutionId", "wfe-1");
        ec.put("currentNodeId", "node-agent");
        ec.put("currentNode", currentNode);
        return HelperJob.builder("job-1", "python").executionContext(ec).build();
    }

    @Test
    @SuppressWarnings("unchecked")
    void grantedPeer_byId_dialsThePeerAddressWithTheMeshBearer() {
        grantPeer("execute");
        Object out = service.handle(job(), "model", Map.of("q", "hi"), null, PEER_ENV_ID);
        assertEquals(Map.of("echo", "ok"), ((Map<String, Object>) out).get("result"),
                "the peer's result unwraps into the helper wire shape");
        assertEquals("Bearer " + MESH_TOKEN, seenAuth.get(),
                "the mesh token authenticates the outbound peer call");
        assertEquals("/sqs/v1/overlay/execute", seenPath.get());
        assertTrue(seenBody.get().contains("node-target"), "the target node rides the peer body");
    }

    @Test
    @SuppressWarnings("unchecked")
    void grantedPeer_byName_resolvesIdentically() {
        grantPeer("execute");
        Object out = service.handle(job(), "model", Map.of("q", "hi"), null, PEER_NAME);
        assertEquals(Map.of("echo", "ok"), ((Map<String, Object>) out).get("result"));
        assertEquals("/sqs/v1/overlay/execute", seenPath.get(),
                "an environment NAME resolves to the same granted peer");
    }

    @Test
    void unknownEnvironment_failsClosed_neverFallsBackToLocal() {
        grantPeer("execute"); // something IS resolvable — just not the value passed
        CrossEnvCallException ex = assertThrows(CrossEnvCallException.class,
                () -> service.handle(job(), "model", Map.of(), null, "no-such-env"));
        assertEquals(CrossEnvCallException.ERROR_UNKNOWN_ENVIRONMENT, ex.errorCode());
        assertTrue(ex.getMessage().contains("no-such-env"),
                "the message names the value passed: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(PEER_NAME) && ex.getMessage().contains(PEER_ENV_ID),
                "the message lists the resolvable names/ids: " + ex.getMessage());
        verifyNoInteractions(sqsEngine);
        assertNull(seenPath.get(), "nothing was dialed");
    }

    @Test
    void ownEnvironmentId_andOwnName_stayLocal() {
        // Local path reaches the engine, which (a mock) has no sub-process deployed — the
        // IllegalStateException proves the call never went cross-env.
        IllegalStateException byId = assertThrows(IllegalStateException.class,
                () -> service.handle(job(), "model", Map.of(), null, SELF_ENV_ID));
        assertTrue(byId.getMessage().contains("not deployed"), byId.getMessage());
        IllegalStateException byName = assertThrows(IllegalStateException.class,
                () -> service.handle(job(), "model", Map.of(), null, SELF_ENV_NAME));
        assertTrue(byName.getMessage().contains("not deployed"), byName.getMessage());
        assertNull(seenPath.get(), "self-targeting never dials a peer");
    }

    @Test
    void scopeMiss_authorizeDenied_throwsNotGrantedNamingAllParties() {
        grantPeer("llm"); // granted for llm, NOT for execute
        when(authz.authorize(PEER_ENV_ID, SELF_ENV_ID, "execute"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.DENIED);
        CrossEnvCallException ex = assertThrows(CrossEnvCallException.class,
                () -> service.handle(job(), "model", Map.of(), null, PEER_NAME));
        assertEquals(CrossEnvCallException.ERROR_NOT_GRANTED, ex.errorCode());
        assertTrue(ex.getMessage().contains(SELF_ENV_ID)
                        && ex.getMessage().contains(PEER_ENV_ID)
                        && ex.getMessage().contains(PEER_NAME)
                        && ex.getMessage().contains("execute")
                        && ex.getMessage().contains("Cross-env access"),
                "caller, target (id+name), scope and the grant location are all named: "
                        + ex.getMessage());
        assertNull(seenPath.get(), "a denied call is never dialed");
    }

    @Test
    void scopeMiss_authorizeUnavailable_isAnExplicitDeny() {
        grantPeer("llm");
        when(authz.authorize(PEER_ENV_ID, SELF_ENV_ID, "execute"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.UNAVAILABLE);
        CrossEnvCallException ex = assertThrows(CrossEnvCallException.class,
                () -> service.handle(job(), "model", Map.of(), null, PEER_NAME));
        assertEquals(CrossEnvCallException.ERROR_AUTHORIZATION_UNAVAILABLE, ex.errorCode());
        assertTrue(ex.getMessage().contains("DENY"), "explicitly a deny, not a pass: " + ex.getMessage());
        assertNull(seenPath.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    void scopeMiss_authorizeAllowed_dials() {
        grantPeer("llm");
        when(authz.authorize(PEER_ENV_ID, SELF_ENV_ID, "execute"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.ALLOWED);
        Object out = service.handle(job(), "model", Map.of(), null, PEER_ENV_ID);
        assertEquals(Map.of("echo", "ok"), ((Map<String, Object>) out).get("result"));
        assertEquals("/sqs/v1/overlay/execute", seenPath.get(),
                "a grant created seconds ago works via the on-miss backend check");
    }

    @Test
    @SuppressWarnings("unchecked")
    void wildcardScope_isGrantedWithoutTheOnMissCall() {
        grantPeer("*");
        Object out = service.handle(job(), "model", Map.of(), null, PEER_ENV_ID);
        assertEquals(Map.of("echo", "ok"), ((Map<String, Object>) out).get("result"));
        verify(authz, never()).authorize(any(), any(), any());
    }

    @Test
    void unreachablePeer_throwsPeerUnreachableWithDirectoryContext() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        grantPeer("execute");
        HelperExecuteService unreachable = newService(closedPort);
        CrossEnvCallException ex = assertThrows(CrossEnvCallException.class,
                () -> unreachable.handle(job(), "model", Map.of(), null, PEER_ENV_ID));
        assertEquals(CrossEnvCallException.ERROR_PEER_UNREACHABLE, ex.errorCode());
        assertTrue(ex.getMessage().contains("granted peer(s) known"),
                "the message carries the tunnel/directory context: " + ex.getMessage());
    }

    @Test
    void noMeshToken_failsBeforeDialing_withRejectedIdentitySemantics() {
        meshIdentity.clear();
        grantPeer("execute");
        CrossEnvCallException ex = assertThrows(CrossEnvCallException.class,
                () -> service.handle(job(), "model", Map.of(), null, PEER_ENV_ID));
        assertEquals(CrossEnvCallException.ERROR_PEER_REJECTED_IDENTITY, ex.errorCode());
        assertNull(seenPath.get(), "no identity to present — fail BEFORE dialing");
    }

    @Test
    void peer401_mapsToPeerRejectedIdentity() {
        grantPeer("execute");
        peerStatus = 401;
        peerBody = "{\"errorCode\":\"Error_PeerRejectedIdentity\",\"message\":\"token expired\"}";
        CrossEnvCallException ex = assertThrows(CrossEnvCallException.class,
                () -> service.handle(job(), "model", Map.of(), null, PEER_ENV_ID));
        assertEquals(CrossEnvCallException.ERROR_PEER_REJECTED_IDENTITY, ex.errorCode());
    }

    /**
     * The ~35-min regression: the held mesh token expired against the peer's clock. The caller
     * re-handshakes (the installed refresher mints a fresh token, as the bootstrap does) and
     * retries ONCE — the peer authenticated before executing, so no double execution is possible.
     */
    @Test
    @SuppressWarnings("unchecked")
    void peer401_reHandshakesAndRetriesOnce_withTheFreshToken() {
        grantPeer("execute");
        rejectAuth = "Bearer " + MESH_TOKEN;
        AtomicInteger refreshes = new AtomicInteger();
        service.meshIdentityRefresher = () -> {
            refreshes.incrementAndGet();
            meshIdentity.update("mesh-token-fresh", null, SELF_ENV_NAME);
            return true;
        };
        Map<String, Object> result = (Map<String, Object>) service.handle(
                job(), "model", Map.of(), null, PEER_ENV_ID);
        assertEquals(Map.of("echo", "ok"), result.get("result"));
        assertEquals(1, refreshes.get(), "exactly one re-handshake");
        assertEquals(2, peerCalls.get(), "exactly one retry");
        assertEquals("Bearer mesh-token-fresh", seenAuth.get(), "the retry presents the fresh token");
    }

    @Test
    void peer403_passesThePeersErrorCodeThrough() {
        grantPeer("execute");
        peerStatus = 403;
        peerBody = "{\"errorCode\":\"Error_AuthorizationUnavailable\",\"message\":\"backend down\"}";
        CrossEnvCallException ex = assertThrows(CrossEnvCallException.class,
                () -> service.handle(job(), "model", Map.of(), null, PEER_ENV_ID));
        assertEquals(CrossEnvCallException.ERROR_AUTHORIZATION_UNAVAILABLE, ex.errorCode(),
                "the callee's own denial code survives the round trip");
    }
}
