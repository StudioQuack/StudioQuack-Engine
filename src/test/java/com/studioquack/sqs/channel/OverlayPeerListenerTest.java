package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.overlay.OverlayPeerListener;
import com.studioquack.sqs.channel.overlay.PeerCallAuthenticator;
import com.studioquack.sqs.sandbox.helperchan.HelperExecuteService;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link OverlayPeerListener} over a real plain-HTTP round-trip on 127.0.0.1 (ephemeral port via
 * the bind test seam), with a mocked {@link HelperExecuteService}. Asserts the exact contract of
 * the retired loopback {@code OverlayExecuteController}, now served on the overlay-bound listener
 */
class OverlayPeerListenerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    @Test
    void postExecute_happyPath_delegatesVerbatim_andReturnsResult() throws Exception {
        HelperExecuteService svc = mock(HelperExecuteService.class);
        AtomicReference<Map<String, Object>> seenEc = new AtomicReference<>();
        AtomicReference<String> seenNode = new AtomicReference<>();
        AtomicReference<String> seenPort = new AtomicReference<>();
        AtomicReference<Object> seenPayload = new AtomicReference<>();
        when(svc.executeForPeer(any(), any(), any(), any(), any())).thenAnswer(inv -> {
            seenEc.set(inv.getArgument(0));
            seenNode.set(inv.getArgument(1));
            seenPort.set(inv.getArgument(2));
            seenPayload.set(inv.getArgument(3));
            return Map.of("echo", "ok");
        });

        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, svc, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            String requestJson = "{\"executionContext\":{\"envId\":\"env-b\",\"workflowExecutionId\":\"wfe-1\"},"
                    + "\"targetNodeId\":\"node-7\",\"portValue\":\"model\",\"payload\":{\"prompt\":\"hi\"}}";
            HttpResponse<String> resp = post(listener.boundPort(), "/sqs/v1/overlay/execute", requestJson);

            assertEquals(200, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals(Map.of("echo", "ok"), body.get("result"));
            assertEquals("env-b", seenEc.get().get("envId"), "executionContext delegated verbatim");
            assertEquals("node-7", seenNode.get());
            assertEquals("model", seenPort.get());
            assertEquals(Map.of("prompt", "hi"), seenPayload.get());
        } finally {
            listener.stop();
        }
    }

    @Test
    void postExecute_withoutContext_returns_400_andNeverTouchesService() throws Exception {
        HelperExecuteService svc = mock(HelperExecuteService.class);
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, svc, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            // No executionContext key at all.
            HttpResponse<String> resp = post(listener.boundPort(), "/sqs/v1/overlay/execute",
                    "{\"targetNodeId\":\"node-7\"}");
            assertEquals(400, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("Error_MissingContext", body.get("errorCode"));

            // Malformed JSON is the same fail-closed 400.
            HttpResponse<String> broken = post(listener.boundPort(), "/sqs/v1/overlay/execute",
                    "{not json");
            assertEquals(400, broken.statusCode());
            assertEquals("Error_MissingContext", MAPPER.readValue(broken.body(), Map.class).get("errorCode"));

            verify(svc, never()).executeForPeer(any(), any(), any(), any(), any());
        } finally {
            listener.stop();
        }
    }

    @Test
    void postExecute_targetNotDeployed_returns_404() throws Exception {
        HelperExecuteService svc = mock(HelperExecuteService.class);
        when(svc.executeForPeer(any(), any(), any(), any(), any()))
                .thenThrow(new IllegalStateException("sub-process definition not deployed: Process_SubProc_n1"));
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, svc, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(), "/sqs/v1/overlay/execute",
                    "{\"executionContext\":{\"envId\":\"env-b\"},\"targetNodeId\":\"n1\"}");
            assertEquals(404, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("Error_TargetNotDeployed", body.get("errorCode"));
            assertTrue(String.valueOf(body.get("message")).contains("Process_SubProc_n1"),
                    "the service message rides the 404 so the caller can name the missing target");
        } finally {
            listener.stop();
        }
    }

    @Test
    void postExecute_genericFailure_returns_500() throws Exception {
        HelperExecuteService svc = mock(HelperExecuteService.class);
        when(svc.executeForPeer(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("engine exploded"));
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, svc, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(), "/sqs/v1/overlay/execute",
                    "{\"executionContext\":{\"envId\":\"env-b\"},\"targetNodeId\":\"n1\"}");
            assertEquals(500, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("Error_ExecuteFailed", body.get("errorCode"));
            assertEquals("execute failed", body.get("message"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void unknownPath_returns_404() throws Exception {
        HelperExecuteService svc = mock(HelperExecuteService.class);
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, svc, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(), "/sqs/v1/overlay/other", "{}");
            assertEquals(404, resp.statusCode());
            assertEquals("Error_NotFound", MAPPER.readValue(resp.body(), Map.class).get("errorCode"));

            // The llm peer route EXISTS since: with the slice-1 constructor (no provider proxy
            // wired) it fails closed as unavailable rather than 404-ing. Route behaviour is covered
            // in OverlayPeerListenerLlmRouteTest.
            HttpResponse<String> llm = post(listener.boundPort(),
                    "/sqs/v1/overlay/llm/chat/completions", "{}");
            assertEquals(503, llm.statusCode());
            assertEquals("Error_LlmUnavailable",
                    MAPPER.readValue(llm.body(), Map.class).get("errorCode"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void getOnExecutePath_returns_405() throws Exception {
        HelperExecuteService svc = mock(HelperExecuteService.class);
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, svc, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            HttpRequest get = HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + listener.boundPort() + "/sqs/v1/overlay/execute"))
                    .GET().build();
            HttpResponse<String> resp = CLIENT.send(get, HttpResponse.BodyHandlers.ofString());
            assertEquals(405, resp.statusCode());
            assertEquals("Error_MethodNotAllowed", MAPPER.readValue(resp.body(), Map.class).get("errorCode"));
            verify(svc, never()).executeForPeer(any(), any(), any(), any(), any());
        } finally {
            listener.stop();
        }
    }

    @Test
    void deniedAuthenticator_shortCircuits_beforeBodyAndService() throws Exception {
        HelperExecuteService svc = mock(HelperExecuteService.class);
        AtomicReference<String> seenHeader = new AtomicReference<>();
        AtomicReference<String> seenScope = new AtomicReference<>();
        PeerCallAuthenticator denying = (authorizationHeader, scope) -> {
            seenHeader.set(authorizationHeader);
            seenScope.set(scope);
            return PeerCallAuthenticator.AuthResult.deny(403, "Error_CrossEnvNotGranted", "no grant");
        };
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0, svc, denying, MAPPER);
        listener.start();
        try {
            HttpRequest req = HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + listener.boundPort() + "/sqs/v1/overlay/execute"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer forged-token")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            "{\"executionContext\":{\"envId\":\"env-b\"}}"))
                    .build();
            HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
            assertEquals(403, resp.statusCode(), "the denial status is written verbatim");
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("Error_CrossEnvNotGranted", body.get("errorCode"));
            assertEquals("Bearer forged-token", seenHeader.get(), "the raw Authorization header reaches the seam");
            assertEquals(PeerCallAuthenticator.SCOPE_EXECUTE, seenScope.get(),
                    "the execute route asks for the execute grant scope");
            verify(svc, never()).executeForPeer(any(), any(), any(), any(), any());
        } finally {
            listener.stop();
        }
    }

    @Test
    void start_and_stop_are_idempotent() throws Exception {
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, mock(HelperExecuteService.class), null, MAPPER);
        assertFalse(listener.isRunning());
        listener.start();
        listener.start(); // second start is a no-op, must not throw or rebind
        assertTrue(listener.isRunning());
        assertTrue(listener.boundPort() > 0);

        listener.stop();
        listener.stop(); // second stop is a no-op
        assertFalse(listener.isRunning());
    }

    // ---- test scaffolding ---------------------------------------------------

    private static HttpResponse<String> post(int port, String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
