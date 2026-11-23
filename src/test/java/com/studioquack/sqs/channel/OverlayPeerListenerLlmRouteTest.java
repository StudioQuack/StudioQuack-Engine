package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderConfig;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.channel.overlay.OverlayPeerListener;
import com.studioquack.sqs.channel.overlay.PeerCallAuthenticator;
import com.studioquack.sqs.sandbox.helperchan.HelperExecuteService;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** The CALLEE side of cross-env {@code studioquack.llm.*}. */
class OverlayPeerListenerLlmRouteTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private static final String SENTINEL_KEY = "sk-SENTINEL-PROVIDER-KEY";

    @Test
    void llmRoute_enforcesScopeLlm_beforeTheProxyIsTouched() throws Exception {
        AiModelProviderProxy proxy = mock(AiModelProviderProxy.class);
        AtomicReference<String> seenScope = new AtomicReference<>();
        AtomicReference<String> seenHeader = new AtomicReference<>();
        PeerCallAuthenticator denying = (header, scope) -> {
            seenHeader.set(header);
            seenScope.set(scope);
            return PeerCallAuthenticator.AuthResult.deny(403, "Error_CrossEnvNotGranted", "no grant");
        };
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0,
                mock(HelperExecuteService.class), denying, proxy, MAPPER);
        listener.start();
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + listener.boundPort()
                            + "/sqs/v1/overlay/llm/chat/completions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer forged-token")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"model\":\"m\"}"))
                    .build();
            HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
            assertEquals(403, resp.statusCode(), "the authenticator's denial is written verbatim");
            assertEquals("Error_CrossEnvNotGranted",
                    MAPPER.readValue(resp.body(), Map.class).get("errorCode"));
            assertEquals(PeerCallAuthenticator.SCOPE_LLM, seenScope.get(),
                    "the llm route asks for the llm grant scope");
            assertEquals("Bearer forged-token", seenHeader.get());
            verify(proxy, never()).forward(any(), any(), any(), any(), any());
        } finally {
            listener.stop();
        }
    }

    @Test
    void llmRoute_happyPath_forwardsVerbatim_subPathWithSlashes() throws Exception {
        AiModelProviderProxy proxy = mock(AiModelProviderProxy.class);
        AtomicReference<String> seenSub = new AtomicReference<>();
        AtomicReference<String> seenMethod = new AtomicReference<>();
        AtomicReference<byte[]> seenBody = new AtomicReference<>();
        when(proxy.forward(any(), any(), any(), any(), any())).thenAnswer(inv -> {
            seenSub.set(inv.getArgument(0));
            seenMethod.set(inv.getArgument(1));
            seenBody.set(inv.getArgument(3));
            return new AiModelProviderProxy.ProxyResult(200, "application/json",
                    "{\"choices\":[]}".getBytes(StandardCharsets.UTF_8));
        });
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0,
                mock(HelperExecuteService.class), PeerCallAuthenticator.allowAll(), proxy, MAPPER);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(),
                    "/sqs/v1/overlay/llm/chat/completions", "{\"model\":\"m\",\"messages\":[]}");
            assertEquals(200, resp.statusCode());
            assertEquals("{\"choices\":[]}", resp.body(), "the proxy body is written verbatim");
            assertEquals("chat/completions", seenSub.get(),
                    "a sub-path with slashes reaches the proxy intact");
            assertEquals("POST", seenMethod.get());
            assertEquals("{\"model\":\"m\",\"messages\":[]}",
                    new String(seenBody.get(), StandardCharsets.UTF_8),
                    "the raw body delegates untouched");
        } finally {
            listener.stop();
        }
    }

    @Test
    void llmRoute_getModels_forwardsAsGet() throws Exception {
        AiModelProviderProxy proxy = mock(AiModelProviderProxy.class);
        AtomicReference<String> seenSub = new AtomicReference<>();
        AtomicReference<String> seenMethod = new AtomicReference<>();
        when(proxy.forward(any(), any(), any(), any(), any())).thenAnswer(inv -> {
            seenSub.set(inv.getArgument(0));
            seenMethod.set(inv.getArgument(1));
            return new AiModelProviderProxy.ProxyResult(200, "application/json",
                    "{\"data\":[]}".getBytes(StandardCharsets.UTF_8));
        });
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0,
                mock(HelperExecuteService.class), PeerCallAuthenticator.allowAll(), proxy, MAPPER);
        listener.start();
        try {
            HttpRequest get = HttpRequest.newBuilder(URI.create("http://127.0.0.1:"
                            + listener.boundPort() + "/sqs/v1/overlay/llm/models"))
                    .GET().build();
            HttpResponse<String> resp = CLIENT.send(get, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            assertEquals("{\"data\":[]}", resp.body());
            assertEquals("models", seenSub.get());
            assertEquals("GET", seenMethod.get());
        } finally {
            listener.stop();
        }
    }

    @Test
    void llmRoute_acceptEventStream_relaysForwardStream() throws Exception {
        AiModelProviderProxy proxy = mock(AiModelProviderProxy.class);
        doAnswer(inv -> {
            AiModelProviderProxy.StreamConsumer consumer = inv.getArgument(4);
            consumer.accept(200, "text/event-stream",
                    "data: {\"id\":\"1\"}\n\n".getBytes(StandardCharsets.UTF_8), false);
            consumer.accept(200, "text/event-stream", new byte[0], true);
            return null;
        }).when(proxy).forwardStream(any(), any(), any(), any(), any(), any());
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0,
                mock(HelperExecuteService.class), PeerCallAuthenticator.allowAll(), proxy, MAPPER);
        listener.start();
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + listener.boundPort()
                            + "/sqs/v1/overlay/llm/chat"))
                    .header("Content-Type", "application/json")
                    .header("Accept", "text/event-stream")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"stream\":true,\"messages\":[]}"))
                    .build();
            HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("data:"), resp.body());
            verify(proxy).forwardStream(any(), any(), any(), any(), any(), any());
            verify(proxy, never()).forward(any(), any(), any(), any(), any());
        } finally {
            listener.stop();
        }
    }

    @Test
    void llmRoute_wrongMethod_405() throws Exception {
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0,
                mock(HelperExecuteService.class), PeerCallAuthenticator.allowAll(),
                mock(AiModelProviderProxy.class), MAPPER);
        listener.start();
        try {
            HttpRequest put = HttpRequest.newBuilder(URI.create("http://127.0.0.1:"
                            + listener.boundPort() + "/sqs/v1/overlay/llm/chat/completions"))
                    .PUT(HttpRequest.BodyPublishers.ofString("{}"))
                    .build();
            HttpResponse<String> resp = CLIENT.send(put, HttpResponse.BodyHandlers.ofString());
            assertEquals(405, resp.statusCode());
            assertEquals("Error_MethodNotAllowed",
                    MAPPER.readValue(resp.body(), Map.class).get("errorCode"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void llmRoute_withoutProxy_failsClosed_503() throws Exception {
        // The slice-1 constructor wires no proxy: the route exists but reports itself unavailable
        // rather than NPE-ing mid-request.
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0,
                mock(HelperExecuteService.class), PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(),
                    "/sqs/v1/overlay/llm/chat/completions", "{}");
            assertEquals(503, resp.statusCode());
            assertEquals("Error_LlmUnavailable",
                    MAPPER.readValue(resp.body(), Map.class).get("errorCode"));
        } finally {
            listener.stop();
        }
    }

    /**
     * The key-stays-home proof. A REAL {@link AiModelProviderProxy} holding a sentinel provider key
     * answers through the peer route: the key reaches the upstream provider's Authorization header
     * (that is the proxy's whole job) and appears NOWHERE in the peer-visible response bytes.
     */
    @Test
    void llmRoute_providerKeyNeverLeavesTheCallee() throws Exception {
        AtomicReference<String> upstreamAuth = new AtomicReference<>();
        HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        upstream.createContext("/", ex -> {
            upstreamAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
            byte[] out = "{\"choices\":[{\"message\":{\"content\":\"pong\"}}]}"
                    .getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        upstream.start();
        AiModelProviderHolder holder = new AiModelProviderHolder();
        holder.apply(new AiModelProviderConfig(
                "http://127.0.0.1:" + upstream.getAddress().getPort(), SENTINEL_KEY,
                "/v1/chat/completions", "/v1/models", null, null, null, null, "gpt-x"));
        AiModelProviderProxy realProxy = new AiModelProviderProxy(holder, MAPPER);
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0,
                mock(HelperExecuteService.class), PeerCallAuthenticator.allowAll(), realProxy, MAPPER);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(),
                    "/sqs/v1/overlay/llm/chat/completions", "{\"model\":\"gpt-x\",\"messages\":[]}");
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("pong"), "the provider answer round-trips");
            assertFalse(resp.body().contains(SENTINEL_KEY),
                    "the provider key must NEVER appear in the peer-visible response");
            assertEquals("Bearer " + SENTINEL_KEY, upstreamAuth.get(),
                    "the key rides the upstream Authorization header only");
        } finally {
            listener.stop();
            upstream.stop(0);
        }
    }

    private static HttpResponse<String> post(int port, String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
