package com.studioquack.sqs.docling;

import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** {@link CrossEnvDoclingBackend} — the caller half of cross-environment Docling. Proves */
class CrossEnvDoclingBackendTest {

    private OverlayPeerDirectory peerDirectory;
    private NetworkRuleStore ruleStore;
    private CrossEnvDoclingBackend backend;
    private HttpServer fakeRemoteDocling;
    private final AtomicReference<String> receivedBody = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        peerDirectory = mock(OverlayPeerDirectory.class);
        ruleStore = NetworkRuleStore.inMemory();
        backend = new CrossEnvDoclingBackend(peerDirectory, ruleStore,
                new DoclingImagePreprocessor(0), DoclingConvertSettings.interactiveDefaults(),
                60, 2);
    }

    @AfterEach
    void tearDown() {
        if (fakeRemoteDocling != null) {
            fakeRemoteDocling.stop(0);
        }
    }

    private void allowOutbound(String targetEnvId) {
        ruleStore.replaceAll(List.of(),
                List.of(new NetworkRuleStore.OutboundRule(targetEnvId,
                        CrossEnvDoclingBackend.DOCLING_OVERLAY_PORT)));
    }

    @Test
    void missing_outbound_rule_fails_with_the_rule_to_add() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> backend.convert("env-2", "pdf-bytes".getBytes(StandardCharsets.UTF_8),
                        "doc.pdf", "application/pdf"));

        assertTrue(ex.getMessage().contains("env-2"), "names the target engine: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("5001"), "names the port: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("Access & Security"),
                "tells the operator where to add the rule: " + ex.getMessage());
    }

    @Test
    void unresolvable_peer_fails_with_a_named_error() {
        allowOutbound("env-2");
        when(peerDirectory.addressOf("env-2")).thenReturn(Optional.empty());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> backend.convert("env-2", "pdf-bytes".getBytes(StandardCharsets.UTF_8),
                        "doc.pdf", "application/pdf"));

        assertTrue(ex.getMessage().contains("not reachable on the overlay"),
                "a non-peer is named as such: " + ex.getMessage());
    }

    @Test
    void converts_against_the_targets_overlay_address_on_the_well_known_port() throws Exception {
        // The overlay port is protocol-fixed (the target publishes containerPort 5001), so the
        // fake remote must answer on 5001 itself. If the dev machine genuinely runs something
        // there, skip rather than fail on the environment.
        org.junit.jupiter.api.Assumptions.assumeTrue(port5001Bindable(),
                "127.0.0.1:5001 is occupied on this machine — skipping the loopback dial test");
        startFakeRemoteDocling(CrossEnvDoclingBackend.DOCLING_OVERLAY_PORT, "success", null);
        allowOutbound("env-2");
        when(peerDirectory.addressOf("env-2")).thenReturn(Optional.of("127.0.0.1"));

        String result = backend.convert("env-2", "pdf-bytes".getBytes(StandardCharsets.UTF_8),
                "doc.pdf", "application/pdf");

        assertEquals("# converted", result);
        assertTrue(receivedBody.get() != null && receivedBody.get().contains("doc.pdf"),
                "the multipart body carries the document filename");
    }

    @Test
    void failed_remote_conversion_carries_the_servers_error_without_the_port_advice() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(port5001Bindable(),
                "127.0.0.1:5001 is occupied on this machine — skipping the loopback dial test");
        startFakeRemoteDocling(CrossEnvDoclingBackend.DOCLING_OVERLAY_PORT, "failure",
                "whisper is not installed");
        allowOutbound("env-2");
        when(peerDirectory.addressOf("env-2")).thenReturn(Optional.of("127.0.0.1"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> backend.convert("env-2", "audio".getBytes(StandardCharsets.UTF_8),
                        "voice.webm", "audio/webm"));

        assertTrue(ex.getMessage().contains("whisper is not installed"),
                "the target's own error reaches the operator: " + ex.getMessage());
        assertFalse(ex.getMessage().contains("allow this engine on port"),
                "a served request must not re-advise the rule that already let it through: "
                        + ex.getMessage());
    }

    @Test
    void sanitised_placeholder_failure_points_at_the_targets_container_logs() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(port5001Bindable(),
                "127.0.0.1:5001 is occupied on this machine — skipping the loopback dial test");
        startFakeRemoteDocling(CrossEnvDoclingBackend.DOCLING_OVERLAY_PORT, "failure",
                "Internal processing error.");
        allowOutbound("env-2");
        when(peerDirectory.addressOf("env-2")).thenReturn(Optional.of("127.0.0.1"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> backend.convert("env-2", "audio".getBytes(StandardCharsets.UTF_8),
                        "voice.webm", "audio/webm"));

        assertTrue(ex.getMessage().contains("Internal processing error."),
                "the server's message is quoted: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("stateful-instance-logs"),
                "the sanitised placeholder comes with where the real traceback lives: "
                        + ex.getMessage());
    }

    private static boolean port5001Bindable() {
        try (java.net.ServerSocket s = new java.net.ServerSocket(
                new java.net.InetSocketAddress("127.0.0.1", CrossEnvDoclingBackend.DOCLING_OVERLAY_PORT)
                        .getPort(), 50, java.net.InetAddress.getByName("127.0.0.1"))) {
            return s.getLocalPort() == CrossEnvDoclingBackend.DOCLING_OVERLAY_PORT;
        } catch (IOException taken) {
            return false;
        }
    }

    /**
     * A loopback stand-in for the target engine's docling-serve behind its reverse relay, speaking
     * the async task API: submit → poll (immediately {@code pollStatus}) → result.
     */
    private void startFakeRemoteDocling(int port, String pollStatus, String errorMessage)
            throws IOException {
        fakeRemoteDocling = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        fakeRemoteDocling.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (DoclingClient.CONVERT_ASYNC_PATH.equals(path)) {
                receivedBody.set(new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8));
                respond(exchange, 200, "{\"task_id\":\"remote-task\",\"task_status\":\"pending\"}");
            } else if ((DoclingClient.STATUS_POLL_PATH + "remote-task").equals(path)) {
                String error = errorMessage == null ? ""
                        : ",\"error_message\":\"" + errorMessage + "\"";
                respond(exchange, 200, "{\"task_id\":\"remote-task\",\"task_status\":\""
                        + pollStatus + "\"" + error + "}");
            } else if ((DoclingClient.RESULT_PATH + "remote-task").equals(path)) {
                respond(exchange, 200, "# converted");
            } else {
                respond(exchange, 404, "{\"detail\":\"unexpected path\"}");
            }
        });
        fakeRemoteDocling.start();
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
