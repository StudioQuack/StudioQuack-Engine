package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.command.DaemonCommandRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * {@link HeartbeatClient} against a mock server: asserts the EXACT {@code /heartbeat?waitMs=} path
 * + camelCase body, and that a pushed {@code deploy} command is parsed and dispatched to a
 * registered {@link DaemonCommandHandler} (the seam), with the result shipped on the next beat.
 */
class HeartbeatClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-1";
    private static final String BEARER = "sqvm_bearer_hb";
    private static final String HEARTBEAT_PATH = "/be/v1/sqvm/environments/" + ENV_ID + "/heartbeat";

    private String base(MockBackend be) {
        return be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
    }

    private HeartbeatClient client(MockBackend be, DaemonCommandRegistry registry) {
        return new HeartbeatClient(base(be), BEARER, "0.0.1", "inst-1", 7L,
                20_000L, 2_000L, new ChannelHttpClients(null), MAPPER, registry,
                new DefaultHeartbeatResourceProvider(), null);
    }

    @Test
    void beat_emits_exact_path_bearer_and_camelcase_body() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 200, "{}");
            client(be, new DaemonCommandRegistry(List.of())).beat();

            MockBackend.Recorded req = be.firstMatching("/heartbeat");
            assertNotNull(req);
            assertEquals("POST", req.method());
            assertEquals(HEARTBEAT_PATH, req.path());
            assertEquals("waitMs=20000", req.rawQuery());
            assertEquals("Bearer " + BEARER, req.headers().get("authorization"));

            Map<?, ?> body = MAPPER.readValue(req.body(), Map.class);
            assertEquals("2026-05", body.get("heartbeatVersion"));
            assertEquals("0.0.1", body.get("agentVersion"));
            assertEquals("inst-1", body.get("instanceId"));
            assertEquals(7, ((Number) body.get("generation")).intValue());
            assertEquals("execution", body.get("instanceRole"));
            assertTrue(body.get("resources") instanceof Map, "resources snapshot present");
        }
    }

    @Test
    void beat_404WithEnvNotFoundCode_throwsEnvNotFound() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 404, "{\"errorCode\":\"Error_EnvNotFound\"}");
            HeartbeatClient client = client(be, new DaemonCommandRegistry(List.of()));
            try {
                client.beat();
                fail("expected HeartbeatException");
            } catch (HeartbeatClient.HeartbeatException he) {
                assertEquals(404, he.statusCode());
                assertTrue(he.isEnvNotFound(), "BE 404 with Error_EnvNotFound is the definitive gone signal");
            } catch (Exception other) {
                fail("unexpected exception: " + other);
            }
        }
    }

    @Test
    void beat_404WithoutEnvNotFoundCode_staysTransient() throws Exception {
        // A proxy / deploy-window 404 carries no Error_EnvNotFound body — the daemon must
        // never self-uninstall on someone else's 404.
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 404, "<html>404 Not Found</html>");
            HeartbeatClient client = client(be, new DaemonCommandRegistry(List.of()));
            try {
                client.beat();
                fail("expected HeartbeatException");
            } catch (HeartbeatClient.HeartbeatException he) {
                assertEquals(404, he.statusCode());
                assertFalse(he.isEnvNotFound(), "plain 404 without the error code is transient");
            } catch (Exception other) {
                fail("unexpected exception: " + other);
            }
        }
    }

    @Test
    void runLoop_envNotFound_runsDeletedEngineHandlerAndStops() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 404, "{\"errorCode\":\"Error_EnvNotFound\"}");
            HeartbeatClient client = client(be, new DaemonCommandRegistry(List.of()));
            AtomicBoolean gone = new AtomicBoolean(false);
            client.setEnvGoneHandler(() -> gone.set(true));

            client.start();
            awaitTrue(gone::get, "env-gone handler ran on 404 Error_EnvNotFound");
            client.stop();
        }
    }

    @Test
    void runLoop_plain404_neverRunsDeletedEngineHandler() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 404, "<html>404 Not Found</html>");
            HeartbeatClient client = client(be, new DaemonCommandRegistry(List.of()));
            AtomicBoolean gone = new AtomicBoolean(false);
            client.setEnvGoneHandler(() -> gone.set(true));

            client.start();
            // Warm-up is 1s; give the loop room for two failed beats, then assert survival.
            Thread.sleep(1_600L);
            client.stop();
            assertFalse(gone.get(), "transient 404 must not take the deleted-engine exit path");
        }
    }

    @Test
    void beat_dispatches_deploy_command_and_reports_result_next_beat() throws Exception {
        CapturingHandler deploy = new CapturingHandler();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(deploy));

        try (MockBackend be = new MockBackend()) {
            AtomicInteger calls = new AtomicInteger();
            String payload = "{\"processKey\":\"Process_wf\",\"workflowExecutionId\":\"wx-9\"}";
            be.stub(HEARTBEAT_PATH, req -> {
                if (calls.getAndIncrement() == 0) {
                    // First beat pushes a deploy command.
                    return MockBackend.Response.json(200,
                            "{\"commands\":[{\"commandId\":\"c1\",\"type\":\"deploy\","
                                    + "\"issuedAt\":\"2026-07-12T00:00:00Z\",\"payload\":"
                                    + MAPPER.writeValueAsString(payload) + "}]}");
                }
                return MockBackend.Response.json(200, "{}");
            });

            HeartbeatClient client = client(be, registry);

            // Beat 1: receives the deploy command and hands it to the command lane (off the beat thread).
            HeartbeatClient.HeartbeatResponse resp = client.beat();
            assertNotNull(resp.commands());
            assertEquals(1, resp.commands().size());
            assertEquals("deploy", resp.commands().get(0).type());
            awaitTrue(() -> "c1".equals(deploy.seenCommandId), "lane ran the deploy handler");
            assertEquals(payload, deploy.seenPayload);
            awaitTrue(() -> client.pendingResultCount() == 1, "lane queued the deploy result");

            // Beat 2: ships the deploy result back to the server.
            client.beat();
            MockBackend.Recorded second = be.requests().get(1);
            Map<?, ?> body = MAPPER.readValue(second.body(), Map.class);
            List<?> results = (List<?>) body.get("commandResults");
            assertNotNull(results, "commandResults present on the next beat");
            assertEquals(1, results.size());
            Map<?, ?> r0 = (Map<?, ?>) results.get(0);
            assertEquals("c1", r0.get("commandId"));
            assertEquals("deploy", r0.get("type"));
            assertEquals("ok", r0.get("status"));
            assertEquals("deployed", r0.get("output"));
        }
    }

    @Test
    void beat_serializes_wg_liveness_when_overlay_up() throws Exception {
        long handshake = 1_752_000_000L;
        WgLivenessProvider liveness = () -> new WgLivenessProvider.WgLiveness(true, handshake);
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 200, "{}");
            HeartbeatClient client = new HeartbeatClient(base(be), BEARER, "0.0.1", "inst-1", 7L,
                    20_000L, 2_000L, new ChannelHttpClients(null), MAPPER,
                    new DaemonCommandRegistry(List.of()), new DefaultHeartbeatResourceProvider(), liveness);
            client.beat();

            MockBackend.Recorded req = be.firstMatching("/heartbeat");
            assertNotNull(req);
            Map<?, ?> body = MAPPER.readValue(req.body(), Map.class);
            assertEquals(Boolean.TRUE, body.get("wgOverlayUp"),
                    "wgOverlayUp reported from the WgLivenessProvider (BE re-arms its reachability gate)");
            assertEquals(handshake, ((Number) body.get("wgLastHandshakeUnix")).longValue(),
                    "wgLastHandshakeUnix reported from the WgLivenessProvider");
        }
    }

    @Test
    void beat_omits_wg_liveness_when_overlay_down_for_backward_compat() throws Exception {
        WgLivenessProvider liveness = WgLivenessProvider.WgLiveness::down;
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 200, "{}");
            HeartbeatClient client = new HeartbeatClient(base(be), BEARER, "0.0.1", "inst-1", 7L,
                    20_000L, 2_000L, new ChannelHttpClients(null), MAPPER,
                    new DaemonCommandRegistry(List.of()), new DefaultHeartbeatResourceProvider(), liveness);
            client.beat();

            MockBackend.Recorded req = be.firstMatching("/heartbeat");
            assertNotNull(req);
            Map<?, ?> body = MAPPER.readValue(req.body(), Map.class);
            assertFalse(body.containsKey("wgOverlayUp"), "wgOverlayUp omitted when overlay down (back-compat)");
            assertFalse(body.containsKey("wgLastHandshakeUnix"),
                    "wgLastHandshakeUnix omitted when overlay down (back-compat)");
        }
    }

    @Test
    void beat_feeds_overlayReachable_true_into_cadence_for_keepalive() throws Exception {
        AtomicBoolean overlayHealthy = new AtomicBoolean(true);
        HeartbeatCadence cadence = new HeartbeatCadence(2_000L, 0L, 60_000L, 300_000L, 30_000L,
                overlayHealthy::get, () -> false);
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 200, "{\"overlayReachable\":true}");
            HeartbeatClient client = new HeartbeatClient(() -> base(be), null, BEARER, "0.0.1", "inst-1", 7L,
                    cadence, new ChannelHttpClients(null), MAPPER,
                    new DaemonCommandRegistry(List.of()), new DefaultHeartbeatResourceProvider(), null);

            HeartbeatClient.HeartbeatResponse resp = client.beat();
            assertEquals(Boolean.TRUE, resp.overlayReachable());

            // First nextDelayMs is the confirmation transition; second is the keepalive interval.
            assertEquals(0L, cadence.nextDelayMs());
            assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, cadence.mode());
            assertEquals(60_000L, cadence.nextDelayMs());
        }
    }

    @Test
    void beat_absent_overlayReachable_leaves_cadence_unconfirmed() throws Exception {
        AtomicBoolean overlayHealthy = new AtomicBoolean(true);
        HeartbeatCadence cadence = new HeartbeatCadence(2_000L, 0L, 60_000L, 300_000L, 30_000L,
                overlayHealthy::get, () -> false);
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 200, "{}");
            HeartbeatClient client = new HeartbeatClient(() -> base(be), null, BEARER, "0.0.1", "inst-1", 7L,
                    cadence, new ChannelHttpClients(null), MAPPER,
                    new DaemonCommandRegistry(List.of()), new DefaultHeartbeatResourceProvider(), null);

            HeartbeatClient.HeartbeatResponse resp = client.beat();
            assertNull(resp.overlayReachable(), "older backends omit the field");

            assertEquals(0L, cadence.nextDelayMs());
            assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, cadence.mode(),
                    "absent must not talk the daemon into keepalive");
            assertEquals(30_000L, cadence.nextDelayMs());
        }
    }

    @Test
    void beat_with_four_field_response_and_commands_resets_fallback_floor() throws Exception {
        CapturingHandler deploy = new CapturingHandler();
        AtomicBoolean overlayHealthy = new AtomicBoolean(false);
        HeartbeatCadence cadence = new HeartbeatCadence(2_000L, 0L, 60_000L, 300_000L, 30_000L,
                overlayHealthy::get, () -> false);
        assertEquals(0L, cadence.nextDelayMs());
        assertEquals(30_000L, cadence.nextDelayMs());
        assertEquals(60_000L, cadence.nextDelayMs());

        try (MockBackend be = new MockBackend()) {
            String payload = "{\"processKey\":\"Process_wf\",\"workflowExecutionId\":\"wx-9\"}";
            be.stubJson(HEARTBEAT_PATH, 200,
                    "{\"commands\":[{\"commandId\":\"c1\",\"type\":\"deploy\","
                            + "\"issuedAt\":\"2026-07-12T00:00:00Z\",\"payload\":"
                            + MAPPER.writeValueAsString(payload) + "}],"
                            + "\"locked\":false,\"overlayReachable\":false}");
            HeartbeatClient client = new HeartbeatClient(() -> base(be), null, BEARER, "0.0.1", "inst-1", 7L,
                    cadence, new ChannelHttpClients(null), MAPPER,
                    new DaemonCommandRegistry(List.of(deploy)), new DefaultHeartbeatResourceProvider(), null);

            HeartbeatClient.HeartbeatResponse resp = client.beat();
            assertEquals(Boolean.FALSE, resp.overlayReachable());
            awaitTrue(() -> "c1".equals(deploy.seenCommandId), "lane ran the deploy handler");
            awaitTrue(() -> client.pendingResultCount() == 1, "lane queued the deploy result");
            // The lane's completion hook fires on the same thread immediately after queueing the
            // result; give it a moment so its sendNow is visible to the cadence assertions below.
            Thread.sleep(100L);

            // A completed command result asks for an immediate beat (so the answer does not wait
            // out a cadence period)…
            assertEquals(0L, cadence.nextDelayMs(), "result-ready beat goes out at once");
            // …and the collected work triggered noteActivity → ladder back at the floor.
            assertEquals(30_000L, cadence.nextDelayMs());
            assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, cadence.mode());
        }
    }

    @Test
    void beat_401_flags_unauthorized_and_backoff_caps_at_5_minutes() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson(HEARTBEAT_PATH, 401, "{\"errorCode\":\"Error_Unauthorized\"}");
            HeartbeatClient client = new HeartbeatClient(
                    base(be), BEARER, "0.0.1", "inst-1", 1L, 0L, 2_000L,
                    new ChannelHttpClients(null), MAPPER, new DaemonCommandRegistry(List.of()),
                    new DefaultHeartbeatResourceProvider(), null);

            HeartbeatClient.HeartbeatException ex = assertThrows(
                    HeartbeatClient.HeartbeatException.class, client::beat);
            assertEquals(401, ex.statusCode());
            assertTrue(ex.isUnauthorized());
            assertFalse(ex.isEnvNotFound());
        }

        assertEquals(30_000L, HeartbeatClient.authErrorBackoffMs(1));
        assertEquals(60_000L, HeartbeatClient.authErrorBackoffMs(2));
        assertEquals(120_000L, HeartbeatClient.authErrorBackoffMs(3));
        assertEquals(240_000L, HeartbeatClient.authErrorBackoffMs(4));
        assertEquals(300_000L, HeartbeatClient.authErrorBackoffMs(5));
        assertEquals(300_000L, HeartbeatClient.authErrorBackoffMs(10));
    }

    /**
     * Commands now execute on the command lanes, off the beat thread: assertions about a handler
     * having run (or its result being queued) must wait for the lane instead of assuming beat()
     * ran it inline. Two seconds is generous for an in-process hand-off and keeps a regression —
     * a lane that never runs — loud rather than silent.
     */
    private static void awaitTrue(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        fail("timed out waiting for: " + what);
    }

    /** A stand-in for the {@code deploy} handler, capturing what it received. */
    private static final class CapturingHandler implements DaemonCommandHandler {
        volatile String seenCommandId;
        volatile String seenPayload;

        @Override
        public String type() {
            return "deploy";
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) {
            this.seenCommandId = command.commandId();
            this.seenPayload = command.payload();
            return DaemonCommandOutcome.ok("deployed");
        }
    }
}
