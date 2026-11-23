package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommandRegistry;
import com.studioquack.sqs.channel.net.HubAddress;
import com.studioquack.sqs.channel.net.HubList;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end proof of the outbound overlay repoint + fallback */
class OverlayOutboundRepointTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-1";
    private static final String BEARER = "sqvm_bearer_wg";
    private static final String SUFFIX = "/be/v1/sqvm/environments/" + ENV_ID;
    private static final String HEARTBEAT_PATH = SUFFIX + "/heartbeat";
    private static final String POLL_PATH = SUFFIX + "/exec-commands";

    private static String base(MockBackend be) {
        return be.baseUrl() + SUFFIX;
    }

    private HeartbeatClient heartbeat(BackendBaseResolver resolver) {
        // longPollWaitMs=0 keeps the request prompt (no ?waitMs); base read from the resolver each beat.
        return new HeartbeatClient(resolver::current, resolver, BEARER, "0.0.1", "inst-1", 0L,
                HeartbeatCadence.alwaysResponsive(2_000L, 0L), new ChannelHttpClients(null), MAPPER,
                new DaemonCommandRegistry(List.of()), new DefaultHeartbeatResourceProvider(), null);
    }

    @Test
    void heartbeat_reads_base_from_resolver_each_beat() throws Exception {
        try (MockBackend publicBe = new MockBackend(); MockBackend overlayBe = new MockBackend()) {
            publicBe.stubJson(HEARTBEAT_PATH, 200, "{}");
            overlayBe.stubJson(HEARTBEAT_PATH, 200, "{}");

            BackendBaseResolver resolver = new BackendBaseResolver(base(publicBe));
            HeartbeatClient client = heartbeat(resolver);

            // Beat 1 — overlay not active → hits the PUBLIC backend.
            client.beat();
            assertEquals(1, publicBe.countMatching("/heartbeat"));
            assertEquals(0, overlayBe.countMatching("/heartbeat"));

            // Repoint to the overlay → the SAME client's next beat hits the OVERLAY backend.
            resolver.activateOverlay(base(overlayBe));
            client.beat();
            assertEquals(1, publicBe.countMatching("/heartbeat"), "public unchanged after repoint");
            assertEquals(1, overlayBe.countMatching("/heartbeat"), "base is re-read from the resolver each beat");
        }
    }

    @Test
    void heartbeat_falls_back_to_public_after_overlay_connection_failures() throws Exception {
        try (MockBackend publicBe = new MockBackend()) {
            publicBe.stubJson(HEARTBEAT_PATH, 200, "{}");

            String deadOverlayBase = "http://127.0.0.1:" + deadPort() + SUFFIX;
            BackendBaseResolver resolver = new BackendBaseResolver(base(publicBe)); // default threshold = 3
            resolver.activateOverlay(deadOverlayBase);
            HeartbeatClient client = heartbeat(resolver);

            // 3 beats hit the dead overlay endpoint (connection refused) and trip the fallback.
            for (int i = 0; i < 3; i++) {
                assertThrows(Exception.class, client::beat, "overlay beat fails while the tunnel is dead");
            }
            assertFalse(resolver.overlayActive(), "3 consecutive overlay failures fell back to public");
            assertEquals(base(publicBe), resolver.current());

            // The control plane is NOT wedged: the next beat rides the public base and succeeds.
            client.beat();
            assertEquals(1, publicBe.countMatching("/heartbeat"),
                    "after fallback the heartbeat reaches the public backend");
        }
    }

    @Test
    void exec_channel_reads_base_from_resolver_each_poll() throws Exception {
        try (MockBackend publicBe = new MockBackend(); MockBackend overlayBe = new MockBackend()) {
            String emptyList = "{\"envId\":\"" + ENV_ID + "\",\"commands\":[]}";
            publicBe.stubJson(POLL_PATH, 200, emptyList);
            overlayBe.stubJson(POLL_PATH, 200, emptyList);

            BackendBaseResolver resolver = new BackendBaseResolver(base(publicBe));
            ExecChannelClient client = new ExecChannelClient(resolver::current, resolver, BEARER, "inst-1",
                    new ChannelHttpClients(null), MAPPER, null, 900_000L, 0L, 600_000L);

            client.pollAndDispatchOnce(32, 0L); // public
            assertEquals(1, publicBe.countMatching("/exec-commands"));
            assertEquals(0, overlayBe.countMatching("/exec-commands"));

            resolver.activateOverlay(base(overlayBe));
            client.pollAndDispatchOnce(32, 0L); // overlay
            assertEquals(1, overlayBe.countMatching("/exec-commands"), "poll base re-read from the resolver");
        }
    }

    @Test
    void outboundOverlaySkipReason_gates_repoint_on_real_wg_up() {
        // Master switch off → never repoint.
        assertEquals("outbound-over-overlay-disabled",
                SqsChannelBootstrap.outboundOverlaySkipReason(false, true, "kernel", true));
        // WG did not come up → stay public.
        assertEquals("wg-not-up",
                SqsChannelBootstrap.outboundOverlaySkipReason(true, false, "kernel", true));
        // No data plane on this host (bean absent — nothing simulates a tunnel) → stay public.
        assertEquals("no-data-plane",
                SqsChannelBootstrap.outboundOverlaySkipReason(true, true, null, true));
        // Real backend but interface down → stay public.
        assertEquals("wg-down",
                SqsChannelBootstrap.outboundOverlaySkipReason(true, true, "kernel", false));
        // Real, up WG data plane with the switch on → repoint (no skip reason).
        assertNull(SqsChannelBootstrap.outboundOverlaySkipReason(true, true, "kernel", true));
    }

    @Test
    void netBringUpSkipReason_gates_handshake_on_config_and_tooling() {
        assertEquals("disabled", SqsChannelBootstrap.netBringUpSkipReason(false, true));
        assertEquals("no-data-plane", SqsChannelBootstrap.netBringUpSkipReason(true, false));
        assertNull(SqsChannelBootstrap.netBringUpSkipReason(true, true));
    }

    @Test
    void hubList_httpsWalk_currentGoneWhenInUseLeavesTheList() throws Exception {
        HubList hubs = new HubList();
        int closed = deadPort();
        HubAddress dark = new HubAddress("g-dark", "127.0.0.1", closed);
        HubAddress next = new HubAddress("g-next", "203.0.113.20", 443);
        hubs.replace(List.of(dark, next));
        hubs.markInUse(dark);
        assertFalse(hubs.currentGone());
        assertFalse(OverlayHealthProbe.httpsReachable(dark.address(), dark.port(), null, 250),
                "a closed hub port is not HTTPS-reachable");

        hubs.replace(List.of(next));
        assertTrue(hubs.currentGone(), "the in-use hub leaving the list forces a walk");
    }

    /** Allocates and immediately frees a local TCP port so a connect to it is refused fast. */
    private static int deadPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
