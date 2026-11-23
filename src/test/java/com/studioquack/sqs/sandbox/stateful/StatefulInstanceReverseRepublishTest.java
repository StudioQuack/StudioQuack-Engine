package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.sandbox.egress.StubSandboxEgressManager;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Residual — a daemon restart must re-publish reverse ports of surviving stateful instances once
 * the WireGuard handshake has delivered Network Access Control rules. Covers the four restart
 * cases: restore, rule-deleted skip, not-running skip, and idempotent second pass.
 */
class StatefulInstanceReverseRepublishTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private FakeOciCli cli;
    private StatefulRuntimeProperties props;
    private NetworkRuleStore ruleStore;
    private OverlayReversePortRelay relay;
    private StatefulInstanceManager manager;
    private int port;

    /** What {@code podman ps -q} returns; empty = no running stateful containers. */
    private final AtomicReference<String> psIds = new AtomicReference<>("ctr-alive\n");
    /** Whether the surviving container's inspect reports Running. */
    private final AtomicReference<Boolean> running = new AtomicReference<>(true);
    /** Reverse-port label on the surviving container; blank = none. */
    private final AtomicReference<String> reverseLabel = new AtomicReference<>();

    @TempDir
    Path blobDir;

    @BeforeEach
    void setUp() throws Exception {
        port = freePort();
        reverseLabel.set(Integer.toString(port));
        cli = new FakeOciCli();
        props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        cli.responder = this::respond;
        ruleStore = NetworkRuleStore.inMemory();
        relay = new OverlayReversePortRelay(ruleStore, new OverlayPeerDirectory());
        relay.overlayUp("127.0.0.1/32");
        // Fresh manager = empty in-memory registry (daemon just restarted).
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                null, false, 10, StubSandboxEgressManager.enforced(), relay, ruleStore);
    }

    @AfterEach
    void tearDown() {
        relay.overlayDown("test-done");
    }

    private OciCli.Result respond(List<String> argv) {
        String sub = argv.size() > 1 ? argv.get(1) : "";
        return switch (sub) {
            case "ps" -> new OciCli.Result(0, psIds.get(), "", false);
            case "inspect" -> new OciCli.Result(0, inspectJson(), "", false);
            default -> new OciCli.Result(0, "", "", false);
        };
    }

    private String inspectJson() {
        String label = reverseLabel.get();
        String reverseLabelJson = (label == null || label.isBlank())
                ? ""
                : ",\"" + StatefulRunArgs.REVERSE_PORT_LABEL + "\":\"" + label + "\"";
        return "[{\"Id\":\"ctr-alive\",\"Name\":\"sq-stateful-inst_surviving\","
                + "\"State\":{\"Running\":" + running.get() + "},"
                + "\"Config\":{\"Labels\":{"
                + "\"studioquack.sqs.stateful\":\"1\","
                + "\"studioquack.sqs.env\":\"env-1\","
                + "\"studioquack.sqs.ref\":\"myenv/pg:1\","
                + "\"studioquack.sqs.instance\":\"inst_surviving\""
                + reverseLabelJson
                + "}},"
                + "\"NetworkSettings\":{\"Networks\":{\"sqs-stateful\":"
                + "{\"IPAddress\":\"10.89.1.7\"}}}}]";
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            return s.getLocalPort();
        }
    }

    @Test
    void survivingInstanceIsRepublishedWhenInboundRuleSurvives() {
        ruleStore.replaceAll(List.of(new NetworkRuleStore.Rule("peer-env", port)), List.of());

        manager.republishSurvivingReversePorts();

        assertTrue(relay.publishedPorts().contains(port),
                "surviving reverse port must be rebound after restart");
        assertEquals(port, manager.inventory().stream()
                .filter(i -> "inst_surviving".equals(i.instanceId()))
                .findFirst().orElseThrow().publishedReversePort());
    }

    @Test
    void ruleDeletedWhileDownIsNotRepublished() {
        // Handshake projection is empty — the inbound rule was deleted while we were down.
        ruleStore.replaceAll(List.of(), List.of());

        manager.republishSurvivingReversePorts();

        assertFalse(relay.publishedPorts().contains(port),
                "a port with no inbound Network Access Control rule must stay unpublished");
        assertTrue(manager.inventory().isEmpty(), "skipped containers must not be adopted");
    }

    @Test
    void instanceNoLongerRunningIsSkipped() {
        ruleStore.replaceAll(List.of(new NetworkRuleStore.Rule("peer-env", port)), List.of());
        psIds.set(""); // podman ps (running only) lists nothing
        running.set(false);

        manager.republishSurvivingReversePorts();

        assertTrue(relay.publishedPorts().isEmpty(), "stopped containers must not be published");
    }

    @Test
    void secondRepublishDoesNotDoublePublish() {
        ruleStore.replaceAll(List.of(new NetworkRuleStore.Rule("peer-env", port)), List.of());

        manager.republishSurvivingReversePorts();
        manager.republishSurvivingReversePorts();

        assertEquals(1, relay.publishedPorts().size(),
                "same-claimant re-publish must replace the listener, not stack another");
        assertTrue(relay.publishedPorts().contains(port));
    }
}
