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

import java.net.ServerSocket;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stateful reverse channel (the overlay publish Docling uses for cross-environment calls): a
 * start with a {@code reversePort} publishes the instance on the engine's overlay address, a port
 * held by another node fails the start LOUDLY (naming the holder, container reaped), and the
 * daemon's reserved ports are refused.
 * Runs against {@link FakeOciCli} + a real loopback relay — no container runtime needed.
 */
class StatefulInstanceReversePublishTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private FakeOciCli cli;
    private StatefulRuntimeProperties props;
    private StatefulInstanceManager manager;
    private OverlayReversePortRelay relay;

    @TempDir
    Path blobDir;

    @BeforeEach
    void setUp() {
        cli = new FakeOciCli();
        props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        cli.responder = this::respond;
        relay = new OverlayReversePortRelay(NetworkRuleStore.inMemory(), new OverlayPeerDirectory());
        relay.overlayUp("127.0.0.1/32");
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                null, false, 10, StubSandboxEgressManager.enforced(), relay);
    }

    @AfterEach
    void tearDown() {
        relay.overlayDown("test-done");
    }

    private OciCli.Result respond(List<String> argv) {
        String sub = argv.size() > 1 ? argv.get(1) : "";
        return switch (sub) {
            case "run" -> new OciCli.Result(0, "container-abc123def456\n", "", false);
            case "volume" -> argv.contains("exists") ? new OciCli.Result(1, "", "no such volume", false)
                    : new OciCli.Result(0, "", "", false);
            // The reverse publish reads the bridge IP from this inspect payload.
            case "inspect" -> new OciCli.Result(0,
                    "[{\"Id\":\"container-abc123def456\",\"State\":{\"Status\":\"running\"},"
                            + "\"NetworkSettings\":{\"Networks\":{\"sqs-stateful\":"
                            + "{\"IPAddress\":\"10.89.1.7\"}}}}]", "", false);
            default -> new OciCli.Result(0, "", "", false);
        };
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            return s.getLocalPort();
        }
    }

    @Test
    void startWithReversePortPublishes_andStopUnpublishes() throws Exception {
        int port = freePort();

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null,
                null, null, null, null, port);

        assertEquals(StatefulStatus.RUNNING, r.status());
        assertTrue(relay.publishedPorts().contains(port), "the port must be bound on the overlay address");
        StatefulInstance inst = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
        assertEquals(port, inst.publishedReversePort());

        manager.stop("env-1", inst.instanceId());

        assertTrue(relay.publishedPorts().isEmpty(), "stop must release the published port");
    }

    @Test
    void portHeldByAnotherNodeFailsTheStartLoudly_andReapsTheContainer() throws Exception {
        int port = freePort();
        // Another node already holds the port on this engine.
        relay.publish(port, "127.0.0.1", freePort(), "instance-db-one");

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null,
                null, null, null, null, port);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertNotNull(r.message());
        assertTrue(r.message().contains("instance-db-one"),
                "the failure must name the node holding the port: " + r.message());
        assertNotNull(cli.firstCallWith("rm"),
                "a start that cannot publish must not leave an unpublished container running");
    }

    @Test
    void daemonReservedPortIsRefused() throws Exception {
        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null,
                null, null, null, null, 9876);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertNotNull(r.message());
        assertTrue(r.message().contains("reserved"), r.message());
    }

    @Test
    void startWithoutReversePortPublishesNothing() {
        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, r.status());
        assertTrue(relay.publishedPorts().isEmpty(), "default no-reverse: nothing is published");
        assertNull(manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().publishedReversePort());
    }
}
