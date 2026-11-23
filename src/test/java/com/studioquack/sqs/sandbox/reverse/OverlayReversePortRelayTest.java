package com.studioquack.sqs.sandbox.reverse;

import com.studioquack.sqs.channel.net.FakeNetAgentClient;
import com.studioquack.sqs.channel.net.PeerAddressResolver;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.net.OverlayAddressChangedEvent;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reverse-port relay, tested the way an attacker would probe it: a connection whose source
 * holds no Network Access Control rule is refused, a rule keyed by engine id is resolved to the
 * source's CURRENT address at accept time, and a second node claiming the port fails loudly. Real
 * loopback sockets stand in for the overlay bind + the container target.
 */
class OverlayReversePortRelayTest {

    private NetworkRuleStore ruleStore;
    private OverlayPeerDirectory peerDirectory;
    private FakeNetAgentClient agent;
    private OverlayReversePortRelay relay;
    private ServerSocket echoServer;
    private volatile boolean echoRun;
    private Thread echoThread;
    private final ExecutorService testIo = Executors.newCachedThreadPool();

    @BeforeEach
    void setUp() {
        ruleStore = NetworkRuleStore.inMemory();
        peerDirectory = new OverlayPeerDirectory();
        agent = new FakeNetAgentClient()
                .addPeer(PeerAddressResolver.hostnameFor("engine-2"), "127.0.0.1", true);
        peerDirectory.setPeerAddressResolver(new PeerAddressResolver(agent));
        peerDirectory.replace(OverlayReversePortRelay.DEFAULT_NETWORK, Map.of("engine-2", "127.0.0.1"));
        relay = new OverlayReversePortRelay(ruleStore, peerDirectory,
                new ReversePortRegistry(), testIo);
        relay.overlayUp("127.0.0.1/32");
    }

    @AfterEach
    void tearDown() throws Exception {
        echoRun = false;
        if (echoServer != null) {
            echoServer.close();
        }
        if (echoThread != null) {
            echoThread.join(2000);
        }
        relay.overlayDown("test-done");
        testIo.shutdownNow();
    }

    /** A one-shot echo server: the "container" the relay dials back to. */
    private int startEchoServer() throws Exception {
        echoServer = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
        echoRun = true;
        echoThread = new Thread(() -> {
            while (echoRun) {
                try {
                    Socket s = echoServer.accept();
                    testIo.submit(() -> {
                        try (s) {
                            InputStream in = s.getInputStream();
                            OutputStream out = s.getOutputStream();
                            byte[] buf = new byte[4096];
                            int n;
                            while ((n = in.read(buf)) != -1) {
                                out.write(buf, 0, n);
                                out.flush();
                            }
                        } catch (Exception ignored) {
                            // echo ends when either side closes
                        }
                    });
                } catch (Exception e) {
                    return;
                }
            }
        }, "test-echo");
        echoThread.setDaemon(true);
        echoThread.start();
        return echoServer.getLocalPort();
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            return s.getLocalPort();
        }
    }

    private static String roundTrip(int port, String payload) throws Exception {
        try (Socket s = new Socket("127.0.0.1", port)) {
            s.setSoTimeout(4000);
            s.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));
            s.getOutputStream().flush();
            byte[] buf = new byte[payload.length()];
            int read = 0;
            InputStream in = s.getInputStream();
            try {
                while (read < buf.length) {
                    int n = in.read(buf, read, buf.length - read);
                    if (n == -1) {
                        break;
                    }
                    read += n;
                }
            } catch (java.net.SocketException reset) {
                // A refused connection is closed with data in flight → RST. That IS the
                // refusal signal the test asserts on; treat it as "no bytes came back".
            }
            return new String(buf, 0, Math.max(read, 0), StandardCharsets.UTF_8);
        }
    }

    @Test
    void ruledSourceEngineReachesThePublishedPort() throws Exception {
        int echoPort = startEchoServer();
        int relayPort = freePort();
        // engine-2 holds the rule; its CURRENT address is 127.0.0.1 (resolved at accept time).
        ruleStore.replaceAll(List.of(new NetworkRuleStore.Rule("engine-2", relayPort)), List.of());

        relay.publish(relayPort, "127.0.0.1", echoPort, "instance-db");

        assertEquals("hello-reverse", roundTrip(relayPort, "hello-reverse"));
    }

    @Test
    void connectionWithoutARuleIsRefused() throws Exception {
        int echoPort = startEchoServer();
        int relayPort = freePort();
        // engine-2 is a known same-owner peer but holds NO rule for this port.

        relay.publish(relayPort, "127.0.0.1", echoPort, "instance-db");

        // The relay accepts-then-closes, so the payload never echoes back.
        assertEquals("", roundTrip(relayPort, "should-never-echo"));
    }

    @Test
    void ruleSurvivesTheSourceEngineReconnectingWithANewAddress() throws Exception {
        int echoPort = startEchoServer();
        int relayPort = freePort();
        ruleStore.replaceAll(List.of(new NetworkRuleStore.Rule("engine-2", relayPort)), List.of());
        // engine-2 reconnected: peer directory now points at 127.0.0.2. Loopback 127.0.0.1 must fail.
        agent.clearPeers().addPeer(PeerAddressResolver.hostnameFor("engine-2"), "127.0.0.2", true);
        peerDirectory.setPeerAddressResolver(new PeerAddressResolver(agent));
        peerDirectory.replace(OverlayReversePortRelay.DEFAULT_NETWORK, Map.of("engine-2", "127.0.0.2"));

        relay.publish(relayPort, "127.0.0.1", echoPort, "instance-db");

        assertEquals("", roundTrip(relayPort, "from-old-address"),
                "the previous address must lose access the moment the id resolves elsewhere");
    }

    @Test
    void emptyPeerDirectoryDeniesEverything() throws Exception {
        int echoPort = startEchoServer();
        int relayPort = freePort();
        ruleStore.replaceAll(List.of(new NetworkRuleStore.Rule("engine-2", relayPort)), List.of());
        // Agent map empty → no source resolves.
        agent.clearPeers();
        peerDirectory.setPeerAddressResolver(new PeerAddressResolver(agent));
        peerDirectory.clear();

        relay.publish(relayPort, "127.0.0.1", echoPort, "instance-db");

        assertEquals("", roundTrip(relayPort, "tunnel-down"));
    }

    @Test
    void secondPublisherFailsLoudly_namingTheHolder() throws Exception {
        int echoPort = startEchoServer();
        int relayPort = freePort();
        relay.publish(relayPort, "127.0.0.1", echoPort, "instance-db-one");

        ReversePortRegistry.ReversePortException ex = assertThrows(
                ReversePortRegistry.ReversePortException.class,
                () -> relay.publish(relayPort, "127.0.0.1", echoPort, "instance-db-two"));
        assertTrue(ex.getMessage().contains("instance-db-one"), ex.getMessage());
    }

    @Test
    void unpublishStopsThePortFromAnswering() throws Exception {
        int echoPort = startEchoServer();
        int relayPort = freePort();
        ruleStore.replaceAll(List.of(new NetworkRuleStore.Rule("engine-2", relayPort)), List.of());

        relay.publish(relayPort, "127.0.0.1", echoPort, "instance-db");
        assertEquals("ping", roundTrip(relayPort, "ping"));

        relay.unpublish(relayPort, "instance-db");
        assertTrue(relay.publishedPorts().isEmpty());
        // The listener is closed: a fresh connection is refused at the TCP level.
        assertThrows(Exception.class, () -> roundTrip(relayPort, "ping"));
    }

    @Test
    void overlayUp_refusesWildcard() {
        assertThrows(IllegalStateException.class, () -> relay.overlayUp("0.0.0.0/32"));
        assertThrows(IllegalStateException.class, () -> relay.overlayUp("::/0"));
    }

    @Test
    void addressChangedEvent_rebindsDefaultNetwork_andSwallowsWildcard() {
        relay.onOverlayAddressChanged(new OverlayAddressChangedEvent(this, "10.1.1.1", "127.0.0.1"));
        assertEquals("127.0.0.1", relay.overlayIp());
        // Wildcard is refused inside overlayUp; the event handler must not propagate or clobber.
        relay.onOverlayAddressChanged(new OverlayAddressChangedEvent(this, "127.0.0.1", "0.0.0.0"));
        assertEquals("127.0.0.1", relay.overlayIp());
    }
}
