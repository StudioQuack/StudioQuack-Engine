package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulEndpointResolver;
import com.studioquack.sqs.sandbox.stateful.StatefulRuntimeProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HelperConnectService} — the daemon side of {@code studioquack.connect(container, port)}.
 * Runs a REAL loopback echo server as the stand-in stateful container and a real {@link
 * StatefulEndpointResolver} over a {@link FakeOciCli} that returns crafted {@code podman inspect}
 * JSON, so the test covers the whole daemon path — label parse, ownership decision, socket, byte
 * round-trip — with no container runtime on the box.
 */
class HelperConnectServiceTest {

    private static final String ENV = "env-1";

    private EchoServer echo;
    private HelperConnectService service;

    @AfterEach
    void tearDown() throws IOException {
        if (service != null) {
            service.shutdown();
        }
        if (echo != null) {
            echo.close();
        }
    }

    /* ------------------------------------------------------------------ */
    /* Happy path                                                         */
    /* ------------------------------------------------------------------ */

    @Test
    @DisplayName("bytes make the full round trip to the named container and back")
    void tunnelRoundTrip() throws Exception {
        echo = new EchoServer();
        service = serviceFor(inspectJson(ENV, "127.0.0.1", true, true));

        HelperJob job = job("job-1", ENV);
        String stream = service.open(job, "my-postgres", echo.port());
        assertEquals(1, service.openStreams());

        service.send(job, stream, "ping".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals("ping".getBytes(StandardCharsets.UTF_8), readAll(job, stream, 4));

        service.close(job, stream);
        assertEquals(0, service.openStreams(), "closing releases the daemon-held socket");
    }

    @Test
    @DisplayName("a name is accepted with or without the sq-stateful- prefix")
    void acceptsBothSpellings() throws Exception {
        echo = new EchoServer();
        service = serviceFor(inspectJson(ENV, "127.0.0.1", true, true));
        HelperJob job = job("job-1", ENV);

        String viaShortName = service.open(job, "my-postgres", echo.port());
        String viaContainerName = service.open(job, "sq-stateful-my-postgres", echo.port());

        assertEquals(2, service.openStreams());
        service.close(job, viaShortName);
        service.close(job, viaContainerName);
    }

    @Test
    @DisplayName("a peer close surfaces as eof, not as an error")
    void peerCloseIsEof() throws Exception {
        echo = new EchoServer();
        echo.closeAfterFirstRead = true;
        service = serviceFor(inspectJson(ENV, "127.0.0.1", true, true));
        HelperJob job = job("job-1", ENV);

        String stream = service.open(job, "my-postgres", echo.port());
        service.send(job, stream, "bye".getBytes(StandardCharsets.UTF_8));

        boolean sawEof = false;
        for (int i = 0; i < 40 && !sawEof; i++) {
            sawEof = service.recv(job, stream, 250).eof();
        }
        assertTrue(sawEof, "the SDK must be told the peer closed so it can stop polling");
    }

    @Test
    @DisplayName("an idle read returns empty without eof rather than failing")
    void idleReadIsNotAnError() throws Exception {
        echo = new EchoServer();
        service = serviceFor(inspectJson(ENV, "127.0.0.1", true, true));
        HelperJob job = job("job-1", ENV);

        String stream = service.open(job, "my-postgres", echo.port());
        HelperConnectService.Chunk chunk = service.recv(job, stream, 50);

        assertEquals(0, chunk.data().length);
        assertFalse(chunk.eof(), "an idle connection is not a closed one");
    }

    /* ------------------------------------------------------------------ */
    /* Authorization                                                      */
    /* ------------------------------------------------------------------ */

    @Test
    @DisplayName("a container owned by another environment is refused")
    void refusesForeignEnvironment() throws Exception {
        echo = new EchoServer();
        service = serviceFor(inspectJson("someone-else", "127.0.0.1", true, true));

        HelperConnectService.ConnectException ex = assertThrows(
                HelperConnectService.ConnectException.class,
                () -> service.open(job("job-1", ENV), "their-postgres", echo.port()));
        assertEquals(HelperConnectService.Failure.FORBIDDEN, ex.failure());
        assertEquals(0, service.openStreams());
    }

    @Test
    @DisplayName("a container that is not a StudioQuack stateful instance is refused")
    void refusesUnmanagedContainer() throws Exception {
        echo = new EchoServer();
        service = serviceFor(inspectJson(ENV, "127.0.0.1", true, /* stateful= */ false));

        HelperConnectService.ConnectException ex = assertThrows(
                HelperConnectService.ConnectException.class,
                () -> service.open(job("job-1", ENV), "someones-redis", echo.port()));
        assertEquals(HelperConnectService.Failure.FORBIDDEN, ex.failure(),
                "sharing the bridge is not a claim on a container");
    }

    @Test
    @DisplayName("an unknown name is NOT_FOUND, a stopped instance is UNAVAILABLE")
    void unknownAndStoppedAreDistinguishable() throws Exception {
        echo = new EchoServer();

        HelperConnectService missing = new HelperConnectService(
                resolverOver(cli -> new OciCli.Result(125, "", "no such container", false)),
                8, 2000, 600);
        assertEquals(HelperConnectService.Failure.NOT_FOUND,
                assertThrows(HelperConnectService.ConnectException.class,
                        () -> missing.open(job("job-1", ENV), "nope", echo.port())).failure());
        missing.shutdown();

        service = serviceFor(inspectJson(ENV, "127.0.0.1", /* running= */ false, true));
        assertEquals(HelperConnectService.Failure.UNAVAILABLE,
                assertThrows(HelperConnectService.ConnectException.class,
                        () -> service.open(job("job-1", ENV), "my-postgres", echo.port())).failure());
    }

    @Test
    @DisplayName("a stream belongs to the job that opened it")
    void streamIsBoundToItsJob() throws Exception {
        echo = new EchoServer();
        service = serviceFor(inspectJson(ENV, "127.0.0.1", true, true));

        HelperJob owner = job("job-1", ENV);
        String stream = service.open(owner, "my-postgres", echo.port());

        HelperJob other = job("job-2", ENV);
        assertEquals(HelperConnectService.Failure.CLOSED,
                assertThrows(HelperConnectService.ConnectException.class,
                        () -> service.send(other, stream, "x".getBytes(StandardCharsets.UTF_8))).failure(),
                "another job must not be able to name someone else's stream");
    }

    @Test
    @DisplayName("the per-job stream cap is enforced")
    void enforcesStreamCap() throws Exception {
        echo = new EchoServer();
        service = new HelperConnectService(
                resolverOver(cli -> new OciCli.Result(0, inspectJson(ENV, "127.0.0.1", true, true), "", false)),
                /* maxStreamsPerJob= */ 2, 2000, 600);
        HelperJob job = job("job-1", ENV);

        service.open(job, "my-postgres", echo.port());
        service.open(job, "my-postgres", echo.port());

        assertEquals(HelperConnectService.Failure.LIMIT,
                assertThrows(HelperConnectService.ConnectException.class,
                        () -> service.open(job, "my-postgres", echo.port())).failure());
    }

    @Test
    @DisplayName("revoking a job closes every tunnel it held")
    void jobRevocationClosesStreams() throws Exception {
        echo = new EchoServer();
        service = serviceFor(inspectJson(ENV, "127.0.0.1", true, true));
        HelperJob job = job("job-1", ENV);

        service.open(job, "my-postgres", echo.port());
        service.open(job, "my-postgres", echo.port());
        assertEquals(2, service.openStreams());

        assertEquals(2, service.closeAllForJob(job.id()));
        assertEquals(0, service.openStreams(), "a grant must not outlive the script that asked for it");
    }

    @Test
    @DisplayName("the router closes tunnels when it revokes the bearer")
    void routerRevokeClosesTunnels() throws Exception {
        echo = new EchoServer();
        service = serviceFor(inspectJson(ENV, "127.0.0.1", true, true));
        HelperRequestRouter router = new HelperRequestRouter(new ObjectMapper(), null, null, service);

        HelperJob job = job("job-1", ENV);
        String token = router.register(job);
        service.open(job, "my-postgres", echo.port());

        router.revoke(token);
        assertEquals(0, service.openStreams());
    }

    /* ------------------------------------------------------------------ */
    /* Helpers                                                            */
    /* ------------------------------------------------------------------ */

    private HelperConnectService serviceFor(String inspectJson) {
        return new HelperConnectService(
                resolverOver(cli -> new OciCli.Result(0, inspectJson, "", false)), 8, 2000, 600);
    }

    private static StatefulEndpointResolver resolverOver(
            java.util.function.Function<java.util.List<String>, OciCli.Result> responder) {
        FakeOciCli cli = new FakeOciCli();
        cli.responder = responder;
        return new StatefulEndpointResolver(cli, new ObjectMapper(), new StatefulRuntimeProperties());
    }

    /** The shape {@code podman inspect <container>} returns, trimmed to what the resolver reads. */
    private static String inspectJson(String envId, String ip, boolean running, boolean stateful) {
        return "[{"
                + "\"State\":{\"Running\":" + running + "},"
                + "\"Config\":{\"Labels\":{"
                + "\"studioquack.sqs.stateful\":\"" + (stateful ? "1" : "0") + "\","
                + "\"studioquack.sqs.env\":\"" + envId + "\","
                + "\"studioquack.sqs.instance\":\"my-postgres\"}},"
                + "\"NetworkSettings\":{\"IPAddress\":\"\",\"Networks\":{\"podman\":{\"IPAddress\":\""
                + ip + "\"}}}}]";
    }

    private static HelperJob job(String id, String envId) {
        return HelperJob.builder(id, "python")
                .executionContext(new java.util.LinkedHashMap<>(Map.of("envId", envId)))
                .build();
    }

    /** Reads until {@code expected} bytes have arrived (the tunnel may chunk). */
    private byte[] readAll(HelperJob job, String stream, int expected) {
        java.io.ByteArrayOutputStream acc = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < 40 && acc.size() < expected; i++) {
            acc.writeBytes(service.recv(job, stream, 250).data());
        }
        return acc.toByteArray();
    }

    /** Stands in for the stateful container: echoes whatever it is sent. */
    private static final class EchoServer implements AutoCloseable {
        private final ServerSocket server;
        private volatile boolean closed;
        volatile boolean closeAfterFirstRead;

        EchoServer() throws IOException {
            this.server = new ServerSocket(0, 8, java.net.InetAddress.getLoopbackAddress());
            Thread t = new Thread(this::acceptLoop, "echo-server");
            t.setDaemon(true);
            t.start();
        }

        int port() {
            return server.getLocalPort();
        }

        private void acceptLoop() {
            while (!closed) {
                try {
                    Socket s = server.accept();
                    Thread w = new Thread(() -> pump(s), "echo-conn");
                    w.setDaemon(true);
                    w.start();
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void pump(Socket s) {
            try (Socket socket = s; InputStream in = socket.getInputStream();
                 OutputStream out = socket.getOutputStream()) {
                byte[] buf = new byte[4096];
                int n;
                while ((n = in.read(buf)) >= 0) {
                    out.write(buf, 0, n);
                    out.flush();
                    if (closeAfterFirstRead) {
                        return;
                    }
                }
            } catch (IOException ignore) {
                // client went away
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            server.close();
        }
    }
}
