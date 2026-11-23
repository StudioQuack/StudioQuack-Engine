package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulEndpointResolver;
import com.studioquack.sqs.sandbox.stateful.StatefulRuntimeProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end proof of {@code studioquack.connect(container_name, port)}. */
class PythonHelperConnectTest {

    private static final String ENV = "env-1";

    private EchoServer echo;
    private HelperConnectService connectService;

    @AfterEach
    void tearDown() throws IOException {
        if (connectService != null) {
            connectService.shutdown();
        }
        if (echo != null) {
            echo.close();
        }
    }

    @Test
    @DisplayName("a script reaches a named instance by name and gets its bytes back")
    void connectCarriesAConversation(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");
        echo = new EchoServer("PONG:");
        connectService = connectService();

        HelperRequestRouter router = new HelperRequestRouter(
                new ObjectMapper(), null, null, connectService);
        String token = router.register(HelperJob.builder("job-1", "python")
                .executionContext(new LinkedHashMap<>(Map.of("envId", ENV)))
                .build());

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixRouterBridge bridge = UnixRouterBridge.over(socketPath, router)) {
            String out = runHelperScript(tmp, socketPath.toString(), token, """
                    import socket
                    tunnel = studioquack.connect("my-postgres", ${PORT})
                    print("TUNNEL " + repr(tunnel))
                    host, port = tunnel
                    s = socket.create_connection((host, port), timeout=15)
                    s.sendall(b"HELLO")
                    s.settimeout(15)
                    print("ECHOED " + s.recv(64).decode())
                    s.close()
                    """.replace("${PORT}", Integer.toString(echo.port())));

            assertTrue(out.contains("ECHOED PONG:HELLO"),
                    "the instance's reply must reach the script through the daemon: " + out);
            assertTrue(out.contains("-> my-postgres:" + echo.port()),
                    "the tunnel must describe itself by INSTANCE NAME, not by address: " + out);
            assertTrue(out.contains("127.0.0.1:"),
                    "the script must be handed a local address it can hand to any client: " + out);
        }
    }

    @Test
    @DisplayName("a container owned by another environment is refused at connect(), by name")
    void foreignEnvironmentIsRefusedAtCallSite(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");
        echo = new EchoServer("X");
        // Same container, labelled as belonging to a different environment.
        connectService = new HelperConnectService(
                resolverReturning(inspectJson("someone-else", true, true)), 8, 2000, 600);

        HelperRequestRouter router = new HelperRequestRouter(
                new ObjectMapper(), null, null, connectService);
        String token = router.register(HelperJob.builder("job-1", "python")
                .executionContext(new LinkedHashMap<>(Map.of("envId", ENV)))
                .build());

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixRouterBridge bridge = UnixRouterBridge.over(socketPath, router)) {
            String out = runHelperScript(tmp, socketPath.toString(), token, """
                    try:
                        studioquack.connect("their-postgres", ${PORT})
                        print("NO_RAISE")
                    except RuntimeError as exc:
                        print("RAISED " + str(exc))
                    """.replace("${PORT}", Integer.toString(echo.port())));

            assertTrue(out.contains("RAISED"),
                    "a refused grant must raise at the call site, not fail later inside a driver: " + out);
            assertTrue(out.contains("status=403"),
                    "the refusal must be distinguishable from 'no such instance': " + out);
            assertTrue(out.contains("studioquack.connect('their-postgres'"),
                    "the error must name the instance the script asked for: " + out);
        }
    }

    /* ------------------------------------------------------------------ */
    /* Harness                                                            */
    /* ------------------------------------------------------------------ */

    private HelperConnectService connectService() {
        return new HelperConnectService(resolverReturning(inspectJson(ENV, true, true)), 8, 2000, 600);
    }

    private static StatefulEndpointResolver resolverReturning(String inspectJson) {
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> new OciCli.Result(0, inspectJson, "", false);
        return new StatefulEndpointResolver(cli, new ObjectMapper(), new StatefulRuntimeProperties());
    }

    /** The container resolves to loopback, where the stand-in instance is listening. */
    private static String inspectJson(String envId, boolean running, boolean stateful) {
        return "[{\"State\":{\"Running\":" + running + "},"
                + "\"Config\":{\"Labels\":{"
                + "\"studioquack.sqs.stateful\":\"" + (stateful ? "1" : "0") + "\","
                + "\"studioquack.sqs.env\":\"" + envId + "\"}},"
                + "\"NetworkSettings\":{\"Networks\":{\"podman\":{\"IPAddress\":\"127.0.0.1\"}}}}]";
    }

    private static String runHelperScript(Path tmp, String helperSocket, String bearer, String body)
            throws Exception {
        String helper = loadHelper().replace("__SQ_CONTEXT__", "");
        Path script = tmp.resolve("connect_smoke.py");
        Files.writeString(script, helper + "\n" + body, StandardCharsets.UTF_8);

        ProcessBuilder pb = new ProcessBuilder("python3", script.toString());
        pb.environment().put("SQ_HELPER_SOCKET", helperSocket);
        pb.environment().put("SQ_HELPER_BEARER", bearer);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(60, TimeUnit.SECONDS), "python smoke timed out; output=" + out);
        return out;
    }

    private static String loadHelper() throws IOException {
        try (InputStream in = PythonHelperConnectTest.class
                .getResourceAsStream("/sandbox/helpers/python.py")) {
            if (in == null) {
                throw new IOException("missing classpath resource /sandbox/helpers/python.py");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static boolean python3Available() {
        try {
            Process p = new ProcessBuilder("python3", "-c", "print(1)").start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    /** Stands in for the stateful container: replies with {@code prefix + whatever it was sent}. */
    private static final class EchoServer implements AutoCloseable {
        private final ServerSocket server;
        private final String prefix;
        private volatile boolean closed;

        EchoServer(String prefix) throws IOException {
            this.prefix = prefix;
            this.server = new ServerSocket(0, 8, InetAddress.getLoopbackAddress());
            Thread t = new Thread(this::acceptLoop, "instance-stand-in");
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
                    Thread w = new Thread(() -> pump(s), "instance-conn");
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
                    out.write((prefix + new String(buf, 0, n, StandardCharsets.UTF_8))
                            .getBytes(StandardCharsets.UTF_8));
                    out.flush();
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
