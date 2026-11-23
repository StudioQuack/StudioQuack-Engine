package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * In-process {@code sq_helper_broker.py}: one-shot replies keep Content-Length; SSE stream
 * frames do not, concatenate chunks, and close on {@code done=true}.
 */
class HelperBrokerStreamTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void oneShotReply_includesContentLength() throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for broker smoke");
        try (BrokerProc broker = BrokerProc.start()) {
            Future<RawHttp> http = broker.httpPost("/v1/helper/log", "{}");
            String id = broker.readRequestId();
            broker.writeFrame("{\"id\":\"" + id + "\",\"status\":200,\"body_b64\":\""
                    + b64("{\"ok\":true}") + "\"}");
            RawHttp resp = http.get(8, TimeUnit.SECONDS);
            assertEquals(200, resp.status);
            assertTrue(resp.hasHeader("content-length"), "one-shot must keep Content-Length: " + resp.headers);
            assertEquals("{\"ok\":true}", resp.body);
        }
    }

    @Test
    void parkedRequest_isAnsweredWithAClearErrorWhenTheEngineSideCloses() throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for broker smoke");
        try (BrokerProc broker = BrokerProc.start()) {
            Future<RawHttp> http = broker.httpPost("/v1/helper/llm/chat/completions", "{}");
            broker.readRequestId();
            broker.closeEngineSide();
            RawHttp resp = http.get(8, TimeUnit.SECONDS);
            assertEquals(502, resp.status);
            assertTrue(resp.body.contains("helper_channel_closed"), resp.body);
        }
    }

    @Test
    void sseStream_noContentLength_concatenatesChunksAndClosesOnDone() throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for broker smoke");
        try (BrokerProc broker = BrokerProc.start()) {
            Future<RawHttp> http = broker.httpPost("/v1/helper/llm/chat", "{\"stream\":true}");
            String id = broker.readRequestId();
            broker.writeFrame(streamFrame(id, 200, "text/event-stream", "data: a\n\n", 0, false));
            broker.writeFrame(streamFrame(id, 200, "text/event-stream", "data: b\n\n", 1, true));
            RawHttp resp = http.get(8, TimeUnit.SECONDS);
            assertEquals(200, resp.status);
            assertFalse(resp.hasHeader("content-length"), "SSE must not send Content-Length: " + resp.headers);
            assertTrue(resp.header("content-type").startsWith("text/event-stream"), resp.headers);
            assertEquals("data: a\n\ndata: b\n\n", resp.body);
        }
    }

    @Test
    void requestFrame_forwardsAcceptWhenPresent_omitsWhenAbsent() throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for broker smoke");
        try (BrokerProc broker = BrokerProc.start()) {
            Future<RawHttp> withAccept = broker.httpPost("/v1/helper/llm/chat", "{\"stream\":true}",
                    "Accept: text/event-stream\r\n");
            JsonNode accepted = broker.readRequestFrame();
            assertEquals("text/event-stream", accepted.get("accept").asText());
            broker.writeFrame("{\"id\":\"" + accepted.get("id").asText()
                    + "\",\"status\":200,\"body_b64\":\"" + b64("{}") + "\"}");
            withAccept.get(8, TimeUnit.SECONDS);

            Future<RawHttp> noAccept = broker.httpPost("/v1/helper/log", "{}");
            JsonNode omitted = broker.readRequestFrame();
            assertFalse(omitted.has("accept"), omitted.toString());
            broker.writeFrame("{\"id\":\"" + omitted.get("id").asText()
                    + "\",\"status\":200,\"body_b64\":\"" + b64("{}") + "\"}");
            noAccept.get(8, TimeUnit.SECONDS);
        }
    }

    @Test
    void jsonErrorDoneTrue_answersAsContentLengthOneShot() throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for broker smoke");
        try (BrokerProc broker = BrokerProc.start()) {
            Future<RawHttp> http = broker.httpPost("/v1/helper/execute/stream", "{}");
            String id = broker.readRequestId();
            broker.writeFrame(streamFrame(id, 503, "application/json", "{\"error\":\"x\"}", 0, true));
            RawHttp resp = http.get(8, TimeUnit.SECONDS);
            assertEquals(503, resp.status);
            assertTrue(resp.hasHeader("content-length"), "non-SSE done=true may use Content-Length: " + resp.headers);
            assertEquals("{\"error\":\"x\"}", resp.body);
        }
    }

    private static String streamFrame(String id, int status, String contentType, String chunk,
                                      int seq, boolean done) {
        return "{\"id\":\"" + id + "\",\"status\":" + status
                + ",\"contentType\":\"" + contentType + "\",\"chunk_b64\":\"" + b64(chunk)
                + "\",\"seq\":" + seq + ",\"done\":" + done + "}";
    }

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static boolean python3Available() {
        try {
            Process p = new ProcessBuilder("python3", "-c", "print(1)").start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static final class RawHttp {
        final int status;
        final String headers;
        final String body;

        private RawHttp(int status, String headers, String body) {
            this.status = status;
            this.headers = headers;
            this.body = body;
        }

        boolean hasHeader(String name) {
            String needle = name.toLowerCase(Locale.ROOT) + ":";
            for (String line : headers.split("\r\n")) {
                if (line.toLowerCase(Locale.ROOT).startsWith(needle)) {
                    return true;
                }
            }
            return false;
        }

        String header(String name) {
            String needle = name.toLowerCase(Locale.ROOT) + ":";
            for (String line : headers.split("\r\n")) {
                if (line.toLowerCase(Locale.ROOT).startsWith(needle)) {
                    return line.substring(line.indexOf(':') + 1).trim();
                }
            }
            return "";
        }

        static RawHttp parse(byte[] raw) {
            String text = new String(raw, StandardCharsets.UTF_8);
            int split = text.indexOf("\r\n\r\n");
            String head = split < 0 ? text : text.substring(0, split);
            String body = split < 0 ? "" : text.substring(split + 4);
            int status = 0;
            String first = head.split("\r\n")[0];
            String[] parts = first.split(" ");
            if (parts.length >= 2) {
                try {
                    status = Integer.parseInt(parts[1]);
                } catch (NumberFormatException ignored) {
                    // leave 0
                }
            }
            return new RawHttp(status, head, body);
        }
    }

    /** Launches {@code sq_helper_broker.py} against a temp UDS and a pipe pair. */
    private static final class BrokerProc implements AutoCloseable {
        private final Path socketPath;
        private final Process process;
        private final BufferedReader stdout;
        private final OutputStream stdin;
        private final ExecutorService httpPool = Executors.newSingleThreadExecutor();

        private BrokerProc(Path socketPath, Process process) {
            this.socketPath = socketPath;
            this.process = process;
            this.stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
            this.stdin = process.getOutputStream();
            Thread err = new Thread(() -> drain(process.getErrorStream()), "broker-test-stderr");
            err.setDaemon(true);
            err.start();
        }

        static BrokerProc start() throws Exception {
            Path script = copyBrokerScript();
            Path sock = Path.of("/tmp/sq-hbr-" + UUID.randomUUID() + ".sock");
            ProcessBuilder pb = new ProcessBuilder("python3", "-u", script.toString());
            pb.environment().put("SQ_HELPER_SOCKET", sock.toString());
            Process p = pb.start();
            waitForSocket(sock, p);
            return new BrokerProc(sock, p);
        }

        Future<RawHttp> httpPost(String path, String body) {
            return httpPost(path, body, "");
        }

        Future<RawHttp> httpPost(String path, String body, String extraHeaders) {
            byte[] payload = body.getBytes(StandardCharsets.UTF_8);
            return httpPool.submit(() -> unixPost(socketPath, path, payload, extraHeaders));
        }

        String readRequestId() throws IOException, InterruptedException {
            return readRequestFrame().get("id").asText();
        }

        JsonNode readRequestFrame() throws IOException, InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (System.nanoTime() < deadline) {
                if (stdout.ready()) {
                    String line = stdout.readLine();
                    if (line == null) {
                        throw new IOException("broker stdout closed before request frame");
                    }
                    return MAPPER.readTree(line);
                }
                if (!process.isAlive()) {
                    throw new IOException("broker died before request frame");
                }
                Thread.sleep(20);
            }
            throw new IOException("timed out waiting for broker request frame");
        }

        void writeFrame(String json) throws IOException {
            stdin.write((json + "\n").getBytes(StandardCharsets.UTF_8));
            stdin.flush();
        }

        /** The daemon end of the response pipe goes away (engine restart, container detach). */
        void closeEngineSide() throws IOException {
            stdin.close();
        }

        @Override
        public void close() {
            httpPool.shutdownNow();
            process.destroy();
            try {
                process.waitFor(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                Files.deleteIfExists(socketPath);
            } catch (IOException ignored) {
                // best-effort
            }
        }

        private static Path copyBrokerScript() throws IOException {
            Path tmp = Files.createTempFile("sq_helper_broker", ".py");
            tmp.toFile().deleteOnExit();
            try (InputStream in = HelperBrokerStreamTest.class
                    .getResourceAsStream("/sandbox/helpers/sq_helper_broker.py")) {
                if (in == null) {
                    throw new IOException("missing classpath resource /sandbox/helpers/sq_helper_broker.py");
                }
                Files.write(tmp, in.readAllBytes());
            }
            return tmp;
        }

        private static void waitForSocket(Path sock, Process p) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (System.nanoTime() < deadline) {
                if (!p.isAlive()) {
                    String err = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
                    throw new IllegalStateException("broker exited " + p.exitValue() + " stderr=" + err);
                }
                if (Files.exists(sock)) {
                    Thread.sleep(50);
                    return;
                }
                Thread.sleep(20);
            }
            throw new IllegalStateException("broker socket never appeared: " + sock);
        }

        private static RawHttp unixPost(Path sock, String path, byte[] body, String extraHeaders)
                throws IOException {
            try (SocketChannel ch = SocketChannel.open(StandardProtocolFamily.UNIX)) {
                ch.connect(UnixDomainSocketAddress.of(sock));
                String extra = extraHeaders == null ? "" : extraHeaders;
                String head = "POST " + path + " HTTP/1.1\r\n"
                        + "Host: localhost\r\n"
                        + "Authorization: Bearer t\r\n"
                        + "Content-Type: application/json\r\n"
                        + extra
                        + "Content-Length: " + body.length + "\r\n"
                        + "Connection: close\r\n\r\n";
                ch.write(ByteBuffer.wrap(head.getBytes(StandardCharsets.UTF_8)));
                if (body.length > 0) {
                    ch.write(ByteBuffer.wrap(body));
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ByteBuffer buf = ByteBuffer.allocate(8192);
                int n;
                while ((n = ch.read(buf)) > 0) {
                    buf.flip();
                    byte[] chunk = new byte[n];
                    buf.get(chunk);
                    out.write(chunk);
                    buf.clear();
                }
                return RawHttp.parse(out.toByteArray());
            }
        }

        private static void drain(InputStream err) {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(err, StandardCharsets.UTF_8))) {
                while (r.readLine() != null) {
                    // drain so the broker never blocks on a full stderr pipe
                }
            } catch (IOException ignored) {
                // process exit
            }
        }
    }
}
