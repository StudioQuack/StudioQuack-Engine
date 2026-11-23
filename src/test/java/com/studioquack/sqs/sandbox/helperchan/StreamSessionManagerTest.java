package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.engine.delegate.DuckbotStreamSink;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stream-through: tagged frames open one NDJSON/SSE session against a fake library instance.
 */
class StreamSessionManagerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration WAIT = Duration.ofSeconds(8);

    private FakeLibrary library;
    private HelperRequestRouter helper;
    private SubProcessCompletionRegistry subPi;
    private StreamSessionManager sessions;
    private StreamEdgeRouter router;
    private final List<ConversationStreamEvent> duckbot = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        library = new FakeLibrary();
        helper = new HelperRequestRouter(MAPPER,
                new AiModelProviderProxy(new AiModelProviderHolder(), MAPPER),
                null);
        subPi = new SubProcessCompletionRegistry(null, null, null, null, null, null, false);
        StreamInstanceLocator locator = (env, node) -> Optional.of(
                new StreamInstanceLocator.Location("127.0.0.1", library.port(), "/invoke",
                        "sq-local/sq-builtin-node-library:1.0.0"));
        sessions = new StreamSessionManager(helper, locator, subPi);
        DuckbotStreamSink sink = new DuckbotStreamSink(duckbot::add);
        router = new StreamEdgeRouter(sink, null, subPi, sessions);
        sessions.wireRouterForTests(router);
        helper.wireStreamingForTests(null, router);
    }

    @AfterEach
    void tearDown() {
        if (sessions != null) {
            sessions.shutdown();
        }
        if (library != null) {
            library.close();
        }
    }

    @Test
    void taggedFrameOpensOneSessionForwardsInOrderAndClosesOnRevoke() {
        HelperJob job = job(graph(false));
        assertTrue(router.route(job, "thoughts", "thinking", "one"));
        assertTrue(router.route(job, "thoughts", "thinking", "two"));
        assertTrue(router.route(job, "thoughts", "thinking", "three"));
        waitUntil(() -> library.lines.stream().anyMatch(l -> l.contains("\"type\":\"frame\"")));
        router.onJobFinished(job);
        waitUntil(() -> library.lines.stream().anyMatch(l -> l.contains("\"type\":\"close\"")));

        List<String> types = library.types();
        assertEquals("open", types.get(0));
        assertTrue(types.contains("frame"));
        assertEquals("close", types.get(types.size() - 1));
        List<String> deltas = library.frameDeltas();
        assertEquals(List.of("one", "two", "three"), deltas);
        assertEquals(1, library.openCount);
        waitUntil(() -> sessions.openSessions() == 0);
    }

    @Test
    void targetSsePartialsReenterRouterAsTheTarget() throws Exception {
        library.emitPartialOnOpen = true;
        library.partialPort = "out";
        library.partialDelta = "delta-from-store";
        HelperJob job = job(graph(true));
        assertTrue(router.route(job, "thoughts", "thinking", "seed"));
        assertTrue(library.opened.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        waitUntil(() -> duckbot.stream().anyMatch(e -> "stream-delta".equals(e.event())
                && "delta-from-store".equals(e.delta())));
        assertTrue(duckbot.stream().anyMatch(e -> "node-store".equals(e.nodeId())));
        router.onJobFinished(job);
    }

    @Test
    void cycleGuardTerminatesAtoBtoA() {
        library.emitPartialOnOpen = true;
        library.partialPort = "out";
        HelperJob job = job(cycleGraph());
        assertTrue(router.route(job, "thoughts", "thinking", "seed"));
        waitUntil(() -> library.openCount >= 1);
        sleep(200);
        assertEquals(1, library.openCount);
        router.onJobFinished(job);
    }

    @Test
    void earlyCompleteStillDeliversCloseLineToTarget() {
        // Regression: the reader used to tear the session down on the target's `complete`
        // event, racing the writer's close line. The target's awaitInputComplete() then
        // never returned and the assembled input (the agent's final answer) was lost.
        sessions.holdReaderUntilWriterClosedForTests = true;
        library.emitPartialOnOpen = true;
        library.partialPort = "out";
        library.partialDelta = "delta-from-store";
        HelperJob job = job(graph(false));
        assertTrue(router.route(job, "thoughts", "thinking", "seed"));
        assertTrue(awaitQuietly(library.opened, WAIT));
        router.onJobFinished(job);
        waitUntil(() -> library.lines.stream().anyMatch(l -> l.contains("\"type\":\"close\"")));
        waitUntil(() -> sessions.openSessions() == 0);
    }

    @Test
    void noLiveInstanceDropsWithoutCrash() {
        StreamSessionManager empty = new StreamSessionManager(helper, (env, node) -> Optional.empty(),
                subPi);
        StreamEdgeRouter r = new StreamEdgeRouter(new DuckbotStreamSink(e -> { }), null, subPi, empty);
        empty.wireRouterForTests(r);
        HelperJob job = job(graph(false));
        assertFalse(r.route(job, "thoughts", "thinking", "x"));
        assertEquals(0, empty.openSessions());
        r.onJobFinished(job);
        empty.shutdown();
    }

    @Test
    void queueCapDropOldestKeepsSessionAlive() throws Exception {
        // The production cap (HelperStreamLimits.MAX_QUEUED_FRAMES, 262k) is a catastrophic guard:
        // once the writer is released every retained frame is flushed to the target before the
        // close line, and 262k frames cannot drain inside the test budget. The behaviour under
        // test — drop-oldest keeps the session alive and the close line still lands — does not
        // depend on the cap's magnitude, so the test runs it at a small cap.
        sessions.queueCapForTests = 1024;
        library.pauseAfterOpen = new CountDownLatch(1);
        HelperJob job = job(graph(false));
        assertTrue(router.route(job, "thoughts", "thinking", "first"));
        assertTrue(library.opened.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        sessions.holdWriterForTests = true;
        for (int i = 0; i < sessions.queueCapForTests + 50; i++) {
            router.route(job, "thoughts", "thinking", "f" + i);
        }
        waitUntil(() -> sessions.droppedOldestTotal() > 0);
        assertTrue(sessions.openSessions() >= 1);
        sessions.holdWriterForTests = false;
        library.pauseAfterOpen.countDown();
        router.onJobFinished(job);
        waitUntil(() -> library.lines.stream().anyMatch(l -> l.contains("\"type\":\"close\"")));
    }

    private static void waitUntil(Check check) {
        assertTimeoutPreemptively(WAIT, () -> {
            while (!check.ok()) {
                Thread.sleep(20);
            }
        });
    }

    /** Latch await that never throws — keeps the regression test body linear. */
    private static boolean awaitQuietly(CountDownLatch latch, Duration timeout) {
        try {
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }

    private static HelperJob job(Map<String, Object> ec) {
        return HelperJob.builder("src-job", "python").executionContext(ec).build();
    }

    /** Agent → streamThrough store; optional store OUTPUT → duckbot end. */
    private static Map<String, Object> graph(boolean storeToEnd) {
        Map<String, Object> ai = Map.of("conversationId", "conv-1", "agentMessageKey", "amk-1");
        Map<String, Object> thoughtsConn = Map.of("portValue", "thoughts",
                "connectionDetailList", List.of(Map.of("nodeId", "node-store",
                        "portDto", Map.of("value", "msg"))));
        Map<String, Object> currentNode = new LinkedHashMap<>();
        currentNode.put("id", "node-a");
        currentNode.put("connectionList", List.of(thoughtsConn));

        Map<String, Object> storeData = new LinkedHashMap<>();
        storeData.put("id", "tpl-store");
        storeData.put("streamThrough", true);
        storeData.put("category", "data");
        storeData.put("nodeType", "");
        storeData.put("executionRequirementsDto", Map.of("ociImageRef", "sq-local/lib:1"));
        storeData.put("ports", List.of(
                Map.of("type", "INPUT", "value", "msg"),
                Map.of("type", "OUTPUT", "value", "out")));
        Map<String, Object> storeNode = new LinkedHashMap<>();
        storeNode.put("id", "node-store");
        storeNode.put("data", storeData);
        if (storeToEnd) {
            storeNode.put("connectionList", List.of(Map.of("portValue", "out",
                    "connectionDetailList", List.of(Map.of("nodeId", "node-end",
                            "portDto", Map.of("value", "msg"))))));
        }

        Map<String, Object> agentNode = Map.of("id", "node-a", "data",
                Map.of("category", "ai", "id", "tpl-agent"));
        Map<String, Object> endNode = Map.of("id", "node-end", "data",
                Map.of("category", "conversion", "nodeType", "end"));

        List<Map<String, Object>> nodes = new ArrayList<>();
        nodes.add(agentNode);
        nodes.add(storeNode);
        nodes.add(endNode);

        Map<String, Object> edge = Map.of("source", "node-a", "target", "node-store",
                "data", Map.of("edgeTransforms", List.of()));
        List<Map<String, Object>> edges = new ArrayList<>();
        edges.add(edge);
        if (storeToEnd) {
            edges.add(Map.of("source", "node-store", "target", "node-end",
                    "data", Map.of("edgeTransforms", List.of())));
        }

        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("workflowExecutionId", "wfe-1");
        ec.put("currentNodeId", "node-a");
        ec.put("envId", "env-1");
        ec.put("agenticInfo", ai);
        ec.put("currentNode", currentNode);
        ec.put("nodes", nodes);
        ec.put("edges", edges);
        return ec;
    }

    /** A.thoughts → B.in ; B.out → A.in. Both streamThrough. */
    private static Map<String, Object> cycleGraph() {
        Map<String, Object> dataA = new LinkedHashMap<>();
        dataA.put("id", "tpl-a");
        dataA.put("streamThrough", true);
        dataA.put("executionRequirementsDto", Map.of("ociImageRef", "sq-local/lib:1"));
        dataA.put("ports", List.of(Map.of("type", "INPUT", "value", "in"),
                Map.of("type", "OUTPUT", "value", "thoughts")));
        Map<String, Object> nodeA = new LinkedHashMap<>();
        nodeA.put("id", "node-a");
        nodeA.put("data", dataA);
        nodeA.put("connectionList", List.of(Map.of("portValue", "thoughts",
                "connectionDetailList", List.of(Map.of("nodeId", "node-b",
                        "portDto", Map.of("value", "in"))))));

        Map<String, Object> dataB = new LinkedHashMap<>();
        dataB.put("id", "tpl-b");
        dataB.put("streamThrough", true);
        dataB.put("executionRequirementsDto", Map.of("ociImageRef", "sq-local/lib:1"));
        dataB.put("ports", List.of(Map.of("type", "INPUT", "value", "in"),
                Map.of("type", "OUTPUT", "value", "out")));
        Map<String, Object> nodeB = new LinkedHashMap<>();
        nodeB.put("id", "node-b");
        nodeB.put("data", dataB);
        nodeB.put("connectionList", List.of(Map.of("portValue", "out",
                "connectionDetailList", List.of(Map.of("nodeId", "node-a",
                        "portDto", Map.of("value", "in"))))));

        Map<String, Object> currentNode = new LinkedHashMap<>();
        currentNode.put("id", "node-a");
        currentNode.put("connectionList", nodeA.get("connectionList"));
        currentNode.put("data", dataA);

        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("workflowExecutionId", "wfe-1");
        ec.put("currentNodeId", "node-a");
        ec.put("envId", "env-1");
        ec.put("agenticInfo", Map.of("conversationId", "conv-1", "agentMessageKey", "amk-1"));
        ec.put("currentNode", currentNode);
        ec.put("nodes", List.of(nodeA, nodeB));
        ec.put("edges", List.of(
                Map.of("source", "node-a", "target", "node-b", "data", Map.of("edgeTransforms", List.of())),
                Map.of("source", "node-b", "target", "node-a", "data", Map.of("edgeTransforms", List.of()))));
        return ec;
    }

    /** In-process stand-in for the node-library {@code POST /invoke/stream} (full-duplex). */
    private static final class FakeLibrary {
        private final ServerSocket server;
        private final Thread acceptor;
        final List<String> lines = new CopyOnWriteArrayList<>();
        final CountDownLatch opened = new CountDownLatch(1);
        volatile int openCount;
        volatile boolean emitPartialOnOpen;
        volatile String partialPort = "out";
        volatile String partialDelta = "p";
        volatile CountDownLatch pauseAfterOpen;

        FakeLibrary() throws Exception {
            server = new ServerSocket();
            server.bind(new InetSocketAddress("127.0.0.1", 0));
            acceptor = Thread.ofVirtual().unstarted(this::acceptLoop);
            acceptor.setName("fake-lib-accept");
            acceptor.start();
        }

        private void acceptLoop() {
            try {
                while (!server.isClosed()) {
                    Socket socket = server.accept();
                    Thread.ofVirtual().start(() -> handle(socket));
                }
            } catch (IOException ignored) {
                // closed
            }
        }

        private void handle(Socket socket) {
            CountDownLatch closed = new CountDownLatch(1);
            try {
                InputStream raw = new BufferedInputStream(socket.getInputStream());
                OutputStream out = socket.getOutputStream();
                socket.setReceiveBufferSize(2048);
                readHeaders(raw);
                out.write(("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                        + "Cache-Control: no-cache\r\nTransfer-Encoding: chunked\r\n\r\n")
                        .getBytes(StandardCharsets.UTF_8));
                out.flush();
                Thread reader = Thread.ofVirtual().start(() -> readNdjson(raw, closed));
                if (emitPartialOnOpen) {
                    opened.await(WAIT.toMillis(), TimeUnit.MILLISECONDS);
                    String data = "{\"channel\":\"thinking\",\"delta\":\"" + partialDelta
                            + "\",\"port\":\"" + partialPort + "\"}";
                    writeChunk(out, "event: partial\ndata: " + data + "\n\n");
                }
                closed.await(WAIT.toMillis(), TimeUnit.MILLISECONDS);
                writeChunk(out, "event: complete\ndata: {}\n\n");
                writeChunk(out, "event: done\ndata: {}\n\n");
                writeChunk(out, "");
                reader.join(WAIT.toMillis());
            } catch (Exception ignored) {
                closed.countDown();
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // teardown
                }
            }
        }

        private void readNdjson(InputStream raw, CountDownLatch closed) {
            try {
                InputStream body = new HttpChunkedInputStream(raw);
                ByteArrayOutputStream lineBuf = new ByteArrayOutputStream();
                int b;
                while ((b = body.read()) != -1) {
                    if (b == '\n') {
                        onLine(lineBuf.toString(StandardCharsets.UTF_8), closed);
                        lineBuf.reset();
                    } else if (b != '\r') {
                        lineBuf.write(b);
                    }
                }
                if (lineBuf.size() > 0) {
                    onLine(lineBuf.toString(StandardCharsets.UTF_8), closed);
                }
            } catch (Exception ignored) {
                // connection closed
            } finally {
                closed.countDown();
            }
        }

        private void onLine(String line, CountDownLatch closed) throws InterruptedException {
            if (line.isBlank()) {
                return;
            }
            lines.add(line);
            if (line.contains("\"type\":\"open\"")) {
                openCount++;
                opened.countDown();
                if (pauseAfterOpen != null) {
                    pauseAfterOpen.await(WAIT.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
            if (line.contains("\"type\":\"close\"")) {
                closed.countDown();
            }
        }

        private static void writeChunk(OutputStream out, String payload) throws IOException {
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            out.write(Integer.toHexString(bytes.length).getBytes(StandardCharsets.US_ASCII));
            out.write('\r');
            out.write('\n');
            if (bytes.length > 0) {
                out.write(bytes);
            }
            out.write('\r');
            out.write('\n');
            out.flush();
        }

        int port() {
            return server.getLocalPort();
        }

        List<String> types() {
            List<String> out = new ArrayList<>();
            for (String line : lines) {
                try {
                    Object t = MAPPER.readValue(line, Map.class).get("type");
                    if (t != null) {
                        out.add(String.valueOf(t));
                    }
                } catch (Exception ignored) {
                    // skip
                }
            }
            return out;
        }

        List<String> frameDeltas() {
            List<String> out = new ArrayList<>();
            for (String line : lines) {
                try {
                    Map<?, ?> m = MAPPER.readValue(line, Map.class);
                    if ("frame".equals(m.get("type")) && m.get("delta") != null) {
                        out.add(String.valueOf(m.get("delta")));
                    }
                } catch (Exception ignored) {
                    // skip
                }
            }
            return out;
        }

        void close() {
            try {
                server.close();
            } catch (IOException ignored) {
                // teardown
            }
        }
    }

    /** Reads HTTP/1.1 headers (CRLF lines) up to the blank line. */
    private static void readHeaders(InputStream in) throws IOException {
        int prev = 0;
        int n;
        int crlf = 0;
        while ((n = in.read()) != -1) {
            if (n == '\n' && prev == '\r') {
                crlf++;
                if (crlf == 2) {
                    return;
                }
            } else if (n != '\r') {
                crlf = 0;
            }
            prev = n;
        }
    }

    /**
     * Decodes HTTP/1.1 chunked bodies so the NDJSON reader sees raw lines. Identity
     * (non-chunked) streams also work: a missing size line is treated as raw bytes.
     */
    private static final class HttpChunkedInputStream extends InputStream {
        private final InputStream in;
        private int remaining = -1;
        private boolean done;

        private HttpChunkedInputStream(InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n <= 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (done) {
                return -1;
            }
            if (remaining <= 0) {
                if (!nextChunk()) {
                    done = true;
                    return -1;
                }
            }
            int n = in.read(b, off, Math.min(len, remaining));
            if (n < 0) {
                done = true;
                return -1;
            }
            remaining -= n;
            if (remaining == 0) {
                // swallow CRLF after the chunk
                int c1 = in.read();
                int c2 = in.read();
                if (c1 == '\r' && c2 == '\n') {
                    // ok
                }
            }
            return n;
        }

        private boolean nextChunk() throws IOException {
            String sizeLine = readAsciiLine(in);
            if (sizeLine == null) {
                return false;
            }
            int semi = sizeLine.indexOf(';');
            String hex = (semi < 0 ? sizeLine : sizeLine.substring(0, semi)).trim();
            int size;
            try {
                size = Integer.parseInt(hex, 16);
            } catch (NumberFormatException e) {
                return false;
            }
            if (size == 0) {
                readAsciiLine(in);
                return false;
            }
            remaining = size;
            return true;
        }

        private static String readAsciiLine(InputStream in) throws IOException {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            int prev = 0;
            int n;
            while ((n = in.read()) != -1) {
                if (n == '\n' && prev == '\r') {
                    byte[] raw = buf.toByteArray();
                    return new String(raw, 0, Math.max(0, raw.length - 1), StandardCharsets.US_ASCII);
                }
                buf.write(n);
                prev = n;
            }
            return buf.size() == 0 ? null : buf.toString(StandardCharsets.US_ASCII);
        }
    }
}
