package com.studioquack.sqs.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ConversationStreamUploader} contract: one chunked NDJSON POST per (run, lane),
 * every frame flushed the moment it is produced (no batching delay), FIFO order, bounded
 * lane lifecycle, and automatic fallback to the per-batch POST path when the control plane
 * predates {@code /live}.
 */
class ConversationStreamUploaderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String RUN_ID = "wx-live-1";

    private HttpServer server;
    private ConversationStreamUploader uploader;

    @AfterEach
    void tearDown() {
        if (uploader != null) {
            uploader.shutdown();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    /** A /live endpoint that reads the streaming body line by line as it arrives. */
    private static final class LiveEndpoint {
        private final List<String> lines = new CopyOnWriteArrayList<>();
        private final List<String> faults = new CopyOnWriteArrayList<>();
        private final AtomicInteger requests = new AtomicInteger();
        private final CountDownLatch firstLine = new CountDownLatch(1);
        private final CountDownLatch endLine = new CountDownLatch(1);
        private final int status;

        private LiveEndpoint(int status) {
            this.status = status;
        }

        private void handle(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException {
            requests.incrementAndGet();
            try {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                        firstLine.countDown();
                        if (line.contains("stream-end")) {
                            endLine.countDown();
                        }
                    }
                }
            } catch (java.io.IOException readFailure) {
                faults.add(readFailure.toString());
                throw readFailure;
            }
            byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        }
    }

    private LiveEndpoint startServer(int status) throws Exception {
        LiveEndpoint endpoint = new LiveEndpoint(status);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/live", endpoint::handle);
        server.start();
        return endpoint;
    }

    private ConversationStreamUploader newUploader(LiveEndpoint endpoint,
                                                   List<ConversationStreamEvent> fallback,
                                                   long idleMs, long maxAgeMs) {
        int port = server.getAddress().getPort();
        return new ConversationStreamUploader(
                runId -> URI.create("http://127.0.0.1:" + port + "/live"),
                () -> "test-bearer",
                () -> null,
                null,
                MAPPER,
                (lane, frame) -> fallback.add(frame),
                idleMs, maxAgeMs);
    }

    private static ConversationStreamEvent frame(String event, String channel, String delta, int seq) {
        return new ConversationStreamEvent(RUN_ID, "conv-1", "msg-1", "node-1",
                event, channel, delta, seq, null, null);
    }

    @Test
    void framesFlushImmediatelyWithoutWaitingForTurnEnd() throws Exception {
        LiveEndpoint endpoint = startServer(200);
        List<ConversationStreamEvent> fallback = new CopyOnWriteArrayList<>();
        uploader = newUploader(endpoint, fallback,
                ConversationStreamUploader.LANE_IDLE_CLOSE_MS, ConversationStreamUploader.LANE_MAX_AGE_MS);

        // No stream-end: the turn is still open. The delta must ALREADY be on the wire —
        // this is the no-buffering property the Duckbot screen depends on.
        uploader.offer("node-1", frame("stream-start", "thinking", null, 0));
        uploader.offer("node-1", frame("stream-delta", "answer", "token-1", 1));

        assertThat(endpoint.firstLine.await(5, TimeUnit.SECONDS))
                .as("first frame reaches the BE before the turn ends (requests=%s lines=%s faults=%s)",
                        endpoint.requests.get(), endpoint.lines, endpoint.faults)
                .isTrue();
        assertThat(endpoint.lines).anySatisfy(line -> assertThat(line).contains("token-1"));
        assertThat(fallback).isEmpty();

        uploader.offer("node-1", frame("stream-end", "answer", null, 2));
        assertThat(endpoint.endLine.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(uploader.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(endpoint.requests).hasValue(1);
        assertThat(endpoint.lines).hasSize(3);
    }

    @Test
    void laneKeepsFifoOrderAcrossManyFrames() throws Exception {
        LiveEndpoint endpoint = startServer(200);
        List<ConversationStreamEvent> fallback = new CopyOnWriteArrayList<>();
        uploader = newUploader(endpoint, fallback,
                ConversationStreamUploader.LANE_IDLE_CLOSE_MS, ConversationStreamUploader.LANE_MAX_AGE_MS);

        uploader.offer("node-1", frame("stream-start", "answer", null, 0));
        StringBuilder expected = new StringBuilder();
        for (int i = 1; i <= 50; i++) {
            expected.append("t").append(i).append(" ");
            uploader.offer("node-1", frame("stream-delta", "answer", "t" + i + " ", i));
        }
        uploader.offer("node-1", frame("stream-end", "answer", null, 51));

        assertThat(endpoint.endLine.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(uploader.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(endpoint.requests).hasValue(1);

        StringBuilder painted = new StringBuilder();
        int lastSeq = -1;
        for (String line : endpoint.lines) {
            var json = MAPPER.readTree(line);
            assertThat(json.get("seq").asInt()).isGreaterThan(lastSeq);
            lastSeq = json.get("seq").asInt();
            if (json.has("delta")) {
                painted.append(json.get("delta").asText());
            }
        }
        assertThat(painted.toString()).isEqualTo(expected.toString());
        assertThat(fallback).isEmpty();
    }

    @Test
    void notFoundParksLivePathAndLaterFramesFallBack() throws Exception {
        LiveEndpoint endpoint = startServer(404);
        List<ConversationStreamEvent> fallback = new CopyOnWriteArrayList<>();
        uploader = newUploader(endpoint, fallback,
                ConversationStreamUploader.LANE_IDLE_CLOSE_MS, ConversationStreamUploader.LANE_MAX_AGE_MS);

        uploader.offer("node-1", frame("stream-end", "answer", null, 1));
        for (int i = 0; i < 250 && !uploader.isLiveDisabled(); i++) {
            Thread.sleep(20);
        }
        assertThat(uploader.isLiveDisabled()).isTrue();

        uploader.offer("node-1", frame("stream-delta", "answer", "later", 2));
        assertThat(fallback).singleElement().satisfies(f -> assertThat(f.delta()).isEqualTo("later"));
    }

    @Test
    void idleLaneClosesAndNextFrameReopensFreshConnection() throws Exception {
        LiveEndpoint endpoint = startServer(200);
        List<ConversationStreamEvent> fallback = new CopyOnWriteArrayList<>();
        uploader = newUploader(endpoint, fallback, 300L,
                ConversationStreamUploader.LANE_MAX_AGE_MS);

        uploader.offer("node-1", frame("stream-delta", "answer", "first", 1));
        assertThat(endpoint.firstLine.await(5, TimeUnit.SECONDS)).isTrue();
        // No stream-end: the lane must idle-close by itself.
        assertThat(uploader.awaitIdle(Duration.ofSeconds(5))).isTrue();

        uploader.offer("node-1", frame("stream-delta", "answer", "second", 2));
        for (int i = 0; i < 250 && endpoint.requests.get() < 2; i++) {
            Thread.sleep(20);
        }
        assertThat(endpoint.requests).hasValue(2);
        assertThat(uploader.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(endpoint.lines).anySatisfy(line -> assertThat(line).contains("second"));
    }
}
