package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Pipe-frame contract: one-shot replies stay {@code {id,status,body_b64}} only; stream frames
 * carry {@code contentType/chunk_b64/seq/done}. Uses a piped fake Process — no container.
 */
class HelperPipeChannelStreamTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration ROUND_TRIP = Duration.ofSeconds(5);

    @Test
    void executeStream_andLlmStreamTrue_areStreaming_parseFailuresAreNot() {
        byte[] streamTrue = "{\"stream\":true}".getBytes(StandardCharsets.UTF_8);
        byte[] streamFalse = "{\"stream\":false}".getBytes(StandardCharsets.UTF_8);
        String sse = "text/event-stream";
        assertTrue(HelperPipeChannel.isStreamingRequest("execute/stream", new byte[0], null));
        assertTrue(HelperPipeChannel.isStreamingRequest("execute/stream?x=1", new byte[0], null));
        assertTrue(HelperPipeChannel.isStreamingRequest("llm/chat/completions", streamTrue, sse));
        assertTrue(HelperPipeChannel.isStreamingRequest("llm/chat/completions", streamTrue,
                "application/json, text/event-stream"));
        assertFalse(HelperPipeChannel.isStreamingRequest("llm/chat/completions", streamTrue, null));
        assertFalse(HelperPipeChannel.isStreamingRequest("llm/chat/completions", streamTrue, "application/json"));
        assertFalse(HelperPipeChannel.isStreamingRequest("llm/chat/completions", streamFalse, sse));
        assertFalse(HelperPipeChannel.isStreamingRequest("llm/chat/completions", "not-json".getBytes(), sse));
        assertFalse(HelperPipeChannel.isStreamingRequest("log", streamTrue, sse));
        assertFalse(HelperPipeChannel.isStreamingRequest("execute", streamTrue, sse));
    }

    @Test
    void oneShotRoute_writesOnlyIdStatusBodyB64() throws Exception {
        HelperRequestRouter router = new HelperRequestRouter(MAPPER,
                new AiModelProviderProxy(new AiModelProviderHolder(), MAPPER),
                null);
        String token = router.register(HelperJob.builder("job-1", "python").build());
        String bodyB64 = Base64.getEncoder().encodeToString("{\"msg\":\"x\"}".getBytes(StandardCharsets.UTF_8));

        assertTimeoutPreemptively(ROUND_TRIP, () -> {
            try (PipeProcess proc = new PipeProcess();
                 HelperPipeChannel channel = new HelperPipeChannel(proc, router);
                 BufferedReader replies = proc.replies()) {
                assertTrue(channel.alive());
                proc.sendRequest("{\"id\":\"r1\",\"method\":\"POST\",\"path\":\"log\",\"bearer\":\""
                        + token + "\",\"body_b64\":\"" + bodyB64 + "\"}");
                JsonNode frame = MAPPER.readTree(replies.readLine());
                assertEquals(Set.of("id", "status", "body_b64"), fieldNames(frame));
                assertEquals("r1", frame.get("id").asText());
                assertEquals(200, frame.get("status").asInt());
                assertFalse(frame.has("contentType"));
                assertFalse(frame.has("done"));
                assertFalse(frame.has("seq"));
                assertFalse(frame.has("chunk_b64"));
            }
        });
    }

    @Test
    void routeStreamOneShot_stillWritesLegacyThreeFieldFrame() throws Exception {
        HelperRequestRouter router = mock(HelperRequestRouter.class);
        doAnswer(inv -> {
            HelperStreamSink sink = inv.getArgument(5);
            sink.oneShot(503, "application/json", "{\"error\":\"x\"}".getBytes(StandardCharsets.UTF_8));
            return null;
        }).when(router).routeStream(any(), any(), any(), any(), any(), any());

        assertTimeoutPreemptively(ROUND_TRIP, () -> {
            try (PipeProcess proc = new PipeProcess();
                 HelperPipeChannel channel = new HelperPipeChannel(proc, router);
                 BufferedReader replies = proc.replies()) {
                assertTrue(channel.alive());
                proc.sendRequest("{\"id\":\"s1\",\"method\":\"POST\",\"path\":\"execute/stream\","
                        + "\"bearer\":\"t\",\"body_b64\":\"e30=\"}");
                JsonNode frame = MAPPER.readTree(replies.readLine());
                assertEquals(Set.of("id", "status", "body_b64"), fieldNames(frame));
                assertEquals("s1", frame.get("id").asText());
                assertEquals(503, frame.get("status").asInt());
                verify(router, never()).route(any(), any(), any(), any());
            }
        });
    }

    @Test
    void routeStreamChunk_writesSeqAndDoneFrames() throws Exception {
        HelperRequestRouter router = mock(HelperRequestRouter.class);
        doAnswer(inv -> {
            HelperStreamSink sink = inv.getArgument(5);
            sink.chunk(200, "text/event-stream", "aaa".getBytes(StandardCharsets.UTF_8), 0, false);
            sink.chunk(200, "text/event-stream", null, 1, true);
            return null;
        }).when(router).routeStream(any(), any(), any(), any(), any(), any());

        assertTimeoutPreemptively(ROUND_TRIP, () -> {
            try (PipeProcess proc = new PipeProcess();
                 HelperPipeChannel channel = new HelperPipeChannel(proc, router);
                 BufferedReader replies = proc.replies()) {
                assertTrue(channel.alive());
                proc.sendRequest("{\"id\":\"c1\",\"method\":\"POST\",\"path\":\"llm/chat\","
                        + "\"bearer\":\"t\",\"accept\":\"text/event-stream\",\"body_b64\":\""
                        + Base64.getEncoder().encodeToString("{\"stream\":true}".getBytes(StandardCharsets.UTF_8))
                        + "\"}");
                JsonNode first = MAPPER.readTree(replies.readLine());
                JsonNode last = MAPPER.readTree(replies.readLine());
                assertEquals("c1", first.get("id").asText());
                assertEquals(200, first.get("status").asInt());
                assertEquals("text/event-stream", first.get("contentType").asText());
                assertEquals(0, first.get("seq").asInt());
                assertFalse(first.get("done").asBoolean());
                assertEquals("aaa", new String(Base64.getDecoder().decode(first.get("chunk_b64").asText()),
                        StandardCharsets.UTF_8));
                assertEquals(1, last.get("seq").asInt());
                assertTrue(last.get("done").asBoolean());
                assertEquals("", last.get("chunk_b64").asText());
                verify(router, never()).route(any(), any(), any(), any());
            }
        });
    }

    @Test
    void llmStreamTrueWithoutAccept_usesBufferedRoute() throws Exception {
        HelperRequestRouter router = mock(HelperRequestRouter.class);
        doAnswer(inv -> new HelperRequestRouter.RoutedResponse(200,
                "{\"ok\":true}".getBytes(StandardCharsets.UTF_8), "application/json"))
                .when(router).route(any(), any(), any(), any());

        assertTimeoutPreemptively(ROUND_TRIP, () -> {
            try (PipeProcess proc = new PipeProcess();
                 HelperPipeChannel channel = new HelperPipeChannel(proc, router);
                 BufferedReader replies = proc.replies()) {
                assertTrue(channel.alive());
                proc.sendRequest("{\"id\":\"b1\",\"method\":\"POST\",\"path\":\"llm/chat\","
                        + "\"bearer\":\"t\",\"body_b64\":\""
                        + Base64.getEncoder().encodeToString("{\"stream\":true}".getBytes(StandardCharsets.UTF_8))
                        + "\"}");
                JsonNode frame = MAPPER.readTree(replies.readLine());
                assertEquals(Set.of("id", "status", "body_b64"), fieldNames(frame));
                verify(router, never()).routeStream(any(), any(), any(), any(), any(), any());
                verify(router).route(any(), any(), any(), any());
            }
        });
    }

    private static Set<String> fieldNames(JsonNode node) {
        Set<String> names = new HashSet<>();
        Iterator<String> it = node.fieldNames();
        while (it.hasNext()) {
            names.add(it.next());
        }
        return names;
    }

    /**
     * Process whose stdout is request frames we write and whose stdin is reply frames we read.
     * Matches {@link HelperPipeChannel}: it reads {@link Process#getInputStream()} and writes
     * {@link Process#getOutputStream()}.
     */
    private static final class PipeProcess extends Process implements AutoCloseable {
        private final PipedOutputStream requestSrc;
        private final PipedInputStream stdout;
        private final PipedOutputStream stdin;
        private final PipedInputStream replySink;
        private final InputStream stderr = new ByteArrayInputStream(new byte[0]);
        private volatile boolean alive = true;

        private PipeProcess() throws IOException {
            this.requestSrc = new PipedOutputStream();
            this.stdout = new PipedInputStream(requestSrc, 1 << 16);
            this.replySink = new PipedInputStream(1 << 16);
            this.stdin = new PipedOutputStream(replySink);
        }

        void sendRequest(String jsonLine) throws IOException {
            requestSrc.write(jsonLine.getBytes(StandardCharsets.UTF_8));
            requestSrc.write('\n');
            requestSrc.flush();
        }

        BufferedReader replies() {
            return new BufferedReader(new InputStreamReader(replySink, StandardCharsets.UTF_8));
        }

        @Override
        public OutputStream getOutputStream() {
            return stdin;
        }

        @Override
        public InputStream getInputStream() {
            return stdout;
        }

        @Override
        public InputStream getErrorStream() {
            return stderr;
        }

        @Override
        public int waitFor() {
            return 0;
        }

        @Override
        public boolean waitFor(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public int exitValue() {
            if (alive) {
                throw new IllegalThreadStateException("alive");
            }
            return 0;
        }

        @Override
        public void destroy() {
            alive = false;
            try {
                requestSrc.close();
            } catch (IOException ignored) {
                // closing for pump EOF
            }
            try {
                stdin.close();
            } catch (IOException ignored) {
                // channel already closed the writer
            }
        }

        @Override
        public boolean isAlive() {
            return alive;
        }

        @Override
        public void close() {
            destroy();
        }
    }
}
