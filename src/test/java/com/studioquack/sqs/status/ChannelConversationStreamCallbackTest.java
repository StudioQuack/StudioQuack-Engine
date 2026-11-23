package com.studioquack.sqs.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.MockBackend;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link ChannelConversationStreamCallback} POSTs camelCase stream frames fire-and-forget on
 * {@link AsyncSandboxLogDispatcher}. Failures never throw. Delta text is in the body, never in logs
 * (asserted by contract in the impl; this test pins URL + body + non-throwing I/O).
 */
class ChannelConversationStreamCallbackTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-1";
    private static final String RUN_ID = "wx-stream-1";
    private static final String ENV_BASE = "/be/v1/sqvm/environments/" + ENV_ID;
    private static final String STREAM_PATH = ENV_BASE + "/runs/" + RUN_ID + "/conversation-stream";

    private static SqsChannelProperties props() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId(ENV_ID);
        props.setBearer("sqvm_bearer_stream");
        return props;
    }

    private static ChannelConversationStreamCallback channel(String backendBaseUrl,
                                                             AsyncSandboxLogDispatcher dispatcher) {
        ChannelConversationStreamCallback cb = new ChannelConversationStreamCallback(props(), backendBaseUrl,
                new ChannelHttpClients(null), MAPPER, new OutboundBaseHolder(), dispatcher);
        // These tests pin the FALLBACK contract (per-batch POSTs); the /live streaming path
        // is covered by ConversationStreamUploaderTest and the live-path test below.
        cb.disableLiveUploadForTests();
        return cb;
    }

    private static ConversationStreamCallback.ConversationStreamEvent deltaFrame() {
        return new ConversationStreamCallback.ConversationStreamEvent(
                RUN_ID, "conv-1", "msg-1", "node-1",
                "stream-delta", "answer", "hello token", 3, 2, null);
    }

    @Test
    @DisplayName("null / blank ids skip HTTP and do not throw")
    void emit_blank_skipsHttp() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(STREAM_PATH, 200, "{\"ok\":true}");
            ChannelConversationStreamCallback cb = channel(be.baseUrl(), dispatcher);
            assertThatCode(() -> cb.emit(null)).doesNotThrowAnyException();
            assertThatCode(() -> cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    "", "conv-1", "msg-1", "node-1", "stream-start", "thinking", null, 0, null, null)))
                    .doesNotThrowAnyException();
            assertThatCode(() -> cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    RUN_ID, "  ", "msg-1", "node-1", "stream-start", "thinking", null, 0, null, null)))
                    .doesNotThrowAnyException();
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(2))).isTrue();
            assertThat(be.countMatching("/conversation-stream")).isZero();
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("POSTs enrolment-bearer JSON; body omits null delta/msgOrder")
    void emit_postsContractBody() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(STREAM_PATH, 200, "{\"ok\":true}");
            ChannelConversationStreamCallback cb = channel(be.baseUrl(), dispatcher);

            cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    RUN_ID, "conv-1", "msg-1", "node-1",
                    "stream-start", "thinking", null, 0, null, null));
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();

            MockBackend.Recorded rec = be.firstMatching("/conversation-stream");
            assertThat(rec).isNotNull();
            assertThat(rec.method()).isEqualTo("POST");
            assertThat(rec.path()).isEqualTo(STREAM_PATH);
            assertThat(rec.headers().get("authorization")).isEqualTo("Bearer sqvm_bearer_stream");
            JsonNode json = MAPPER.readTree(rec.body());
            assertThat(json.get("conversationId").asText()).isEqualTo("conv-1");
            assertThat(json.get("agentMessageKey").asText()).isEqualTo("msg-1");
            assertThat(json.get("nodeId").asText()).isEqualTo("node-1");
            assertThat(json.get("event").asText()).isEqualTo("stream-start");
            assertThat(json.get("channel").asText()).isEqualTo("thinking");
            assertThat(json.get("seq").asInt()).isEqualTo(0);
            assertThat(json.has("delta")).isFalse();
            assertThat(json.has("msgOrder")).isFalse();

            cb.emit(deltaFrame());
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();
            MockBackend.Recorded delta = be.requests().get(be.requests().size() - 1);
            JsonNode deltaJson = MAPPER.readTree(delta.body());
            assertThat(deltaJson.get("delta").asText()).isEqualTo("hello token");
            assertThat(deltaJson.get("seq").asInt()).isEqualTo(3);
            assertThat(deltaJson.get("msgOrder").asInt()).isEqualTo(2);
            assertThat(deltaJson.get("event").asText()).isEqualTo("stream-delta");
            assertThat(deltaJson.get("channel").asText()).isEqualTo("answer");
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("blank nodeId still POSTs (lane key '_')")
    void emit_blankNodeId_stillPosts() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(STREAM_PATH, 200, "{\"ok\":true}");
            ChannelConversationStreamCallback cb = channel(be.baseUrl(), dispatcher);
            cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    RUN_ID, "conv-1", "msg-1", " ",
                    "stream-end", "answer", null, 9, 1, null));
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();
            assertThat(be.countMatching("/conversation-stream")).isEqualTo(1);
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("emit returns before a delayed POST completes")
    void emit_returnsBeforeDelayedPost() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stub(STREAM_PATH, req -> {
                Thread.sleep(1_000);
                return MockBackend.Response.json(200, "{\"ok\":true}");
            });
            ChannelConversationStreamCallback cb = channel(be.baseUrl(), dispatcher);
            long started = System.nanoTime();
            cb.emit(deltaFrame());
            long elapsedMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
            assertThat(elapsedMs)
                    .as("emit() must only enqueue; the delayed POST stays off the caller")
                    .isLessThan(200);
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();
            assertThat(be.countMatching("/conversation-stream")).isEqualTo(1);
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("failed POST does not throw")
    void emit_failedPost_doesNotThrow() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(STREAM_PATH, 500, "{\"error\":\"nope\"}");
            ChannelConversationStreamCallback cb = channel(be.baseUrl(), dispatcher);
            assertThatCode(() -> cb.emit(deltaFrame())).doesNotThrowAnyException();
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();
            assertThat(be.countMatching("/conversation-stream")).isEqualTo(1);
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("function-port child run POSTs under the parent run the BE registered")
    void emit_subPiChild_postsUnderTopLevelRun() throws Exception {
        String childRun = "child-subpi";
        SubProcessCompletionRegistry registry = mock(SubProcessCompletionRegistry.class);
        when(registry.resolveReportableRunId(anyString())).thenAnswer(inv ->
                childRun.equals(inv.getArgument(0)) ? RUN_ID : inv.getArgument(0));
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(STREAM_PATH, 200, "{\"ok\":true}");
            ChannelConversationStreamCallback cb = channel(be.baseUrl(), dispatcher);
            cb.wireSubProcessRegistryForTests(new ObjectProvider<>() {
                @Override
                public SubProcessCompletionRegistry getObject() {
                    return registry;
                }

                @Override
                public SubProcessCompletionRegistry getIfAvailable() {
                    return registry;
                }
            });
            cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    childRun, "conv-1", "msg-1", "node-1",
                    "stream-delta", "thinking", "token", 1, null, null));
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();
            assertThat(be.requests())
                    .as("thinking frames from a Duckbot-end sub-PI must hit the parent run")
                    .anySatisfy(r -> assertThat(r.path()).isEqualTo(STREAM_PATH));
            assertThat(be.requests())
                    .noneSatisfy(r -> assertThat(r.path()).contains(childRun));
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("a token burst during a slow POST coalesces into one merged frame per round-trip")
    void emit_burstDuringSlowPost_coalesces() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            // Every POST takes 300ms — per-token POSTs would serialize the whole burst;
            // the coalescer must instead merge everything buffered during each round-trip.
            be.stub(STREAM_PATH, req -> {
                Thread.sleep(300);
                return MockBackend.Response.json(200, "{\"ok\":true}");
            });
            ChannelConversationStreamCallback cb = channel(be.baseUrl(), dispatcher);

            cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    RUN_ID, "conv-1", "msg-1", "node-1", "stream-start", "thinking", null, 0, null, null));
            StringBuilder expected = new StringBuilder();
            for (int i = 1; i <= 40; i++) {
                String token = "t" + i + " ";
                expected.append(token);
                cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                        RUN_ID, "conv-1", "msg-1", "node-1", "stream-delta", "thinking", token, i, null, null));
            }
            cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    RUN_ID, "conv-1", "msg-1", "node-1", "stream-end", "thinking", null, 41, null, null));

            assertThat(cb.awaitStreamIdle(Duration.ofSeconds(10))).isTrue();

            List<MockBackend.Recorded> posts = be.requests().stream()
                    .filter(r -> r.path().equals(STREAM_PATH))
                    .toList();
            assertThat(posts.size())
                    .as("40 tokens over a 300ms round-trip must batch, not serialize one POST per token")
                    .isLessThan(10);
            StringBuilder painted = new StringBuilder();
            int lastSeq = -1;
            for (MockBackend.Recorded rec : posts) {
                JsonNode json = MAPPER.readTree(rec.body());
                assertThat(json.get("seq").asInt())
                        .as("seq stays monotonic across merged batches")
                        .isGreaterThan(lastSeq);
                lastSeq = json.get("seq").asInt();
                if (json.has("delta")) {
                    painted.append(json.get("delta").asText());
                }
            }
            assertThat(painted.toString()).isEqualTo(expected.toString());
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("live path: one NDJSON upload carries the whole turn, flushed per frame")
    void emit_livePath_streamsNdjson() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(STREAM_PATH + "/live", 200, "{\"ok\":true}");
            ChannelConversationStreamCallback cb = new ChannelConversationStreamCallback(
                    props(), be.baseUrl(), new ChannelHttpClients(null), MAPPER,
                    new OutboundBaseHolder(), dispatcher);

            cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    RUN_ID, "conv-1", "msg-1", "node-1", "stream-start", "thinking", null, 0, null, null));
            cb.emit(deltaFrame());
            cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    RUN_ID, "conv-1", "msg-1", "node-1", "stream-end", "answer", null, 4, 2, null));

            assertThat(cb.awaitStreamIdle(Duration.ofSeconds(5))).isTrue();
            // MockBackend records after the body closes; give the recording a beat to land.
            MockBackend.Recorded live = null;
            for (int i = 0; i < 100 && live == null; i++) {
                live = be.firstMatching("/conversation-stream/live");
                if (live == null) {
                    Thread.sleep(20);
                }
            }
            assertThat(live).isNotNull();
            assertThat(live.method()).isEqualTo("POST");
            assertThat(live.headers().get("authorization")).isEqualTo("Bearer sqvm_bearer_stream");
            String[] lines = live.body().split("\n");
            assertThat(lines).hasSize(3);
            JsonNode first = MAPPER.readTree(lines[0]);
            assertThat(first.get("event").asText()).isEqualTo("stream-start");
            JsonNode second = MAPPER.readTree(lines[1]);
            assertThat(second.get("event").asText()).isEqualTo("stream-delta");
            assertThat(second.get("delta").asText()).isEqualTo("hello token");
            assertThat(second.get("seq").asInt()).isEqualTo(3);
            JsonNode third = MAPPER.readTree(lines[2]);
            assertThat(third.get("event").asText()).isEqualTo("stream-end");
            // The per-batch fallback path stays silent while /live works.
            assertThat(be.countMatching("/conversation-stream")).isZero();
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("live path: 404 from an older BE falls back to per-batch POSTs")
    void emit_livePath_404FallsBack() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            // No /live stub → MockBackend answers 404 once the upload body closes.
            be.stubJson(STREAM_PATH, 200, "{\"ok\":true}");
            ChannelConversationStreamCallback cb = new ChannelConversationStreamCallback(
                    props(), be.baseUrl(), new ChannelHttpClients(null), MAPPER,
                    new OutboundBaseHolder(), dispatcher);

            // Turn 1 closes the upload (stream-end) so the 404 lands and flips the fallback.
            cb.emit(new ConversationStreamCallback.ConversationStreamEvent(
                    RUN_ID, "conv-1", "msg-1", "node-1", "stream-end", "answer", null, 1, null, null));
            assertThat(cb.awaitStreamIdle(Duration.ofSeconds(5))).isTrue();
            // Wait for the 404 round-trip to actually park the live path (deterministic).
            for (int i = 0; i < 250 && !cb.liveUploadDisabled(); i++) {
                Thread.sleep(20);
            }
            assertThat(cb.liveUploadDisabled()).isTrue();

            // Turn 2 must ride the coalescing per-batch POST path.
            cb.emit(deltaFrame());
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(5))).isTrue();
            MockBackend.Recorded rec = be.firstMatching("/conversation-stream");
            assertThat(rec).isNotNull();
            assertThat(rec.path()).isEqualTo(STREAM_PATH);
            JsonNode json = MAPPER.readTree(rec.body());
            assertThat(json.get("delta").asText()).isEqualTo("hello token");
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("blank backend base URL skips HTTP")
    void emit_blankBackend_skipsHttp() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(STREAM_PATH, 200, "{\"ok\":true}");
            ChannelConversationStreamCallback cb = channel("  ", dispatcher);
            cb.emit(deltaFrame());
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(2))).isTrue();
            assertThat(be.countMatching("/conversation-stream")).isZero();
        } finally {
            dispatcher.shutdown();
        }
    }
}
