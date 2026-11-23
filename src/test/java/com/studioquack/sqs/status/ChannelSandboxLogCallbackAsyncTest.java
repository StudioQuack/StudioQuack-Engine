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

/**
 * Channel hops ride {@link AsyncSandboxLogDispatcher}: same-node STARTED then terminal stay
 * ordered, and {@code report} returns without waiting for the 60 s POST.
 */
class ChannelSandboxLogCallbackAsyncTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-1";
    private static final String RUN_ID = "wx-async-1";
    private static final String NODE_ID = "node-1";
    private static final String ENV_BASE = "/be/v1/sqvm/environments/" + ENV_ID;
    private static final String SANDBOX_LOGS = ENV_BASE + "/runs/" + RUN_ID + "/sandbox-logs";

    /** No sub-process registry: run ids are top-level, so nothing is remapped. */
    private static final ObjectProvider<SubProcessCompletionRegistry> NO_REGISTRY =
            new ObjectProvider<>() {
                @Override
                public SubProcessCompletionRegistry getObject() {
                    return null;
                }

                @Override
                public SubProcessCompletionRegistry getIfAvailable() {
                    return null;
                }
            };

    private static SqsChannelProperties props() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId(ENV_ID);
        props.setBearer("sqvm_bearer_async");
        return props;
    }

    private static ChannelSandboxLogCallback channel(String backendBaseUrl,
                                                     AsyncSandboxLogDispatcher dispatcher) {
        return new ChannelSandboxLogCallback(props(), backendBaseUrl,
                new ChannelHttpClients(null), MAPPER, NO_REGISTRY,
                new OutboundBaseHolder(), null, dispatcher);
    }

    @Test
    @DisplayName("same-node STARTED is POSTed before COMPLETED")
    void reportStartedThenReport_preservesPostOrder() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(SANDBOX_LOGS, 200, "{\"persisted\":true}");
            ChannelSandboxLogCallback cb = channel(be.baseUrl(), dispatcher);

            cb.reportStarted(new SandboxLogCallback.SandboxStartRecord(
                    RUN_ID, NODE_ID, "Step", "python"));
            cb.report(new SandboxLogCallback.SandboxLogRecord(
                    RUN_ID, NODE_ID, "Step", "COMPLETED", 0, 12L, "python",
                    null, "hello", null, null, null, null, null, null, "NONE", null));

            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();
            List<String> statuses = be.requests().stream()
                    .filter(r -> r.path().endsWith("/sandbox-logs"))
                    .map(r -> statusOf(r.body()))
                    .toList();
            assertThat(statuses).containsExactly("STARTED", "COMPLETED");
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("report() returns before a delayed 60s POST completes")
    void report_returnsBeforeDelayedPost() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try (MockBackend be = new MockBackend()) {
            // Sleep before the canned body is written — that delays the first response byte.
            be.stub(SANDBOX_LOGS, req -> {
                Thread.sleep(1_000);
                return MockBackend.Response.json(200, "{\"persisted\":true}");
            });
            ChannelSandboxLogCallback cb = channel(be.baseUrl(), dispatcher);

            long started = System.nanoTime();
            cb.report(new SandboxLogCallback.SandboxLogRecord(
                    RUN_ID, NODE_ID, "Step", "COMPLETED", 0, 12L, "python",
                    null, "hello", null, null, null, null, null, null, "NONE", null));
            long elapsedMs = Duration.ofNanos(System.nanoTime() - started).toMillis();

            assertThat(elapsedMs)
                    .as("report() must only enqueue; the delayed POST stays off the caller")
                    .isLessThan(200);
            assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();
            assertThat(be.countMatching("/sandbox-logs")).isEqualTo(1);
        } finally {
            dispatcher.shutdown();
        }
    }

    private static String statusOf(String body) {
        try {
            JsonNode node = MAPPER.readTree(body);
            return node.path("status").asText();
        } catch (Exception e) {
            throw new IllegalStateException("sandbox-log body was not JSON: " + body, e);
        }
    }
}
