package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderConfig;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.activiti.engine.runtime.Execution;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Router-level coverage for the helper channel (no HTTP): unknown/revoked bearer → 401,
 * context endpoints, result/fail/log/metric, llm/* proxy, and execute.
 */
class HelperRequestRouterTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final AiModelProviderHolder providerHolder = new AiModelProviderHolder();
    private HelperRequestRouter router;
    private String token;

    @BeforeEach
    void setUp() {
        HelperExecuteService executeService = new HelperExecuteService(
                mapper, mock(SqsEngine.class), mock(SubProcessCompletionRegistry.class),
                mock(RefOnlyVarBuilder.class), mock(TransformDirectiveApplier.class),
                mock(ExecutionContextStore.class),
                new com.studioquack.sqs.channel.wg.OverlayPeerDirectory(),
                new com.studioquack.sqs.channel.wg.mesh.MeshIdentity());
        router = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(providerHolder, mapper),
                executeService);

        Map<String, Object> currentNode = new LinkedHashMap<>();
        currentNode.put("id", "nodeX");
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("workflowExecutionId", "wfe-1");
        ec.put("currentNodeId", "nodeX");
        ec.put("currentNode", currentNode);
        ec.put("parameters", java.util.List.of(Map.of("k", "v")));

        token = router.register(HelperJob.builder("job-1", "python").executionContext(ec).build());
    }

    @Test
    void unauthorized_without_bearer() {
        HelperRequestRouter.RoutedResponse r = router.route("GET", "init", null, new byte[0]);
        assertEquals(401, r.status());
        assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("\"error\":\"unauthorized\""));
    }

    @Test
    void job_id_is_not_a_valid_bearer() {
        assertTrue(token.startsWith("h_"));
        assertNotEquals("h_job-1", token);
        HelperRequestRouter.RoutedResponse r = router.route("GET", "init", "h_job-1", new byte[0]);
        assertEquals(401, r.status());
    }

    @Test
    void revoke_marksTheJobSoAParkedExecuteKnowsItsCallerIsGone() {
        HelperJob job = router.lookup(token).orElseThrow();
        assertTrue(!job.revoked());
        router.revoke(token);
        assertTrue(job.revoked());
    }

    @Test
    void unauthorized_when_revoked() {
        router.revoke(token);
        HelperRequestRouter.RoutedResponse r = router.route("GET", "init", token, new byte[0]);
        assertEquals(401, r.status());
        assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("\"error\":\"unauthorized\""));
    }

    @Test
    void init_and_context_endpoints_serve_the_execution_context() {
        assertTrue(get("init").contains("\"id\":\"job-1\""));
        assertTrue(get("init").contains("\"language\":\"python\""));
        assertTrue(get("workflow-execution-id").contains("wfe-1"));
        assertTrue(get("current-node-id").contains("nodeX"));
        assertTrue(get("current-node").contains("\"id\":\"nodeX\""));
        assertTrue(get("parameters").contains("\"k\":\"v\""));
        assertTrue(get("full-context").contains("currentNode"));
    }

    @Test
    void full_context_strips_other_nodes_secret_handles() {
        Map<String, Object> currentNode = new LinkedHashMap<>();
        currentNode.put("id", "nodeA");
        currentNode.put("secret", "own-plaintext");
        Map<String, Object> other = new LinkedHashMap<>();
        other.put("id", "nodeB");
        other.put("language", "python");
        other.put("$secretRef", "sec_0000000000000000000000bb:v1");
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("currentNodeId", "nodeA");
        ec.put("currentNode", currentNode);
        ec.put("nodes", List.of(other));
        String t = router.register(HelperJob.builder("job-scope", "python").executionContext(ec).build());
        HelperRequestRouter.RoutedResponse r = router.route("GET", "full-context", t, new byte[0]);
        String body = new String(r.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("own-plaintext"));
        assertTrue(body.contains("nodeB"));
        assertFalse(body.contains("sec_0000000000000000000000bb"));
        assertTrue(ec.toString().contains("sec_0000000000000000000000bb"));
    }

    @Test
    void result_post_is_recorded_on_the_job() {
        HelperRequestRouter.RoutedResponse r = post("result",
                "{\"ok\":true,\"items\":[{\"port\":\"out\",\"value\":\"42\"}]}");
        assertEquals(200, r.status());
        HelperResult result = router.lookup(token).orElseThrow().result();
        assertTrue(result.ok());
        assertEquals("out", result.items().get(0).port());
        assertEquals("42", result.items().get(0).value());
    }

    @Test
    void fail_post_records_typed_error_on_the_job() {
        HelperRequestRouter.RoutedResponse r = post("fail",
                "{\"errorCode\":\"VALIDATION_FAILED\",\"message\":\"bad input\",\"data\":{\"field\":\"email\"}}");
        assertEquals(200, r.status());
        HelperFailure f = router.lookup(token).orElseThrow().failure();
        assertTrue(f.hasCode());
        assertEquals("VALIDATION_FAILED", f.errorCode());
        assertEquals("bad input", f.message());
        assertEquals("email", f.data().get("field"));
    }

    @Test
    void fail_post_without_code_is_rejected() {
        HelperRequestRouter.RoutedResponse r = post("fail", "{\"message\":\"no code\"}");
        assertEquals(400, r.status());
        assertTrue(router.lookup(token).orElseThrow().failure() == null,
                "no failure recorded for a blank code");
    }

    @Test
    void fail_post_rejects_reserved_sq_skip_code() {
        HelperRequestRouter.RoutedResponse r = post("fail",
                "{\"errorCode\":\"SQ_SKIP\",\"message\":\"nope\"}");
        assertEquals(400, r.status());
        assertTrue(router.lookup(token).orElseThrow().failure() == null,
                "reserved SQ_SKIP is not recorded");
    }

    @Test
    void escalate_post_fires_instance_scoped_reserved_signal() {
        SqsEngine engine = mock(SqsEngine.class);
        Execution parked = mock(Execution.class);
        when(parked.getId()).thenReturn("ex-1");
        when(parked.getProcessInstanceId()).thenReturn("pi-1");
        when(engine.resolveProcessInstanceId("wfe-1")).thenReturn("pi-1");
        when(engine.findSignalEventExecutions("__sq_esc_TIMEOUT")).thenReturn(List.of(parked));
        router.wireEventServicesForTests(engine, null, null, null);

        HelperRequestRouter.RoutedResponse r = post("escalate",
                "{\"escalationCode\":\"TIMEOUT\",\"message\":\"late\"}");
        assertEquals(200, r.status());
        assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("\"ok\":true"));
        verify(engine).signalEventReceived("__sq_esc_TIMEOUT", "ex-1");
    }

    @Test
    void escalate_post_blank_code_is_rejected() {
        HelperRequestRouter.RoutedResponse r = post("escalate", "{\"message\":\"no code\"}");
        assertEquals(400, r.status());
        assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("escalationCode is required"));
    }

    @Test
    void signal_post_reserved_prefix_is_rejected() {
        SqsEngine engine = mock(SqsEngine.class);
        router.wireEventServicesForTests(engine, null, null, null);

        HelperRequestRouter.RoutedResponse r = post("signal", "{\"signalName\":\"__sq_foo\"}");
        assertEquals(400, r.status());
        assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("\"error\":\"reserved\""));
        verify(engine, never()).signalEventReceived(anyString(), anyString());
        verify(engine, never()).signalEventReceived(anyString(), anyMap());
    }

    @Test
    void signal_post_global_broadcasts() {
        SqsEngine engine = mock(SqsEngine.class);
        router.wireEventServicesForTests(engine, null, null, null);

        HelperRequestRouter.RoutedResponse r = post("signal", "{\"signalName\":\"orderPlaced\"}");
        assertEquals(200, r.status());
        assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("\"ok\":true"));
        verify(engine).signalEventReceived(eq("orderPlaced"), eq(Map.of()));
    }

    @Test
    void message_post_uses_workflow_execution_id_when_key_omitted() {
        MessageCorrelationService correlation = mock(MessageCorrelationService.class);
        when(correlation.correlate(eq("orderPaid"), eq("wfe-1"), eq(Map.of())))
                .thenReturn(MessageCorrelationService.MessageCorrelationOutcome.correlated("ex-9", "orderPaid"));
        router.wireEventServicesForTests(null, correlation, null, null);

        HelperRequestRouter.RoutedResponse r = post("message", "{\"messageName\":\"orderPaid\"}");
        assertEquals(200, r.status());
        String body = new String(r.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"ok\":true"), body);
        assertTrue(body.contains("\"delivered\":1"), body);
        verify(correlation).correlate("orderPaid", "wfe-1", Map.of());
    }

    @Test
    void message_post_not_correlated() {
        MessageCorrelationService correlation = mock(MessageCorrelationService.class);
        when(correlation.correlate(eq("orderPaid"), eq("wfe-1"), eq(Map.of())))
                .thenReturn(MessageCorrelationService.MessageCorrelationOutcome.notCorrelated("orderPaid"));
        router.wireEventServicesForTests(null, correlation, null, null);

        HelperRequestRouter.RoutedResponse r = post("message", "{\"messageName\":\"orderPaid\"}");
        assertEquals(200, r.status());
        String body = new String(r.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"ok\":false"), body);
        assertTrue(body.contains("\"reason\":\"not_correlated\""), body);
    }

    @Test
    void cancelled_poll_false_then_true() {
        DispatchCancellationRegistry registry = mock(DispatchCancellationRegistry.class);
        when(registry.isCancelled("job-1")).thenReturn(false);
        router.wireEventServicesForTests(null, null, registry, null);

        HelperRequestRouter.RoutedResponse first = router.route("GET", "cancelled", token, new byte[0]);
        assertEquals(200, first.status());
        assertTrue(new String(first.body(), StandardCharsets.UTF_8).contains("\"cancelled\":false"));

        when(registry.isCancelled("job-1")).thenReturn(true);
        when(registry.cancelReason("job-1")).thenReturn("operator stop");
        HelperRequestRouter.RoutedResponse second = post("cancelled", "{}");
        assertEquals(200, second.status());
        String body = new String(second.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"cancelled\":true"), body);
        assertTrue(body.contains("\"reason\":\"operator stop\""), body);
    }

    @Test
    void result_and_fail_after_cancelled_are_conflict() {
        DispatchCancellationRegistry registry = mock(DispatchCancellationRegistry.class);
        when(registry.isCancelled("job-1")).thenReturn(true);
        router.wireEventServicesForTests(null, null, registry, null);

        HelperRequestRouter.RoutedResponse result = post("result",
                "{\"ok\":true,\"items\":[{\"port\":\"out\",\"value\":\"42\"}]}");
        assertEquals(409, result.status());
        assertTrue(new String(result.body(), StandardCharsets.UTF_8).contains("\"error\":\"cancelled\""));
        assertNull(router.lookup(token).orElseThrow().result());

        HelperRequestRouter.RoutedResponse fail = post("fail",
                "{\"errorCode\":\"VALIDATION_FAILED\",\"message\":\"bad\"}");
        assertEquals(409, fail.status());
        assertNull(router.lookup(token).orElseThrow().failure());
    }

    @Test
    void metric_and_log_are_recorded() {
        assertEquals(200, post("metric", "{\"name\":\"cpu\",\"value\":1.5}").status());
        assertEquals(200, post("log", "{\"message\":\"hi\"}").status());
        HelperJob job = router.lookup(token).orElseThrow();
        assertEquals(1, job.metrics().size());
        assertEquals(1, job.logs().size());
    }

    @Test
    void helper_log_redacts_pii_and_secret_literals() {
        String t = router.register(HelperJob.builder("job-log", "python")
                .executionContext(Map.of())
                .secretLiterals(java.util.Set.of("ab"))
                .build());
        assertEquals(200, router.route("POST", "log", t,
                "{\"message\":\"user@example.com pin=ab\"}".getBytes(StandardCharsets.UTF_8)).status());
        String recorded = String.valueOf(router.lookup(t).orElseThrow().logs().get(0).get("message"));
        assertTrue(recorded.contains("[redacted-email]"), recorded);
        assertTrue(recorded.contains("[redacted-secret]"), recorded);
        assertFalse(recorded.contains("user@example.com"), recorded);
        assertFalse(recorded.contains("ab"), recorded);
    }

    @Test
    void helper_log_caps_count() {
        HelperRequestRouter.RoutedResponse last = null;
        for (int i = 0; i < HelperJob.MAX_LOGS + 2; i++) {
            last = post("log", "{\"message\":\"n" + i + "\"}");
            assertEquals(200, last.status());
        }
        HelperJob job = router.lookup(token).orElseThrow();
        assertEquals(HelperJob.MAX_LOGS, job.logs().size());
        assertEquals("truncated", job.logs().get(HelperJob.MAX_LOGS - 1).get("kind"));
        assertTrue(new String(last.body(), StandardCharsets.UTF_8).contains("\"truncated\":true"));
    }

    @Test
    void helper_log_clamps_keys_depth_and_string() throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("message", "m".repeat(HelperJob.MAX_LOG_STRING_CHARS + 20));
        body.put("nest", Map.of("d1", Map.of("d2", Map.of("d3", Map.of("d4", "x")))));
        for (int i = 0; i < 20; i++) {
            body.put("k" + i, "v");
        }
        assertEquals(200, router.route("POST", "log", token, mapper.writeValueAsBytes(body)).status());
        Map<String, Object> got = router.lookup(token).orElseThrow().logs().get(0);
        assertTrue(got.size() <= HelperJob.MAX_LOG_KEYS);
        assertEquals(HelperJob.MAX_LOG_STRING_CHARS, String.valueOf(got.get("message")).length());
        @SuppressWarnings("unchecked")
        Map<String, Object> nest = (Map<String, Object>) got.get("nest");
        @SuppressWarnings("unchecked")
        Map<String, Object> d1 = (Map<String, Object>) nest.get("d1");
        assertEquals("[truncated]", d1.get("d2"));
    }

    @Test
    void execute_returns_empty_when_no_connection_details() {
        HelperRequestRouter.RoutedResponse r = post("execute",
                "{\"portValue\":\"model\",\"payload\":{}}");
        assertEquals(200, r.status());
        assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("\"result\""));
    }

    @Test
    void emit_partial_without_listener_returns_not_delivered() {
        HelperRequestRouter.RoutedResponse r = post("emit-partial", "{\"chunk\":1}");
        assertEquals(200, r.status());
        String body = new String(r.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"delivered\":false"), body);
        HelperRequestRouter.RoutedResponse llm = post("llm/chat", "{}");
        assertEquals(503, llm.status());
        assertTrue(new String(llm.body(), StandardCharsets.UTF_8).contains("provider_not_configured"));
    }

    @Test
    void llm_proxied_to_configured_provider() throws Exception {
        HttpServer provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        final String[] seenAuth = new String[1];
        final String[] seenPath = new String[1];
        final String[] seenBody = new String[1];
        provider.createContext("/", ex -> {
            seenAuth[0] = ex.getRequestHeaders().getFirst("Authorization");
            seenPath[0] = ex.getRequestURI().getPath();
            seenBody[0] = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] out = "{\"choices\":[{\"message\":{\"content\":\"pong\"}}]}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        provider.start();
        try {
            providerHolder.apply(new AiModelProviderConfig(
                    "http://127.0.0.1:" + provider.getAddress().getPort(), "prov-key",
                    "/v1/chat/completions", "/v1/models",
                    null, null, null, null, "gpt-x"));

            HelperRequestRouter.RoutedResponse r = post("llm/completion",
                    "{\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}");

            assertEquals(200, r.status());
            assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("pong"),
                    "provider body returned verbatim");
            assertEquals("Bearer prov-key", seenAuth[0]);
            assertEquals("/v1/chat/completions", seenPath[0]);
            assertTrue(seenBody[0].contains("ping"), "request body forwarded verbatim");
        } finally {
            provider.stop(0);
        }
    }

    @Test
    void llm_chat_records_content_free_request_summary_on_the_job() throws Exception {
        HttpServer provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        provider.createContext("/", ex -> {
            byte[] out = "{\"choices\":[{\"message\":{\"content\":\"pong\"}}]}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        provider.start();
        try {
            providerHolder.apply(new AiModelProviderConfig(
                    "http://127.0.0.1:" + provider.getAddress().getPort(), "prov-key",
                    "/v1/chat/completions", "/v1/models",
                    null, null, null, null, "gpt-x"));

            HelperRequestRouter.RoutedResponse r = post("llm/completion",
                    "{\"model\":\"gpt-x\",\"temperature\":0.7,"
                            + "\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}");
            assertEquals(200, r.status());

            HelperJob job = router.lookup(token).orElseThrow();
            List<Map<String, Object>> llmLogs = job.logs().stream()
                    .filter(l -> "llm-request".equals(l.get("kind"))).toList();
            assertEquals(1, llmLogs.size(), "one llm-request node-log entry per chat call");
            String msg = String.valueOf(llmLogs.get(0).get("message"));
            assertTrue(msg.contains("model=gpt-x"), msg);
            assertTrue(msg.contains("temperature=0.7"), msg);
            assertTrue(msg.contains("user:text(chars=4)"), msg);
            assertFalse(msg.contains("ping"), "prompt bodies never enter the node log: " + msg);
        } finally {
            provider.stop(0);
        }
    }

    @Test
    void llm_non_chat_ops_record_no_request_summary() {
        post("llm/load", "{\"model\":\"gpt-x\"}");
        HelperJob job = router.lookup(token).orElseThrow();
        assertTrue(job.logs().stream().noneMatch(l -> "llm-request".equals(l.get("kind"))),
                "load/models/etc. are not chat calls — no summary expected");
    }

    @Test
    void llm_stream_call_records_request_summary_on_the_job() throws Exception {
        HttpServer provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        provider.createContext("/", ex -> {
            byte[] out = "data: {\"choices\":[]}\n\ndata: [DONE]\n\n".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/event-stream");
            ex.sendResponseHeaders(200, 0);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
                os.flush();
            }
        });
        provider.start();
        try {
            providerHolder.apply(new AiModelProviderConfig(
                    "http://127.0.0.1:" + provider.getAddress().getPort(), "prov-key",
                    "/v1/chat/completions", "/v1/models",
                    null, null, null, null, "gpt-x"));
            router.routeStream("POST", "llm/completion", token,
                    ("{\"stream\":true,\"model\":\"gpt-x\","
                            + "\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}")
                            .getBytes(StandardCharsets.UTF_8),
                    "text/event-stream", new HelperStreamSink() {
                        @Override
                        public void oneShot(int status, String contentType, byte[] body) {
                        }

                        @Override
                        public void chunk(int status, String contentType, byte[] chunk, int seq, boolean done) {
                        }
                    });
            HelperJob job = router.lookup(token).orElseThrow();
            List<Map<String, Object>> llmLogs = job.logs().stream()
                    .filter(l -> "llm-request".equals(l.get("kind"))).toList();
            assertEquals(1, llmLogs.size(), "streamed chat calls are summarised too");
            assertTrue(String.valueOf(llmLogs.get(0).get("message")).contains("stream=true"),
                    String.valueOf(llmLogs.get(0).get("message")));
        } finally {
            provider.stop(0);
        }
    }

    @Test
    void llm_streamTrue_withoutEventStreamAccept_isBufferedAndForcesStreamFalse() throws Exception {
        HttpServer provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        final String[] seenBody = new String[1];
        provider.createContext("/", ex -> {
            seenBody[0] = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] out = "{\"choices\":[{\"message\":{\"content\":\"pong\"}}]}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        provider.start();
        try {
            providerHolder.apply(new AiModelProviderConfig(
                    "http://127.0.0.1:" + provider.getAddress().getPort(), "prov-key",
                    "/v1/chat/completions", "/v1/models",
                    null, null, null, null, "gpt-x"));

            HelperRequestRouter.RoutedResponse r = post("llm/completion",
                    "{\"stream\":true,\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}");
            assertEquals(200, r.status());
            assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("pong"));
            assertTrue(seenBody[0].contains("\"stream\":false") || seenBody[0].contains("\"stream\": false"),
                    "buffered llm must force stream=false: " + seenBody[0]);

            List<byte[]> chunks = new ArrayList<>();
            boolean[] oneShot = {false};
            router.routeStream("POST", "llm/completion", token,
                    "{\"stream\":true}".getBytes(StandardCharsets.UTF_8),
                    "application/json", new HelperStreamSink() {
                        @Override
                        public void oneShot(int status, String contentType, byte[] body) {
                            oneShot[0] = true;
                        }

                        @Override
                        public void chunk(int status, String contentType, byte[] chunk, int seq, boolean done) {
                            chunks.add(chunk == null ? new byte[0] : chunk);
                        }
                    });
            assertTrue(oneShot[0], "no SSE Accept must stay one-shot");
            assertTrue(chunks.isEmpty());
        } finally {
            provider.stop(0);
        }
    }

    @Test
    void llm_streamTrue_withEventStreamAccept_streamsProviderChunks() throws Exception {
        HttpServer provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        final String[] seenBody = new String[1];
        provider.createContext("/", ex -> {
            seenBody[0] = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] out = "data: {\"id\":\"1\"}\n\n".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/event-stream");
            ex.sendResponseHeaders(200, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        provider.start();
        try {
            providerHolder.apply(new AiModelProviderConfig(
                    "http://127.0.0.1:" + provider.getAddress().getPort(), "prov-key",
                    "/v1/chat/completions", "/v1/models",
                    null, null, null, null, "gpt-x"));

            List<String> chunks = new ArrayList<>();
            boolean[] oneShot = {false};
            router.routeStream("POST", "llm/completion", token,
                    "{\"stream\":true,\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}"
                            .getBytes(StandardCharsets.UTF_8),
                    "text/event-stream", new HelperStreamSink() {
                        @Override
                        public void oneShot(int status, String contentType, byte[] body) {
                            oneShot[0] = true;
                        }

                        @Override
                        public void chunk(int status, String contentType, byte[] chunk, int seq, boolean done) {
                            if (chunk != null && chunk.length > 0) {
                                chunks.add(new String(chunk, StandardCharsets.UTF_8));
                            }
                        }
                    });
            assertFalse(oneShot[0], "SSE Accept must not collapse to one-shot");
            assertTrue(chunks.stream().anyMatch(c -> c.contains("data:")), chunks.toString());
            assertTrue(seenBody[0].contains("\"stream\":true") || seenBody[0].contains("\"stream\": true"),
                    "negotiated stream must keep stream=true: " + seenBody[0]);
        } finally {
            provider.stop(0);
        }
    }

    @Test
    void revoke_removes_the_binding() {
        assertTrue(router.lookup(token).isPresent());
        router.revoke(token);
        assertTrue(router.lookup(token).isEmpty());
    }

    @Test
    void emit_partial_delivers_to_parent_execute_stream_bus() {
        HelperPartialEventBus bus = new HelperPartialEventBus();
        java.util.List<String> seen = new java.util.ArrayList<>();
        bus.register("wfe-1", (ch, payload) -> seen.add(ch + ":" + payload));
        router.wireStreamingForTests(bus, null);
        HelperRequestRouter.RoutedResponse r = post("emit-partial",
                "{\"delta\":\"hi\",\"channel\":\"thinking\"}");
        assertEquals(200, r.status());
        assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("\"delivered\":true"));
        assertEquals(java.util.List.of("thinking:hi"), seen);
    }

    @Test
    void execute_stream_with_no_connections_emits_complete_then_done() {
        HelperRequestRouter.RoutedResponse r = post("execute/stream",
                "{\"portValue\":\"model\",\"payload\":{}}");
        assertEquals(200, r.status());
        String body = new String(r.body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("event: complete"), body);
        assertTrue(body.contains("event: done"), body);
        assertTrue("text/event-stream".equals(r.contentType()), r.contentType());
    }

    @Test
    void register_refusesWhenJobCapReached() {
        for (int i = 2; i <= HelperRequestRouter.MAX_JOBS; i++) {
            router.register(HelperJob.builder("job-" + i, "python").executionContext(Map.of()).build());
        }
        assertThrows(IllegalStateException.class,
                () -> router.register(HelperJob.builder("overflow", "python").executionContext(Map.of()).build()));
    }

    private String get(String sub) {
        HelperRequestRouter.RoutedResponse r = router.route("GET", sub, token, new byte[0]);
        assertEquals(200, r.status(), "GET " + sub);
        return new String(r.body(), StandardCharsets.UTF_8);
    }

    private HelperRequestRouter.RoutedResponse post(String sub, String json) {
        return router.route("POST", sub, token, json.getBytes(StandardCharsets.UTF_8));
    }
}
