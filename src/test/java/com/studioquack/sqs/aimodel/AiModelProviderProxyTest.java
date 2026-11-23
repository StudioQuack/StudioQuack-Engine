package com.studioquack.sqs.aimodel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link AiModelProviderProxy}: the SDK sub-path → provider path mapping, verbatim
 * body/status pass-through, bearer injection (the sandbox never sends it — the daemon does), and
 * the structured failure modes (not-configured, unsupported op, unreachable provider). A real
 * loopback HTTP server stands in for the OpenAI-compatible provider.
 */
class AiModelProviderProxyTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private HttpServer provider;
    private String providerBase;
    private AiModelLoadState loadState;
    private final AtomicReference<String> lastPath = new AtomicReference<>();
    private final AtomicReference<String> lastMethod = new AtomicReference<>();
    private final AtomicReference<String> lastAuth = new AtomicReference<>();
    private final AtomicReference<String> lastBody = new AtomicReference<>();
    private final AtomicReference<String> lastAccept = new AtomicReference<>();
    private volatile int providerStatus = 200;
    private volatile String providerBody = "{\"ok\":true}";
    private volatile String providerContentType = "application/json";
    private volatile boolean chunkedResponse;
    /** Stall stub: headers out, then NOT A SINGLE body byte until the test releases the latch. */
    private volatile boolean hangBody;
    /** Slow-but-alive stub: chunks written with a per-chunk gap below the idle budget. */
    private volatile byte[][] dripChunks;
    private volatile long dripGapMs = 150L;
    private final CountDownLatch hangRelease = new CountDownLatch(1);

    @BeforeEach
    void setUp() throws Exception {
        loadState = new AiModelLoadState();
        provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        provider.createContext("/", ex -> {
            lastPath.set(ex.getRequestURI().getPath());
            lastMethod.set(ex.getRequestMethod());
            lastAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
            lastAccept.set(ex.getRequestHeaders().getFirst("Accept"));
            lastBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] out = providerBody.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", providerContentType);
            if (hangBody) {
                // Chunked headers go out, then silence — a local model server that accepted
                // the request and choked before producing a single byte. The explicit flush
                // pushes the headers onto the wire NOW: HttpServer buffers them until the
                // first body write otherwise, and the client would never see them.
                ex.sendResponseHeaders(providerStatus, 0);
                ex.getResponseBody().flush();
                try {
                    hangRelease.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                ex.close();
                return;
            }
            if (dripChunks != null) {
                // Every gap stays below the idle timeout under test: a slow live stream.
                ex.sendResponseHeaders(providerStatus, 0);
                try (OutputStream os = ex.getResponseBody()) {
                    for (byte[] part : dripChunks) {
                        os.write(part);
                        os.flush();
                        try {
                            Thread.sleep(dripGapMs);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                return;
            }
            // 0 → chunked transfer (SSE) so the client need not wait on Content-Length.
            ex.sendResponseHeaders(providerStatus, chunkedResponse ? 0 : out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
                os.flush();
            }
        });
        provider.start();
        providerBase = "http://127.0.0.1:" + provider.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        hangRelease.countDown(); // unpark a handler stuck in the headers-then-silence stub
        provider.stop(0);
    }

    private AiModelProviderProxy proxyFor(AiModelProviderConfig config) {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        holder.apply(config);
        return new AiModelProviderProxy(holder, loadState, mapper);
    }

    /** Proxy wired to a cancel registry, as in production. */
    private AiModelProviderProxy proxyFor(AiModelProviderConfig config, LlmStreamCancellationRegistry cancellation) {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        holder.apply(config);
        return new AiModelProviderProxy(holder, loadState, new AiModelCapabilityState(), mapper, cancellation);
    }

    private AiModelProviderConfig fullProvider() {
        return new AiModelProviderConfig(
                providerBase, "secret-key",
                "/v1/chat/completions", "/v1/models",
                "/admin/load", "/admin/unload", "/admin/download", "/admin/download/status",
                "gpt-x");
    }

    @Test
    void completionForwardsBodyAndInjectsBearer() throws Exception {
        providerBody = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"hi\"}}]}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        // fullProvider() is MANAGED (load path set): the no-JIT guard admits only loaded models.
        loadState.markLoaded("gpt-x");

        String reqBody = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"model\":\"gpt-x\"}";
        AiModelProviderProxy.ProxyResult result =
                proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        assertEquals(200, result.status());
        // The provider path was the configured chat path…
        assertEquals("/v1/chat/completions", lastPath.get());
        assertEquals("POST", lastMethod.get());
        // …the daemon injected the provider bearer (the sandbox never sent it)…
        assertEquals("Bearer secret-key", lastAuth.get());
        // …the body was forwarded verbatim…
        assertEquals(reqBody, lastBody.get());
        // …and the provider envelope came back unchanged (SDK does response.json() on it).
        JsonNode json = mapper.readTree(result.body());
        assertEquals("hi", json.path("choices").path(0).path("message").path("content").asText());
    }

    @Test
    void chatRefusesLinkLocalProviderHost() {
        AiModelProviderProxy proxy = proxyFor(new AiModelProviderConfig(
                "http://169.254.169.254", null,
                "/v1/chat/completions", "/v1/models",
                null, null, null, null, null));
        AiModelProviderProxy.ProxyResult result =
                proxy.forward("chat", "POST", "{}".getBytes(StandardCharsets.UTF_8));
        assertEquals(403, result.status());
        assertTrue(new String(result.body(), StandardCharsets.UTF_8).toLowerCase().contains("not allowed"),
                new String(result.body(), StandardCharsets.UTF_8));
    }

    @Test
    void internalToolsAreConvertedToOpenAiFunctionShape() throws Exception {
        // The ai-agent emits tools in the StudioQuack-internal form (schema rides `request`,
        // plus agent routing metadata). A strict OpenAI-compatible provider 400s that; the
        // proxy — owner of the provider contract — must emit {type:function, function:{…}}.
        providerBody = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"hi\"}}]}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        loadState.markLoaded("gpt-x");
        String reqBody = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"model\":\"gpt-x\","
                + "\"tools\":[{\"name\":\"search\",\"description\":\"Search the web\","
                + "\"request\":{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\","
                + "\"description\":\"Search query\"}},\"required\":[\"query\"]},"
                + "\"node_id\":\"n1\",\"port_value\":\"in\",\"aliases\":[\"web_search\"]}]}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode tool = mapper.readTree(lastBody.get()).path("tools").get(0);
        assertEquals("function", tool.path("type").asText());
        JsonNode fn = tool.path("function");
        assertEquals("search", fn.path("name").asText());
        assertEquals("Search the web", fn.path("description").asText());
        assertEquals("string", fn.path("parameters").path("properties").path("query").path("type").asText());
        assertEquals("Search query",
                fn.path("parameters").path("properties").path("query").path("description").asText());
        // internal routing metadata never reaches the provider
        assertTrue(tool.path("node_id").isMissingNode());
        assertTrue(tool.path("port_value").isMissingNode());
        assertTrue(tool.path("aliases").isMissingNode());
    }

    @Test
    void openAiToolsPassThroughUnchanged() throws Exception {
        // A Local LLM node forwarding caller-supplied OpenAI tools must not be rewritten.
        providerBody = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"hi\"}}]}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        loadState.markLoaded("gpt-x");
        String reqBody = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"model\":\"gpt-x\","
                + "\"tools\":[{\"type\":\"function\",\"function\":{\"name\":\"search\","
                + "\"description\":\"d\",\"parameters\":{\"type\":\"object\",\"properties\":{}}}}]}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode tool = mapper.readTree(lastBody.get()).path("tools").get(0);
        assertEquals("function", tool.path("type").asText());
        assertEquals("search", tool.path("function").path("name").asText());
    }

    @Test
    void historyToolCallsAreConvertedToOpenAiShape() throws Exception {
        // The agent records assistant tool_calls internally as {id,name,args}; on the wire the
        // provider needs {id,type:function,function:{name,arguments:<json-string>}}. Tool-result
        // messages ({role:tool, tool_call_id, content}) are already OpenAI-shaped — untouched.
        providerBody = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"hi\"}}]}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        loadState.markLoaded("gpt-x");
        String reqBody = "{\"model\":\"gpt-x\",\"messages\":["
                + "{\"role\":\"user\",\"content\":\"hi\"},"
                + "{\"role\":\"assistant\",\"tool_calls\":[{\"id\":\"c1\",\"name\":\"search\",\"args\":{\"query\":\"q\"}}]},"
                + "{\"role\":\"tool\",\"tool_call_id\":\"c1\",\"name\":\"search\",\"content\":\"result\"}]}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode messages = mapper.readTree(lastBody.get()).path("messages");
        JsonNode tc = messages.get(1).path("tool_calls").get(0);
        assertEquals("function", tc.path("type").asText());
        assertEquals("c1", tc.path("id").asText());
        assertEquals("search", tc.path("function").path("name").asText());
        assertEquals("{\"query\":\"q\"}", tc.path("function").path("arguments").asText());
        JsonNode toolMsg = messages.get(2);
        assertEquals("tool", toolMsg.path("role").asText());
        assertEquals("c1", toolMsg.path("tool_call_id").asText());
    }

    @Test
    void delegationToolKeepsAgentIdEnum() throws Exception {
        // sub_agent port → one fixed-name delegate_to_agent tool; the agent_id enum lists the
        // connected agents so the model can address any of them. The enum must survive.
        providerBody = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"hi\"}}]}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        loadState.markLoaded("gpt-x");
        String reqBody = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"model\":\"gpt-x\","
                + "\"tools\":[{\"name\":\"delegate_to_agent\",\"description\":\"Delegate. Agents:\","
                + "\"request\":{\"type\":\"object\",\"properties\":{\"agent_id\":{\"type\":\"string\",\"enum\":[\"n1\",\"n2\"]},"
                + "\"prompt\":{\"type\":\"string\"}},\"required\":[\"agent_id\",\"prompt\"]}}]}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode fn = mapper.readTree(lastBody.get()).path("tools").get(0).path("function");
        assertEquals("function", mapper.readTree(lastBody.get()).path("tools").get(0).path("type").asText());
        assertEquals("delegate_to_agent", fn.path("name").asText());
        JsonNode agentId = fn.path("parameters").path("properties").path("agent_id");
        assertEquals("n1", agentId.path("enum").get(0).asText());
        assertEquals("n2", agentId.path("enum").get(1).asText());
        assertEquals("agent_id", fn.path("parameters").path("required").get(0).asText());
    }

    @Test
    void dataPortToolKeepsQuerySchema() throws Exception {
        // data port → user-configured tool name (edge transform) + a fixed required `query` arg.
        providerBody = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"hi\"}}]}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        loadState.markLoaded("gpt-x");
        String reqBody = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"model\":\"gpt-x\","
                + "\"tools\":[{\"name\":\"knowledge_base\",\"description\":\"Search docs\","
                + "\"request\":{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\"}},\"required\":[\"query\"]},"
                + "\"node_id\":\"n9\",\"port_value\":\"data\"}]}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode fn = mapper.readTree(lastBody.get()).path("tools").get(0).path("function");
        assertEquals("knowledge_base", fn.path("name").asText());
        assertEquals("string", fn.path("parameters").path("properties").path("query").path("type").asText());
        assertEquals("query", fn.path("parameters").path("required").get(0).asText());
    }

    @Test
    void invalidToolNameCharsAreSanitisedForOpenAi() throws Exception {
        // A user/node-configured name with spaces/punctuation would be rejected by a strict
        // provider; the proxy maps it onto the OpenAI name alphabet. The agent resolves the
        // sanitised name via its case/format-insensitive alias matching.
        providerBody = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"hi\"}}]}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        loadState.markLoaded("gpt-x");
        String reqBody = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"model\":\"gpt-x\","
                + "\"tools\":[{\"name\":\"HTTP Request (v2).\",\"description\":\"d\","
                + "\"request\":{\"type\":\"object\",\"properties\":{}}}]}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode fn = mapper.readTree(lastBody.get()).path("tools").get(0).path("function");
        assertEquals("HTTP_Request_v2", fn.path("name").asText());
    }

    @Test
    void jsonObjectResponseFormatIsRewrittenToJsonSchema() throws Exception {
        // LM Studio rejects response_format.type=json_object; the proxy must rewrite it to a
        // provider-accepted json_schema wrapper while preserving the "reply with JSON" intent.
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],"
                + "\"response_format\":{\"type\":\"json_object\"}}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("json_schema", sent.path("response_format").path("type").asText());
        // A valid json_schema wrapper (name + schema) so the provider does not 400.
        assertTrue(sent.path("response_format").path("json_schema").path("schema").isObject(),
                "json_schema must carry a schema object");
        // Other fields survive untouched.
        assertEquals("hi", sent.path("messages").path(0).path("content").asText());
    }

    @Test
    void stringJsonLiteralResponseFormatBecomesJsonSchema() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"response_format\":\"json\"}";

        proxy.forward("chat", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("json_schema", sent.path("response_format").path("type").asText());
    }

    @Test
    void textResponseFormatIsForwardedUnchanged() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"response_format\":{\"type\":\"text\"}}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        // Already provider-valid → forwarded byte-for-byte (no reserialisation).
        assertEquals(reqBody, lastBody.get());
    }

    @Test
    void validJsonSchemaResponseFormatIsForwardedVerbatim() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"response_format\":{\"type\":\"json_schema\","
                + "\"json_schema\":{\"name\":\"r\",\"schema\":{\"type\":\"object\"}}}}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        assertEquals(reqBody, lastBody.get(), "an already-valid json_schema must pass through verbatim");
    }

    @Test
    void unknownResponseFormatTypeIsDropped() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"response_format\":{\"type\":\"yaml\"}}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertTrue(sent.path("response_format").isMissingNode(),
                "an unsupported response_format type must be dropped, not forwarded");
    }

    @Test
    void numericReasoningEffortIsCoercedToString() throws Exception {
        // A picker value serialised as a number makes strict providers 400 with
        // "'reasoning_effort' must be a string"; the daemon repairs the TYPE.
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"reasoning_effort\":2,\"verbosity\":1}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertTrue(sent.path("reasoning_effort").isTextual(), "reasoning_effort must be sent as a string");
        assertEquals("2", sent.path("reasoning_effort").asText());
        assertTrue(sent.path("verbosity").isTextual(), "verbosity must be sent as a string");
        assertEquals("1", sent.path("verbosity").asText());
    }

    @Test
    void stringReasoningEffortIsForwardedVerbatim() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"reasoning_effort\":\"high\"}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        // Already a clean string → forwarded byte-for-byte (no reserialisation).
        assertEquals(reqBody, lastBody.get());
    }

    @Test
    void blankReasoningEffortIsDropped() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"reasoning_effort\":\"\"}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertTrue(sent.path("reasoning_effort").isMissingNode(),
                "a blank reasoning_effort must be dropped so the provider applies its default");
    }

    @Test
    void chatAliasMapsToChatUrl() {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        proxy.forward("chat", "POST", "{}".getBytes(StandardCharsets.UTF_8));
        assertEquals("/v1/chat/completions", lastPath.get());
    }

    @Test
    void modelsIsAGetToTheModelsPath() {
        providerBody = "{\"data\":[{\"id\":\"gpt-x\"}]}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        AiModelProviderProxy.ProxyResult result = proxy.forward("models", "GET", null);

        assertEquals(200, result.status());
        assertEquals("/v1/models", lastPath.get());
        assertEquals("GET", lastMethod.get());
    }

    @Test
    void downloadStatusMapsToTheOptionalPath() {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        proxy.forward("download/status", "GET", null);
        assertEquals("/admin/download/status", lastPath.get());
        assertEquals("GET", lastMethod.get());
    }

    /**
     * LM Studio keys download status on a PATH-embedded job id
     * (/api/v1/models/download/status/{job_id}); the helper channel can't carry a path segment,
     * so the SDK sends ?job_id= and the proxy splices it onto the configured base.
     */
    @Test
    void downloadStatusSplicesTheJobIdIntoThePath() {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        proxy.forward("download/status", "GET", "job_id=job_493c7c9ded", null);
        assertEquals("/admin/download/status/job_493c7c9ded", lastPath.get());
        assertEquals("GET", lastMethod.get());
    }

    @Test
    void upstreamNon2xxIsPassedThroughVerbatim() {
        providerStatus = 500;
        providerBody = "{\"error\":\"boom\"}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        AiModelProviderProxy.ProxyResult result =
                proxy.forward("completion", "POST", "{}".getBytes(StandardCharsets.UTF_8));

        assertEquals(500, result.status());
        assertEquals("{\"error\":\"boom\"}", new String(result.body(), StandardCharsets.UTF_8));
    }

    @Test
    void notConfiguredReturns503() throws Exception {
        AiModelProviderProxy proxy = proxyFor(AiModelProviderConfig.EMPTY);

        AiModelProviderProxy.ProxyResult result = proxy.forward("completion", "POST", "{}".getBytes());

        assertEquals(503, result.status());
        assertNull(lastPath.get(), "no upstream call when unconfigured");
        JsonNode json = mapper.readTree(result.body());
        assertEquals("provider_not_configured", json.get("error").asText());
    }

    @Test
    void unsupportedOpWhenProviderHasNoLoadPath() throws Exception {
        // A plain OpenAI-compatible endpoint: only server + chat/models paths, no admin ops.
        AiModelProviderConfig plain = new AiModelProviderConfig(
                providerBase, "k", "/v1/chat/completions", "/v1/models",
                null, null, null, null, null);
        AiModelProviderProxy proxy = proxyFor(plain);

        AiModelProviderProxy.ProxyResult result = proxy.forward("load", "POST", "{}".getBytes());

        assertEquals(501, result.status());
        assertNull(lastPath.get(), "no upstream call for an unsupported op");
        JsonNode json = mapper.readTree(result.body());
        assertEquals("unsupported_llm_op", json.get("error").asText());
    }

    @Test
    void unknownSubPathReturns404() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        AiModelProviderProxy.ProxyResult result = proxy.forward("nonsense", "POST", "{}".getBytes());
        assertEquals(404, result.status());
        JsonNode json = mapper.readTree(result.body());
        assertEquals("unknown_llm_endpoint", json.get("error").asText());
    }

    @Test
    void unreachableProviderReturns502() throws Exception {
        // Point at a closed port so the connect fails fast.
        AiModelProviderConfig dead = new AiModelProviderConfig(
                "http://127.0.0.1:1", "k", "/v1/chat/completions", "/v1/models",
                null, null, null, null, null);
        AiModelProviderProxy proxy = proxyFor(dead);

        AiModelProviderProxy.ProxyResult result = proxy.forward("completion", "POST", "{}".getBytes());

        assertEquals(502, result.status());
        JsonNode json = mapper.readTree(result.body());
        assertEquals("provider_unreachable", json.get("error").asText());
        assertNotNull(json.get("detail"));
    }

    @Test
    void noBearerWhenProviderHasNoKey() {
        AiModelProviderConfig noKey = new AiModelProviderConfig(
                providerBase, null, "/v1/chat/completions", "/v1/models",
                null, null, null, null, null);
        AiModelProviderProxy proxy = proxyFor(noKey);

        proxy.forward("completion", "POST", "{}".getBytes(StandardCharsets.UTF_8));

        assertNull(lastAuth.get(), "no Authorization header injected when the provider has no key");
        assertTrue(lastPath.get().endsWith("/v1/chat/completions"));
    }

    /**
     * The no-JIT guard: on a MANAGED provider (load path configured) a chat call naming a model
     * the daemon has not recorded as loaded is refused with {@code 409 model_not_loaded} BEFORE
     * any upstream call — execution never triggers a provider-side JIT load.
     */
    @Test
    void chatForNotLoadedModelIsRefusedOnManagedProvider() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        String reqBody = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"model\":\"gpt-y\"}";
        AiModelProviderProxy.ProxyResult result =
                proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        assertEquals(409, result.status());
        assertNull(lastPath.get(), "a refused call must never reach the provider");
        JsonNode json = mapper.readTree(result.body());
        assertEquals("model_not_loaded", json.get("error").asText());
        assertTrue(json.get("detail").asText().contains("gpt-y"), "detail names the model");
        assertTrue(json.get("detail").asText().contains("AI Model"), "detail points at the settings surface");
    }

    /** The guard admits a model the daemon HAS recorded as loaded. */
    @Test
    void chatForLoadedModelPassesTheGuard() {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        loadState.markLoaded("gpt-x");

        AiModelProviderProxy.ProxyResult result = proxy.forward("completion", "POST",
                "{\"messages\":[],\"model\":\"gpt-x\"}".getBytes(StandardCharsets.UTF_8));

        assertEquals(200, result.status());
        assertEquals("/v1/chat/completions", lastPath.get());
    }

    /**
     * An UNMANAGED provider (no load path — plain OpenAI-compatible, Ollama auto-load) is never
     * guarded: there is no load concept to violate, so any model name forwards as before.
     */
    @Test
    void unmanagedProviderIsNeverGuarded() {
        AiModelProviderConfig plain = new AiModelProviderConfig(
                providerBase, "k", "/v1/chat/completions", "/v1/models",
                null, null, null, null, null);
        AiModelProviderProxy proxy = proxyFor(plain);

        AiModelProviderProxy.ProxyResult result = proxy.forward("completion", "POST",
                "{\"messages\":[],\"model\":\"anything\"}".getBytes(StandardCharsets.UTF_8));

        assertEquals(200, result.status());
        assertEquals("/v1/chat/completions", lastPath.get());
    }

    /** A chat body WITHOUT a model field is never guarded — the provider's default applies. */
    @Test
    void chatWithoutModelFieldIsNeverGuarded() {
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        AiModelProviderProxy.ProxyResult result = proxy.forward("completion", "POST",
                "{\"messages\":[]}".getBytes(StandardCharsets.UTF_8));

        assertEquals(200, result.status());
        assertEquals("/v1/chat/completions", lastPath.get());
    }

    /** A successful sandbox-initiated load marks the model loaded (state stays in step). */
    @Test
    void successfulLoadForwardMarksModelLoaded() {
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        AiModelProviderProxy.ProxyResult result = proxy.forward("load", "POST",
                "{\"model\":\"gpt-x\"}".getBytes(StandardCharsets.UTF_8));

        assertEquals(200, result.status());
        assertEquals("/admin/load", lastPath.get());
        assertTrue(loadState.isLoaded("gpt-x"));
    }

    /** A successful sandbox-initiated unload (keyed by instance_id) drops the loaded record. */
    @Test
    void successfulUnloadForwardMarksModelUnloaded() {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        loadState.markLoaded("gpt-x");

        AiModelProviderProxy.ProxyResult result = proxy.forward("unload", "POST",
                "{\"instance_id\":\"gpt-x\"}".getBytes(StandardCharsets.UTF_8));

        assertEquals(200, result.status());
        assertEquals("/admin/unload", lastPath.get());
        assertTrue(!loadState.isLoaded("gpt-x"), "unload must drop the loaded record");
    }

    /** A FAILED load forward must NOT mark the model loaded. */
    @Test
    void failedLoadForwardDoesNotMarkLoaded() {
        providerStatus = 500;
        providerBody = "{\"error\":\"oom\"}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        proxy.forward("load", "POST", "{\"model\":\"gpt-x\"}".getBytes(StandardCharsets.UTF_8));

        assertTrue(!loadState.isLoaded("gpt-x"), "a non-2xx load must not touch the loaded record");
    }

    /**
     * Node parameters are stored as strings; a strict provider (LM Studio) 400s on
     * {@code "logprobs":"false"} with "'logprobs' must be a boolean or null". The daemon owns the
     * provider contract, so the boolean/integer/number chat scalars are type-repaired on the way
     * out — the exact failure the cross-env path surfaced.
     */
    @Test
    void stringlyTypedSamplingScalarsAreCoerced() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"logprobs\":\"false\","
                + "\"parallel_tool_calls\":\"0\",\"top_logprobs\":\"5\",\"max_tokens\":\"128\","
                + "\"seed\":\"42\",\"temperature\":\"0.7\",\"presence_penalty\":\"1.5\"}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertTrue(sent.path("logprobs").isBoolean() && !sent.path("logprobs").asBoolean(),
                "logprobs must leave as a real boolean");
        assertTrue(sent.path("parallel_tool_calls").isBoolean() && !sent.path("parallel_tool_calls").asBoolean(),
                "the '0' string maps to false");
        assertTrue(sent.path("top_logprobs").isInt() && sent.path("top_logprobs").asInt() == 5);
        assertTrue(sent.path("max_tokens").isInt() && sent.path("max_tokens").asInt() == 128);
        assertTrue(sent.path("seed").isInt() && sent.path("seed").asInt() == 42);
        assertTrue(sent.path("temperature").isNumber() && sent.path("temperature").asDouble() == 0.7d);
        assertTrue(sent.path("presence_penalty").isNumber() && sent.path("presence_penalty").asDouble() == 1.5d);
    }

    /** Junk scalar values are dropped (provider default applies) instead of 400-ing the call. */
    @Test
    void unparseableSamplingScalarsAreDropped() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"logprobs\":\"maybe\",\"max_tokens\":\"abc\","
                + "\"temperature\":\"\",\"n\":[1]}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertTrue(sent.path("logprobs").isMissingNode(), "unmappable boolean must be dropped");
        assertTrue(sent.path("max_tokens").isMissingNode(), "unmappable integer must be dropped");
        assertTrue(sent.path("temperature").isMissingNode(), "blank number must be dropped");
        assertTrue(sent.path("n").isMissingNode(), "a container is never a valid scalar");
    }

    /** Already-correct scalar types forward byte-for-byte (no reserialisation). */
    @Test
    void wellTypedScalarsAreForwardedVerbatim() {
        AiModelProviderProxy proxy = proxyFor(fullProvider());
        String reqBody = "{\"messages\":[],\"logprobs\":false,\"max_tokens\":128,\"temperature\":0.7}";

        proxy.forward("completion", "POST", reqBody.getBytes(StandardCharsets.UTF_8));

        assertEquals(reqBody, lastBody.get());
    }

    /**
     * The helper channel is request/response: stream=true would buffer the provider's whole SSE
     * body and hand the SDK an unparseable blob, so the proxy normalises it to a plain completion.
     */
    @Test
    void streamTrueIsNormalisedToFalse() throws Exception {
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        proxy.forward("completion", "POST",
                "{\"messages\":[],\"stream\":true}".getBytes(StandardCharsets.UTF_8));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertTrue(sent.path("stream").isBoolean() && !sent.path("stream").asBoolean(),
                "stream must leave as false — the channel cannot deliver tokens incrementally");
    }

    /**
     * {@code forwardStream} with {@code stream=true} must keep stream=true on the wire and
     * deliver the provider's SSE bytes to the consumer (chunked, no Content-Length).
     */
    @Test
    void forwardStreamWithStreamTrueSendsStreamTrueAndDeliversSse() throws Exception {
        providerContentType = "text/event-stream";
        chunkedResponse = true;
        String sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
                + "data: [DONE]\n\n";
        providerBody = sse;
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        List<Collected> collected = new ArrayList<>();
        proxy.forwardStream("completion", "POST",
                "{\"messages\":[],\"stream\":true}".getBytes(StandardCharsets.UTF_8),
                (status, contentType, chunk, last) ->
                        collected.add(new Collected(status, contentType, chunk, last)));

        JsonNode sent = mapper.readTree(lastBody.get());
        assertTrue(sent.path("stream").isBoolean() && sent.path("stream").asBoolean(),
                "forwardStream must send stream=true to the provider");
        assertTrue(lastAccept.get() != null && lastAccept.get().contains("text/event-stream"),
                "Accept must allow text/event-stream");

        assertTrue(!collected.isEmpty(), "consumer must receive at least the terminator");
        Collected last = collected.get(collected.size() - 1);
        assertTrue(last.last, "final callback is last=true");
        assertEquals(0, last.chunk.length, "terminator is empty bytes");
        for (int i = 0; i < collected.size() - 1; i++) {
            assertTrue(!collected.get(i).last, "data chunks are last=false");
        }
        assertEquals(sse, new String(concatData(collected), StandardCharsets.UTF_8));
        assertTrue(collected.get(0).contentType.contains("text/event-stream"));
        assertEquals(200, collected.get(0).status);
    }

    /**
     * Absent or false {@code stream} uses the buffered path: stream is omitted/false on the
     * wire and the consumer gets one JSON body with {@code last=true}.
     */
    @Test
    void forwardStreamWithoutStreamDeliversOneBufferedBody() throws Exception {
        providerBody = "{\"ok\":true}";
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        List<Collected> absent = new ArrayList<>();
        proxy.forwardStream("completion", "POST",
                "{\"messages\":[]}".getBytes(StandardCharsets.UTF_8),
                (status, contentType, chunk, last) ->
                        absent.add(new Collected(status, contentType, chunk, last)));

        JsonNode sentAbsent = mapper.readTree(lastBody.get());
        assertTrue(sentAbsent.path("stream").isMissingNode()
                        || (sentAbsent.path("stream").isBoolean() && !sentAbsent.path("stream").asBoolean()),
                "absent stream must stay omitted or false");
        assertEquals(1, absent.size());
        assertTrue(absent.get(0).last);
        assertEquals("{\"ok\":true}", new String(absent.get(0).chunk, StandardCharsets.UTF_8));
        assertTrue(absent.get(0).contentType.contains("application/json"));

        lastPath.set(null);
        lastBody.set(null);
        List<Collected> explicitFalse = new ArrayList<>();
        proxy.forwardStream("completion", "POST",
                "{\"messages\":[],\"stream\":false}".getBytes(StandardCharsets.UTF_8),
                (status, contentType, chunk, last) ->
                        explicitFalse.add(new Collected(status, contentType, chunk, last)));

        JsonNode sentFalse = mapper.readTree(lastBody.get());
        assertTrue(sentFalse.path("stream").isMissingNode()
                        || (sentFalse.path("stream").isBoolean() && !sentFalse.path("stream").asBoolean()),
                "stream=false must stay false or be omitted");
        assertEquals(1, explicitFalse.size());
        assertTrue(explicitFalse.get(0).last);
        assertEquals("{\"ok\":true}", new String(explicitFalse.get(0).chunk, StandardCharsets.UTF_8));
    }

    /** Unconfigured provider: 503 JSON to the consumer, no upstream call, no NPE. */
    @Test
    void forwardStreamUnconfiguredReturns503Json() throws Exception {
        AiModelProviderProxy proxy = proxyFor(AiModelProviderConfig.EMPTY);
        List<Collected> collected = new ArrayList<>();
        proxy.forwardStream("completion", "POST", "{}".getBytes(StandardCharsets.UTF_8),
                (status, contentType, chunk, last) ->
                        collected.add(new Collected(status, contentType, chunk, last)));

        assertEquals(1, collected.size());
        assertEquals(503, collected.get(0).status);
        assertTrue(collected.get(0).last);
        assertTrue(collected.get(0).contentType.contains("application/json"));
        assertNull(lastPath.get(), "no upstream call when unconfigured");
        JsonNode json = mapper.readTree(collected.get(0).chunk);
        assertEquals("provider_not_configured", json.get("error").asText());
    }

    /**
     * A local model that sends response HEADERS and then stays silent (thinking, or a long
     * prompt being processed) is waited on — the proxy has no idle cutoff. Cancelling the run
     * is what ends it: the consumer then gets its terminal callback promptly.
     */
    @Test
    void silentStreamIsWaitedOnUntilTheRunIsCancelled() throws Exception {
        providerContentType = "text/event-stream";
        hangBody = true;
        LlmStreamCancellationRegistry cancellation = new LlmStreamCancellationRegistry();
        AiModelProviderProxy proxy = proxyFor(fullProvider(), cancellation);

        List<Collected> collected = new java.util.concurrent.CopyOnWriteArrayList<>();
        Thread call = new Thread(() -> proxy.forwardStream("completion", "POST",
                "{\"messages\":[],\"stream\":true}".getBytes(StandardCharsets.UTF_8),
                (status, contentType, chunk, last) -> collected.add(new Collected(status, contentType, chunk, last)),
                "run-silent"));
        call.start();
        call.join(1_500);
        assertTrue(call.isAlive(), "a silent provider must not be cut by the proxy");

        assertEquals(1, cancellation.abortRun("run-silent"));
        call.join(5_000);
        assertTrue(!call.isAlive(), "cancelling the run must release the parked read");
        assertTrue(collected.get(collected.size() - 1).last, "the consumer still gets its terminal callback");
    }

    /** A buffered call has no time limit either, and is likewise ended by cancelling its run. */
    @Test
    void bufferedCallWithNoAnswerIsEndedByCancellingTheRun() throws Exception {
        hangBody = true;
        LlmStreamCancellationRegistry cancellation = new LlmStreamCancellationRegistry();
        AiModelProviderProxy proxy = proxyFor(fullProvider(), cancellation);

        java.util.concurrent.atomic.AtomicReference<AiModelProviderProxy.ProxyResult> result =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread call = new Thread(() -> result.set(proxy.forward("completion", "POST", null,
                "{\"messages\":[]}".getBytes(StandardCharsets.UTF_8), "run-buffered")));
        call.start();
        call.join(1_500);
        assertTrue(call.isAlive(), "a slow provider must not be cut by the proxy");

        assertEquals(1, cancellation.abortRun("run-buffered"));
        call.join(5_000);
        assertTrue(!call.isAlive(), "cancelling the run must release the waiting call");
        assertEquals(502, result.get().status());
        String body = new String(result.get().body(), StandardCharsets.UTF_8);
        assertTrue(body.contains("its run was cancelled"), body);
    }

    /** A slow-but-alive stream (gaps between chunks) is delivered in full with a clean terminator. */
    @Test
    void forwardStreamDeliversASlowStreamInFull() throws Exception {
        providerContentType = "text/event-stream";
        String sse = "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n"
                + "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n"
                + "data: [DONE]\n\n";
        dripChunks = new byte[][]{
                "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n".getBytes(StandardCharsets.UTF_8),
                "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n".getBytes(StandardCharsets.UTF_8),
                "data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8)};
        dripGapMs = 150L;
        AiModelProviderProxy proxy = proxyFor(fullProvider());

        List<Collected> collected = new ArrayList<>();
        proxy.forwardStream("completion", "POST",
                "{\"messages\":[],\"stream\":true}".getBytes(StandardCharsets.UTF_8),
                (status, contentType, chunk, last) ->
                        collected.add(new Collected(status, contentType, chunk, last)));

        assertEquals(sse, new String(concatData(collected), StandardCharsets.UTF_8),
                "every dripped byte must arrive");
        Collected last = collected.get(collected.size() - 1);
        assertTrue(last.last);
        assertEquals(200, last.status);
        assertEquals(0, last.chunk.length, "clean EOF terminator, not a stall error");
    }

    // ---- chatRequestSummary -------------------------------------------------

    @Test
    void chatRequestSummaryShowsParamsShapeAndAttachmentMarkersNotContent() throws Exception {
        JsonNode body = mapper.readTree("""
                {
                  "model": "gpt-x", "stream": true, "temperature": 0.7, "max_tokens": 512,
                  "api_key": "sk-should-never-appear",
                  "messages": [
                    {"role": "system", "content": "you are helpful"},
                    {"role": "user", "content": [
                      {"type": "text", "text": "dosyaiceriginde ne var?"},
                      {"type": "text", "text": "[File 'costs.xlsx' (application/vnd...sheet), extracted text]\\nname\\tage\\nAda\\t36"},
                      {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8=", "detail": "auto"}}
                    ]}
                  ]
                }
                """);
        String summary = AiModelProviderProxy.chatRequestSummary(body, true);
        assertTrue(summary.contains("model=gpt-x"), summary);
        assertTrue(summary.contains("stream=true"), summary);
        assertTrue(summary.contains("temperature=0.7"), summary);
        assertTrue(summary.contains("max_tokens=512"), summary);
        assertTrue(summary.contains("messages=2"), summary);
        assertTrue(summary.contains("system:text(chars=15)"), summary);
        assertTrue(summary.contains("[File 'costs.xlsx'"), summary);
        assertTrue(summary.contains("image_url(data-url"), summary);
        // Content-free contract: no prompt prose, no credential-shaped fields.
        assertFalse(summary.contains("dosyaiceriginde"), summary);
        assertFalse(summary.contains("you are helpful"), summary);
        assertFalse(summary.contains("sk-should-never-appear"), summary);
        assertFalse(summary.contains("api_key"), summary);
    }

    @Test
    void chatRequestSummaryToleratesUnparsedAndEmptyBodies() {
        assertEquals("model=<unparsed> stream=false messages=?",
                AiModelProviderProxy.chatRequestSummary(null, false));
        String summary = AiModelProviderProxy.chatRequestSummary(mapper.createObjectNode(), false);
        assertTrue(summary.contains("messages=none"), summary);
    }

    private static byte[] concatData(List<Collected> chunks) {
        ByteArrayOutputStream acc = new ByteArrayOutputStream();
        for (Collected c : chunks) {
            if (!c.last && c.chunk != null && c.chunk.length > 0) {
                acc.writeBytes(c.chunk);
            }
        }
        return acc.toByteArray();
    }

    /** Test-side capture of a {@link AiModelProviderProxy.StreamConsumer} callback. */
    private static final class Collected {
        final int status;
        final String contentType;
        final byte[] chunk;
        final boolean last;

        Collected(int status, String contentType, byte[] chunk, boolean last) {
            this.status = status;
            this.contentType = contentType;
            this.chunk = chunk == null ? new byte[0] : chunk;
            this.last = last;
        }
    }
}
