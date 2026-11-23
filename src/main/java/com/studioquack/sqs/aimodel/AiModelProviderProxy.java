package com.studioquack.sqs.aimodel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Daemon-side proxy that routes in-sandbox {@code studioquack.llm.*} calls (arriving on the
 * loopback {@code /v1/helper/llm/...} helper channel) to the OpenAI-compatible provider held live
 * in {@link AiModelProviderHolder}. SQS-Q-01 seams (keep them distinct when this class is
 * touched): authz (who may dial — {@code ProviderDialGuard}), dispatch (local vs overlay peer),
 * provider HTTP (headers, stream, status verbatim).
 */
@Slf4j
@Component
public class AiModelProviderProxy {

    /** Empty body used for null chunks and the stream terminator. */
    private static final byte[] EMPTY = new byte[0];

    /** Bounded SSE/body read size so a long generation is not held whole on the heap. */
    private static final int STREAM_CHUNK_BYTES = 8192;

    /** Chat-body fields never echoed into the request-summary log line. */
    private static final java.util.Set<String> SENSITIVE_PARAMS = java.util.Set.of(
            "api_key", "apikey", "authorization", "auth", "access_token", "refresh_token",
            "id_token", "client_secret", "password");

    private final AiModelProviderHolder holder;
    private final AiModelLoadState loadState;
    private final AiModelCapabilityState capabilityState;
    private final ObjectMapper mapper;

    private final LlmStreamCancellationRegistry streamCancellation;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Autowired
    public AiModelProviderProxy(AiModelProviderHolder holder, AiModelLoadState loadState,
                                AiModelCapabilityState capabilityState, ObjectMapper mapper,
                                LlmStreamCancellationRegistry streamCancellation) {
        this.holder = holder;
        this.loadState = loadState;
        this.capabilityState = capabilityState == null ? new AiModelCapabilityState() : capabilityState;
        this.mapper = mapper;
        this.streamCancellation = streamCancellation;
    }

    /** Explicit constructor with capability state and no cancel registry. */
    public AiModelProviderProxy(AiModelProviderHolder holder, AiModelLoadState loadState,
                                AiModelCapabilityState capabilityState, ObjectMapper mapper) {
        this(holder, loadState, capabilityState, mapper, null);
    }


    public AiModelProviderProxy(AiModelProviderHolder holder, AiModelLoadState loadState,
                                ObjectMapper mapper) {
        this(holder, loadState, new AiModelCapabilityState(), mapper);
    }

    /**
     * Backwards-compatible constructor for call sites (and existing tests) that predate the
     * loaded-model guard: a private {@link AiModelLoadState} means "nothing recorded as loaded",
     * which only matters for MANAGED providers (load path configured).
     */
    public AiModelProviderProxy(AiModelProviderHolder holder, ObjectMapper mapper) {
        this(holder, new AiModelLoadState(), new AiModelCapabilityState(), mapper);
    }

    /**
     * Forwards an llm helper-channel call to the configured provider.
     *
     * @param llmSubPath The path AFTER {@code /v1/helper/llm/} (e.g. {@code completion},
     * {@code models}, {@code download/status})
     * @param method The inbound HTTP method (informational; the upstream verb is fixed per op so a
     * stray verb can't reshape the provider call)
     * @param body The raw request body bytes (may be {@code null}/empty for GETs)
     * @return The normalised {@link ProxyResult} the helper channel writes back verbatim
     */
    public ProxyResult forward(String llmSubPath, String method, byte[] body) {
        return forward(llmSubPath, method, null, body);
    }

    /**
     * {@link #forward(String, String, byte[])} plus the raw query string the helper channel
     * carried ({@code job_id=…} on {@code download/status}); {@code null} for a bare op.
     */
    public ProxyResult forward(String llmSubPath, String method, String query, byte[] body) {
        return forward(llmSubPath, method, query, body, null);
    }

    /**
     * Aborts every in-flight provider call registered under {@code runKey} — how the overlay peer
     * listener honours a caller's cancel for a cross-env llm call it is serving.
     *
     * @return how many calls were aborted
     */
    public int abortRun(String runKey) {
        return streamCancellation == null ? 0 : streamCancellation.abortRun(runKey);
    }

    /**
     * {@link #forward(String, String, String, byte[])} plus the owning run's
     * {@code workflowExecutionId}: the call is registered in {@link LlmStreamCancellationRegistry}
     * so {@code cancel-instance} ends it — there is no time limit that would.
     */
    public ProxyResult forward(String llmSubPath, String method, String query, byte[] body, String runKey) {
        PreparedCall prepared = prepare(llmSubPath, method, query, body, false);
        if (prepared.error() != null) {
            return prepared.error();
        }
        try {
            return sendBuffered(prepared, method, runKey);
        } catch (Exception e) {
            return unreachable(method, prepared.op(), e);
        }
    }

    /**
     * Same routing, loaded-model guard, sanitise and auth-header contract as {@link #forward}, but
     * optionally streams the provider body when the chat JSON has {@code stream=true} after boolean
     * coerce. When {@code stream} is absent, false, or unparseable this is one buffered
     * {@link #forward} delivered as a single {@code last=true} callback.
     *
     * @param llmSubPath The path AFTER {@code /v1/helper/llm/}
     * @param method Inbound HTTP method (informational; upstream verb is fixed per op)
     * @param body Raw request body (may be {@code null}/empty for GETs)
     * @param consumer Receives each chunk then a terminator; must not be {@code null}
     */
    public void forwardStream(String llmSubPath, String method, byte[] body, StreamConsumer consumer) {
        forwardStream(llmSubPath, method, body, consumer, null);
    }

    /**
     * {@link #forwardStream(String, String, byte[], StreamConsumer)} plus the owning run's
     * {@code workflowExecutionId}.
     */
    public void forwardStream(String llmSubPath, String method, byte[] body, StreamConsumer consumer,
                              String runKey) {
        forwardStream(llmSubPath, method, body, null, consumer, runKey);
    }

    /**
     * {@link #forwardStream(String, String, byte[], StreamConsumer, String)} plus the raw query
     * string the helper channel carried; {@code null} for a bare op.
     */
    public void forwardStream(String llmSubPath, String method, byte[] body, String query,
                              StreamConsumer consumer, String runKey) {
        Objects.requireNonNull(consumer, "consumer");
        PreparedCall prepared = prepare(llmSubPath, method, query, body, true);
        if (prepared.error() != null) {
            emit(consumer, prepared.error());
            return;
        }
        if (!prepared.stream()) {
            try {
                emit(consumer, sendBuffered(prepared, method, runKey));
            } catch (Exception e) {
                emit(consumer, unreachable(method, prepared.op(), e));
            }
            return;
        }
        final HttpResponse<InputStream> resp;
        try {
            resp = send(prepared.request(), HttpResponse.BodyHandlers.ofInputStream(), runKey);
        } catch (Exception e) {
            // Connection refused / DNS / run cancelled — same 502 JSON as {@link #forward}, one shot.
            emit(consumer, unreachable(method, prepared.op(), e));
            return;
        }
        String contentType = resp.headers().firstValue("Content-Type").orElse("application/json");
        int status = resp.statusCode();
        InputStream in = resp.body();
        log.debug("llm-proxy: stream {} {} -> upstream status={} contentType={}",
                method, prepared.op(), status, contentType);
        if (streamCancellation != null) {
            streamCancellation.register(runKey, in);
        }
        try {
            pumpStream(status, contentType, in, consumer);
        } catch (Exception e) {
            log.debug("llm-proxy: stream {} {} read failed: {}", method, prepared.op(), e.toString());
        } finally {
            if (streamCancellation != null) {
                streamCancellation.unregister(runKey, in);
            }
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ignored) {
                    // already drained or the provider hung up
                }
            }
        }
    }

    /**
     * {@code allowStream=false} keeps today's buffered contract ({@code stream=true} forced to
     * {@code false}). {@code allowStream=true} leaves {@code stream=true} after coerce and sets
     * {@code Accept} to prefer {@code text/event-stream}.
     */
    private PreparedCall prepare(String llmSubPath, String method, String query, byte[] body, boolean allowStream) {
        AiModelProviderConfig config = holder.current();
        if (!config.configured()) {
            return PreparedCall.fail(error(503, "provider_not_configured",
                    "No AI Model provider is configured for this environment. Set one on "
                            + "Environment Settings → AI Model."));
        }

        String op = normalise(llmSubPath);
        Route route = routeFor(op, config);
        if (route == null) {
            return PreparedCall.fail(error(404, "unknown_llm_endpoint", "No such llm endpoint: " + op));
        }
        if (route.url() == null) {
            return PreparedCall.fail(error(501, "unsupported_llm_op",
                    "The configured provider does not expose a '" + op + "' path. Configure it on "
                            + "Environment Settings → AI Model, or use a provider that supports it."));
        }
        String blockedHost = ProviderDialGuard.refusalIfPresent(route.url());
        if (blockedHost != null) {
            return PreparedCall.fail(error(403, "provider_host_not_allowed", blockedHost));
        }
        // LM Studio keys download status on a PATH-embedded job id
        // (/api/v1/models/download/status/{job_id}); the helper channel can't carry a path
        // segment, so the job id arrives as ?job_id= and is spliced onto the configured base.
        String url = route.url();
        if ("download/status".equals(op) || "download-status".equals(op)) {
            String jobId = queryParam(query, "job_id");
            if (jobId != null) {
                url = joinPath(url, urlEncode(jobId));
            }
        }

        try {
            JsonNode parsedBody = null;
            boolean stream = false;
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url));
            // NEVER log the key — only inject it on the wire.
            if (config.apiKey() != null && !config.apiKey().isBlank()) {
                builder.header("Authorization", "Bearer " + config.apiKey());
            }
            if (route.post()) {
                byte[] payload = (body == null || body.length == 0)
                        ? "{}".getBytes(StandardCharsets.UTF_8) : body;
                // Chat path only: (1) the no-JIT guard — a managed provider never loads a model
                // mid-run, a not-loaded model is refused fast with an actionable error; (2)
                // sanitise response_format so a strict OpenAI-compatible provider (e.g. LM Studio)
                // never 400s on an unsupported type. Both work on ONE parse of the body.
                if (isChatOp(op)) {
                    parsedBody = parseOrNull(payload);
                    ProxyResult refused = enforceModelLoaded(config, parsedBody);
                    if (refused != null) {
                        return PreparedCall.fail(refused);
                    }
                    payload = normaliseChatBody(parsedBody, payload, allowStream);
                    stream = allowStream && parsedBody != null
                            && parsedBody.path("stream").asBoolean(false);
                    log.info("llm-proxy: chat request {}", chatRequestSummary(parsedBody, stream));
                }
                builder.header("Content-Type", "application/json");
                builder.POST(HttpRequest.BodyPublishers.ofByteArray(payload));
            } else {
                builder.GET();
            }
            builder.header("Accept", stream
                    ? "text/event-stream, application/json"
                    : "application/json");
            return PreparedCall.ok(op, parsedBody, body, builder.build(), stream);
        } catch (Exception e) {
            return PreparedCall.fail(unreachable(method, op, e));
        }
    }

    /**
     * Sends {@code request} with no time limit. While it is in flight it is registered under
     * {@code runKey}, so {@code cancel-instance} releases the waiting thread at once and asks the
     * client to abort the exchange (the provider sees the disconnect).
     */
    private <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler,
                                     String runKey) throws Exception {
        CompletableFuture<HttpResponse<T>> call = http.sendAsync(request, handler);
        AtomicBoolean aborted = new AtomicBoolean();
        Closeable abort = () -> {
            aborted.set(true);
            call.cancel(true);
        };
        if (streamCancellation != null) {
            streamCancellation.register(runKey, abort);
        }
        try {
            return call.get();
        } catch (ExecutionException | CancellationException e) {
            // The JDK client fails a cancelled exchange with its own "Request cancelled" IOException.
            if (aborted.get()) {
                throw new IOException("LLM call aborted: its run was cancelled", e);
            }
            throw e instanceof ExecutionException ee && ee.getCause() instanceof Exception cause ? cause : e;
        } catch (InterruptedException e) {
            call.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            if (streamCancellation != null) {
                streamCancellation.unregister(runKey, abort);
            }
        }
    }

    /** Buffered provider round-trip used by {@link #forward} and non-SSE {@link #forwardStream}. */
    private ProxyResult sendBuffered(PreparedCall prepared, String method, String runKey) throws Exception {
        HttpResponse<byte[]> resp = send(prepared.request(), HttpResponse.BodyHandlers.ofByteArray(), runKey);
        com.studioquack.sqs.transport.TransferByteMeter.add(
                prepared.inboundBody() == null ? 0 : prepared.inboundBody().length);
        com.studioquack.sqs.transport.TransferByteMeter.add(resp.body() == null ? 0 : resp.body().length);
        String contentType = resp.headers().firstValue("Content-Type").orElse("application/json");
        byte[] respBody = resp.body() == null ? EMPTY : resp.body();
        log.debug("llm-proxy: {} {} -> upstream status={} bytes={}",
                method, prepared.op(), resp.statusCode(), respBody.length);
        // A successful operator/script load/unload keeps the daemon's loaded record in step
        // with the provider, so the settings toggles and the picker stay truthful.
        if (resp.statusCode() >= 200 && resp.statusCode() < 300
                && ("load".equals(prepared.op()) || "unload".equals(prepared.op()))) {
            trackLoadState(prepared.op(),
                    prepared.parsedBody() != null ? prepared.parsedBody() : parseOrNull(prepared.inboundBody()));
        }
        return new ProxyResult(resp.statusCode(), contentType, respBody);
    }

    /**
     * Reads {@code in} in {@link #STREAM_CHUNK_BYTES} slices. Each data slice is {@code last=false};
     * exactly ONE terminal callback always runs so the consumer can close its side: the trailing empty
     * {@code last=true} on EOF or on a read failure (the run's cancel closing the stream included).
     */
    private void pumpStream(int status, String contentType, InputStream in, StreamConsumer consumer)
            throws IOException {
        try {
            if (in != null) {
                byte[] buf = new byte[STREAM_CHUNK_BYTES];
                int n;
                while ((n = in.read(buf)) != -1) {
                    if (n <= 0) {
                        continue;
                    }
                    byte[] chunk = Arrays.copyOf(buf, n);
                    com.studioquack.sqs.transport.TransferByteMeter.add(n);
                    log.debug("llm-proxy: stream chunk status={} contentType={} bytes={}",
                            status, contentType, n);
                    consumer.accept(status, contentType, chunk, false);
                }
            }
        } finally {
            consumer.accept(status, contentType, EMPTY, true);
        }
    }

    /** One-shot consumer delivery of a buffered {@link ProxyResult} ({@code last=true}). */
    private static void emit(StreamConsumer consumer, ProxyResult result) {
        byte[] chunk = result.body() == null ? EMPTY : result.body();
        consumer.accept(result.status(), result.contentType(), chunk, true);
    }

    /**
     * Connection refused / timeout / DNS — the provider is unreachable. 502 with a
     * compact reason (never the URL host details beyond the message the JDK gives us).
     */
    private ProxyResult unreachable(String method, String op, Exception e) {
        log.debug("llm-proxy: {} {} failed: {}", method, op, e.toString());
        return error(502, "provider_unreachable",
                "The AI Model provider could not be reached: "
                        + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
    }

    /** Resolves the sub-path to a provider URL + upstream verb, or {@code null} for an unknown op. */
    private Route routeFor(String op, AiModelProviderConfig config) {
        return switch (op) {
            case "completion", "chat", "chat/completions", "completions" -> new Route(config.chatUrl(), true);
            case "models" -> new Route(config.modelsUrl(), false);
            case "load" -> new Route(config.loadUrl(), true);
            case "unload" -> new Route(config.unloadUrl(), true);
            case "download" -> new Route(config.downloadUrl(), true);
            case "download/status", "download-status" -> new Route(config.downloadStatusUrl(), false);
            default -> null;
        };
    }

    /** One query value off a raw query string ({@code job_id=x&y=z}), or null. */
    private static String queryParam(String query, String key) {
        if (query == null || query.isBlank()) {
            return null;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && pair.substring(0, eq).equals(key) && eq < pair.length() - 1) {
                return pair.substring(eq + 1);
            }
        }
        return null;
    }

    /** Appends one path segment, tolerant of a trailing slash on the base. */
    private static String joinPath(String base, String segment) {
        return (base.endsWith("/") ? base.substring(0, base.length() - 1) : base) + "/" + segment;
    }

    /** Minimal path-segment encoder (encodes everything except unreserved chars). */
    private static String urlEncode(String value) {
        try {
            return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8)
                    .replace("+", "%20");
        } catch (Exception e) {
            return value;
        }
    }

    /** Chat/completions ops whose body carries a {@code response_format} worth sanitising. */
    private static boolean isChatOp(String op) {
        return switch (op) {
            case "completion", "chat", "chat/completions", "completions" -> true;
            default -> false;
        };
    }

    /**
     * The no-JIT guard at the heart of the managed-model contract: when the provider exposes explicit
     * load management ({@link AiModelProviderConfig#loadUrl} non-null) a chat call naming a model the
     * daemon has NOT recorded as loaded is refused. Bodies without a {@code model} field forward
     * untouched (the provider's default applies), and UNMANAGED providers are never gated — a plain
     * OpenAI-compatible endpoint has no load concept to violate.
     *
     * @return The refusal result, or {@code null} when the call may proceed
     */
    private ProxyResult enforceModelLoaded(AiModelProviderConfig config, JsonNode body) {
        // Gate only a MANAGED provider (the daemon owns load/unload). A PROVIDER_MANAGED one — vLLM's
        // fixed model, Ollama/Unsloth JIT — owns residency itself, so gating would wrongly refuse a model
        // the provider will happily serve.
        boolean managed = capabilityState.isProbed() ? capabilityState.managed() : config.loadUrl() != null;
        if (!managed || body == null || !body.isObject()) {
            return null;
        }
        JsonNode modelNode = body.get("model");
        if (modelNode == null || !modelNode.isTextual() || modelNode.asText().isBlank()) {
            return null;
        }
        String model = modelNode.asText();
        if (loadState.isLoaded(model)) {
            return null;
        }
        log.info("llm-proxy: refused chat for not-loaded model '{}' (managed provider)", model);
        return error(409, "model_not_loaded",
                "Model '" + model + "' is not loaded on the configured provider. Load it from "
                        + "Engine Settings → AI Model first — an execution never loads models on demand.");
    }

    /** Keeps {@link AiModelLoadState} in step after a successful load/unload forward. */
    private void trackLoadState(String op, JsonNode body) {
        if (body == null || !body.isObject()) {
            return;
        }
        // load payloads key on `model`; unload payloads may key on `instance_id` instead.
        String model = null;
        JsonNode modelNode = body.get("model");
        if (modelNode != null && modelNode.isTextual() && !modelNode.asText().isBlank()) {
            model = modelNode.asText();
        } else {
            JsonNode instanceNode = body.get("instance_id");
            if (instanceNode != null && instanceNode.isTextual() && !instanceNode.asText().isBlank()) {
                model = instanceNode.asText();
            }
        }
        if (model == null) {
            return;
        }
        if ("load".equals(op)) {
            loadState.markLoaded(model);
        } else {
            loadState.markUnloaded(model);
        }
    }

    /** Parses a request body to a tree, or {@code null} when it is not JSON we can reason about. */
    private JsonNode parseOrNull(byte[] body) {
        if (body == null || body.length == 0) {
            return null;
        }
        try {
            return mapper.readTree(body);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Normalises the outbound chat body's {@code response_format} so a strict OpenAI-compatible
     * provider accepts it. {@code root} is the already-parsed {@code body} (the caller parses once for
     * the no-JIT guard and reuses the tree here); a {@code null}/non-object root returns the body
     * verbatim.
     *
     * @param allowStream {@code false} (buffered {@link #forward}) forces {@code stream=true} to
     * {@code false} after boolean coerce; {@code true} leaves {@code stream=true} so
     * {@link #forwardStream} can consume SSE
     */
    private byte[] normaliseChatBody(JsonNode root, byte[] body, boolean allowStream) {
        if (body == null || body.length == 0) {
            return body;
        }
        if (root == null || !root.isObject()) {
            return body;
        }
        ObjectNode obj = (ObjectNode) root;
        boolean changed = false;

        // (a) response_format → a provider-accepted shape (rewrite/drop).
        if (obj.has("response_format")) {
            JsonNode current = obj.get("response_format");
            JsonNode normalised = normaliseResponseFormat(current);
            if (normalised == null) {
                obj.remove("response_format");
                changed = true;
            } else if (!normalised.equals(current)) {
                obj.set("response_format", normalised);
                changed = true;
            }
            if (changed) {
                log.debug("llm-proxy: normalised response_format {} -> {}",
                        current, normalised == null ? "<dropped>" : normalised.path("type").asText());
            }
        }

        // (b) reasoning knobs are string enums on every OpenAI-compatible provider; a picker
        // value serialised as a number/boolean makes the provider 400 with
        // "'reasoning_effort' must be a string". The daemon owns the provider contract, so we
        // repair the TYPE here and keep the sandbox node provider-agnostic.
        changed |= coerceToStringField(obj, "reasoning_effort");
        changed |= coerceToStringField(obj, "verbosity");

        // Node parameters are stored as STRINGS (the designer has no typed inputs), and a strict provider
        // 400s on a stringly-typed scalar — e.g. LM Studio: "'logprobs' must be a boolean or null". We
        // coerce the documented boolean/integer/number fields; junk values are DROPPED so the provider
        // applies its own default instead of rejecting the call outright.
        changed |= coerceToBooleanField(obj, "logprobs");
        changed |= coerceToBooleanField(obj, "parallel_tool_calls");
        changed |= coerceToBooleanField(obj, "stream");
        changed |= coerceToBooleanField(obj, "store");
        changed |= coerceToIntField(obj, "top_logprobs");
        changed |= coerceToIntField(obj, "max_tokens");
        changed |= coerceToIntField(obj, "max_completion_tokens");
        changed |= coerceToIntField(obj, "n");
        changed |= coerceToIntField(obj, "seed");
        changed |= coerceToNumberField(obj, "temperature");
        changed |= coerceToNumberField(obj, "top_p");
        changed |= coerceToNumberField(obj, "frequency_penalty");
        changed |= coerceToNumberField(obj, "presence_penalty");

        // The ai-agent builds tools in the StudioQuack-internal form {name, description, request, node_id,
        // port_value, aliases} (the JSON Schema rides `request`) and records assistant tool_calls as {id,
        // name, args}. The proxy owns the provider contract, so reconcile here: internal entries become
        // {type:"function", function:{name, ...}}; entries already in function shape pass through.
        changed |= normaliseTools(obj);
        changed |= normaliseHistoryToolCalls(obj);

        // (d) Buffered helper-channel calls cannot deliver tokens incrementally, so
        // {@link #forward} ({@code allowStream=false}) still forces stream=true → false.
        // {@link #forwardStream} ({@code allowStream=true}) keeps stream=true after the
        // boolean coerce above so the provider can emit SSE.
        if (!allowStream && obj.path("stream").asBoolean(false)) {
            obj.put("stream", false);
            changed = true;
            log.debug("llm-proxy: stream=true normalised to false (helper channel is request/response)");
        }

        if (!changed) {
            return body; // nothing to reconcile — forward verbatim, no reserialise
        }
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (Exception e) {
            return body;
        }
    }

    /**
     * Converts the chat body's {@code tools} to the OpenAI function-calling shape a strict
     * OpenAI-compatible provider validates against. The ai-agent emits tools in the
     * StudioQuack-internal form {@code {name, description, request, node_id, port_value,
aliases}} —
     * the JSON Schema rides {@code request}; the rest is agent-side routing metadatthe provider must
     * never see.
     */
    private boolean normaliseTools(ObjectNode obj) {
        JsonNode tools = obj.get("tools");
        if (tools == null || !tools.isArray() || tools.isEmpty()) {
            return false;
        }
        boolean anyConverted = false;
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode tool : tools) {
            JsonNode converted = toOpenAiTool(tool);
            anyConverted |= (converted != tool);
            out.add(converted);
        }
        if (anyConverted) {
            obj.set("tools", out);
            log.debug("llm-proxy: normalised {} tool definition(s) to OpenAI function shape", tools.size());
        }
        return anyConverted;
    }

    /**
     * OpenAI function names must match {@code ^[a-zA-Z0-9_-]+$} (max 64). Tool/data-port names are
     * user- or node-configured and may carry spaces or punctuation, which a strict provider rejects
     * wholesale — so map any other char onto {@code _} and collapse the runs.
     */
    private static String sanitiseToolName(String name) {
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            sb.append(Character.isLetterOrDigit(c) || c == '_' || c == '-' ? c : '_');
        }
        String out = sb.toString().replaceAll("_+", "_").replaceAll("^_+|_+$", "");
        if (out.length() > 64) {
            out = out.substring(0, 64);
        }
        return out.isEmpty() ? "tool" : out;
    }

    /** Maps one internal tool definition to {@code {type:"function", function:{…}}}; identity when already so. */
    private JsonNode toOpenAiTool(JsonNode tool) {
        if (tool == null || !tool.isObject()) {
            return tool;
        }
        if ("function".equals(tool.path("type").asText(null)) && tool.has("function")) {
            return tool; // already OpenAI function shape
        }
        String name = tool.path("name").asText(null);
        if (name == null || name.isBlank()) {
            return tool; // unrecognised shape — leave for the provider to judge
        }
        ObjectNode fn = mapper.createObjectNode();
        fn.put("name", sanitiseToolName(name));
        JsonNode description = tool.get("description");
        if (description != null && description.isTextual()) {
            fn.set("description", description);
        }
        JsonNode schema = firstObjectField(tool, "request", "parameters", "input_schema");
        if (schema != null) {
            fn.set("parameters", schema);
        } else {
            ObjectNode empty = mapper.createObjectNode();
            empty.put("type", "object");
            empty.set("properties", mapper.createObjectNode());
            fn.set("parameters", empty);
        }
        ObjectNode wrapped = mapper.createObjectNode();
        wrapped.put("type", "function");
        wrapped.set("function", fn);
        return wrapped;
    }

    /**
     * Converts assistant {@code tool_calls} recorded in the conversation {@code messages} to the
     * OpenAI shape. The agent stores them internally as {@code {id, name, args}} (args a map or JSON
     * string); OpenAI expects {@code {id, type:"function", function:{name,
arguments:<json-string>}}}.
     */
    private boolean normaliseHistoryToolCalls(ObjectNode obj) {
        JsonNode messages = obj.get("messages");
        if (messages == null || !messages.isArray()) {
            return false;
        }
        boolean changed = false;
        for (JsonNode msg : messages) {
            if (msg == null || !msg.isObject()) {
                continue;
            }
            JsonNode toolCalls = msg.get("tool_calls");
            if (toolCalls == null || !toolCalls.isArray() || toolCalls.isEmpty()) {
                continue;
            }
            boolean msgChanged = false;
            ArrayNode out = mapper.createArrayNode();
            for (JsonNode tc : toolCalls) {
                JsonNode converted = toOpenAiToolCall(tc);
                msgChanged |= (converted != tc);
                out.add(converted);
            }
            if (msgChanged) {
                ((ObjectNode) msg).set("tool_calls", out);
                changed = true;
            }
        }
        return changed;
    }

    /** Maps one internal tool_call {@code {id, name, args}} to the OpenAI shape; identity when already so. */
    private JsonNode toOpenAiToolCall(JsonNode tc) {
        if (tc == null || !tc.isObject()) {
            return tc;
        }
        if (tc.has("function")) {
            return tc; // already OpenAI shape
        }
        String name = tc.path("name").asText(null);
        if (name == null || name.isBlank()) {
            return tc;
        }
        ObjectNode fn = mapper.createObjectNode();
        fn.put("name", sanitiseToolName(name));
        fn.put("arguments", argsAsJsonString(tc.get("args")));
        ObjectNode wrapped = mapper.createObjectNode();
        if (tc.has("id")) {
            wrapped.set("id", tc.get("id"));
        }
        wrapped.put("type", "function");
        wrapped.set("function", fn);
        return wrapped;
    }

    /** OpenAI {@code arguments} is a JSON string; the agent's {@code args} may be a map or already a string. */
    private String argsAsJsonString(JsonNode args) {
        if (args == null || args.isNull()) {
            return "{}";
        }
        if (args.isTextual()) {
            return args.asText();
        }
        try {
            return mapper.writeValueAsString(args);
        } catch (Exception e) {
            return "{}";
        }
    }

    /** First object-typed field among {@code keys}, else null. */
    private static JsonNode firstObjectField(JsonNode node, String... keys) {
        for (String k : keys) {
            JsonNode v = node.get(k);
            if (v != null && v.isObject()) {
                return v;
            }
        }
        return null;
    }

    /**
     * Content-free, single-line summary of an outbound chat request for the daemon log: model, stream
     * flag, scalar sampling params, and a per-message structural descriptor (role, text length, part
     * kinds, attachment name/mime/chars). Prompt bodies and API keys are NEVER logged — the summary
     * lets Engine Settings → Logs answer "what did the node send the model?" (params, message shape,
     * whether a file's text rode along) without persisting prompt content.
     */
    public static String chatRequestSummary(JsonNode body, boolean stream) {
        if (body == null || !body.isObject()) {
            return "model=<unparsed> stream=" + stream + " messages=?";
        }
        StringBuilder sb = new StringBuilder(256);
        sb.append("model=").append(body.path("model").asText(""));
        sb.append(" stream=").append(stream);
        StringBuilder params = new StringBuilder();
        body.fields().forEachRemaining(e -> {
            String k = e.getKey();
            JsonNode v = e.getValue();
            if ("messages".equals(k) || "tools".equals(k) || "tool_choice".equals(k)
                    || "model".equals(k) || "stream".equals(k)) {
                return;
            }
            String lk = k.toLowerCase(Locale.ROOT);
            if (SENSITIVE_PARAMS.contains(lk) || lk.endsWith("_key") || lk.endsWith("_token")
                    || lk.endsWith("_secret")) {
                return; // never echo credential-shaped fields (max_tokens etc. stay)
            }
            if (v.isNumber() || v.isBoolean()) {
                params.append(params.length() == 0 ? "" : ",").append(k).append('=').append(v.asText());
            } else if (v.isTextual()) {
                String t = v.asText();
                params.append(params.length() == 0 ? "" : ",").append(k).append('=')
                        .append(t.length() <= 60 ? t : t.substring(0, 60) + "…");
            }
        });
        if (params.length() > 0) {
            sb.append(" params{").append(params).append('}');
        }
        JsonNode messages = body.path("messages");
        if (!messages.isArray()) {
            sb.append(" messages=none");
            return sb.toString();
        }
        sb.append(" messages=").append(messages.size()).append(" [");
        int i = 0;
        for (JsonNode m : messages) {
            if (i++ >= 24) {
                sb.append(", +").append(messages.size() - 24).append(" more");
                break;
            }
            if (i > 1) {
                sb.append(", ");
            }
            sb.append(m.path("role").asText("?")).append(':');
            appendContentDescriptor(sb, m.path("content"));
            JsonNode files = m.path("files");
            if (files.isArray() && !files.isEmpty()) {
                sb.append("+files(").append(files.size()).append(')');
            }
        }
        sb.append(']');
        return sb.toString();
    }

    /** Structural descriptor for one message {@code content} value — lengths and kinds only. */
    private static void appendContentDescriptor(StringBuilder sb, JsonNode content) {
        if (content.isTextual()) {
            sb.append("text(chars=").append(content.asText().length()).append(')');
            return;
        }
        if (content.isArray()) {
            sb.append("parts[");
            int i = 0;
            for (JsonNode part : content) {
                if (i++ >= 12) {
                    sb.append(", +").append(content.size() - 12).append(" more");
                    break;
                }
                if (i > 1) {
                    sb.append(", ");
                }
                String type = part.path("type").asText("?");
                sb.append(type);
                if ("text".equals(type)) {
                    String t = part.path("text").asText("");
                    sb.append('(').append(t.length()).append(" chars");
                    // Platform-generated markers are safe to quote — they carry the file name
                    // and the delivery decision (inlined / extracted / dropped), not user prose.
                    if (t.startsWith("[File '") || t.startsWith("[Attachment '") || t.startsWith("[Document '")) {
                        int nl = t.indexOf('\n');
                        String marker = nl < 0 ? t : t.substring(0, nl);
                        sb.append(" \"").append(marker.length() <= 100 ? marker : marker.substring(0, 100) + "…")
                                .append('"');
                    }
                    sb.append(')');
                } else if ("image_url".equals(type)) {
                    String url = part.path("image_url").path("url").asText("");
                    sb.append('(').append(url.startsWith("data:")
                            ? "data-url " + url.length() + " chars" : "url").append(')');
                } else if ("file".equals(type)) {
                    sb.append('(').append(part.path("file").path("filename").asText("")).append(')');
                } else if ("input_audio".equals(type)) {
                    sb.append('(').append(part.path("input_audio").path("data").asText("").length())
                            .append(" chars)");
                }
            }
            sb.append(']');
            return;
        }
        if (content.isMissingNode() || content.isNull()) {
            sb.append("-");
            return;
        }
        sb.append(content.getNodeType().name().toLowerCase(Locale.ROOT));
    }

    /**
     * Coerces a scalar chat field to a JSON string in place, returning whether the body was changed.
     * Reasoning knobs such as {@code reasoning_effort} / {@code verbosity} must be STRINGS.
     */
    private boolean coerceToStringField(ObjectNode obj, String field) {
        if (!obj.has(field)) {
            return false;
        }
        JsonNode v = obj.get(field);
        if (v == null || v.isNull() || v.isContainerNode()) {
            obj.remove(field); // null / object / array is never a valid scalar enum
            return true;
        }
        if (v.isTextual()) {
            String trimmed = v.asText().trim();
            if (trimmed.isEmpty()) {
                obj.remove(field);
                return true;
            }
            if (!trimmed.equals(v.asText())) {
                obj.put(field, trimmed);
                return true;
            }
            return false; // already a clean string
        }
        obj.put(field, v.asText()); // number / boolean → correct the type to a string
        log.debug("llm-proxy: coerced {} to string '{}'", field, v.asText());
        return true;
    }

    /**
     * Mirrors {@link #coerceToStringField}: an existing boolean passes; a string (
     * {@code "true"/"1"/"yes"/"on"} → true, {@code "false"/"0"/"no"/"off"} → false) or a number (0 →
     * false, non-zero → true) is converted.
     */
    private boolean coerceToBooleanField(ObjectNode obj, String field) {
        if (!obj.has(field)) {
            return false;
        }
        JsonNode v = obj.get(field);
        if (v == null || v.isNull() || v.isContainerNode()) {
            obj.remove(field);
            return true;
        }
        if (v.isBoolean()) {
            return false;
        }
        Boolean parsed = null;
        if (v.isNumber()) {
            parsed = v.asDouble() != 0d;
        } else if (v.isTextual()) {
            String s = v.asText().trim().toLowerCase(Locale.ROOT);
            if (s.isEmpty()) {
                obj.remove(field);
                return true;
            }
            if (s.equals("true") || s.equals("1") || s.equals("yes") || s.equals("on")) {
                parsed = Boolean.TRUE;
            } else if (s.equals("false") || s.equals("0") || s.equals("no") || s.equals("off")) {
                parsed = Boolean.FALSE;
            }
        }
        if (parsed == null) {
            // Unmappable junk (e.g. "maybe") — drop rather than let the provider 400 the call.
            obj.remove(field);
            log.debug("llm-proxy: dropped unparseable boolean {} ", field);
            return true;
        }
        obj.put(field, parsed.booleanValue());
        log.debug("llm-proxy: coerced {} to boolean {}", field, parsed);
        return true;
    }

    /**
     * Coerces a chat field to a JSON integer in place ({@code "8"} → {@code 8}, {@code 8.0} →
     * {@code 8}), dropping blank/null/container/unparseable values. Same discipline as
     * {@link #coerceToBooleanField}: repair the TYPE, never invent a value.
     */
    private boolean coerceToIntField(ObjectNode obj, String field) {
        if (!obj.has(field)) {
            return false;
        }
        JsonNode v = obj.get(field);
        if (v == null || v.isNull() || v.isContainerNode()) {
            obj.remove(field);
            return true;
        }
        if (v.isIntegralNumber()) {
            return false;
        }
        Long parsed = null;
        if (v.isNumber()) {
            double d = v.asDouble();
            if (d == Math.rint(d) && !Double.isInfinite(d) && !Double.isNaN(d)) {
                parsed = (long) d;
            }
        } else if (v.isTextual()) {
            String s = v.asText().trim();
            if (s.isEmpty()) {
                obj.remove(field);
                return true;
            }
            try {
                parsed = Long.parseLong(s);
            } catch (NumberFormatException notLong) {
                try {
                    double d = Double.parseDouble(s);
                    if (d == Math.rint(d) && !Double.isInfinite(d) && !Double.isNaN(d)) {
                        parsed = (long) d;
                    }
                } catch (NumberFormatException notNumber) {
                    // falls through: unparseable
                }
            }
        }
        if (parsed == null) {
            obj.remove(field);
            log.debug("llm-proxy: dropped unparseable integer {}", field);
            return true;
        }
        obj.put(field, parsed.longValue());
        log.debug("llm-proxy: coerced {} to integer {}", field, parsed);
        return true;
    }

    /**
     * Coerces a chat field to a JSON number in place ({@code "0.7"} → {@code 0.7}), dropping
     * blank/null/container/unparseable values. Same discipline as {@link #coerceToIntField}.
     */
    private boolean coerceToNumberField(ObjectNode obj, String field) {
        if (!obj.has(field)) {
            return false;
        }
        JsonNode v = obj.get(field);
        if (v == null || v.isNull() || v.isContainerNode()) {
            obj.remove(field);
            return true;
        }
        if (v.isNumber()) {
            return false;
        }
        Double parsed = null;
        if (v.isTextual()) {
            String s = v.asText().trim();
            if (s.isEmpty()) {
                obj.remove(field);
                return true;
            }
            try {
                parsed = Double.parseDouble(s);
            } catch (NumberFormatException notNumber) {
                // falls through: unparseable
            }
        } else if (v.isBoolean()) {
            // A boolean where a number belongs is never the caller's intent — drop it.
            parsed = null;
        }
        if (parsed == null) {
            obj.remove(field);
            log.debug("llm-proxy: dropped unparseable number {}", field);
            return true;
        }
        obj.put(field, parsed.doubleValue());
        log.debug("llm-proxy: coerced {} to number {}", field, parsed);
        return true;
    }

    /**
     * Maps any inbound {@code response_format} to a provider-accepted node, or {@code null} to drop
     * it. Accepts the OpenAI object form, the bare string literals ({@code json}/{@code json_object}/
     * {@code text}), and a schema-only object.
     */
    private JsonNode normaliseResponseFormat(JsonNode rf) {
        if (rf == null || rf.isNull()) {
            return null;
        }
        if (rf.isTextual()) {
            String v = rf.asText().trim().toLowerCase(Locale.ROOT);
            return switch (v) {
                case "text" -> textFormat();
                case "json", "json_object", "json_schema" -> jsonSchemaFormat(null);
                default -> null; // unknown literal → drop rather than 400
            };
        }
        if (rf.isObject()) {
            String type = rf.path("type").asText("").trim().toLowerCase(Locale.ROOT);
            switch (type) {
                case "text":
                    return rf; // valid — pass through unchanged
                case "json_schema":
                    // Valid only when it actually carries a json_schema object; else wrap the schema.
                    return rf.has("json_schema") && rf.get("json_schema").isObject()
                            ? rf : jsonSchemaFormat(extractSchema(rf));
                case "json_object":
                    return jsonSchemaFormat(extractSchema(rf));
                case "":
                    if (rf.has("json_schema") && rf.get("json_schema").isObject()) {
                        ObjectNode wrap = mapper.createObjectNode();
                        wrap.put("type", "json_schema");
                        wrap.set("json_schema", rf.get("json_schema"));
                        return wrap;
                    }
                    if (rf.has("schema") && rf.get("schema").isObject()) {
                        return jsonSchemaFormat(rf.get("schema"));
                    }
                    return null; // no type, no schema → drop
                default:
                    return null; // unknown type → drop rather than 400
            }
        }
        return null;
    }

    /** {@code {"type":"text"}} — the provider's plain-text response format. */
    private ObjectNode textFormat() {
        ObjectNode n = mapper.createObjectNode();
        n.put("type", "text");
        return n;
    }

    /**
     * A provider-accepted {@code json_schema} response format wrapping {@code schema} (or a permissive
     * open-object schema when {@code schema} is absent), preserving the "reply with JSON" intent of a
     * {@code json_object} request.
     */
    private ObjectNode jsonSchemaFormat(JsonNode schema) {
        ObjectNode js = mapper.createObjectNode();
        js.put("name", "response");
        js.put("strict", false);
        if (schema != null && schema.isObject()) {
            js.set("schema", schema);
        } else {
            ObjectNode open = mapper.createObjectNode();
            open.put("type", "object");
            open.put("additionalProperties", true);
            js.set("schema", open);
        }
        ObjectNode rf = mapper.createObjectNode();
        rf.put("type", "json_schema");
        rf.set("json_schema", js);
        return rf;
    }

    /** Pulls a usable JSON-schema object out of a caller's response_format, or {@code null}. */
    private JsonNode extractSchema(JsonNode rf) {
        JsonNode js = rf.get("json_schema");
        if (js != null && js.isObject() && js.has("schema") && js.get("schema").isObject()) {
            return js.get("schema");
        }
        JsonNode schema = rf.get("schema");
        return schema != null && schema.isObject() ? schema : null;
    }

    /** Builds a structured JSON error result (bytes) so the SDK sees a parseable body. */
    private ProxyResult error(int status, String code, String detail) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", code);
        body.put("detail", detail);
        byte[] bytes;
        try {
            bytes = mapper.writeValueAsBytes(body);
        } catch (Exception e) {
            bytes = ("{\"error\":\"" + code + "\"}").getBytes(StandardCharsets.UTF_8);
        }
        return new ProxyResult(status, "application/json", bytes);
    }

    private static String normalise(String sub) {
        if (sub == null) {
            return "";
        }
        String s = sub.trim().toLowerCase(Locale.ROOT);
        while (s.startsWith("/")) {
            s = s.substring(1);
        }
        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    /** A resolved upstream target: the absolute URL (nullable for unconfigured optional ops) + verb. */
    private record Route(String url, boolean post) {
    }

    /**
     * Shared request-build outcome: either a structured {@code error} (no upstream call)
     * or a ready {@link HttpRequest}. {@code stream} is true only when {@code allowStream}
     * was set and the chat body has {@code stream=true} after boolean coerce.
     */
    private record PreparedCall(ProxyResult error, String op, JsonNode parsedBody, byte[] inboundBody,
                                HttpRequest request, boolean stream) {
        static PreparedCall fail(ProxyResult error) {
            return new PreparedCall(error, null, null, null, null, false);
        }

        static PreparedCall ok(String op, JsonNode parsedBody, byte[] inboundBody,
                               HttpRequest request, boolean stream) {
            return new PreparedCall(null, op, parsedBody, inboundBody, request, stream);
        }
    }

    /**
     * {@code last=true} is the terminator (empty chunk when the preceding data callback was
     * {@code last=false}).
     */
    @FunctionalInterface
    public interface StreamConsumer {
        void accept(int status, String contentType, byte[] chunk, boolean last);
    }

    /**
     * The normalised proxy outcome the helper channel writes back verbatim:
     * the HTTP {@code status}, the {@code contentType} to echo, and the raw
     * {@code body} bytes (provider body on success, structured JSON error otherwise).
     */
    public record ProxyResult(int status, String contentType, byte[] body) {
    }
}
