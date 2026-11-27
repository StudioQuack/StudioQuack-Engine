package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient;
import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.testrun.TestSandboxRunService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLException;

/**
 * THE helper channel — the in-JVM {@code /v1/helper/*} job registry and request router. All frames
 * arrive over {@link HelperPipeChannel} broker pipes (in-container UDS + podman-exec); the former
 * loopback HTTP adapter is gone.
 */
@Slf4j
@Component
public class HelperRequestRouter {

    private static final String RESERVED_SKIP_CODE = "SQ_SKIP";
    private static final DateTimeFormatter RFC3339 = DateTimeFormatter.ISO_INSTANT;
    /**
     * Fallback for the slice constructors that take no port — mirrors the
     * {@code sqs.overlay.execute-port} default pinned in application-base.yml (8443).
     */
    private static final int DEFAULT_OVERLAY_EXECUTE_PORT = 8443;
    /**
     * Peer-leg connect attempts. The first dial triggers the WireGuard (re-)handshake at the
     * driver level; that handshake keeps running after a connect attempt times out, so a retry
     * window lets a slow/flapping overlay settle instead of failing the run on a transient.
     */
    private static final int PEER_CONNECT_MAX_ATTEMPTS = 4;
    /** Base backoff between peer connect retries — doubled each attempt (1 s, 2 s, 4 s). */
    private static final long PEER_CONNECT_BACKOFF_MS = 1_000L;
    /** Peer SSE relay slice size — mirrors AiModelProviderProxy's bounded 8 KB stream reads. */
    private static final int PEER_STREAM_SLICE_BYTES = 8192;
    /** The OpenAI-style stream terminator frame used when a buffered peer answer is reframed as SSE. */
    private static final byte[] SSE_DONE_FRAME = "data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8);

    /** Cap on per-job {@code llm-request} node-log entries (agent loops can call per iteration). */
    private static final int MAX_LLM_REQUEST_LOGS_PER_JOB = 32;

    /** One routed result: an HTTP-shaped status/body/contentType tuple, transport-independent. */
    public record RoutedResponse(int status, byte[] body, String contentType) {

        public static RoutedResponse json(int status, byte[] body) {
            return new RoutedResponse(status, body == null ? new byte[0] : body, "application/json");
        }

        public static RoutedResponse raw(int status, byte[] body, String contentType) {
            return new RoutedResponse(status, body == null ? new byte[0] : body,
                    contentType == null || contentType.isBlank() ? "application/json" : contentType);
        }
    }

    private final ObjectMapper mapper;
    private final AiModelProviderProxy llmProxy;
    private final HelperExecuteService executeService;
    private final HelperConnectService connectService;
    private final com.studioquack.sqs.channel.wg.OverlayPeerDirectory peerDirectory;
    /**
     * The GRANTED-peers view the llm leg's {@code envId} resolves against, the mesh identity presented
     * to the peer, and the caller-side on-miss authorize client. All {@code null} only in slice tests:
     * a cross-env llm call then fails closed with {@code Error_AuthorizationUnavailable} before
     * anything is dialed.
     */
    private final GrantedPeerDirectory grantedPeerDirectory;
    private final com.studioquack.sqs.channel.wg.mesh.MeshIdentity meshIdentity;
    /**
     * Reactive mesh-token refresh: a peer's 401 means the held token expired against the peer's
     * clock, so the daemon re-handshakes and retries the call once. Package-private so slice
     * tests install a lambda directly; {@code null} there (a 401 then surfaces as before).
     */
    java.util.function.BooleanSupplier meshIdentityRefresher;

    /** Spring wiring — lazy in both directions so the bootstrap↔router pair cannot cycle. */
    @Autowired(required = false)
    void setMeshIdentityRefresher(
            org.springframework.beans.factory.ObjectProvider<com.studioquack.sqs.channel.SqsChannelBootstrap>
                    bootstrap) {
        this.meshIdentityRefresher = () -> {
            com.studioquack.sqs.channel.SqsChannelBootstrap b = bootstrap.getIfAvailable();
            return b != null && b.refreshMeshIdentityNow();
        };
    }
    private final CrossEnvAuthorizationClient crossEnvAuthorizationClient;
    /** The peer listener port; config-defined so caller and listener cannot drift. */
    private final int overlayExecutePort;
    /**
     * Daemon-owned test/build-run orchestrator. {@code null} in slice tests — {@code test/*} then
     * answers 503 {@code test_build_unavailable}.
     */
    private final TestSandboxRunService testSandboxRunService;
    /** Peer-leg client: 5 s overlay connect budget; the per-request budget is set per call. */
    private HttpClient peerLlmHttp = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    @Autowired(required = false)
    private com.studioquack.sqs.channel.net.PeerAddressResolver peerAddressResolver;
    @Autowired(required = false)
    private com.studioquack.sqs.channel.net.MeshPeerHttp meshPeerHttp;
    /** Run-key cancel registry: a cross-env llm call ends when its run is cancelled. Null in slice tests. */
    @Autowired(required = false)
    private com.studioquack.sqs.aimodel.LlmStreamCancellationRegistry llmCancellation;
    @Autowired(required = false)
    private Redactor redactor;
    private final Map<String, HelperJob> jobsByToken = new ConcurrentHashMap<>();
    /** Per-token 401 counts in a rolling minute; caps log spam and marks a throttled window. */
    private final ConcurrentHashMap<String, long[]> unauthWindow = new ConcurrentHashMap<>();
    private static final int UNAUTH_WARN_EVERY = 8;
    private static final long UNAUTH_WINDOW_MS = 60_000L;
    /**
     * Bearer of the broker round-trip probe ({@code HelperBroker.probeScript()}), which requires
     * this 401 back. It never matches a job token, so it is refused like any other — only its
     * expected 401 stays out of the unauthorized-bearer warnings.
     */
    public static final String PROBE_BEARER = "sq-broker-probe";
    /**
     * Live emit-partial fan-in for execute/stream. Null in slice tests.
     */
    @Autowired(required = false)
    private HelperPartialEventBus partialBus;
    /**
     * Graph-routed streaming: port-tagged emit-partials travel the workflow edges to a
     * live-render sink (Duckbot end) instead of bypassing the graph. Null in slice tests.
     */
    @Autowired(required = false)
    private StreamEdgeRouter streamEdgeRouter;
    /**
     * Throw/poll collaborators. {@code @Autowired(required=false)} so the 3-arg / 5-arg
     * slice constructors stay source-compatible — a missing engine answers 503
     * {@code unavailable} rather than NPE.
     */
    /**
     * Shared escalate/signal/message seam ({@link HelperBpmnEventService}) — also the method
     * {@code PodmanSandboxExecution.drainStdoutEvents} will call for stdout {@code sqEvent} markers.
     */
    @Autowired(required = false)
    private HelperBpmnEventService bpmnEventService;
    @Autowired(required = false)
    private DispatchCancellationRegistry dispatchCancellationRegistry;

    /** Test/slice constructor — connect leg reports unavailable when {@code null}. */
    public HelperRequestRouter(ObjectMapper mapper, AiModelProviderProxy llmProxy,
                               HelperExecuteService executeService) {
        this(mapper, llmProxy, executeService, null, null);
    }

    /** Slice constructor without the overlay leg — {@code engine-ip} reports unavailable. */
    public HelperRequestRouter(ObjectMapper mapper, AiModelProviderProxy llmProxy,
                               HelperExecuteService executeService,
                               HelperConnectService connectService) {
        this(mapper, llmProxy, executeService, connectService, null);
    }

    /**
     * Slice constructor without the cross-env collaborators — a cross-env {@code envId} on the llm leg
     * fails closed with {@code Error_AuthorizationUnavailable}.
     *
     * @param connectService The stateful-instance tunnel behind {@code studioquack.connect}, or {.
     * @param peerDirectory The same-owner engine-id → overlay-address view behind {@code studioqu.
     */
    public HelperRequestRouter(ObjectMapper mapper, AiModelProviderProxy llmProxy,
                               HelperExecuteService executeService,
                               HelperConnectService connectService,
                               com.studioquack.sqs.channel.wg.OverlayPeerDirectory peerDirectory) {
        this(mapper, llmProxy, executeService, connectService, peerDirectory, null, null, null,
                DEFAULT_OVERLAY_EXECUTE_PORT);
    }

    /**
     * Slice constructor without the test-sandbox orchestrator — {@code test/*} reports unavailable.
     * Existing 9-arg call sites (cross-env llm tests) stay source-compatible.
     */
    public HelperRequestRouter(ObjectMapper mapper, AiModelProviderProxy llmProxy,
                               HelperExecuteService executeService,
                               HelperConnectService connectService,
                               com.studioquack.sqs.channel.wg.OverlayPeerDirectory peerDirectory,
                               GrantedPeerDirectory grantedPeerDirectory,
                               com.studioquack.sqs.channel.wg.mesh.MeshIdentity meshIdentity,
                               @Nullable CrossEnvAuthorizationClient crossEnvAuthorizationClient,
                               int overlayExecutePort) {
        this(mapper, llmProxy, executeService, connectService, peerDirectory, grantedPeerDirectory,
                meshIdentity, crossEnvAuthorizationClient, overlayExecutePort, null);
    }

    /**
     * @param grantedPeerDirectory The handshake-pushed granted-peers view the llm leg resolves {@.
     * @param meshIdentity This daemon's mesh identity — its token is the Bearer credential presen.
     * @param crossEnvAuthorizationClient The caller-side on-miss authorize client; {@code null} w.
     * @param overlayExecutePort The peer listener port ({@code sqs.overlay.execute-port}).
     * @param testSandboxRunService Daemon-owned test/build-run orchestrator; {@code null} in slic.
     */
    @Autowired
    public HelperRequestRouter(ObjectMapper mapper, AiModelProviderProxy llmProxy,
                               HelperExecuteService executeService,
                               HelperConnectService connectService,
                               com.studioquack.sqs.channel.wg.OverlayPeerDirectory peerDirectory,
                               GrantedPeerDirectory grantedPeerDirectory,
                               com.studioquack.sqs.channel.wg.mesh.MeshIdentity meshIdentity,
                               @Nullable CrossEnvAuthorizationClient crossEnvAuthorizationClient,
                               @Value("${sqs.overlay.execute-port:8443}") int overlayExecutePort,
                               @Nullable @Lazy TestSandboxRunService testSandboxRunService) {
        this.mapper = mapper;
        this.llmProxy = llmProxy;
        this.executeService = executeService;
        this.connectService = connectService;
        this.peerDirectory = peerDirectory;
        this.grantedPeerDirectory = grantedPeerDirectory;
        this.meshIdentity = meshIdentity;
        this.crossEnvAuthorizationClient = crossEnvAuthorizationClient;
        this.overlayExecutePort = overlayExecutePort;
        this.testSandboxRunService = testSandboxRunService;
    }

    /* ------------------------------------------------------------------ */
    /* Job registry                                                       */
    /* ------------------------------------------------------------------ */

    static final int MAX_JOBS = 512;
    private static final SecureRandom BEARER_RNG = new SecureRandom();
    /** 18 bytes → 24-char URL-safe payload; {@code h_} prefix keeps existing redactor patterns. */
    private static final int BEARER_BYTES = 18;

    /** Registers a job under a random {@code h_…} bearer distinct from the job id. */
    public String register(HelperJob job) {
        if (jobsByToken.size() >= MAX_JOBS) {
            throw new IllegalStateException("helper job registry cap (" + MAX_JOBS + ") reached");
        }
        for (int i = 0; i < 8; i++) {
            String token = mintBearer();
            if (jobsByToken.putIfAbsent(token, job) == null) {
                return token;
            }
        }
        throw new IllegalStateException("helper bearer mint collision");
    }

    private static String mintBearer() {
        byte[] raw = new byte[BEARER_BYTES];
        BEARER_RNG.nextBytes(raw);
        return "h_" + Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
    }

    /** Registers a PERSISTENT, per-request stateful job (not revoked between mid-run calls). */
    public String registerPersistent(HelperJob job) {
        String token = register(job);
        // The token is the job's bearer on the helper channel — do not log it.
        log.debug("helper-router: persistent per-request job registered (activeJobs={})", jobsByToken.size());
        return token;
    }

    /** Removes the job binding and returns it (for the dispatcher to read the recorded result). */
    public Optional<HelperJob> revoke(String token) {
        HelperJob job = jobsByToken.remove(token);
        if (job != null) {
            job.markRevoked();
        }
        // A connect grant must not outlive the script that asked for it: the bearer is gone, so the
        // sockets it authorised go with it — otherwise a job that died mid-tunnel would leave the
        // daemon holding an open connection into a stateful instance with nobody to close it.
        if (job != null && connectService != null) {
            connectService.closeAllForJob(job.id());
        }
        // Close graph-routed stream sessions this job's node opened. Best-effort.
        if (job != null && streamEdgeRouter != null) {
            streamEdgeRouter.onJobFinished(job);
        }
        return Optional.ofNullable(job);
    }

    /** @return the currently-registered job for a token (diagnostics/tests). */
    public Optional<HelperJob> lookup(String token) {
        return Optional.ofNullable(jobsByToken.get(token));
    }

    /** @return how many jobs are currently registered (diagnostics/tests). */
    public int activeJobs() {
        return jobsByToken.size();
    }

    /* ------------------------------------------------------------------ */
    /* Transport-agnostic entrypoint                                      */
    /* ------------------------------------------------------------------ */

    /**
     * Routes one request. {@code sub} is the path AFTER {@code /v1/helper/}; {@code bearer} is the raw
     * token with NO {@code "Bearer "} prefix (the transport adapter strips it); {@code body} is the
     * raw request bytes (may be empty, never {@code null}-unsafe — pass {@code new byte[0]}).
     *
     * @return The identical status/body the HTTP handler used to write directly to the socket, in.
     */
    public RoutedResponse route(String method, String sub, String bearer, byte[] body) {
        try {
            HelperJob job = authorize(bearer);
            if (job == null) {
                noteUnauthorized(bearer);
                return RoutedResponse.json(401, jsonBytes(Map.of("error", "unauthorized")));
            }
            job.beginRequest();
            try {
                return dispatch(method, sub == null ? "" : sub, job, body == null ? new byte[0] : body);
            } finally {
                job.endRequest();
            }
        } catch (Exception e) {
            log.debug("helper-router: handler error sub={}: {}", sub, e.toString());
            return RoutedResponse.json(500, jsonBytes(Map.of("error", "internal")));
        }
    }

    /**
     * Streaming entrypoint used by {@link HelperPipeChannel} for {@code execute/stream} and
     * {@code llm/*} when the body has {@code stream=true} <em>and</em> {@code accept} lists
     * {@code text/event-stream}. Other llm calls fall through to buffered {@link #handleLlm}
     * ({@code forward} still forces {@code stream=false}).
     */
    public void routeStream(String method, String sub, String bearer, byte[] body, String accept,
                            HelperStreamSink sink) {
        if (sink == null) {
            return;
        }
        HelperJob job = authorize(bearer);
        if (job == null) {
            noteUnauthorized(bearer);
            sink.oneShot(401, "application/json", jsonBytes(Map.of("error", "unauthorized")));
            return;
        }
        String path = sub == null ? "" : sub;
        byte[] safeBody = body == null ? new byte[0] : body;
        job.beginRequest();
        try {
            if ("execute/stream".equals(path)) {
                handleExecuteStream(job, safeBody, sink);
                return;
            }
            if (path.startsWith("llm/") && wantsStream(safeBody) && HelperSse.acceptsEventStream(accept)) {
                handleLlmStream(job, method, path.substring("llm/".length()), safeBody, sink);
                return;
            }
            RoutedResponse r = dispatch(method, path, job, safeBody);
            sink.oneShot(r.status(), r.contentType(), r.body());
        } catch (Exception e) {
            log.debug("helper-router: stream handler error sub={}: {}", path, e.toString());
            sink.oneShot(500, "application/json", jsonBytes(Map.of("error", "internal")));
        } finally {
            job.endRequest();
        }
    }

    private HelperJob authorize(String bearer) {
        if (bearer == null || bearer.isBlank()) {
            return null;
        }
        return jobsByToken.get(bearer);
    }

    private void noteUnauthorized(String bearer) {
        if (PROBE_BEARER.equals(bearer)) {
            log.debug("helper-router: broker round-trip probe answered 401");
            return;
        }
        String key = bearer == null ? "" : bearer;
        long now = System.currentTimeMillis();
        long[] w = unauthWindow.compute(key, (k, prev) -> {
            if (prev == null || now - prev[0] > UNAUTH_WINDOW_MS) {
                return new long[]{now, 1L};
            }
            prev[1]++;
            return prev;
        });
        long n = w[1];
        if (n <= 3 || n % UNAUTH_WARN_EVERY == 0) {
            log.warn("helper-router: unauthorized bearer (count={} in {}ms window, tokenLen={}, jobs={})",
                    n, UNAUTH_WINDOW_MS, key.length(), jobsByToken.size());
        }
        if (unauthWindow.size() > 2048) {
            unauthWindow.entrySet().removeIf(e -> now - e.getValue()[0] > UNAUTH_WINDOW_MS);
        }
    }

    private RoutedResponse dispatch(String method, String sub, HelperJob job, byte[] body) {
        if (sub.startsWith("input/")) {
            return handleInput(job, sub.substring("input/".length()));
        }
        if (sub.startsWith("llm/")) {
            // The helper pipe strips the query string for LOGGING but routes the raw path, so a
            // job-scoped op like download/status?job_id=… arrives intact — split it here: the
            // op stays routable and the query rides along for the proxy to consume.
            String llmSub = sub.substring("llm/".length());
            String llmQuery = null;
            int q = llmSub.indexOf('?');
            if (q >= 0) {
                llmQuery = llmSub.substring(q + 1);
                llmSub = llmSub.substring(0, q);
            }
            return handleLlm(job, method, llmSub, llmQuery, body);
        }
        // studioquack.connect(container_name, port) — the daemon-mediated tunnel to a named stateful
        // instance. Byte ops rather than a socket because this channel is request/response only.
        if (sub.startsWith("connect/")) {
            return handleConnect(job, sub.substring("connect/".length()), body);
        }
        if (sub.startsWith("test/")) {
            return handleTest(job, sub.substring("test/".length()), body);
        }
        return switch (sub) {
            case "init" -> handleInit(job);
            case "inputs" -> contextValue(job, "input", Map.of());
            case "execution-context" -> contextValueProjected(job, "executionContext", job.executionContext());
            case "full-context" -> RoutedResponse.json(200, jsonBytes(
                    NodeScopedHelperContext.project(job.executionContext())));
            case "current-node" -> RoutedResponse.json(200, jsonBytes(job.executionContext().get("currentNode")));
            case "current-node-id" -> RoutedResponse.json(200, jsonBytes(
                    Map.of("current_node_id", str(job.executionContext().get("currentNodeId")))));
            case "workflow-execution-id" -> RoutedResponse.json(200, jsonBytes(
                    Map.of("workflow_execution_id", str(job.executionContext().get("workflowExecutionId")))));
            case "parameters" -> contextValue(job, "parameters", List.of());
            case "connections" -> contextValue(job, "connections", List.of());
            case "result" -> handleResult(job, body);
            case "fail" -> handleFail(job, body);
            case "escalate" -> handleEscalate(job, body);
            case "signal" -> handleSignal(job, body);
            case "message" -> handleMessage(job, body);
            case "cancelled" -> handleCancelled(job);
            case "metric" -> handleMetric(job, body);
            case "log" -> handleLog(job, body);
            case "dispatch" -> handleDispatch(job, body);
            case "transform-context" -> handleTransformContext(job, body);
            // Studioquack.execute(port, payload, node_id?, env_id?) — resolves the current node's
            // function-port connections and runs the sub-workflow locally (same env) or forwards to the
            // control plane (cross-env).
            case "execute" -> handleExecute(job, body);
            // Daemon-owned test sandbox build-run (also matched by the test/ prefix above).
            case "test/build-run" -> handleTest(job, "build-run", body);
            // Studioquack.engineIP(engine_id) — resolve an engine id to its CURRENT overlay address so a
            // Network Access Control rule keyed by id survives the target engine reconnecting. Answers only
            // from the same-owner peer directory, so it can never name another owner's engine.
            case "engine-ip" -> handleEngineIp(body);
            case "execute/stream" -> handleExecuteStreamBuffered(job, body);
            case "emit-partial" -> handleEmitPartial(job, body);
            case "webhook-challenge" -> handleWebhookChallenge(job, body);
            case "webhook-events" -> handleWebhookEvents(job, body);
            case "webhook-reject" -> handleWebhookReject(job, body);
            case "correlation-key" -> handleCorrelationKey(job, body);
            default -> RoutedResponse.json(404, jsonBytes(Map.of("error", "no such endpoint", "path", sub)));
        };
    }

    /* ------------------------------------------------------------------ */
    /* Endpoint handlers (body-in/out)                                    */
    /* ------------------------------------------------------------------ */

    private RoutedResponse handleInit(HelperJob job) {
        Map<String, Object> ec = job.executionContext();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("id", job.id());
        resp.put("language", job.language());
        resp.put("enqueued_at", RFC3339.format(job.enqueuedAt()));
        resp.put("input_keys", new ArrayList<>(job.inputs().keySet()));
        resp.put("limits", job.limits());
        resp.put("workflow_execution_id", str(ec.get("workflowExecutionId")));
        resp.put("current_node_id", str(ec.get("currentNodeId")));
        resp.put("has_execution_context", !ec.isEmpty());
        resp.put("job_mode", job.jobMode());
        return RoutedResponse.json(200, jsonBytes(resp));
    }

    private RoutedResponse handleInput(HelperJob job, String key) {
        if (key.isEmpty()) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "missing key")));
        }
        byte[] value = job.inputs().get(key);
        if (value == null) {
            return RoutedResponse.json(404, jsonBytes(Map.of("error", "no such input")));
        }
        return RoutedResponse.raw(200, value, "application/octet-stream");
    }

    /** Serves {@code ec[key]} when present, else the supplied default (mirrors the Go fallbacks). */
    private RoutedResponse contextValue(HelperJob job, String key, Object fallback) {
        Object v = job.executionContext().get(key);
        return RoutedResponse.json(200, jsonBytes(v != null ? v : fallback));
    }

    /** Same as {@link #contextValue} after node-scoped secret-handle strip. */
    private RoutedResponse contextValueProjected(HelperJob job, String key, Object fallback) {
        Object v = job.executionContext().get(key);
        return RoutedResponse.json(200, jsonBytes(NodeScopedHelperContext.project(v != null ? v : fallback)));
    }

    private RoutedResponse handleResult(HelperJob job, byte[] body) {
        RoutedResponse cancelled = refuseIfCancelled(job);
        if (cancelled != null) {
            return cancelled;
        }
        HelperResult r = readBody(body, HelperResult.class);
        if (r == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        job.setResult(r);
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
    }

    /**
     * Records the structured typed error the SDK raised via {@code studioquack.fail(code,msg)}. A
     * blank code is rejected; {@code SQ_SKIP} is reserved for the monitoring/repair skip action.
     */
    private RoutedResponse handleFail(HelperJob job, byte[] body) {
        RoutedResponse cancelled = refuseIfCancelled(job);
        if (cancelled != null) {
            return cancelled;
        }
        HelperFailure f = readBody(body, HelperFailure.class);
        if (f == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        if (!f.hasCode()) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "errorCode is required")));
        }
        if (RESERVED_SKIP_CODE.equalsIgnoreCase(f.errorCode())) {
            return RoutedResponse.json(400, jsonBytes(
                    Map.of("error", "errorCode '" + RESERVED_SKIP_CODE + "' is reserved")));
        }
        job.setFailure(f);
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
    }

    /**
     * {@code POST /v1/helper/escalate} {@code {escalationCode, message?, data?}} → {@code {ok:true}}.
     * Delegates to {@link HelperBpmnEventService#escalate} (shared with the stdout fold).
     */
    private RoutedResponse handleEscalate(HelperJob job, byte[] body) {
        HelperEventRequest req = readBody(body, HelperEventRequest.class);
        if (req == null || str(req.escalationCode()).isBlank()) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "escalationCode is required")));
        }
        if (bpmnEventService == null) {
            return RoutedResponse.json(503, jsonBytes(Map.of("error", "unavailable")));
        }
        return routed(bpmnEventService.escalate(job, req));
    }

    /**
     * {@code POST /v1/helper/signal} {@code {signalName, scope?: "instance"|"global"}} →
     * {@code {ok:true}}. Delegates to {@link HelperBpmnEventService#signal}.
     */
    private RoutedResponse handleSignal(HelperJob job, byte[] body) {
        if (bpmnEventService == null) {
            return RoutedResponse.json(503, jsonBytes(Map.of("error", "unavailable")));
        }
        return routed(bpmnEventService.signal(job, readBody(body, HelperEventRequest.class)));
    }

    /**
     * {@code POST /v1/helper/message} {@code {messageName, correlationKey?}} →
     * {@code {ok, delivered}} or {@code {ok:false, reason}}. Delegates to
     * {@link HelperBpmnEventService#message}.
     */
    private RoutedResponse handleMessage(HelperJob job, byte[] body) {
        if (bpmnEventService == null) {
            return RoutedResponse.json(503, jsonBytes(Map.of("error", "unavailable")));
        }
        return routed(bpmnEventService.message(job, readBody(body, HelperEventRequest.class)));
    }

    /** {@code POST /v1/helper/webhook-challenge} {@code {status?, contentType?, body}} → {@code {ok:true}}. */
    @SuppressWarnings("unchecked")
    private RoutedResponse handleWebhookChallenge(HelperJob job, byte[] body) {
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        int status = intOf(b.get("status"), 200);
        String contentType = str(b.get("contentType"));
        if (contentType.isBlank()) {
            contentType = str(b.get("content_type"));
        }
        if (contentType.isBlank()) {
            contentType = "text/plain";
        }
        String respBody = b.get("body") == null ? "" : String.valueOf(b.get("body"));
        job.setWebhookChallenge(new WebhookIngressOutcome.WebhookChallenge(status, contentType, respBody));
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
    }

    /** {@code POST /v1/helper/webhook-events} {@code {events:[{idempotencyKey?,correlationKey?,payload}]}} (max 100). */
    @SuppressWarnings("unchecked")
    private RoutedResponse handleWebhookEvents(HelperJob job, byte[] body) {
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        Object rawEvents = b.get("events");
        if (!(rawEvents instanceof List<?> list)) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "events array is required")));
        }
        List<WebhookIngressOutcome.WebhookEmittedEvent> parsed = parseWebhookEvents(list);
        int accepted = job.appendWebhookEvents(parsed);
        boolean capped = list.size() > accepted || parsed.size() > accepted;
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true, "accepted", accepted, "capped", capped)));
    }

    /** {@code POST /v1/helper/webhook-reject} {@code {status?, reason?}} → {@code {ok:true}}. */
    @SuppressWarnings("unchecked")
    private RoutedResponse handleWebhookReject(HelperJob job, byte[] body) {
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        int status = intOf(b.get("status"), 401);
        String reason = str(b.get("reason"));
        if (reason.isBlank()) {
            reason = "unauthorized";
        }
        job.setWebhookReject(new WebhookIngressOutcome.WebhookReject(status, reason));
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
    }

    /** {@code POST /v1/helper/correlation-key} {@code {correlationKey}} → {@code {ok:true}}. */
    @SuppressWarnings("unchecked")
    private RoutedResponse handleCorrelationKey(HelperJob job, byte[] body) {
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        String key = str(b.get("correlationKey"));
        if (key.isBlank()) {
            key = str(b.get("correlation_key"));
        }
        if (key.isBlank()) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "correlationKey is required")));
        }
        job.setCorrelationKey(key.trim());
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
    }

    /** Shared event-list parser (capped at {@link WebhookIngressOutcome#MAX_EVENTS}). */
    @SuppressWarnings("unchecked")
    public static List<WebhookIngressOutcome.WebhookEmittedEvent> parseWebhookEvents(List<?> rawList) {
        if (rawList == null || rawList.isEmpty()) {
            return List.of();
        }
        List<WebhookIngressOutcome.WebhookEmittedEvent> out =
                new ArrayList<>(Math.min(rawList.size(), WebhookIngressOutcome.MAX_EVENTS));
        for (Object item : rawList) {
            if (out.size() >= WebhookIngressOutcome.MAX_EVENTS) {
                break;
            }
            if (!(item instanceof Map<?, ?> m)) {
                continue;
            }
            String idempotencyKey = emptyToNull(str(m.get("idempotencyKey")));
            if (idempotencyKey == null) {
                idempotencyKey = emptyToNull(str(m.get("idempotency_key")));
            }
            String correlationKey = emptyToNull(str(m.get("correlationKey")));
            if (correlationKey == null) {
                correlationKey = emptyToNull(str(m.get("correlation_key")));
            }
            Map<String, Object> payload;
            Object rawPayload = m.get("payload");
            if (rawPayload instanceof Map<?, ?> pm) {
                Map<String, Object> copy = new LinkedHashMap<>();
                pm.forEach((k, v) -> copy.put(String.valueOf(k), v));
                payload = copy;
            } else if (rawPayload != null) {
                payload = Map.of("value", rawPayload);
            } else {
                payload = Map.of();
            }
            out.add(new WebhookIngressOutcome.WebhookEmittedEvent(idempotencyKey, correlationKey, payload));
        }
        return out;
    }

    /**
     * Folds one {@code sqEvent:::} stdout marker of the emit-only SDKs ({@code bash.sh}, {@code
     * node.js} — no transport in the image) through the SAME handler the socket endpoint uses, so a
     * typed failure or a thrown escalation/signal/message behaves identically whichever transport
     * carried it. Called by {@code PodmanSandboxExecution.drainStdoutEvents} after the container
     * exits, which is why these events are documented as "delivered at exit" for those runtimes.
     */
    public boolean foldStdoutEvent(HelperJob job, String kind, Map<String, Object> event) {
        if (job == null || kind == null) {
            return false;
        }
        byte[] body = jsonBytes(event == null ? Map.of() : event);
        RoutedResponse response = switch (kind) {
            case "fail" -> handleFail(job, body);
            case "escalate" -> handleEscalate(job, body);
            case "signal" -> handleSignal(job, body);
            case "message" -> handleMessage(job, body);
            case "webhook_challenge" -> handleWebhookChallenge(job, body);
            case "webhook_events" -> handleWebhookEvents(job, body);
            case "webhook_reject" -> handleWebhookReject(job, body);
            case "correlation_key" -> handleCorrelationKey(job, body);
            default -> null;
        };
        if (response == null) {
            return false;
        }
        if (response.status() >= 400) {
            // The script has already exited; the only audience is the operator log.
            log.warn("helper stdout {} marker refused for job {}: HTTP {}", kind, job.id(), response.status());
        }
        return true;
    }

    /** Maps {@link HelperBpmnEventService.EventOutcome} onto the helper-channel response shape. */
    private RoutedResponse routed(HelperBpmnEventService.EventOutcome outcome) {
        if (outcome == null) {
            return RoutedResponse.json(503, jsonBytes(Map.of("error", "unavailable")));
        }
        return RoutedResponse.json(outcome.httpStatus(), jsonBytes(outcome.body()));
    }

    /** {@code GET|POST /v1/helper/cancelled} → {@code {cancelled, reason?}}. Body ignored. */
    private RoutedResponse handleCancelled(HelperJob job) {
        boolean cancelled = dispatchCancellationRegistry != null
                && dispatchCancellationRegistry.isCancelled(job.id());
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("cancelled", cancelled);
        if (cancelled) {
            String reason = dispatchCancellationRegistry.cancelReason(job.id());
            if (reason != null) {
                resp.put("reason", reason);
            }
        }
        return RoutedResponse.json(200, jsonBytes(resp));
    }

    /** 409 when the job's dispatch was cancelled — do not record a result or run execute. */
    private RoutedResponse refuseIfCancelled(HelperJob job) {
        if (dispatchCancellationRegistry != null && dispatchCancellationRegistry.isCancelled(job.id())) {
            return RoutedResponse.json(409, jsonBytes(Map.of("error", "cancelled")));
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private RoutedResponse handleMetric(HelperJob job, byte[] body) {
        Map<String, Object> m = readBody(body, Map.class);
        if (m == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        m.putIfAbsent("at", RFC3339.format(java.time.Instant.now()));
        job.metrics().add(m);
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
    }

    @SuppressWarnings("unchecked")
    private RoutedResponse handleLog(HelperJob job, byte[] body) {
        Map<String, Object> l = readBody(body, Map.class);
        if (l == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        l.putIfAbsent("at", RFC3339.format(java.time.Instant.now()));
        l.putIfAbsent("level", "info");
        Redactor r = redactor != null ? redactor : new Redactor();
        Object scrubbed = r.redactTree(l, job.secretLiterals());
        boolean stored;
        if (scrubbed instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.put(String.valueOf(e.getKey()), e.getValue());
            }
            stored = job.tryAppendLog(out);
        } else {
            stored = job.tryAppendLog(l);
        }
        if (!stored) {
            return RoutedResponse.json(200, jsonBytes(Map.of("ok", false, "truncated", true)));
        }
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
    }

    @SuppressWarnings("unchecked")
    private RoutedResponse handleDispatch(HelperJob job, byte[] body) {
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        job.dispatch().add(b);
        // Offline/dev echo: hand back an empty list so node-to-node chains are deterministic.
        return RoutedResponse.json(200, jsonBytes(List.of()));
    }

    @SuppressWarnings("unchecked")
    private RoutedResponse handleTransformContext(HelperJob job, byte[] body) {
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        Object ec = b.get("executionContext");
        job.setTransformedContext(ec instanceof Map ? (Map<String, Object>) ec : new LinkedHashMap<>());
        return RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
    }

    /**
     * Proxies a {@code studioquack.llm.*} call to the configured OpenAI-compatible provider — THIS
     * environment's, or a granted sibling's when the body carries {@code envId} . The per-job bearer
     * was already validated in {@link #route}.
     */
    private RoutedResponse handleLlm(HelperJob job, String method, String llmSub, String llmQuery, byte[] body) {
        // TEST-MOCK: no provider proxy and no envId strip — llm is disabled for the job.
        if (job.mockResponses() != null) {
            return RoutedResponse.json(501, jsonBytes(Map.of("error", "llm_disabled_in_test")));
        }
        String envId = null;
        byte[] forwardBody = body;
        if (body != null && body.length > 0) {
            try {
                JsonNode tree = mapper.readTree(body);
                if (tree instanceof ObjectNode obj && obj.has("envId")) {
                    JsonNode removed = obj.remove("envId");
                    envId = removed == null || removed.isNull() ? null : removed.asText(null);
                    forwardBody = mapper.writeValueAsBytes(obj);
                }
            } catch (Exception notJson) {
                // Not parseable JSON — nothing to strip; forward verbatim (today's behaviour).
            }
        }
        recordLlmRequestSummary(job, llmSub, forwardBody, false);
        String selfEnvId = str(job.executionContext().get("envId"));
        Optional<GrantedPeerDirectory.GrantedPeer> peer;
        try {
            peer = CrossEnvPeerResolver.resolvePeer(envId, selfEnvId,
                    meshIdentity == null ? null : meshIdentity.ownEnvName().orElse(null),
                    grantedPeerDirectory, crossEnvAuthorizationClient, "llm");
        } catch (CrossEnvCallException e) {
            // Unknown env / not granted / authorization unavailable — the llm leg reports in the proxy's
            // {error, detail} envelope so the SDK's response.json() reads the code.
            return llmError(502, e.errorCode(), e.getMessage());
        }
        if (peer.isEmpty()) {
            // Local path — byte-for-byte today's behaviour when no envId was present.
            AiModelProviderProxy.ProxyResult result = llmProxy.forward(llmSub, method, llmQuery, forwardBody,
                    str(job.executionContext().get("workflowExecutionId")));
            return RoutedResponse.raw(result.status(), result.body(), result.contentType());
        }
        return forwardLlmToPeer(peer.get(), method, llmSub, llmQuery, forwardBody,
                str(job.executionContext().get("workflowExecutionId")));
    }

    /**
     * Quack Logs visibility: record a content-free summary of every chat call on the job ({@code
     * kind:"llm-request"}) so the run's node log can show what went to the model — {@code
     * StatefulSandboxExecution} folds these into the record's stdout. Recorded BEFORE the
     * local/cross-env split so a peer-routed call stays attributed to the calling node.
     */
    private void recordLlmRequestSummary(HelperJob job, String llmSub, byte[] body, boolean stream) {
        if (body == null || body.length == 0) {
            return;
        }
        if (!"completion".equals(llmSub) && !"chat".equals(llmSub)) {
            return;
        }
        try {
            long recorded = job.logs().stream()
                    .filter(l -> "llm-request".equals(l.get("kind"))).count();
            if (recorded >= MAX_LLM_REQUEST_LOGS_PER_JOB) {
                if (recorded == MAX_LLM_REQUEST_LOGS_PER_JOB) {
                    Map<String, Object> note = new LinkedHashMap<>();
                    note.put("kind", "llm-request");
                    note.put("level", "info");
                    note.put("at", RFC3339.format(java.time.Instant.now()));
                    note.put("message", "further llm requests in this run are omitted from the node log");
                    job.tryAppendLog(note);
                }
                return;
            }
            JsonNode tree = mapper.readTree(body);
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("kind", "llm-request");
            entry.put("level", "info");
            entry.put("at", RFC3339.format(java.time.Instant.now()));
            entry.put("message", AiModelProviderProxy.chatRequestSummary(tree,
                    stream || tree.path("stream").asBoolean(false)));
            job.tryAppendLog(entry);
        } catch (Exception e) {
            // Visibility must never break the call.
        }
    }

    /**
     * Sends one peer-leg request, retrying only dial-phase failures. A connect timeout or refusal
     * means the TCP/WireGuard flow was never established — the request never left this daemon, so
     * re-dialling is safe (no double execution) and gives a slow overlay re-handshake a window to
     * complete.
     */
    private <T> HttpResponse<T> sendPeerWithConnectRetry(HttpRequest request,
                                                         HttpResponse.BodyHandler<T> handler,
                                                         GrantedPeerDirectory.GrantedPeer peer,
                                                         PeerLlmCall call)
            throws IOException, InterruptedException {
        IOException last = null;
        for (int attempt = 1; attempt <= PEER_CONNECT_MAX_ATTEMPTS; attempt++) {
            if (call.cancelled) {
                throw new IOException("cross-env llm call cancelled with its run");
            }
            try {
                return call.await(peerHttp(peer.envId()).sendAsync(request, handler));
            } catch (IOException e) {
                boolean tlsDialFailure = e instanceof SSLException;
                if ((!tlsDialFailure && !isConnectPhaseFailure(e)) || attempt == PEER_CONNECT_MAX_ATTEMPTS) {
                    throw e;
                }
                if (tlsDialFailure && meshPeerHttp != null) {
                    // Stale pooled socket or a flow dropped mid-handshake: rebuild the peer
                    // client so the retry does not reuse the dead pool.
                    meshPeerHttp.invalidate(peer.envId());
                }
                last = e;
                long backoffMs = PEER_CONNECT_BACKOFF_MS * (1L << (attempt - 1));
                log.info("helper-router: cross-env llm connect to peer {} ('{}') attempt {}/{} "
                        + "failed ({}); the overlay re-handshake may still be settling — "
                        + "retrying in {} ms", peer.envId(), peer.name(), attempt,
                        PEER_CONNECT_MAX_ATTEMPTS, e.getClass().getSimpleName(), backoffMs);
                Thread.sleep(backoffMs);
            }
        }
        throw last; // unreachable — the loop either returns or throws
    }

    /**
     * One cross-env llm call, registered under the caller's run key for its whole life — there is
     * no time limit to end it. Cancelling the run cancels the in-flight request or closes the
     * relayed stream here, and tells the peer to abort its provider call; nothing waits on that.
     */
    private final class PeerLlmCall implements java.io.Closeable {
        private final String callId = com.studioquack.sqs.channel.overlay.PeerCallRegistry.newCallId();
        private final GrantedPeerDirectory.GrantedPeer peer;
        private final String baseUrl;
        private volatile boolean cancelled;
        private volatile java.util.concurrent.CompletableFuture<?> inFlight;
        private volatile InputStream body;

        PeerLlmCall(GrantedPeerDirectory.GrantedPeer peer, String baseUrl) {
            this.peer = peer;
            this.baseUrl = baseUrl;
        }

        /** Parks on {@code response} (nothing polls); a cancelled run surfaces as an IOException. */
        <T> HttpResponse<T> await(java.util.concurrent.CompletableFuture<HttpResponse<T>> response)
                throws IOException, InterruptedException {
            inFlight = response;
            if (cancelled) {
                response.cancel(true);
            }
            try {
                return response.get();
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.CancellationException e) {
                if (cancelled) {
                    throw new IOException("cross-env llm call cancelled with its run", e);
                }
                if (e instanceof java.util.concurrent.ExecutionException ee && ee.getCause() instanceof IOException io) {
                    throw io;
                }
                throw new IOException(e.getCause() == null ? e : e.getCause());
            } catch (InterruptedException e) {
                response.cancel(true);
                throw e;
            }
        }

        /** The relayed peer body, closed by a cancel so the parked read ends. */
        void relaying(InputStream in) throws IOException {
            body = in;
            if (cancelled && in != null) {
                in.close();
            }
        }

        @Override
        public void close() {
            cancelled = true;
            java.util.concurrent.CompletableFuture<?> response = inFlight;
            if (response != null) {
                response.cancel(true);
            }
            InputStream in = body;
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignored) {
                    // the relay read fails either way
                }
            }
            String token = meshIdentity == null ? null : meshIdentity.currentToken().orElse(null);
            if (token != null) {
                peerHttp(peer.envId())
                        .sendAsync(com.studioquack.sqs.channel.overlay.PeerCallRegistry.cancelRequest(
                                baseUrl, token, callId), HttpResponse.BodyHandlers.discarding())
                        .exceptionally(failure -> null);
            }
        }
    }

    /** Connect-phase failures are safe to retry: the request never left this daemon. */
    private static boolean isConnectPhaseFailure(IOException e) {
        return e instanceof HttpConnectTimeoutException || e instanceof ConnectException;
    }

    /** The caller side of a cross-env {@code studioquack.llm.*} call: POST the stripped body (or GET, body-less, for the models/download-status subs) to the peer's overlay llm route with the mesh Bearer, and hand the peer's status/body/contentType back VERBATIM — except the three answers the script must distinguish: 401 → {@code Error_PeerRejectedIdentity}, 403 → the peer's own code passed through, and the peer's 503 {@code provider_not_configured} → {@code Error_PeerProviderNotConfigured} naming the PEER env (or operators debug the wrong machine). */
    private RoutedResponse forwardLlmToPeer(GrantedPeerDirectory.GrantedPeer peer, String method,
                                            String llmSub, String llmQuery, byte[] body, String runKey) {
        String meshToken = meshIdentity == null ? null : meshIdentity.currentToken().orElse(null);
        if (meshToken == null) {
            // No backend-signed identity to present — fail BEFORE dialing (identity unavailable).
            return llmError(502, CrossEnvCallException.ERROR_PEER_REJECTED_IDENTITY,
                    "cross-env llm to '" + peer.envId() + "' cannot be placed: this daemon holds no "
                            + "backend-signed mesh identity (the handshake delivered no meshToken — a "
                            + "backend that cannot sign cannot cross environments). Re-handshake "
                            + "against a signing backend and retry.");
        }
        String address = resolvePeerIp(peer);
        if (address == null || address.isBlank()) {
            return llmError(502, CrossEnvCallException.ERROR_PEER_UNREACHABLE,
                    "cross-env llm: granted peer " + peer.envId() + " ('" + peer.name()
                            + "') is absent or offline in the local mesh map. "
                            + grantedPeerDirectory.size() + " granted peer(s) known.");
        }
        String baseUrl = peerScheme() + "://" + address + ":" + overlayExecutePort;
        String url = baseUrl + "/sqs/v1/overlay/llm/" + llmSub;
        boolean get = "GET".equalsIgnoreCase(method);
        log.info("helper-router: cross-env llm {} {} → peer env={} ('{}') addr={}",
                method, llmSub, peer.envId(), peer.name(), address);
        PeerLlmCall call = new PeerLlmCall(peer, baseUrl);
        if (llmCancellation != null) {
            llmCancellation.register(runKey, call);
        }
        try {
            HttpResponse<byte[]> resp = sendPeerWithConnectRetry(
                    buildPeerLlmRequest(url, get, body, meshToken, "application/json", call.callId),
                    HttpResponse.BodyHandlers.ofByteArray(), peer, call);
            if (resp.statusCode() == 401 && refreshMeshIdentity()) {
                // The callee authenticates BEFORE parsing the body (MeshPeerAuthenticator), so a
                // 401 means nothing ran on the peer — one retry cannot double-execute.
                String fresh = meshIdentity == null ? null : meshIdentity.currentToken().orElse(null);
                if (fresh != null && !fresh.equals(meshToken)) {
                    log.info("helper-router: peer {} ('{}') refused the mesh token (401) — "
                            + "re-handshook; retrying once with the fresh identity",
                            peer.envId(), peer.name());
                    resp = sendPeerWithConnectRetry(
                            buildPeerLlmRequest(url, get, body, fresh, "application/json", call.callId),
                            HttpResponse.BodyHandlers.ofByteArray(), peer, call);
                }
            }
            return mapPeerLlmResponse(peer, resp.statusCode(), resp.body(),
                    resp.headers().firstValue("Content-Type").orElse("application/json"));
        } catch (IOException e) {
            if (call.cancelled) {
                return llmError(502, "peer_call_cancelled", "cross-env llm to " + peer.envId()
                        + " cancelled with its run");
            }
            // Connect refused / connect or read timeout / reset at dial time: "peer unreachable", with the
            // tunnel/directory context — never confused with a slow peer model.
            return llmError(502, CrossEnvCallException.ERROR_PEER_UNREACHABLE,
                    "cross-env llm: granted peer " + peer.envId() + " ('" + peer.name() + "') at "
                            + address + " is not reachable on the overlay ("
                            + e.getClass().getSimpleName() + ") — the tunnel is down or the gateway "
                            + "drops the flow. " + grantedPeerDirectory.size()
                            + " granted peer(s) known.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return llmError(502, CrossEnvCallException.ERROR_PEER_UNREACHABLE,
                    "cross-env llm to " + peer.envId() + " interrupted");
        } catch (Exception e) {
            return llmError(502, "peer_call_failed",
                    "cross-env llm to " + peer.envId() + " failed: " + str(e.getMessage()));
        } finally {
            if (llmCancellation != null) {
                llmCancellation.unregister(runKey, call);
            }
        }
    }

    /**
     * Caller half of the cross-env llm stream hop. Same identity/address checks as {@link
     * #forwardLlmToPeer}, but {@code Accept} lists {@code text/event-stream} so a current peer relays
     * provider SSE chunk-by-chunk (the callee {@code OverlayPeerListener} already does this).
     */
    private void forwardLlmToPeerStream(GrantedPeerDirectory.GrantedPeer peer, String method,
                                        String llmSub, byte[] body, HelperStreamSink sink, String runKey) {
        String meshToken = meshIdentity == null ? null : meshIdentity.currentToken().orElse(null);
        if (meshToken == null) {
            RoutedResponse err = llmError(502, CrossEnvCallException.ERROR_PEER_REJECTED_IDENTITY,
                    "cross-env llm to '" + peer.envId() + "' cannot be placed: this daemon holds no "
                            + "backend-signed mesh identity (the handshake delivered no meshToken — a "
                            + "backend that cannot sign cannot cross environments). Re-handshake "
                            + "against a signing backend and retry.");
            sink.oneShot(err.status(), err.contentType(), err.body());
            return;
        }
        String address = resolvePeerIp(peer);
        if (address == null || address.isBlank()) {
            RoutedResponse err = llmError(502, CrossEnvCallException.ERROR_PEER_UNREACHABLE,
                    "cross-env llm: granted peer " + peer.envId() + " ('" + peer.name()
                            + "') is absent or offline in the local mesh map. "
                            + grantedPeerDirectory.size() + " granted peer(s) known.");
            sink.oneShot(err.status(), err.contentType(), err.body());
            return;
        }
        String baseUrl = peerScheme() + "://" + address + ":" + overlayExecutePort;
        String url = baseUrl + "/sqs/v1/overlay/llm/" + llmSub;
        boolean get = "GET".equalsIgnoreCase(method);
        log.info("helper-router: cross-env llm {} {} → peer env={} ('{}') addr={} stream=true",
                method, llmSub, peer.envId(), peer.name(), address);
        PeerLlmCall call = new PeerLlmCall(peer, baseUrl);
        if (llmCancellation != null) {
            llmCancellation.register(runKey, call);
        }
        try {
            HttpResponse<InputStream> resp = sendPeerWithConnectRetry(
                    buildPeerLlmRequest(url, get, body, meshToken,
                            "text/event-stream, application/json", call.callId),
                    HttpResponse.BodyHandlers.ofInputStream(), peer, call);
            if (resp.statusCode() == 401 && refreshMeshIdentity()) {
                // 401 = rejected pre-body (MeshPeerAuthenticator); nothing ran — retry once.
                String fresh = meshIdentity == null ? null : meshIdentity.currentToken().orElse(null);
                if (fresh != null && !fresh.equals(meshToken)) {
                    log.info("helper-router: peer {} ('{}') refused the mesh token (401) — "
                            + "re-handshook; retrying the stream once with the fresh identity",
                            peer.envId(), peer.name());
                    resp = sendPeerWithConnectRetry(
                            buildPeerLlmRequest(url, get, body, fresh,
                                    "text/event-stream, application/json", call.callId),
                            HttpResponse.BodyHandlers.ofInputStream(), peer, call);
                }
            }
            relayPeerLlmStream(peer, resp, sink, call);
        } catch (IOException e) {
            if (call.cancelled) {
                RoutedResponse err = llmError(502, "peer_call_cancelled",
                        "cross-env llm to " + peer.envId() + " cancelled with its run");
                sink.oneShot(err.status(), err.contentType(), err.body());
                return;
            }
            RoutedResponse err = llmError(502, CrossEnvCallException.ERROR_PEER_UNREACHABLE,
                    "cross-env llm: granted peer " + peer.envId() + " ('" + peer.name() + "') at "
                            + address + " is not reachable on the overlay ("
                            + e.getClass().getSimpleName() + ") — the tunnel is down or the gateway "
                            + "drops the flow. " + grantedPeerDirectory.size()
                            + " granted peer(s) known.");
            sink.oneShot(err.status(), err.contentType(), err.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            RoutedResponse err = llmError(502, CrossEnvCallException.ERROR_PEER_UNREACHABLE,
                    "cross-env llm to " + peer.envId() + " interrupted");
            sink.oneShot(err.status(), err.contentType(), err.body());
        } catch (Exception e) {
            RoutedResponse err = llmError(502, "peer_call_failed",
                    "cross-env llm to " + peer.envId() + " failed: " + str(e.getMessage()));
            sink.oneShot(err.status(), err.contentType(), err.body());
        } finally {
            if (llmCancellation != null) {
                llmCancellation.unregister(runKey, call);
            }
        }
    }

    /**
     * Relays a peer llm response onto the helper stream sink. Authz/provider-not-configured JSON stays
     * one-shot (the SDK reads {@code {error, detail}}).
     */
    private void relayPeerLlmStream(GrantedPeerDirectory.GrantedPeer peer,
                                    HttpResponse<InputStream> resp, HelperStreamSink sink,
                                    PeerLlmCall call)
            throws IOException {
        int status = resp.statusCode();
        String contentType = resp.headers().firstValue("Content-Type").orElse("application/json");
        try (InputStream in = resp.body()) {
            call.relaying(in);
            boolean sse = HelperSse.acceptsEventStream(contentType);
            if (!sse || status == 401 || status == 403) {
                byte[] all = in == null ? new byte[0] : in.readAllBytes();
                RoutedResponse mapped = mapPeerLlmResponse(peer, status, all, contentType);
                if (mapped.status() >= 400 || !isJsonCompletion(mapped.body())) {
                    sink.oneShot(mapped.status(), mapped.contentType(), mapped.body());
                    return;
                }
                reframeBufferedPeerAsSse(mapped.body(), sink);
                return;
            }
            AtomicInteger seq = new AtomicInteger();
            if (in != null) {
                byte[] buf = new byte[PEER_STREAM_SLICE_BYTES];
                int n;
                while ((n = in.read(buf)) != -1) {
                    if (n <= 0) {
                        continue;
                    }
                    sink.chunk(status, contentType, Arrays.copyOf(buf, n),
                            seq.getAndIncrement(), false);
                }
            }
            sink.chunk(status, contentType, new byte[0], seq.getAndIncrement(), true);
        }
    }

    /** True when {@code body} is a 2xx-shaped provider envelope, not a proxy {@code {error}} object. */
    private boolean isJsonCompletion(byte[] body) {
        if (body == null || body.length == 0) {
            return false;
        }
        try {
            JsonNode tree = mapper.readTree(body);
            return tree != null && tree.isObject() && !tree.has("error") && !tree.has("errorCode");
        } catch (Exception e) {
            return false;
        }
    }

    /** Wraps a unary peer completion so {@code studioquack.llm.chatStream} can ingest it. */
    private void reframeBufferedPeerAsSse(byte[] jsonBody, HelperStreamSink sink) {
        byte[] frame;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(
                    8 + (jsonBody == null ? 0 : jsonBody.length));
            out.write("data: ".getBytes(StandardCharsets.UTF_8));
            if (jsonBody != null && jsonBody.length > 0) {
                out.write(jsonBody);
            }
            out.write("\n\n".getBytes(StandardCharsets.UTF_8));
            frame = out.toByteArray();
        } catch (IOException e) {
            sink.oneShot(502, "application/json",
                    jsonBytes(Map.of("error", "peer_call_failed", "detail", str(e.getMessage()))));
            return;
        }
        sink.chunk(200, HelperSse.CONTENT_TYPE, frame, 0, false);
        sink.chunk(200, HelperSse.CONTENT_TYPE, SSE_DONE_FRAME, 1, true);
    }

    /**
     * One cross-env llm request against the peer's overlay route with {@code token}. No request
     * timeout — a slow peer model is waited on; the caller's run cancel ends the call on both sides.
     */
    private static HttpRequest buildPeerLlmRequest(String url, boolean get, byte[] body,
                                                   String token, String accept, String callId) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .header("Accept", accept)
                .header("Authorization", "Bearer " + token)
                .header(com.studioquack.sqs.channel.overlay.PeerCallRegistry.CALL_ID_HEADER, callId);
        if (get) {
            builder.GET();
        } else {
            builder.header("Content-Type", "application/json");
            builder.POST(HttpRequest.BodyPublishers.ofByteArray(
                    body == null || body.length == 0
                            ? "{}".getBytes(StandardCharsets.UTF_8) : body));
        }
        return builder.build();
    }

    /**
     * Re-handshake once to mint a fresh mesh token after a peer rejected the held one (401).
     * {@code false} when the secure channel is off or the refresh itself failed.
     */
    private boolean refreshMeshIdentity() {
        try {
            return meshIdentityRefresher != null && meshIdentityRefresher.getAsBoolean();
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * Mapping for a peer llm status/body: 401 → {@code Error_PeerRejectedIdentity}, 403 → the peer's
     * own code, 503 {@code provider_not_configured} → {@code Error_PeerProviderNotConfigured} naming
     * the PEER. Everything else is verbatim.
     */
    private RoutedResponse mapPeerLlmResponse(GrantedPeerDirectory.GrantedPeer peer,
                                              int status, byte[] respBody, String contentType) {
        if (status == 401) {
            return llmError(502, CrossEnvCallException.ERROR_PEER_REJECTED_IDENTITY,
                    "peer " + peer.envId() + " ('" + peer.name() + "') refused this daemon's mesh "
                            + "identity (401) — this daemon re-handshook and retried once; check "
                            + "the clock skew between the two daemons.");
        }
        if (status == 403) {
            return llmError(502, peerErrorCode(respBody, CrossEnvCallException.ERROR_NOT_GRANTED),
                    "peer " + peer.envId() + " ('" + peer.name() + "') denied the cross-env llm "
                            + "call (403): " + trimBody(respBody));
        }
        if (status == 503 && hasProxyError(respBody, "provider_not_configured")) {
            return llmError(503, CrossEnvCallException.ERROR_PEER_PROVIDER_NOT_CONFIGURED,
                    "the peer environment '" + str(peer.name()) + "' <" + peer.envId() + "> has no "
                            + "AI Model provider configured — set it on THAT environment's "
                            + "Settings → AI Model");
        }
        return RoutedResponse.raw(status, respBody, contentType);
    }

    /** Structured llm-leg error in the proxy's {@code {error, detail}} envelope — what the SDK reads. */
    private RoutedResponse llmError(int status, String code, String detail) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", code);
        body.put("detail", detail == null ? "" : detail);
        return RoutedResponse.json(status, jsonBytes(body));
    }

    /** True when a peer response body is the proxy's structured error carrying {@code code}. */
    private boolean hasProxyError(byte[] body, String code) {
        try {
            Map<?, ?> parsed = mapper.readValue(body, Map.class);
            return parsed != null && code.equals(parsed.get("error"));
        } catch (Exception e) {
            return false;
        }
    }

    /** Reads the callee's {@code errorCode} out of a 403 denial body; {@code fallback} when absent. */
    private String peerErrorCode(byte[] body, String fallback) {
        try {
            Map<?, ?> parsed = mapper.readValue(body, Map.class);
            Object code = parsed == null ? null : parsed.get("errorCode");
            return code == null ? fallback : String.valueOf(code);
        } catch (Exception e) {
            return fallback;
        }
    }

    private static String trimBody(byte[] body) {
        if (body == null || body.length == 0) {
            return "";
        }
        String s = new String(body, StandardCharsets.UTF_8);
        return s.length() <= 300 ? s : s.substring(0, 300) + "…";
    }

    /**
     * Daemon-mediated {@code studioquack.execute()}. Wire shape: request
     * {@code {portValue, payload, nodeId?, envId?}} → response {@code {result: <scalar|list>}}.
     */
    @SuppressWarnings("unchecked")
    private RoutedResponse handleExecute(HelperJob job, byte[] body) {
        RoutedResponse cancelled = refuseIfCancelled(job);
        if (cancelled != null) {
            return cancelled;
        }
        if (job.mockResponses() != null) {
            return handleMockExecute(job, body);
        }
        if (executeService == null) {
            return beForwardUnavailable("execute");
        }
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        String portValue = str(b.get("portValue"));
        if (portValue.isBlank()) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "portValue is required")));
        }
        try {
            Object result = executeService.handle(job, portValue, b.get("payload"),
                    emptyToNull(str(b.get("nodeId"))), emptyToNull(str(b.get("envId"))));
            return RoutedResponse.json(200, jsonBytes(result));
        } catch (CrossEnvCallException crossEnv) {
            // A recognised cross-env code rides the 502 in an errorCode field. 400-family codes stay 502: the
            // helper channel is not the auth boundary — the CODE is what the script/user reads.
            log.warn("helper-router: cross-env execute failed job={} port={} code={}: {}",
                    job.id(), portValue, crossEnv.errorCode(), crossEnv.getMessage());
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("errorCode", crossEnv.errorCode());
            err.put("error", "execute failed");
            err.put("detail", str(crossEnv.getMessage()));
            return RoutedResponse.json(502, jsonBytes(err));
        } catch (Exception e) {
            log.warn("helper-router: execute failed job={} port={}: {}", job.id(), portValue, e.toString());
            return RoutedResponse.json(502, jsonBytes(Map.of("error", "execute failed", "detail", str(e.getMessage()))));
        }
    }

    /**
     * TEST-MOCK {@code execute}: answer from {@link HelperJob#mockResponses()} and never call the
     * live sidecar. A miss still 200s with an empty result so the script stays deterministic.
     */
    @SuppressWarnings("unchecked")
    private RoutedResponse handleMockExecute(HelperJob job, byte[] body) {
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        String portValue = str(b.get("portValue"));
        if (portValue.isBlank()) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "portValue is required")));
        }
        boolean hit = job.mockResponses().containsKey(portValue);
        Map<String, Object> invocation = new LinkedHashMap<>();
        invocation.put("portValue", portValue);
        invocation.put("payload", b.get("payload"));
        invocation.put("mocked", hit);
        invocation.put("at", RFC3339.format(java.time.Instant.now()));
        job.recordMockInvocation(invocation);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("result", hit ? job.mockResponses().get(portValue) : Map.of());
        return RoutedResponse.json(200, jsonBytes(resp));
    }

    /**
     * Daemon-owned {@code test/*} surface. 503 when the orchestrator is absent, 403 when the job
     * is not a persistent allowlisted caller — deny before any build-run work.
     */
    private RoutedResponse handleTest(HelperJob job, String rest, byte[] body) {
        if (testSandboxRunService == null) {
            return RoutedResponse.json(503, jsonBytes(Map.of("error", "test_build_unavailable")));
        }
        if (!job.persistent() || !testSandboxRunService.allowsCaller(job.callerImageRef())) {
            return RoutedResponse.json(403, jsonBytes(Map.of("error", "test_build_forbidden")));
        }
        if ("build-run".equals(rest)) {
            return testSandboxRunService.buildRun(job, body);
        }
        return RoutedResponse.json(404, jsonBytes(Map.of("error", "no such endpoint", "path", "test/" + rest)));
    }

    /**
     * {@code studioquack.engineIP(engine_id)} — / . Request {@code {engineId}} (alias {@code envId}) →
     * {@code {ip}}.
     */
    private RoutedResponse handleEngineIp(byte[] body) {
        if (peerDirectory == null) {
            return beForwardUnavailable("engine-ip");
        }
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        String requested = emptyToNull(str(b.get("engineId")));
        if (requested == null) {
            requested = emptyToNull(str(b.get("envId")));
        }
        if (requested == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "engineId is required")));
        }
        if (peerDirectory.size() == 0) {
            return RoutedResponse.json(503, jsonBytes(Map.of("error", "overlay down",
                    "detail", "no same-owner peers known — the tunnel is not up")));
        }
        final String engineId = requested;
        return peerDirectory.addressOf(engineId)
                .map(ip -> RoutedResponse.json(200, jsonBytes(Map.of("engineId", engineId, "ip", ip))))
                .orElseGet(() -> RoutedResponse.json(404, jsonBytes(Map.of("error", "unknown engine",
                        "detail", "engine " + engineId + " is not a same-owner peer of this engine"))));
    }

    /** Daemon-mediated tunnel ops behind {@code studioquack.connect}. Wire shapes. */
    @SuppressWarnings("unchecked")
    private RoutedResponse handleConnect(HelperJob job, String op, byte[] body) {
        if (job.mockResponses() != null) {
            return RoutedResponse.json(403, jsonBytes(Map.of("error", "connect_disabled_in_test")));
        }
        if (connectService == null) {
            return RoutedResponse.json(503, jsonBytes(Map.of(
                    "error", "connect unavailable",
                    "detail", "the stateful-instance tunnel is not wired on this daemon")));
        }
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        try {
            return switch (op) {
                case "open" -> {
                    String name = str(b.get("containerName")).trim();
                    if (name.isBlank()) {
                        yield RoutedResponse.json(400, jsonBytes(Map.of("error", "containerName is required")));
                    }
                    int port = intOf(b.get("port"), -1);
                    String streamId = connectService.open(job, name, port);
                    yield RoutedResponse.json(200, jsonBytes(Map.of("streamId", streamId)));
                }
                case "send" -> {
                    connectService.send(job, str(b.get("streamId")), decodeB64(b.get("dataB64")));
                    yield RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
                }
                case "recv" -> {
                    HelperConnectService.Chunk chunk = connectService.recv(
                            job, str(b.get("streamId")), intOf(b.get("timeoutMs"), 5_000));
                    yield RoutedResponse.json(200, jsonBytes(Map.of(
                            "dataB64", Base64.getEncoder().encodeToString(chunk.data()),
                            "eof", chunk.eof())));
                }
                case "close" -> {
                    connectService.close(job, str(b.get("streamId")));
                    yield RoutedResponse.json(200, jsonBytes(Map.of("ok", true)));
                }
                default -> RoutedResponse.json(404, jsonBytes(Map.of(
                        "error", "no such endpoint", "path", "connect/" + op)));
            };
        } catch (HelperConnectService.ConnectException e) {
            return RoutedResponse.json(connectStatus(e.failure()), jsonBytes(Map.of(
                    "error", e.failure().name().toLowerCase(java.util.Locale.ROOT),
                    "detail", str(e.getMessage()))));
        } catch (IllegalArgumentException badBase64) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "dataB64 is not valid base64")));
        }
    }

    /** Keeps the SDK's failure taxonomy readable on the wire instead of collapsing it into one 500. */
    private static int connectStatus(HelperConnectService.Failure failure) {
        return switch (failure) {
            case NOT_FOUND -> 404;
            case FORBIDDEN -> 403;
            case UNAVAILABLE -> 503;
            case LIMIT -> 429;
            case CLOSED -> 409;
            case IO -> 502;
        };
    }

    private static byte[] decodeB64(Object raw) {
        String s = str(raw);
        return s.isEmpty() ? new byte[0] : Base64.getDecoder().decode(s);
    }

    private static int intOf(Object raw, int fallback) {
        if (raw instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(str(raw).trim());
        } catch (RuntimeException e) {
            return fallback;
        }
    }

    /**
     * All remaining the control plane-forwarding legs fail-closed until the secure channel owns the
     * bearer.
     */
    private RoutedResponse beForwardUnavailable(String endpoint) {
        return RoutedResponse.json(503, jsonBytes(Map.of(
                "error", "BE forward unavailable",
                "endpoint", endpoint,
                "detail", "cross-env forwarding (studioquack.execute/llm) lands with the secure "
                        + "channel; the daemon holds no BE bearer/URL yet.")));
    }

    /**
     * Test seam so slice tests can bind the emit-partial bus + graph-routed stream router
     * without going through Spring field injection.
     */
    public void wireStreamingForTests(HelperPartialEventBus bus, StreamEdgeRouter router) {
        this.partialBus = bus;
        this.streamEdgeRouter = router;
    }

    /**
     * Test seam so slice tests can bind throw/poll collaborators without Spring field injection.
     */
    void wireEventServicesForTests(SqsEngine engine,
                                   MessageCorrelationService correlation,
                                   DispatchCancellationRegistry cancellation,
                                   Redactor eventRedactor) {
        this.dispatchCancellationRegistry = cancellation;
        this.bpmnEventService = (engine != null || correlation != null)
                ? new HelperBpmnEventService(engine, correlation, eventRedactor)
                : null;
    }

    /**
     * {@code POST /v1/helper/emit-partial} {@code {port?, channel?, delta|chunk}} → {@code
     * {delivered:bool}}. A frame reaches the conversation only through the graph: when {@code port}
     * names one of the emitting node's own ports and an edge from that port reaches a live-render sink
     * (Duckbot end), the frame travels the workflow edges ({@link StreamEdgeRouter}).
     */
    @SuppressWarnings("unchecked")
    private RoutedResponse handleEmitPartial(HelperJob job, byte[] body) {
        RoutedResponse cancelled = refuseIfCancelled(job);
        if (cancelled != null) {
            return cancelled;
        }
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            return RoutedResponse.json(400, jsonBytes(Map.of("error", "bad json")));
        }
        String channel = str(b.get("channel"));
        if (channel.isBlank()) {
            channel = "answer";
        }
        String port = str(b.get("port"));
        Object raw = b.get("delta");
        if (raw == null) {
            raw = b.get("chunk");
        }
        String delta = deltaToString(raw);
        String childWfeId = str(job.executionContext().get("workflowExecutionId"));
        boolean toParent = partialBus != null && partialBus.emit(childWfeId, channel, delta);
        // Port-tagged frames are graph-routed, full stop: they render only when an edge from that port
        // reaches a live-render sink (Duckbot end). A wired-but-sinkless port or an unwired port drops the
        // frame — the graph, not the producer, decides what reaches the conversation.
        boolean tagged = !port.isBlank();
        boolean routed = tagged && streamEdgeRouter != null
                && streamEdgeRouter.route(job, port, channel, delta);
        if (log.isDebugEnabled()) {
            log.debug("helper-router: emit-partial job={} childWfeId={} port={} channel={} chars={} parent={} routed={}",
                    job.id(), childWfeId, port, channel, delta.length(), toParent, routed);
        }
        return RoutedResponse.json(200, jsonBytes(Map.of("delivered", toParent || routed)));
    }

    private String deltaToString(Object raw) {
        // No size cap here: the delta travels whole and is chunked into ≤64 KiB frames
        // only where per-frame seq is assigned (session enqueue / Duckbot sink emit).
        if (raw == null) {
            return "";
        }
        if (raw instanceof String s) {
            return s;
        }
        try {
            return mapper.writeValueAsString(raw);
        } catch (Exception e) {
            return String.valueOf(raw);
        }
    }

    /** {@code route()} fallback: collect SSE bytes then return a single RoutedResponse. */
    private RoutedResponse handleExecuteStreamBuffered(HelperJob job, byte[] body) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int[] status = {200};
        String[] contentType = {HelperSse.CONTENT_TYPE};
        handleExecuteStream(job, body, new HelperStreamSink() {
            @Override
            public void oneShot(int s, String ct, byte[] bytes) {
                status[0] = s;
                contentType[0] = ct == null ? "application/json" : ct;
                if (bytes != null && bytes.length > 0) {
                    buf.writeBytes(bytes);
                }
            }

            @Override
            public void chunk(int s, String ct, byte[] chunk, int seq, boolean done) {
                status[0] = s;
                if (ct != null && !ct.isBlank()) {
                    contentType[0] = ct;
                }
                if (chunk != null && chunk.length > 0) {
                    buf.writeBytes(chunk);
                }
            }
        });
        return RoutedResponse.raw(status[0], buf.toByteArray(), contentType[0]);
    }

    /**
     * Local execute/stream: child emit-partials become {@code event: partial}; the aggregated
     * result is {@code event: complete} then {@code event: done}. Errors are {@code event: error}.
     */
    @SuppressWarnings("unchecked")
    private void handleExecuteStream(HelperJob job, byte[] body, HelperStreamSink sink) {
        RoutedResponse cancelled = refuseIfCancelled(job);
        if (cancelled != null) {
            sink.oneShot(cancelled.status(), cancelled.contentType(), cancelled.body());
            return;
        }
        if (job.mockResponses() != null) {
            RoutedResponse mock = handleMockExecute(job, body);
            if (mock.status() != 200) {
                sink.oneShot(mock.status(), mock.contentType(), mock.body());
                return;
            }
            streamComplete(sink, readBody(mock.body(), Map.class));
            return;
        }
        if (executeService == null) {
            RoutedResponse unavailable = beForwardUnavailable("execute/stream");
            sink.oneShot(unavailable.status(), unavailable.contentType(), unavailable.body());
            return;
        }
        Map<String, Object> b = readBody(body, Map.class);
        if (b == null) {
            sink.oneShot(400, "application/json", jsonBytes(Map.of("error", "bad json")));
            return;
        }
        String portValue = str(b.get("portValue"));
        if (portValue.isBlank()) {
            sink.oneShot(400, "application/json", jsonBytes(Map.of("error", "portValue is required")));
            return;
        }
        AtomicInteger seq = new AtomicInteger();
        HelperExecuteService.ExecuteStreamListener listener = new HelperExecuteService.ExecuteStreamListener() {
            @Override
            public void onPartial(String channel, Object payload) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("channel", (channel == null || channel.isBlank()) ? "answer" : channel);
                data.put("delta", payload);
                sendSse(sink, seq, "partial", data, false);
            }

            @Override
            public void onComplete(Object result) {
                sendSse(sink, seq, "complete", result == null ? Map.of() : result, false);
                sendSse(sink, seq, "done", Map.of(), true);
            }

            @Override
            public void onError(String message) {
                sendSse(sink, seq, "error", Map.of("message", message == null ? "stream error" : message), false);
                sendSse(sink, seq, "done", Map.of(), true);
            }
        };
        try {
            executeService.handleStream(job, portValue, b.get("payload"),
                    emptyToNull(str(b.get("nodeId"))), emptyToNull(str(b.get("envId"))), listener);
        } catch (CrossEnvCallException crossEnv) {
            log.warn("helper-router: cross-env execute/stream failed job={} port={} code={}: {}",
                    job.id(), portValue, crossEnv.errorCode(), crossEnv.getMessage());
            listener.onError(str(crossEnv.getMessage()));
        } catch (Exception e) {
            log.warn("helper-router: execute/stream failed job={} port={}: {}",
                    job.id(), portValue, e.toString());
            listener.onError(str(e.getMessage()));
        }
    }

    private void streamComplete(HelperStreamSink sink, Object result) {
        AtomicInteger seq = new AtomicInteger();
        sendSse(sink, seq, "complete", result == null ? Map.of() : result, false);
        sendSse(sink, seq, "done", Map.of(), true);
    }

    private void sendSse(HelperStreamSink sink, AtomicInteger seq, String event, Object data, boolean done) {
        sink.chunk(200, HelperSse.CONTENT_TYPE, HelperSse.frame(mapper, event, data),
                seq.getAndIncrement(), done);
    }

    /**
     * Local llm/* with {@code stream=true}: forward provider SSE chunks verbatim.
     * Cross-env overlay asks the peer for SSE ({@link #forwardLlmToPeerStream}) and
     * relays slices onto this sink — a one-shot JSON peer body is reframed as SSE.
     */
    private void handleLlmStream(HelperJob job, String method, String llmSub, byte[] body,
                                 HelperStreamSink sink) {
        if (job.mockResponses() != null) {
            sink.oneShot(501, "application/json", jsonBytes(Map.of("error", "llm_disabled_in_test")));
            return;
        }
        String envId = null;
        byte[] forwardBody = body;
        if (body != null && body.length > 0) {
            try {
                JsonNode tree = mapper.readTree(body);
                if (tree instanceof ObjectNode obj && obj.has("envId")) {
                    JsonNode removed = obj.remove("envId");
                    envId = removed == null || removed.isNull() ? null : removed.asText(null);
                    forwardBody = mapper.writeValueAsBytes(obj);
                }
            } catch (Exception notJson) {
                // forward verbatim
            }
        }
        String selfEnvId = str(job.executionContext().get("envId"));
        Optional<GrantedPeerDirectory.GrantedPeer> peer;
        try {
            peer = CrossEnvPeerResolver.resolvePeer(envId, selfEnvId,
                    meshIdentity == null ? null : meshIdentity.ownEnvName().orElse(null),
                    grantedPeerDirectory, crossEnvAuthorizationClient, "llm");
        } catch (CrossEnvCallException e) {
            sink.oneShot(502, "application/json", jsonBytes(Map.of(
                    "error", e.errorCode(), "detail", str(e.getMessage()))));
            return;
        }
        recordLlmRequestSummary(job, llmSub, forwardBody, true);
        if (peer.isPresent()) {
            forwardLlmToPeerStream(peer.get(), method, llmSub, forwardBody, sink,
                    str(job.executionContext().get("workflowExecutionId")));
            return;
        }
        AtomicInteger seq = new AtomicInteger();
        AtomicBoolean usedOneShot = new AtomicBoolean();
        // The owning run's id lets cancel-instance abort this provider stream mid-generation
        // (LlmStreamCancellationRegistry) — otherwise the local LLM server keeps producing
        // tokens for a run that no longer exists.
        String runKey = str(job.executionContext().get("workflowExecutionId"));
        llmProxy.forwardStream(llmSub, method, forwardBody, (status, contentType, chunk, last) -> {
            byte[] bytes = chunk == null ? new byte[0] : chunk;
            String ct = contentType == null || contentType.isBlank() ? "application/json" : contentType;
            if (!usedOneShot.get() && seq.get() == 0 && last && !ct.toLowerCase(java.util.Locale.ROOT)
                    .contains("event-stream")) {
                sink.oneShot(status, ct, bytes);
                usedOneShot.set(true);
                return;
            }
            sink.chunk(status, ct, bytes, seq.getAndIncrement(), last);
        }, runKey);
    }

    /** True when the JSON body is an object with boolean {@code stream:true}. */
    static boolean wantsStream(byte[] body) {
        if (body == null || body.length == 0) {
            return false;
        }
        try {
            JsonNode tree = new ObjectMapper().readTree(body);
            JsonNode stream = tree == null ? null : tree.get("stream");
            return stream != null && stream.isBoolean() && stream.booleanValue();
        } catch (Exception e) {
            return false;
        }
    }

    /* ------------------------------------------------------------------ */
    /* JSON plumbing                                                      */
    /* ------------------------------------------------------------------ */

    private <T> T readBody(byte[] body, Class<T> type) {
        try {
            if (body == null || body.length == 0) {
                return null;
            }
            return mapper.readValue(body, type);
        } catch (Exception e) {
            return null;
        }
    }

    private byte[] jsonBytes(Object body) {
        try {
            return mapper.writeValueAsBytes(body == null ? Map.of() : body);
        } catch (Exception e) {
            return "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    private String resolvePeerIp(GrantedPeerDirectory.GrantedPeer peer) {
        if (peer == null) {
            return null;
        }
        if (peerAddressResolver != null) {
            return peerAddressResolver.resolve(peer.envId()).orElse(null);
        }
        return null;
    }

    private String peerScheme() {
        return meshPeerHttp != null && meshPeerHttp.meshCaPem() != null ? "https" : "http";
    }

    private HttpClient peerHttp(String targetEnvId) {
        if (meshPeerHttp != null && meshPeerHttp.meshCaPem() != null) {
            return meshPeerHttp.clientFor(targetEnvId);
        }
        return peerLlmHttp;
    }

    void setPeerAddressResolver(com.studioquack.sqs.channel.net.PeerAddressResolver resolver) {
        this.peerAddressResolver = resolver;
    }

    void setMeshPeerHttp(com.studioquack.sqs.channel.net.MeshPeerHttp http) {
        this.meshPeerHttp = http;
    }

    /** Test seam — swaps the fallback (non-mesh) peer client so dial retries can be driven. */
    void setPeerLlmHttp(HttpClient client) {
        this.peerLlmHttp = client;
    }
}
