package com.studioquack.sqs.channel.overlay;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.common.security.tls.HybridTls;
import com.studioquack.sqs.channel.BoundedExchangeBody;
import com.studioquack.sqs.channel.ListenerExecutors;
import com.studioquack.sqs.channel.net.MeshTls;
import com.studioquack.sqs.channel.net.OverlayAddressChangedEvent;
import com.studioquack.sqs.sandbox.helperchan.HelperExecuteService;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperSse;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The inbound daemon→daemon PEER listener. An embedded, dependency-free JDK server the daemon binds to its OWN
 * overlay IP (never {@code 0.0.0.0}) so sibling daemons can deliver a cross-environment
 * {@code studioquack.execute()} directly, without a control-plane hop.
 */
@Slf4j
public final class OverlayPeerListener {

    /**
     * The cross-env execute route — the far side of {@code HelperExecuteService.forwardToPeer}.
     */
    public static final String EXECUTE_PATH = "/sqs/v1/overlay/execute";
    /**
     * The cross-env LLM route prefix — the remainder (which may contain slashes, e.g. {@code chat/completions}) is
     * the sub-path handed to {@link AiModelProviderProxy}.
     */
    public static final String LLM_PATH_PREFIX = "/sqs/v1/overlay/llm/";

    /**
     * Concurrent peer requests served at once; each parks on a virtual thread while it runs.
     */
    static final int MAX_IN_FLIGHT = 256;

    /**
     * Config bind-address override. Blank (default) means the host follows the agent-reported overlay IP and
     * {@link OverlayAddressChangedEvent} may rebind.
     */
    private volatile String bindOverride = "";
    /**
     * Current bind host — mutates on {@link #rebind(String)}.
     */
    private volatile String bindHost;
    private final int port;
    private final HelperExecuteService helperExecuteService;
    private final PeerCallAuthenticator authenticator;
    private volatile com.studioquack.sqs.channel.net.NetTelemetry telemetry;
    /**
     * The provider proxy behind the llm route; {@code null} only in slice-1 tests.
     */
    private final AiModelProviderProxy llmProxy;
    private final ObjectMapper mapper;
    /**
     * Production TLS context (listener leaf + key). {@code null} keeps a plain {@link HttpServer} for unit tests
     * that speak HTTP to {@code 127.0.0.1}.
     */
    private final SSLContext sslContext;

    /**
     * Calls being served for peers, so each caller can cancel its own when its run ends.
     */
    private final PeerCallRegistry peerCalls = new PeerCallRegistry();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile HttpServer server;
    private volatile ExecutorService pool;
    /**
     * Log hybrid-TLS availability once per JVM at the first TLS start.
     */
    private static final AtomicBoolean TLS_PROBE_LOGGED = new AtomicBoolean(false);

    /**
     * Slice-1 constructor — no llm route backing; {@code /sqs/v1/overlay/llm/*} answers 503.
     */
    public OverlayPeerListener(String bindHost, int port, HelperExecuteService helperExecuteService,
                               PeerCallAuthenticator authenticator, ObjectMapper mapper) {
        this(bindHost, port, helperExecuteService, authenticator, null, mapper, (SSLContext) null);
    }

    public OverlayPeerListener(String bindHost, int port, HelperExecuteService helperExecuteService,
                               PeerCallAuthenticator authenticator, AiModelProviderProxy llmProxy,
                               ObjectMapper mapper) {
        this(bindHost, port, helperExecuteService, authenticator, llmProxy, mapper, (SSLContext) null);
    }

    /**
     * Production path with TLS-error telemetry (status.net.tlsHandshakeErrors).
     */
    public OverlayPeerListener(String bindHost, int port, HelperExecuteService helperExecuteService,
                               PeerCallAuthenticator authenticator, AiModelProviderProxy llmProxy,
                               ObjectMapper mapper, com.studioquack.sqs.channel.net.NetTelemetry telemetry) {
        this(bindHost, port, helperExecuteService, authenticator, llmProxy, mapper, (SSLContext) null);
        this.telemetry = telemetry;
    }

    /**
     * Production constructor: pass a non-null {@code ssl} from {@link MeshTls#serverContext} so {@link #start()}
     * binds {@link HttpsServer}. {@code ssl == null} keeps the HTTP test seam used by existing unit tests.
     */
    public OverlayPeerListener(String bindHost, int port, HelperExecuteService helperExecuteService,
                               PeerCallAuthenticator authenticator, AiModelProviderProxy llmProxy,
                               ObjectMapper mapper, SSLContext ssl) {
        this.bindHost = bindHost;
        this.port = port;
        this.helperExecuteService = helperExecuteService;
        this.authenticator = authenticator == null ? PeerCallAuthenticator.allowAll() : authenticator;
        this.llmProxy = llmProxy;
        this.mapper = mapper == null ? new ObjectMapper() : mapper;
        this.sslContext = ssl;
    }

    /**
     * Pins the bind-address override (blank = follow overlay IP changes).
     */
    public void setBindOverride(String bindOverride) {
        this.bindOverride = bindOverride == null ? "" : bindOverride.trim();
    }

    /**
     * Current bind host (after construction or the last successful {@link #rebind}).
     */
    public String bindHost() {
        return bindHost;
    }

    /**
     * Binds + starts the server (idempotent). Refuses a blank/wildcard bind host.
     */
    public synchronized void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            bindAndServe();
        } catch (IOException | RuntimeException ex) {
            running.set(false);
            throw ex;
        }
    }

    /**
     * Stop + start on {@code newHost}. Refuses the wildcard.
     */
    public synchronized void rebind(String newHost) throws IOException {
        assertOverlayBindAddress(newHost);
        String next = newHost.trim();
        if (running.get() && next.equals(this.bindHost)) {
            return;
        }
        boolean wasRunning = running.get();
        if (wasRunning) {
            stopInternal();
        }
        this.bindHost = next;
        if (wasRunning) {
            start();
        }
    }

    /**
     * Agent overlay IP changed. Rebind only while serving and only when the config override is blank — an explicit
     * bind-address pin must stay put.
     */
    @EventListener
    public void onOverlayAddressChanged(OverlayAddressChangedEvent event) {
        if (event == null || event.currentIp() == null || event.currentIp().isBlank()) {
            return;
        }
        if (!running.get() || !bindOverride.isBlank()) {
            return;
        }
        try {
            rebind(event.currentIp());
        } catch (Exception ex) {
            log.warn("channel/overlay: peer listener rebind to {} failed: {}",
                    event.currentIp(), ex.toString());
        }
    }

    private void bindAndServe() throws IOException {
        assertOverlayBindAddress(bindHost);
        HttpServer s;
        if (sslContext != null) {
            // Production: TLS with hybrid named groups; TLS 1.3 preferred.
            HttpsServer hs = HttpsServer.create(new InetSocketAddress(bindHost, port), 0);
            hs.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                @Override
                public void configure(HttpsParameters params) {
                    SSLParameters sp = HybridTls.hybridParameters(getSSLContext(), false);
                    params.setSSLParameters(sp);
                    // A refused peer handshake (handshake_failure / certificate_unknown) is
                    // otherwise silent on this side: the caller gets 502 and the run fails with
                    // error_PeerUnreachable while this daemon shows nothing. The HttpsParameters
                    // API exposes no engine/socket hook, so the refusal is counted and logged in
                    // route()'s catch (below) — the only place the exception surfaces.
                    
                }
            });
            s = hs;
            if (TLS_PROBE_LOGGED.compareAndSet(false, true)) {
                log.info("channel/overlay: peer listener TLS available={} groups={}",
                        HybridTls.available(), String.join(",", HybridTls.NAMED_GROUPS));
            }
        } else {
            // Test seam: existing HTTP clients on 127.0.0.1.
            s = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        }
        s.createContext("/", this::route);
        // Virtual threads: a cross-env llm or execute call stays open for as long as it runs.
        ExecutorService ex = ListenerExecutors.virtual(
                "sqs-overlay-peer", OverlayPeerListener.class.getClassLoader(), MAX_IN_FLIGHT);
        s.setExecutor(ex);
        s.start();
        this.pool = ex;
        this.server = s;
        if (sslContext != null) {
            log.info("channel/overlay: peer listener up on {}:{} (TLS, hybrid groups)",
                    bindHost, s.getAddress().getPort());
        } else {
            log.info("channel/overlay: peer listener up on {}:{} (HTTP test seam)",
                    bindHost, s.getAddress().getPort());
        }
    }

    /**
     * Stops the server + drains the pool (idempotent, best-effort).
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        stopInternal();
        log.info("channel/overlay: peer listener stopped");
    }

    /**
     * Server teardown; forces {@link #running} false so {@link #start()} can bind again.
     */
    private void stopInternal() {
        HttpServer s = this.server;
        if (s != null) {
            s.stop(0);
        }
        ExecutorService p = this.pool;
        if (p != null) {
            p.shutdownNow();
        }
        this.server = null;
        this.pool = null;
        running.set(false);
    }

    /**
     * True while the listener is bound + serving.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * The bound port (the ephemeral one when the constructor port was 0 — handy for tests).
     */
    public int boundPort() {
        HttpServer s = this.server;
        return s == null ? port : s.getAddress().getPort();
    }

    /**
     * Fail closed at the last responsible moment: the peer endpoint binds the overlay IP ONLY. A blank host means
     * no overlay address was resolved, and a wildcard would expose the route to the client LAN — both refuse the
     * bind rather than opening an unauthenticated socket.
     */
    static void assertOverlayBindAddress(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalStateException("overlay peer listener: no overlay bind address — "
                    + "refusing to start (fail closed; the wildcard is never a fallback)");
        }
        InetAddress addr;
        try {
            addr = InetAddress.getByName(host.trim());
        } catch (java.net.UnknownHostException e) {
            throw new IllegalStateException("overlay peer listener: unresolvable bind host " + host, e);
        }
        if (addr.isAnyLocalAddress()) {
            throw new IllegalStateException("overlay peer listener: wildcard bind address " + host
                    + " refused — the peer endpoint must bind the WireGuard overlay IP only");
        }
    }

    // Routing ------------------------------------------------------------

    private void route(HttpExchange ex) throws IOException {
        try {
            if (ex instanceof com.sun.net.httpserver.HttpsExchange https) {
                HybridTls.logNegotiatedOnce(https.getSSLSession(), "overlay-peer");
            }
            String path = ex.getRequestURI().getPath();
            if (EXECUTE_PATH.equals(path)) {
                handleExecute(ex);
            } else if (path != null && path.startsWith(LLM_PATH_PREFIX)) {
                handleLlm(ex, path.substring(LLM_PATH_PREFIX.length()));
            } else if (PeerCallRegistry.CANCEL_PATH.equals(path)) {
                handleCancel(ex);
            } else {
                writeError(ex, 404, "Error_NotFound", "no such overlay route: " + path);
            }
        } catch (Exception e) {
            com.studioquack.sqs.channel.net.NetTelemetry t = telemetry;
            if (t != null && (e instanceof javax.net.ssl.SSLHandshakeException
                    || String.valueOf(e.getCause()).contains("SSLHandshake"))) {
                t.noteTlsHandshakeError();
            }
            log.warn("channel/overlay: peer request handling failed {} {}: {}",
                    ex.getRequestMethod(), ex.getRequestURI(), e.toString());
            safeWriteError(ex, 500, "Error_Internal", "internal error");
        } finally {
            ex.close();
        }
    }

    /**
     * {@code POST /sqs/v1/overlay/execute} — runs the function-port target on THIS daemon's engine via
     * {@link HelperExecuteService#executeForPeer}, deliberately the same execution path a same-environment call
     * uses. Error mapping mirrors the retired controller exactly so the caller's status-based reporting is
     * unchanged by the move off the loopback server.
     */
    private void handleExecute(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            writeError(ex, 405, "Error_MethodNotAllowed", "POST required");
            return;
        }
        // Mesh token + inbound grant for SCOPE_EXECUTE, before any body is read.
        PeerCallAuthenticator.AuthResult auth = authenticator.authenticate(
                ex.getRequestHeaders().getFirst("Authorization"), PeerCallAuthenticator.SCOPE_EXECUTE);
        if (!auth.allowed()) {
            writeError(ex, auth.httpStatus(), auth.errorCode(), auth.message());
            return;
        }
        byte[] raw;
        try {
            raw = BoundedExchangeBody.read(ex.getRequestBody());
        } catch (BoundedExchangeBody.TooLarge tooLarge) {
            writeError(ex, 413, "Error_RequestBodyTooLarge", tooLarge.getMessage());
            return;
        }
        ExecuteRequest body;
        try {
            body = mapper.readValue(raw, ExecuteRequest.class);
        } catch (Exception parseErr) {
            writeMissingContext(ex);
            return;
        }
        if (body == null || body.executionContext() == null) {
            writeMissingContext(ex);
            return;
        }
        // The caller's cancel revokes this stand-in for its request, which cancels the child run here.
        String callId = ex.getRequestHeaders().getFirst(PeerCallRegistry.CALL_ID_HEADER);
        HelperJob caller = callId == null || callId.isBlank() ? null
                : HelperJob.builder("peer-call:" + callId, "peer").executionContext(Map.of()).build();
        boolean cancellable = caller != null && peerCalls.open(
                callId, auth.callerEnvId(), PeerCallAuthenticator.SCOPE_EXECUTE, caller::markRevoked);
        try {
            Object result = helperExecuteService.executeForPeer(
                    body.executionContext(), body.targetNodeId(), body.portValue(), body.payload(),
                    cancellable ? caller : null);
            Map<String, Object> ok = new LinkedHashMap<>();
            ok.put("result", result);
            writeJson(ex, 200, mapper.writeValueAsString(ok));
        } catch (IllegalStateException notDeployed) {
            // The caller named a node this environment does not have deployed. A 404 lets the caller
            // report "the target does not exist here" rather than a generic failure.
            log.warn("overlay-execute: {}", notDeployed.getMessage());
            writeError(ex, 404, "Error_TargetNotDeployed", notDeployed.getMessage());
        } catch (Exception e) {
            log.warn("overlay-execute failed node={}: {}", body.targetNodeId(), e.toString());
            writeError(ex, 500, "Error_ExecuteFailed", "execute failed");
        } finally {
            if (cancellable) {
                peerCalls.close(callId);
            }
        }
    }

    /**
     * {@code POST /sqs/v1/overlay/cancel} — the caller's cancel for one call it placed here (its run ended while
     * the call was in flight). Authenticated with the cancelled call's own scope, and honoured only from the
     * environment that placed it; there is no body to read.
     */
    private void handleCancel(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            writeError(ex, 405, "Error_MethodNotAllowed", "POST required");
            return;
        }
        String callId = ex.getRequestHeaders().getFirst(PeerCallRegistry.CALL_ID_HEADER);
        String scope = peerCalls.scopeOf(callId);
        if (scope == null) {
            ex.sendResponseHeaders(404, -1);
            return;
        }
        PeerCallAuthenticator.AuthResult auth = authenticator.authenticate(
                ex.getRequestHeaders().getFirst("Authorization"), scope);
        if (!auth.allowed()) {
            writeError(ex, auth.httpStatus(), auth.errorCode(), auth.message());
            return;
        }
        boolean cancelled = peerCalls.cancel(callId, auth.callerEnvId());
        log.info("channel/overlay: peer cancel call={} scope={} cancelled={}", callId, scope, cancelled);
        ex.sendResponseHeaders(cancelled ? 204 : 404, -1);
    }

    /**
     * {@code /sqs/v1/overlay/llm/{sub}} —, the callee side of a cross-env {@code studioquack.llm.*} call. A thin
     * authorization wrapper around {@link AiModelProviderProxy}: the authenticator runs FIRST with
     * {@code SCOPE_LLM} (before the body is read), then the raw body delegates to the proxy and its result is
     * written verbatim.
     */
    private void handleLlm(HttpExchange ex, String sub) throws IOException {
        String method = ex.getRequestMethod();
        if (!"POST".equalsIgnoreCase(method) && !"GET".equalsIgnoreCase(method)) {
            writeError(ex, 405, "Error_MethodNotAllowed", "POST or GET required");
            return;
        }
        if (sub == null || sub.isBlank()) {
            writeError(ex, 404, "Error_NotFound", "no such overlay route: " + LLM_PATH_PREFIX);
            return;
        }
        // Authz BEFORE the body is read — identical seam and denial contract as the execute route,
        // scoped to llm.
        PeerCallAuthenticator.AuthResult auth = authenticator.authenticate(
                ex.getRequestHeaders().getFirst("Authorization"), PeerCallAuthenticator.SCOPE_LLM);
        if (!auth.allowed()) {
            writeError(ex, auth.httpStatus(), auth.errorCode(), auth.message());
            return;
        }
        if (llmProxy == null) {
            // Slice-1 wiring (tests): fail closed and distinguishable rather than NPE mid-request.
            writeError(ex, 503, "Error_LlmUnavailable",
                    "no AI model provider proxy is wired on this daemon");
            return;
        }
        byte[] body;
        try {
            body = BoundedExchangeBody.read(ex.getRequestBody());
        } catch (BoundedExchangeBody.TooLarge tooLarge) {
            writeError(ex, 413, "Error_RequestBodyTooLarge", tooLarge.getMessage());
            return;
        }
        boolean stream = HelperSse.acceptsEventStream(ex.getRequestHeaders().getFirst("Accept"));
        // Shapes only — never the prompt/completion body (PHI).
        log.info("channel/overlay: llm {} {} stream={}", method, sub, stream);
        // getPath() drops the query; a job-scoped op (download/status?job_id=…) needs it back.
        String query = ex.getRequestURI().getRawQuery();
        // The caller's cancel aborts the provider call through the proxy's run-key registry.
        String callId = ex.getRequestHeaders().getFirst(PeerCallRegistry.CALL_ID_HEADER);
        String runKey = "peer-call:" + callId;
        boolean cancellable = callId != null && !callId.isBlank() && peerCalls.open(
                callId, auth.callerEnvId(), PeerCallAuthenticator.SCOPE_LLM, () -> llmProxy.abortRun(runKey));
        try {
            if (stream) {
                handleLlmStream(ex, sub, method.toUpperCase(), query, body, cancellable ? runKey : null);
                return;
            }
            AiModelProviderProxy.ProxyResult result = llmProxy.forward(
                    sub, method.toUpperCase(), query, body, cancellable ? runKey : null);
            writeRaw(ex, result.status(), result.contentType(), result.body());
        } finally {
            if (cancellable) {
                peerCalls.close(callId);
            }
        }
    }

    /**
     * The SSE half of the llm route: the proxy's stream consumer fires synchronously on THIS thread, so each
     * provider chunk is written to the peer response as it arrives — chunked transfer (
     * {@code sendResponseHeaders(status, 0)}) because the length is unknowable until the provider hangs up. The
     * first callback decides the shape: {@code last=true} means a buffered outcome (pre-flight error, or a body
     * that was not actually {@code stream=true}) and gets an ordinary fixed-length response; {@code last=false}
     * commits the chunked stream.
     */
    private void handleLlmStream(HttpExchange ex, String sub, String method, String query, byte[] body,
                                 String runKey) {
        AtomicBoolean started = new AtomicBoolean(false);
        AtomicReference<OutputStream> streamOut = new AtomicReference<>();
        try {
            llmProxy.forwardStream(sub, method, body, query,
                    (AiModelProviderProxy.StreamConsumer) (status, contentType, chunk, last) -> {
                        byte[] bytes = chunk == null ? new byte[0] : chunk;
                try {
                    if (started.compareAndSet(false, true) && last) {
                        writeRaw(ex, status, contentType, bytes);
                        return;
                    }
                    OutputStream out = streamOut.get();
                    if (out == null) {
                        ex.getResponseHeaders().add("Content-Type",
                                contentType == null || contentType.isBlank()
                                        ? HelperSse.CONTENT_TYPE : contentType);
                        ex.sendResponseHeaders(status, 0);
                        out = ex.getResponseBody();
                        streamOut.set(out);
                    }
                    if (bytes.length > 0) {
                        out.write(bytes);
                        out.flush();
                    }
                } catch (IOException io) {
                    throw new UncheckedIOException(io);
                }
            }, runKey);
        } catch (UncheckedIOException io) {
            log.debug("channel/overlay: llm stream to peer failed mid-response: {}", io.toString());
        }
    }

    /**
     * The exact 400 body the loopback controller returned: {@code errorCode} only, no message key.
     */
    private void writeMissingContext(HttpExchange ex) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", "Error_MissingContext");
        writeJson(ex, 400, mapper.writeValueAsString(body));
    }

    // Helpers ------------------------------------------------------------

    private void writeJson(HttpExchange ex, int status, String json) throws IOException {
        byte[] out = json == null ? new byte[0] : json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, out.length == 0 ? -1 : out.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(out);
        }
    }

    /**
     * Writes a proxied result VERBATIM — the provider's status, content type and raw bytes.
     */
    private void writeRaw(HttpExchange ex, int status, String contentType, byte[] body) throws IOException {
        byte[] out = body == null ? new byte[0] : body;
        ex.getResponseHeaders().add("Content-Type",
                contentType == null || contentType.isBlank() ? "application/json" : contentType);
        ex.sendResponseHeaders(status, out.length == 0 ? -1 : out.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(out);
        }
    }

    private void writeError(HttpExchange ex, int status, String code, String message) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", code);
        body.put("message", message == null ? "" : message);
        writeJson(ex, status, mapper.writeValueAsString(body));
    }

    private void safeWriteError(HttpExchange ex, int status, String code, String message) {
        try {
            writeError(ex, status, code, message);
        } catch (IOException ignored) {
            // Headers already flushed (handler failed mid-response) — nothing more we can send.
        }
    }

    /**
     * @param executionContext the caller's context, already transformed for the target port
     * @param targetNodeId the function-port target to run here
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record ExecuteRequest(Map<String, Object> executionContext, String targetNodeId,
                          String portValue, Object payload) {
    }
}
