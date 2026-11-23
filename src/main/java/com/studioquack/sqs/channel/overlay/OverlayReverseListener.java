package com.studioquack.sqs.channel.overlay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.BoundedExchangeBody;
import com.studioquack.sqs.channel.DispatchExecutor;
import com.studioquack.sqs.channel.ListenerExecutors;
import com.studioquack.sqs.channel.MtlsIdentity;
import com.studioquack.sqs.channel.command.CommandLaneDispatcher;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.command.DaemonCommandRegistry;
import com.studioquack.sqs.channel.command.DaemonCommandResult;
import com.studioquack.common.security.tls.HybridTls;
import com.studioquack.sqs.channel.net.OverlayAddressChangedEvent;
import com.studioquack.sqs.sandbox.stateful.OciImageBlobIngestService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The inbound WireGuard-overlay reverse listener. An embedded, dependency-free mTLS HTTPS server (JDK
 * {@link HttpsServer}) the daemon binds to its OWN overlay IP so it is reachable EXCLUSIVELY through the
 * decrypted WireGuard tunnel.
 */
@Slf4j
public final class OverlayReverseListener {

    /**
     * BE→daemon synchronous dispatch (mirrors the control plane overlay target).
     */
    static final String JOBS_PATH = "/sqvm/v1/jobs";
    /**
     * BE→daemon synchronous command execution (commit-secret / set-deps / status / …) over the tunnel.
     */
    static final String COMMAND_PATH = "/sqvm/v1/command";
    /**
     * Prefix of the image-blob relay path. {@link #OCI_SUFFIX} is the matching suffix.
     */
    static final String OCI_PREFIX = "/sqvm/v1/oci-images/";
    static final String OCI_SUFFIX = "/blob";

    /**
     * Wall-clock hint handed to the {@link DispatchExecutor}. Matches the BE's 7-day {@code RESPONSE_TIMEOUT} /
     * {@code HEARTBEAT_AWAIT} so a legitimately long agentic job (multi-leg LLM + tool fan-outs) is never capped
     * here; the executor owns the real budget.
     */
    private static final Duration DISPATCH_DEADLINE = Duration.ofDays(7);

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
    private final SSLContext sslContext;
    private final boolean requireClientAuth;
    private final DispatchExecutor dispatchExecutor;
    private final OciImageBlobIngestService ociIngest;
    private final DaemonCommandRegistry commandRegistry;
    /**
     * Serial/immediate lanes for overlay {@code deploy}/{@code start-run}. Null only when the registry itself is
     * unwired (the 503 path).
     */
    private final CommandLaneDispatcher commandLane;
    private final ObjectMapper mapper;

    /**
     * Optional hook fired after a NON-POLL command executes on this channel. An inbound {@code start-run} /
     * {@code deploy} / secret write is a change of daemon state the cadence cannot observe on its own — without
     * the nudge, a daemon sleeping in the 60s keepalive would report the resulting RUNNING/status flip only on its
     * next scheduled beat, so a command that arrived instantly would still LOOK late.
     */
    private volatile Runnable commandActivityHook;

    /**
     * Optional hook fired when ANY authenticated request reaches this listener — including poll types and even
     * unknown paths. The request arriving at all is the only locally observable proof that the backend can reach
     * this daemon inbound over the tunnel, which is what {@code HeartbeatCadence}'s OVERLAY_SILENT mode rests on
     * once beat-carried {@code overlayReachable} confirmations stop.
     */
    private volatile Runnable inboundTrafficHook;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile HttpsServer server;
    private volatile ExecutorService pool;

    public OverlayReverseListener(String bindHost, int port, SSLContext sslContext, boolean requireClientAuth,
                                  DispatchExecutor dispatchExecutor, OciImageBlobIngestService ociIngest,
                                  DaemonCommandRegistry commandRegistry, ObjectMapper mapper) {
        this.bindHost = bindHost;
        this.port = port;
        this.sslContext = sslContext;
        this.requireClientAuth = requireClientAuth;
        this.dispatchExecutor = dispatchExecutor;
        this.ociIngest = ociIngest;
        this.commandRegistry = commandRegistry;
        // Own lanes: overlay deploy then start-run stay FIFO even though each HTTPS hop returns
        // on ACCEPT. Result sink is a no-op — prefer-overlay callers treat ACCEPT as delivery;
        // run/deploy outcomes ride status callbacks. Wake is a no-op; commandActivityHook fires
        // on ACCEPT and already wakes the beat loop.
        this.commandLane = commandRegistry == null
                ? null
                : new CommandLaneDispatcher(commandRegistry, ignored -> { }, () -> { });
        this.mapper = mapper == null ? new ObjectMapper() : mapper;
    }

    /**
     * Wires the optional post-command activity hook (nullable). Setter keeps {@link #forMtls} and existing
     * constructors backward compatible for unit tests.
     */
    public void setCommandActivityHook(Runnable commandActivityHook) {
        this.commandActivityHook = commandActivityHook;
    }

    /**
     * Wires the optional inbound-traffic hook (nullable). Same seam pattern as {@link #setCommandActivityHook} —
     * setter keeps the constructors backward compatible.
     */
    public void setInboundTrafficHook(Runnable inboundTrafficHook) {
        this.inboundTrafficHook = inboundTrafficHook;
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
     * Production factory — client-cert REQUIRED, TLS material sourced from the enrolled {@link MtlsIdentity}
     * (per-env leaf presented as the server cert, env CA pinned as the sole client-trust anchor). Throws if the
     * identity is not enrolled, so bootstrap can log-and-skip rather than binding an unauthenticated socket on the
     * overlay.
     */
    public static OverlayReverseListener forMtls(String bindHost, int port, MtlsIdentity identity,
                                                 DispatchExecutor dispatchExecutor,
                                                 OciImageBlobIngestService ociIngest,
                                                 DaemonCommandRegistry commandRegistry,
                                                 ObjectMapper mapper) throws Exception {
        if (identity == null || !identity.isEnrolled()) {
            throw new IllegalStateException("overlay listener: mTLS identity not enrolled");
        }
        // The key manager follows the identity's leaf (RotatingKeyManager): a leaf renewed by
        // the daily mTLS rotation loop is presented on the next handshake — no listener
        // restart, so a cert renewal can never drop the reverse channel.

        SSLContext ctx = HybridTls.newContext(
                new javax.net.ssl.KeyManager[]{new com.studioquack.common.security.tls.RotatingKeyManager(
                        identity::leafCert, identity::identityPrivateKey)},
                identity.trustManagers());
        return new OverlayReverseListener(bindHost, port, ctx, true, dispatchExecutor, ociIngest,
                commandRegistry, mapper);
    }

    /**
     * Binds + starts the HTTPS server (idempotent).
     */
    public synchronized void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            assertOverlayBindAddress(bindHost);
            HttpsServer s = HttpsServer.create(new InetSocketAddress(bindHost, port), 0);
            s.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                @Override
                public void configure(HttpsParameters params) {
                    SSLParameters sp = HybridTls.hybridParameters(getSSLContext(), false);
                    // Client-cert REQUIRED for the production (mTLS) posture; relaxed only via the
                    // test seam.
                    sp.setNeedClientAuth(requireClientAuth);
                    params.setSSLParameters(sp);
                }
            });
            s.createContext("/", this::route);
            ExecutorService ex = ListenerExecutors.bounded(
                    "sqs-overlay-listener", OverlayReverseListener.class.getClassLoader());
            s.setExecutor(ex);
            s.start();
            this.pool = ex;
            this.server = s;
            log.info("channel/overlay: reverse listener up on {}:{} (mTLS clientAuth={}, peer reverse lookups {})",
                    bindHost, s.getAddress().getPort(), requireClientAuth,
                    com.studioquack.sqs.channel.net.OverlayReverseLookupProvider.active()
                            ? "answered locally" : "by the OS resolver (seconds per connection on Windows)");
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
            stopServerOnly();
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
            log.warn("channel/overlay: reverse listener rebind to {} failed: {}",
                    event.currentIp(), ex.toString());
        }
    }

    /**
     * Stops the server + drains the pool (idempotent, best-effort).
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        stopServerOnly();
        CommandLaneDispatcher lanes = this.commandLane;
        if (lanes != null) {
            lanes.shutdown();
        }
        log.info("channel/overlay: reverse listener stopped");
    }

    /**
     * HTTPS teardown without flipping {@link #running} or shutting command lanes.
     */
    private void stopServerOnly() {
        HttpsServer s = this.server;
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
     * Fail closed: the reverse listener binds the overlay IP ONLY. A wildcard would expose BE command ingress to
     * the client LAN.
     */
    static void assertOverlayBindAddress(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalStateException("overlay reverse listener: no overlay bind address — "
                    + "refusing to start (fail closed; the wildcard is never a fallback)");
        }
        InetAddress addr;
        try {
            addr = InetAddress.getByName(host.trim());
        } catch (java.net.UnknownHostException e) {
            throw new IllegalStateException("overlay reverse listener: unresolvable bind host " + host, e);
        }
        if (addr.isAnyLocalAddress()) {
            throw new IllegalStateException("overlay reverse listener: wildcard bind address " + host
                    + " refused — the reverse endpoint must bind the overlay IP only");
        }
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
        HttpsServer s = this.server;
        return s == null ? port : s.getAddress().getPort();
    }

    // Routing ------------------------------------------------------------

    private void route(HttpExchange ex) throws IOException {
        try {
            if (ex instanceof com.sun.net.httpserver.HttpsExchange https) {
                HybridTls.logNegotiatedOnce(https.getSSLSession(), "overlay-reverse");
            }
            // Any authenticated request — command, poll, even a 404 — proves the backend reached
            // this daemon inbound over the tunnel. Stamp it before anything can throw; the hook is
            // A notification and must never break the reply.
            Runnable inbound = this.inboundTrafficHook;
            if (inbound != null) {
                try {
                    inbound.run();
                } catch (Exception hookErr) {
                    log.debug("channel/overlay: inbound-traffic hook failed: {}", hookErr.toString());
                }
            }
            String path = ex.getRequestURI().getPath();
            if (JOBS_PATH.equals(path)) {
                handleJobs(ex);
            } else if (COMMAND_PATH.equals(path)) {
                handleCommand(ex);
            } else if (path.startsWith(OCI_PREFIX) && path.endsWith(OCI_SUFFIX)) {
                handleOciBlob(ex, path);
            } else {
                writeError(ex, 404, "Error_NotFound", "no such overlay route: " + path);
            }
        } catch (Exception e) {
            log.warn("channel/overlay: request handling failed {} {}: {}",
                    ex.getRequestMethod(), ex.getRequestURI(), e.toString());
            safeWriteError(ex, 500, "Error_Internal", "internal error");
        } finally {
            ex.close();
        }
    }

    /**
     * {@code POST the jobs endpoint} — runs the verbatim the control plane JSON through the shared
     * {@link DispatchExecutor} (the EXACT path the exec-channel uses) and returns the executor's the control plane
     * JSON. No second execution path, no re-implementation of dispatch.
     */
    private void handleJobs(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            writeError(ex, 405, "Error_MethodNotAllowed", "POST required");
            return;
        }
        if (dispatchExecutor == null) {
            writeError(ex, 503, "Error_DispatchUnavailable",
                    "dispatch executor not wired on this daemon (WP-E3/E5)");
            return;
        }
        String requestJson;
        try {
            requestJson = new String(BoundedExchangeBody.read(ex.getRequestBody()), StandardCharsets.UTF_8);
        } catch (BoundedExchangeBody.TooLarge tooLarge) {
            writeError(ex, 413, "Error_RequestBodyTooLarge", tooLarge.getMessage());
            return;
        }
        try {
            String responseJson = dispatchExecutor.dispatch(requestJson, DISPATCH_DEADLINE);
            writeJson(ex, 200, responseJson == null || responseJson.isBlank() ? "{}" : responseJson);
        } catch (Exception dispatchErr) {
            // Executor-harness failure (not a job FAILED status — that rides inside the 200 JSON).
            // Surface a well-formed the job response-shaped error so the BE's onStatus mapping carries
            // an actionable message.
            log.warn("channel/overlay: dispatch failed: {}", dispatchErr.toString());
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("status", "FAILED");
            body.put("errorMessage", dispatchErr.getMessage() == null
                    ? dispatchErr.toString() : dispatchErr.getMessage());
            writeJson(ex, 500, mapper.writeValueAsString(body));
        }
    }

    /**
     * {@code POST the command endpoint} — same {@link DaemonCommandRegistry} the heartbeat path uses. Lightweight
     * commands run on this HTTPS thread and return the handler result.
     */
    private void handleCommand(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            writeError(ex, 405, "Error_MethodNotAllowed", "POST required");
            return;
        }
        if (commandRegistry == null) {
            writeError(ex, 503, "Error_CommandChannelUnavailable",
                    "command registry not wired on this daemon");
            return;
        }
        String requestJson;
        try {
            requestJson = new String(BoundedExchangeBody.read(ex.getRequestBody()), StandardCharsets.UTF_8);
        } catch (BoundedExchangeBody.TooLarge tooLarge) {
            writeError(ex, 413, "Error_RequestBodyTooLarge", tooLarge.getMessage());
            return;
        }
        DaemonCommand command;
        try {
            command = mapper.readValue(requestJson, DaemonCommand.class);
        } catch (Exception parseErr) {
            writeError(ex, 400, "Error_BadCommand", "malformed command JSON: " + parseErr.getMessage());
            return;
        }
        if (command == null || command.commandId() == null || command.commandId().isBlank()
                || command.type() == null || command.type().isBlank()) {
            writeError(ex, 400, "Error_BadCommand", "command requires a non-blank commandId and type");
            return;
        }
        if (DaemonCommandRegistry.isAcceptAndRunType(command.type())) {
            Optional<DaemonCommandResult> refused = commandRegistry.refuseIfNotAdmitted(
                    command, DaemonCommandRegistry.INBOUND_OVERLAY);
            if (refused.isPresent()) {
                DaemonCommandResult result = refused.get();
                log.warn("channel/overlay: command refused before ACCEPT id={} type={} status={}",
                        result.commandId(), result.type(), result.status());
                writeJson(ex, 200, mapper.writeValueAsString(result));
                return;
            }
            writeAcceptedOrCached(ex, command);
            return;
        }
        DaemonCommandResult result = commandRegistry
                .dispatch(command, DaemonCommandRegistry.INBOUND_OVERLAY)
                .orElseGet(() -> new DaemonCommandResult(command.commandId(), command.type(),
                        "failed", null, "daemon dropped command (no commandId)"));
        log.info("channel/overlay: command executed over WG id={} type={} status={}",
                result.commandId(), result.type(), result.status());
        noteCommandActivity(command.type());
        writeJson(ex, 200, mapper.writeValueAsString(result));
    }

    /**
     * Queue {@code deploy}/{@code start-run} on the serial lane and return HTTP 200 on ACCEPT. A finished
     * commandId is re-reported from the registry cache (same dedup as the sync path).
     */
    private void writeAcceptedOrCached(HttpExchange ex, DaemonCommand command) throws IOException {
        DaemonCommandResult cached = commandRegistry.peekCompleted(command.commandId()).orElse(null);
        if (cached != null) {
            log.info("channel/overlay: command re-reported over WG id={} type={} status={}",
                    cached.commandId(), cached.type(), cached.status());
            writeJson(ex, 200, mapper.writeValueAsString(cached));
            return;
        }
        if (commandLane != null) {
            commandLane.submit(command, DaemonCommandRegistry.INBOUND_OVERLAY);
        }
        DaemonCommandResult accepted = DaemonCommandResult.of(command, DaemonCommandOutcome.accepted());
        log.info("channel/overlay: command accepted over WG id={} type={} status={}",
                accepted.commandId(), accepted.type(), accepted.status());
        noteCommandActivity(command.type());
        writeJson(ex, 200, mapper.writeValueAsString(accepted));
    }

    /**
     * Cadence nudge for a state-changing inbound command; must never break the reply.
     */
    private void noteCommandActivity(String type) {
        if (DaemonCommandRegistry.isHighFrequencyPoll(type)) {
            return;
        }
        Runnable hook = this.commandActivityHook;
        if (hook == null) {
            return;
        }
        try {
            hook.run();
        } catch (RuntimeException hookErr) {
            log.debug("channel/overlay: command activity hook failed: {}", hookErr.toString());
        }
    }

    /**
     * {@code POST the image-blob endpoint} — delegates to the shared {@link OciImageBlobIngestService} (same logic
     * the servlet controller uses). The bearer + upload token check lives in the service, so the security posture
     * is identical on both ingress fronts.
     */
    private void handleOciBlob(HttpExchange ex, String path) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            writeError(ex, 405, "Error_MethodNotAllowed", "POST required");
            return;
        }
        if (ociIngest == null) {
            writeError(ex, 503, "Error_IngressDisabled", "OCI image ingress not available on this daemon");
            return;
        }
        String ref = decodePathSegment(path.substring(OCI_PREFIX.length(), path.length() - OCI_SUFFIX.length()));
        Map<String, String> query = parseQuery(ex.getRequestURI().getRawQuery());
        String bearer = bearer(ex.getRequestHeaders().getFirst("Authorization"));
        OciImageBlobIngestService.Result result = ociIngest.ingest(
                ref, query.get("envId"), query.get("displayName"), ex.getRequestBody(), bearer);
        writeJson(ex, result.httpStatus(), mapper.writeValueAsString(result.body()));
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

    private static String bearer(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            return null;
        }
        return authorization.substring("Bearer ".length()).trim();
    }

    private static String decodePathSegment(String raw) {
        try {
            return URLDecoder.decode(raw, StandardCharsets.UTF_8);
        } catch (RuntimeException ex) {
            return raw;
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> out = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return out;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String k = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            String v = URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            out.putIfAbsent(k, v);
        }
        return out;
    }
}
