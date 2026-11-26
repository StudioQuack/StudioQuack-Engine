package com.studioquack.sqs.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Daemon exec-channel client. Carries the hot dispatch path: {@code GET …/exec-channel} — SSE stream (events
 * {@code ready|dispatch-job|keepalive|session-end|reset}).
 */
@Slf4j
public class ExecChannelClient {

    /**
     * Supplies the per-environment base each loop iteration, so an overlay repoint takes effect live.
     */
    private final Supplier<String> environmentBaseSupplier;
    /**
     * Optional resolver notified of transport success/failure for overlay auto-fallback (nullable).
     */
    private final BackendBaseResolver resolver;
    private final String bearer;
    private final String instanceId;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final DispatchExecutor executor;
    private final long sseIdleWindowMs;
    private final long pullWaitMs;
    private final long dedupTtlMs;

    /**
     * DispatchId → claimed-at ms, for de-dup across SSE + poll re-delivery.
     */
    private final ConcurrentHashMap<String, Long> inflight = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread loopThread;

    /**
     * Back-compat constructor with a FIXED environment base (no overlay repoint / no fallback reporting). Used by
     * the offline unit tests; delegates to the supplier form with a constant base and no resolver.
     */
    public ExecChannelClient(String environmentBase, String bearer, String instanceId,
                             ChannelHttpClients httpClients, ObjectMapper mapper, DispatchExecutor executor,
                             long sseIdleWindowMs, long pullWaitMs, long dedupTtlMs) {
        this(() -> environmentBase, null, bearer, instanceId, httpClients, mapper, executor,
                sseIdleWindowMs, pullWaitMs, dedupTtlMs);
    }

    /**
     * Constructor: the environment base is read from {@code environmentBaseSupplier} on every poll / SSE open /
     * result POST (so an overlay repoint takes effect on the next iteration), and each round-trip reports
     * success/failure to {@code resolver} so a wedged overlay auto-falls- back to the public base.
     * {@code resolver} may be null to disable reporting.
     */
    public ExecChannelClient(Supplier<String> environmentBaseSupplier, BackendBaseResolver resolver,
                             String bearer, String instanceId,
                             ChannelHttpClients httpClients, ObjectMapper mapper, DispatchExecutor executor,
                             long sseIdleWindowMs, long pullWaitMs, long dedupTtlMs) {
        this.environmentBaseSupplier = environmentBaseSupplier;
        this.resolver = resolver;
        this.bearer = bearer;
        this.instanceId = instanceId;
        this.httpClients = httpClients;
        this.mapper = mapper;
        this.executor = executor;
        this.sseIdleWindowMs = sseIdleWindowMs;
        this.pullWaitMs = pullWaitMs;
        this.dedupTtlMs = dedupTtlMs;
    }

    /**
     * Drains {@code GET …/exec-commands} once and dispatches every returned command. Returns the number of
     * commands received (0 = idle).
     */
    public int pollAndDispatchOnce(int limit, long waitMs) throws Exception {
        String url = environmentBaseSupplier.get() + "/exec-commands?limit=" + limit;
        if (waitMs > 0) {
            url += "&waitMs=" + waitMs;
        }
        HttpResponse<String> resp;
        try {
            resp = ChannelHttp.getJson(
                    httpClients.get(), url, bearer, instanceId,
                    Duration.ofMillis(waitMs > 0 ? waitMs + 5_000L : 15_000L));
        } catch (Exception ex) {
            if (resolver != null && ChannelHttp.isConnectionFailure(ex)) {
                resolver.reportFailure();
            }
            throw ex;
        }
        if (resolver != null) {
            resolver.reportResponse(resp.statusCode());
        }
        if (!ChannelHttp.isOk(resp.statusCode())) {
            throw new ExecChannelException("exec-commands: status " + resp.statusCode()
                    + " body=" + ChannelHttp.trim(resp.body()), resp.statusCode());
        }
        PullResponse parsed = mapper.readValue(resp.body(), PullResponse.class);
        List<DispatchEvent> commands = parsed.commands() == null ? List.of() : parsed.commands();
        for (DispatchEvent ev : commands) {
            handleDispatch(ev);
        }
        return commands.size();
    }

    /**
     * Runs one dispatch end-to-end: dedup by {@code dispatchId} → execute vithe {@link DispatchExecutor} → POST
     * the result (or {@code /error}). A re-delivery inside the dedup window is skipped.
     */
    public void handleDispatch(DispatchEvent ev) {
        if (ev == null || ev.dispatchId() == null || ev.dispatchId().isBlank()) {
            log.warn("channel/exec: dropping dispatch with no dispatchId");
            return;
        }
        if (!tryClaim(ev.dispatchId())) {
            log.debug("channel/exec: re-delivery skipped dispatchId={}", ev.dispatchId());
            return;
        }
        String requestPayload = (ev.request() != null && !ev.request().isEmpty()) ? ev.request() : ev.requestJson();
        boolean handled = false;
        try {
            if (executor == null) {
                postError(ev, "dispatch executor not wired on this daemon (WP-E3/E5)");
                return;
            }
            String resultJson = executor.dispatch(requestPayload, Duration.ofHours(1));
            postResult(ev, resultJson);
            handled = true;
        } catch (Exception ex) {
            log.warn("channel/exec: dispatch failed dispatchId={}: {}", ev.dispatchId(), ex.toString());
            postError(ev, ex.getMessage() == null ? ex.toString() : ex.getMessage());
        } finally {
            // Keep the claim for the dedup TTL on success so a BE re-delivery is
            // skipped (exactly-once execution). On failure / no-executor, release
            // it so the dispatch can be retried on the next delivery.
            if (!handled) {
                inflight.remove(ev.dispatchId());
            }
        }
    }

    /**
     * {@code POST …/exec-results/{dispatchId}} with the delivery-token header.
     */
    public void postResult(DispatchEvent ev, String resultJson) {
        String url = environmentBaseSupplier.get() + "/exec-results/" + ev.dispatchId();
        postWithToken(url, ev.deliveryToken(), resultJson == null ? "{}" : resultJson, "result");
    }

    /**
     * {@code POST …/exec-results/{dispatchId}/error} with {@code {error}}.
     */
    public void postError(DispatchEvent ev, String message) {
        String url = environmentBaseSupplier.get() + "/exec-results/" + ev.dispatchId() + "/error";
        String body;
        try {
            body = mapper.writeValueAsString(Map.of("error", message == null ? "" : message));
        } catch (Exception ex) {
            body = "{\"error\":\"\"}";
        }
        postWithToken(url, ev.deliveryToken(), body, "error");
    }

    private void postWithToken(String url, String deliveryToken, String body, String kind) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .header(ChannelHttp.CONTENT_TYPE, ChannelHttp.APPLICATION_JSON)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
            if (bearer != null && !bearer.isBlank()) {
                b.header(ChannelHttp.AUTHORIZATION, ChannelHttp.BEARER_PREFIX + bearer);
            }
            if (deliveryToken != null && !deliveryToken.isBlank()) {
                b.header(ChannelHttp.DELIVERY_TOKEN_HEADER, deliveryToken);
            }
            if (instanceId != null && !instanceId.isBlank()) {
                b.header(ChannelHttp.INSTANCE_HEADER, instanceId);
            }
            HttpResponse<String> resp = httpClients.get().send(
                    b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resolver != null) {
                resolver.reportResponse(resp.statusCode());
            }
            if (!ChannelHttp.isOk(resp.statusCode())) {
                log.warn("channel/exec: {} POST non-2xx status={} body={}",
                        kind, resp.statusCode(), ChannelHttp.trim(resp.body()));
            }
        } catch (Exception ex) {
            if (resolver != null && ChannelHttp.isConnectionFailure(ex)) {
                resolver.reportFailure();
            }
            log.warn("channel/exec: {} POST failed: {}", kind, ex.toString());
        }
    }

    /**
     * Opens the SSE stream once and dispatches {@code dispatch-job} events until the stream closes (BE
     * {@code session-end}/{@code reset}, idle window, or I/O error). Returns true if any job arrived (so the
     * caller rewinds to the hot tier).
     */
    public boolean openSseOnce() throws Exception {
        String url = environmentBaseSupplier.get() + "/exec-channel";
        if (instanceId != null && !instanceId.isBlank()) {
            url += "?instanceId=" + instanceId;
        }
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(sseIdleWindowMs + 30_000L))
                .header(ChannelHttp.ACCEPT, ChannelHttp.TEXT_EVENT_STREAM)
                .GET();
        if (bearer != null && !bearer.isBlank()) {
            b.header(ChannelHttp.AUTHORIZATION, ChannelHttp.BEARER_PREFIX + bearer);
        }
        if (instanceId != null && !instanceId.isBlank()) {
            b.header(ChannelHttp.INSTANCE_HEADER, instanceId);
        }
        HttpResponse<InputStream> resp;
        try {
            resp = httpClients.streaming().send(
                    b.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (Exception ex) {
            if (resolver != null && ChannelHttp.isConnectionFailure(ex)) {
                resolver.reportFailure();
            }
            throw ex;
        }
        if (resolver != null) {
            resolver.reportResponse(resp.statusCode());
        }
        if (!ChannelHttp.isOk(resp.statusCode())) {
            throw new ExecChannelException("exec-channel SSE: status " + resp.statusCode(),
                    resp.statusCode());
        }
        boolean gotWork = false;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
            String event;
            String data;
            Frame frame;
            while (running.get() && (frame = readFrame(reader)) != null) {
                event = frame.event();
                data = frame.data();
                switch (event) {
                    case "ready", "keepalive" -> { }
                    case "session-end", "reset" -> {
                        return gotWork;
                    }
                    case "dispatch-job" -> {
                        DispatchEvent ev = mapper.readValue(data, DispatchEvent.class);
                        gotWork = true;
                        handleDispatch(ev);
                    }
                    default -> log.debug("channel/exec: unknown SSE event '{}'", event);
                }
            }
        }
        return gotWork;
    }

    /**
     * Starts the adaptive loop (SSE hot → poll fallback, rewind on work) on a background thread.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Thread t = new Thread(this::runLoop, "sqs-channel-exec");
        t.setDaemon(true);
        this.loopThread = t;
        t.start();
        log.info("channel/exec: loop started (SSE idle window {}ms, pull wait {}ms)", sseIdleWindowMs, pullWaitMs);
    }

    /**
     * Stops the adaptive loop (best-effort).
     */
    public void stop() {
        running.set(false);
        Thread t = loopThread;
        if (t != null) {
            t.interrupt();
        }
    }

    /** Transient error retry ladder: 2s → 4s → 8s → 16s → 30s (fast recovery, never a 1s flood). */
    static final long TRANSIENT_ERROR_BASE_MS = 2_000L;
    static final long TRANSIENT_ERROR_MAX_MS = 30_000L;
    /**
     * Auth/missing-env retry ladder: 30s → 60s → 120s → 240s → 300s (5 min ceiling).
     * Prevents a deactivated or reinstalled daemon from hammering /exec-commands at 1 req/s
     * while still self-healing if credentials recover.
     */
    static final long AUTH_ERROR_BASE_MS = 30_000L;
    static final long AUTH_ERROR_MAX_MS = 300_000L;

    static long transientErrorBackoffMs(int consecutiveErrors) {
        int shift = Math.min(Math.max(consecutiveErrors - 1, 0), 4);
        return Math.min(TRANSIENT_ERROR_BASE_MS << shift, TRANSIENT_ERROR_MAX_MS);
    }

    static long authErrorBackoffMs(int consecutiveAuthErrors) {
        int shift = Math.min(Math.max(consecutiveAuthErrors - 1, 0), 4);
        return Math.min(AUTH_ERROR_BASE_MS << shift, AUTH_ERROR_MAX_MS);
    }

    private void runLoop() {
        // Poll cadence tiers after the SSE window closes (matches the Go tiers).
        long[] pollTierMs = {1_000L, 5_000L, 15_000L};
        int tier = -1; // 1 = SSE hot
        int consecutiveTransientErrors = 0;
        int consecutiveAuthErrors = 0;
        boolean authFailureLogged = false;
        while (running.get()) {
            try {
                boolean gotWork;
                if (tier < 0) {
                    gotWork = openSseOnce();
                } else {
                    gotWork = pollAndDispatchOnce(32, pullWaitMs) > 0;
                    if (!gotWork) {
                        sleepQuietly(pollTierMs[Math.min(tier, pollTierMs.length - 1)]);
                    }
                }
                consecutiveTransientErrors = 0;
                consecutiveAuthErrors = 0;
                authFailureLogged = false;
                tier = gotWork ? -1 : Math.min(tier + 1, pollTierMs.length - 1);
            } catch (Exception ex) {
                if (running.get()) {
                    tier = Math.min(Math.max(tier, 0) + 1, pollTierMs.length - 1);
                    if (ex instanceof ExecChannelException ece
                            && (ece.isUnauthorized() || ece.isEnvNotFound())) {
                        consecutiveAuthErrors++;
                        consecutiveTransientErrors = 0;
                        long delayMs = authErrorBackoffMs(consecutiveAuthErrors);
                        if (!authFailureLogged) {
                            authFailureLogged = true;
                            log.warn("channel/exec: control plane rejected this daemon's credential or environment "
                                            + "({}) — backing off (next retry in {}s, ceiling {}s); if this engine "
                                            + "was reinstalled or deactivated, stop or re-run the newest install command",
                                    ece.getMessage(), delayMs / 1_000L, AUTH_ERROR_MAX_MS / 1_000L);
                        } else {
                            log.debug("channel/exec: auth/env error (backoff {}ms): {}", delayMs, ece.toString());
                        }
                        sleepQuietly(delayMs);
                    } else {
                        consecutiveTransientErrors++;
                        consecutiveAuthErrors = 0;
                        authFailureLogged = false;
                        long delayMs = transientErrorBackoffMs(consecutiveTransientErrors);
                        log.debug("channel/exec: tier error (backoff {}ms): {}", delayMs, ex.toString());
                        sleepQuietly(delayMs);
                    }
                }
            }
        }
        log.info("channel/exec: loop stopped");
    }

    private boolean tryClaim(String dispatchId) {
        long now = System.currentTimeMillis();
        inflight.entrySet().removeIf(e -> now - e.getValue() > dedupTtlMs);
        return inflight.putIfAbsent(dispatchId, now) == null;
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Reads one SSE frame (blank-line-delimited); returns null at end of stream.
     */
    private Frame readFrame(BufferedReader reader) throws Exception {
        String event = null;
        List<String> dataLines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
                if (event == null && dataLines.isEmpty()) {
                    continue; // Skip leading blank lines
                }
                break;
            }
            if (line.startsWith(":")) {
                continue; // Comment / keepalive
            }
            if (line.startsWith("event:")) {
                event = line.substring("event:".length()).trim();
            } else if (line.startsWith("data:")) {
                dataLines.add(line.substring("data:".length()).trim());
            }
            // Id:/retry: ignored
        }
        if (line == null && event == null && dataLines.isEmpty()) {
            return null;
        }
        return new Frame(event == null ? "message" : event, String.join("\n", dataLines));
    }

    private record Frame(String event, String data) {
    }

    /**
     * One dispatch-job payload (SSE event data + poll list entry).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DispatchEvent(String dispatchId, String envId, long jobKey, String deliveryToken,
                                String request, String requestJson) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record PullResponse(String envId, List<DispatchEvent> commands) {
    }

    /**
     * Raised on a non-success exec-channel status.
     */
    public static class ExecChannelException extends RuntimeException {
        private final int statusCode;

        public ExecChannelException(String message) {
            this(message, 0);
        }

        public ExecChannelException(String message, int statusCode) {
            super(message);
            this.statusCode = statusCode;
        }

        public int statusCode() {
            return statusCode;
        }

        public boolean isUnauthorized() {
            return statusCode == 401;
        }

        public boolean isEnvNotFound() {
            return statusCode == 404;
        }
    }
}
