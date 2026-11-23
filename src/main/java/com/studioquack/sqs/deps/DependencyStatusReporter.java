package com.studioquack.sqs.deps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Reports each dependency install state back to the control plane so the
 * Dependencies panel can show queued, installing, installed, or failed.
 */
@Slf4j
@Component
public class DependencyStatusReporter {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    /** Optimistic-lock 500s and a blip on the overlay dial. The installer thread can wait this out. */
    private static final int MAX_POST_ATTEMPTS = 4;
    private static final long RETRY_BACKOFF_MS = 40L;
    /** Bootstrap installs the kemIkm credential a few hundred ms after hybrid enrol; this bounds the wait. */
    static final java.time.Duration CREDENTIAL_SWITCH_WAIT = java.time.Duration.ofSeconds(10);

    /**
     * Circuit breaker: once the control plane answers 401/404 the environment row is gone (uninstalled
     * / never enrolled) and NO report can ever succeed. Trips once, stays tripped for the process
     * lifetime; a reinstall is a new process anyway.
     */
    private final java.util.concurrent.atomic.AtomicBoolean channelGone = new java.util.concurrent.atomic.AtomicBoolean(false);

    private final ObjectProvider<ChannelHttpClients> httpClients;
    private final SqsChannelProperties channelProps;
    private final ObjectMapper mapper;
    private final String backendBaseUrl;
    private final OutboundBaseHolder outboundBaseHolder;

    public DependencyStatusReporter(ObjectProvider<ChannelHttpClients> httpClients,
                                    SqsChannelProperties channelProps,
                                    ObjectMapper mapper,
                                    @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                    OutboundBaseHolder outboundBaseHolder) {
        this.httpClients = httpClients;
        this.channelProps = channelProps;
        this.mapper = mapper;
        this.backendBaseUrl = backendBaseUrl;
        this.outboundBaseHolder = outboundBaseHolder;
    }

    /**
     * Returns true on a 2xx BE response; false (with a WARN) when the channel is unavailable or the
     * control plane rejected the report — never throws so an install task can't die on a transient
     * reporting failure.
     *
     * @param entry
     * @param state
     * @param resolvedVersion
     * @param digest
     * @param bytes
     * @param message
     */
    public boolean report(DependencyEntry entry, String state, String resolvedVersion,
                          String digest, Long bytes, String message) {
        return post(entry, state, resolvedVersion, digest, bytes, message, false, null);
    }

    /**
     * {@code preload} is echoed only when the live manifest recorded it (nullable). Same best-effort
     * degrade-on-no-channel semantics as {@link #report}.
     */
    public boolean reportDiscovered(DependencyEntry entry, String resolvedVersion,
                                    String digest, Long bytes, Boolean preload) {
        return post(entry, "installed", resolvedVersion, digest, bytes,
                "discovered in dependency cache", true, preload);
    }

    /**
     * True when the secure channel is enrolled and the control-plane base URL is configured, i.e. a
     * report would actually be delivered. The cache-discovery scan waits on this so it fires once,
     * right after enrolment, instead of uselessly at boot before the channel is up.
     */
    public boolean isChannelReady() {
        return httpClients.getIfAvailable() != null && backendBaseUrl != null && !backendBaseUrl.isBlank();
    }

    /**
     * True once the channel credential differs from {@code presented}, an installer bearer. A 401 on
     * the kemIkm credential itself is never a switch in progress.
     */
    private boolean credentialSwitched(String presented) {
        if (presented == null
                || com.studioquack.common.security.envelope.DaemonAuthCredential.looksLike(presented)) {
            return false;
        }
        long deadline = System.nanoTime() + CREDENTIAL_SWITCH_WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            String current = channelProps.getBearer();
            if (current != null && !current.equals(presented)) {
                return true;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /** Shared daemon→BE POST for both the install-status echo and the cache-discovery report. */
    private boolean post(DependencyEntry entry, String state, String resolvedVersion,
                         String digest, Long bytes, String message, boolean discovered, Boolean preload) {
        if (entry == null) {
            return false;
        }
        if (channelGone.get()) {
            // Env deleted on the BE — every report would 401/404. Silent skip (one WARN was
            // already logged at trip time); the next reconcile must not re-storm.
            return false;
        }
        ChannelHttpClients clients = httpClients.getIfAvailable();
        if (clients == null) {
            log.info("deps: status report skipped name={} state={} — secure channel not enabled",
                    entry.name(), state);
            return false;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.warn("deps: status report skipped name={} — sqs.backend.base-url not set", entry.name());
            return false;
        }
        String envId = channelProps.getEnvId();
        String bodyJson;
        try {
            ObjectNode body = mapper.createObjectNode();
            body.put("envId", envId == null ? "" : envId);
            body.put("language", entry.language() == null ? "" : entry.language());
            body.put("name", entry.name() == null ? "" : entry.name());
            // The tuple key is the ORIGINAL manifest version ("" for latest) — NOT the resolved one,
            // so the BE matches the same row the set-deps push created. For a discovered package
            // (no manifest row) the daemon passes the concrete cached version as the tuple key.
            body.put("version", entry.version() == null ? "" : entry.version());
            body.put("state", state == null ? "" : state);
            if (resolvedVersion != null && !resolvedVersion.isBlank()) {
                body.put("resolvedVersion", resolvedVersion);
            }
            if (digest != null && !digest.isBlank()) {
                body.put("digest", digest);
            }
            if (bytes != null) {
                body.put("bytes", bytes);
            }
            if (message != null && !message.isBlank()) {
                body.put("message", message);
            }
            if (discovered) {
                body.put("discovered", true);
            }
            if (preload != null) {
                body.put("preload", preload);
            }
            bodyJson = mapper.writeValueAsString(body);
        } catch (Exception e) {
            log.warn("deps: status report name={} state={} failed: {}", entry.name(), state, e.toString());
            return false;
        }
        for (int attempt = 1; attempt <= MAX_POST_ATTEMPTS; attempt++) {
            if (channelGone.get()) {
                return false;
            }
            // Re-read each attempt: a heartbeat fallback can move the base from overlay to public.
            String url = statusUrl(envId);
            String presented = channelProps.getBearer();
            try {
                HttpResponse<String> resp = ChannelHttp.postJson(
                        clients.get(), url, presented, bodyJson, TIMEOUT);
                if (ChannelHttp.isOk(resp.statusCode())) {
                    log.info("deps: status report name={} version='{}' state={} discovered={} → BE ok",
                            entry.name(), entry.version(), state, discovered);
                    return true;
                }
                if (resp.statusCode() == 401 && attempt < MAX_POST_ATTEMPTS && credentialSwitched(presented)) {
                    // The installer bearer is burned at hybrid enrolment: a report sent with it just before
                    // bootstrap switched to the kemIkm credential is retried, not taken for a deleted env.
                    continue;
                }
                if (resp.statusCode() == 401 || resp.statusCode() == 404) {
                    // 401/404 on the enrolment bearer means the control plane.
                    channelGone.set(true);
                    log.warn("deps: status report name={} state={} → BE {} — environment unknown to the backend; "
                            + "dependency reporting disabled for this process (env uninstalled?)",
                            entry.name(), state, resp.statusCode());
                    return false;
                }
                if (resp.statusCode() >= 500 && attempt < MAX_POST_ATTEMPTS && pauseBeforeRetry()) {
                    log.warn("deps: status report name={} state={} → BE {} (attempt {}/{}); retrying",
                            entry.name(), state, resp.statusCode(), attempt, MAX_POST_ATTEMPTS);
                    continue;
                }
                log.warn("deps: status report name={} state={} → BE {}: {}",
                        entry.name(), state, resp.statusCode(), ChannelHttp.trim(resp.body()));
                return false;
            } catch (Exception e) {
                if (isInterrupted(e)) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                if (attempt < MAX_POST_ATTEMPTS && ChannelHttp.isConnectionFailure(e) && pauseBeforeRetry()) {
                    log.warn("deps: status report name={} state={} transport failure (attempt {}/{}): {}",
                            entry.name(), state, attempt, MAX_POST_ATTEMPTS, e.toString());
                    continue;
                }
                log.warn("deps: status report name={} state={} failed: {}", entry.name(), state, e.toString());
                return false;
            }
        }
        return false;
    }

    /** Builds the daemon-callback dependency-status URL from the live outbound base. */
    String statusUrl(String envId) {
        String fallback = channelProps.environmentBase(backendBaseUrl);
        String base = outboundBaseHolder.currentEnvironmentBase(fallback);
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/dependencies/status";
    }

    private static boolean pauseBeforeRetry() {
        try {
            Thread.sleep(RETRY_BACKOFF_MS);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static boolean isInterrupted(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }
}
