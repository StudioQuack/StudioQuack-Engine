package com.studioquack.sqs.channel.wg.mesh;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ON-MISS cross-env authorization call to the control plane. When the inbound grant set does not cover
 * {@code (callerEnvId, scope)}, the daemon asks the backend once, synchronously, before denying:
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class CrossEnvAuthorizationClient {

    /**
     * The decision handed back to the authenticator. UNAVAILABLE is always a deny upstream.
     */
    public enum Outcome {ALLOWED, DENIED, UNAVAILABLE}

    /**
     * Per-request budget for the authorize call (spec: 5 s).
     */
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    /**
     * Deny answers are never cached longer than this, whatever the backend's ttl says.
     */
    private static final long DENY_CACHE_CAP_SECONDS = 30L;

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final OutboundBaseHolder outboundBaseHolder;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;

    /**
     * (pathEnvId, callerEnvId, scope) → cached decision with its expiry.
     */
    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public CrossEnvAuthorizationClient(SqsChannelProperties props,
                                       @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                       OutboundBaseHolder outboundBaseHolder,
                                       ChannelHttpClients httpClients,
                                       ObjectMapper mapper) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.outboundBaseHolder = outboundBaseHolder;
        this.httpClients = httpClients;
        this.mapper = mapper;
    }

    /**
     * Asks the control plane whether {@code callerEnvId} may call the environment identified by {@code pathEnvId}
     * for {@code scope}, serving a fresh cached answer when one exists. Never throws; any failure ends in
     * {@link Outcome#UNAVAILABLE} (a deny upstream).
     */
    public Outcome authorize(String pathEnvId, String callerEnvId, String scope) {
        if (isBlank(pathEnvId) || isBlank(callerEnvId) || isBlank(scope)) {
            log.warn("channel/mesh: cross-env authorize called with a blank parameter — refusing "
                    + "(fail closed)");
            return Outcome.UNAVAILABLE;
        }
        String key = cacheKey(pathEnvId, callerEnvId, scope);
        long now = System.currentTimeMillis();
        CacheEntry cached = cache.get(key);
        if (cached != null && cached.expiresAtMillis() > now) {
            return cached.allowed() ? Outcome.ALLOWED : Outcome.DENIED;
        }
        try {
            String url = environmentBaseFor(pathEnvId) + "/cross-env/authorize";
            String body = mapper.writeValueAsString(new AuthorizeRequest(callerEnvId, scope));
            HttpResponse<String> resp = ChannelHttp.postJson(
                    httpClients.get(), url, props.getBearer(), body, REQUEST_TIMEOUT);
            if (resp.statusCode() == 200) {
                AuthorizeResponse answer = mapper.readValue(resp.body(), AuthorizeResponse.class);
                long ttl = Math.max(0L, answer.ttlSeconds());
                long cacheSeconds = answer.allowed() ? ttl : Math.min(ttl, DENY_CACHE_CAP_SECONDS);
                if (cacheSeconds > 0) {
                    cache.put(key, new CacheEntry(answer.allowed(), now + cacheSeconds * 1_000L));
                }
                log.info("channel/mesh: cross-env authorize caller={} scope={} pathEnv={} → {} (ttl={}s)",
                        callerEnvId, scope, pathEnvId, answer.allowed() ? "ALLOW" : "DENY", ttl);
                return answer.allowed() ? Outcome.ALLOWED : Outcome.DENIED;
            }
            // 401 (bad bearer) / 400 / 404 / 5xx all land here: the backend gave no usable answer.
            log.warn("channel/mesh: cross-env authorize answered status {} body={} — treating as "
                    + "unavailable (fail closed)", resp.statusCode(), ChannelHttp.trim(resp.body()));
        } catch (Exception ex) {
            log.warn("channel/mesh: cross-env authorize call failed ({}) — treating as unavailable "
                    + "(fail closed)", ex.toString());
        }
        // A fresh entry may have raced in while this call was in flight; it may still serve.
        CacheEntry raced = cache.get(key);
        if (raced != null && raced.expiresAtMillis() > System.currentTimeMillis()) {
            return raced.allowed() ? Outcome.ALLOWED : Outcome.DENIED;
        }
        return Outcome.UNAVAILABLE;
    }

    /**
     * The environment base for the ON-MISS call: the live outbound base (overlay hub when active, else the public
     * base) with the trailing env-id segment replaced by {@code pathEnvId}.
     */
    private String environmentBaseFor(String pathEnvId) {
        String current = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl));
        int slash = current.lastIndexOf('/');
        String root = slash > 0 ? current.substring(0, slash) : current;
        return root + "/" + pathEnvId;
    }

    private static String cacheKey(String pathEnvId, String callerEnvId, String scope) {
        return pathEnvId + '|' + callerEnvId + '|' + scope;
    }

    /**
     * Drops every cached decision. Called when a new authoritative inbound grant set lands (the reload-config
     * push): a cached ALLOW must never outlive the revocation it contradicts — the push is the moment the
     * backend's decision changes, so every cached answer ends with it.
     */
    public void evictAll() {
        cache.clear();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    /**
     * A cached backend answer; served only while fresh.
     */
    private record CacheEntry(boolean allowed, long expiresAtMillis) {
    }

    /**
     * Request body of {@code POST …/cross-env/authorize}.
     */
    private record AuthorizeRequest(String callerEnvId, String scope) {
    }

    /**
     * Backend answer; unknown fields tolerated so a richer future answer never breaks the daemon.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record AuthorizeResponse(boolean allowed, long ttlSeconds) {
    }
}
