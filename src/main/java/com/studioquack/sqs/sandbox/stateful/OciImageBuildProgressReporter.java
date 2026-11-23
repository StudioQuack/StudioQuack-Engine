package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Streams local {@code podman build} progress (state + optional log chunk) back to the control
 * plane registry so the Images UI / SSE channel can show live output. Reuses the SAME secure
 * channel as {@link OciImageDigestReporter} / {@code DependencyStatusReporter}.
 */
@Slf4j
@Component
public class OciImageBuildProgressReporter {

    /** Matches the control plane {@code BUILD_LOG_CHUNK_MAX_BYTES} — char ceiling before POST. */
    static final int LOG_CHUNK_MAX_CHARS = 64 * 1024;
    /** Short status line for the Images UI. */
    static final int MESSAGE_MAX_CHARS = 512;

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private final ObjectProvider<ChannelHttpClients> httpClients;
    private final SqsChannelProperties channelProps;
    private final ObjectMapper mapper;
    private final String backendBaseUrl;

    public OciImageBuildProgressReporter(ObjectProvider<ChannelHttpClients> httpClients,
                                         SqsChannelProperties channelProps,
                                         ObjectMapper mapper,
                                         @Value("${sqs.backend.base-url:}") String backendBaseUrl) {
        this.httpClients = httpClients;
        this.channelProps = channelProps;
        this.mapper = mapper;
        this.backendBaseUrl = backendBaseUrl;
    }

    /**
     * Reports one progress event. Returns true on a 2xx the control plane response; false when the
     * channel is unavailable or the control plane rejected the report — never throws.
     *
     * @param envId Environment id (falls back to enrolled env when blank).
     * @param buildId Staging / build correlation id (required by the control plane).
     */
    public boolean report(String envId, String buildId, String ref, String state,
                          String logChunk, String message) {
        ChannelHttpClients clients = httpClients.getIfAvailable();
        if (clients == null) {
            log.info("[T-31] build-progress skipped ref={} buildId={} — secure channel not enabled",
                    ref, buildId);
            return false;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.warn("[T-31] build-progress skipped ref={} — sqs.backend.base-url not set", ref);
            return false;
        }
        String effectiveEnv = (envId == null || envId.isBlank()) ? channelProps.getEnvId() : envId;
        String url = progressUrl(effectiveEnv);
        try {
            ObjectNode body = mapper.createObjectNode();
            if (effectiveEnv != null && !effectiveEnv.isBlank()) {
                body.put("envId", effectiveEnv);
            }
            body.put("buildId", buildId == null ? "" : buildId);
            body.put("ref", ref == null ? "" : ref);
            body.put("state", state == null ? "" : state);
            String cappedChunk = truncate(logChunk, LOG_CHUNK_MAX_CHARS);
            if (cappedChunk != null) {
                body.put("logChunk", cappedChunk);
            }
            String cappedMessage = truncate(message, MESSAGE_MAX_CHARS);
            if (cappedMessage != null) {
                body.put("message", cappedMessage);
            }
            HttpResponse<String> resp = ChannelHttp.postJson(
                    clients.get(), url, channelProps.getBearer(), mapper.writeValueAsString(body), TIMEOUT);
            if (!ChannelHttp.isOk(resp.statusCode())) {
                log.warn("[T-31] build-progress ref={} buildId={} → BE {}: {}",
                        ref, buildId, resp.statusCode(), ChannelHttp.trim(resp.body()));
                return false;
            }
            log.debug("[T-31] build-progress ref={} buildId={} state={} → BE ok", ref, buildId, state);
            return true;
        } catch (Exception e) {
            log.warn("[T-31] build-progress ref={} buildId={} failed: {}", ref, buildId, e.toString());
            return false;
        }
    }

    /** Builds the daemon-callback build-progress URL: {@code {beBase}. */
    String progressUrl(String envId) {
        return buildProgressUrl(backendBaseUrl, envId);
    }

    /** Package-visible URL composition for unit tests (trailing slashes on base are stripped). */
    static String buildProgressUrl(String backendBaseUrl, String envId) {
        String base = backendBaseUrl == null ? "" : backendBaseUrl.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/be/v1/sqvm/environments/" + envId + "/oci-images/build-progress";
    }

    /**
     * Returns null when {@code value} is null/blank; otherwise truncates to {@code maxChars}.
     * Package-visible for unit tests.
     */
    static String truncate(String value, int maxChars) {
        if (value == null || value.isBlank()) {
            return null;
        }
        if (maxChars <= 0 || value.length() <= maxChars) {
            return value;
        }
        return value.substring(0, maxChars);
    }
}
