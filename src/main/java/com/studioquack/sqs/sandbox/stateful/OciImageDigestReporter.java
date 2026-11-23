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
 * ITEM 5/4 — reports a pulled image's content digest back to the control plane registry after
 * {@code podman pull} completes, flipping the {@code env_custom_oci_images} row {@code DOWNLOADING
 * → ACTIVE} (or {@code FAILED}). Optional {@code error} (≤ 2048) and {@code buildId} support
 * terminal local-build ACTIVE/FAILED reports; pull callers omit them via the 4-arg overload.
 */
@Slf4j
@Component
public class OciImageDigestReporter {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final ObjectProvider<ChannelHttpClients> httpClients;
    private final SqsChannelProperties channelProps;
    private final ObjectMapper mapper;
    private final String backendBaseUrl;

    public OciImageDigestReporter(ObjectProvider<ChannelHttpClients> httpClients,
                                  SqsChannelProperties channelProps,
                                  ObjectMapper mapper,
                                  @Value("${sqs.backend.base-url:}") String backendBaseUrl) {
        this.httpClients = httpClients;
        this.channelProps = channelProps;
        this.mapper = mapper;
        this.backendBaseUrl = backendBaseUrl;
    }

    /** Cap for optional terminal {@code error} (matches the control plane digest callback). */
    static final int ERROR_MAX_CHARS = 2048;

    /**
     * Reports {@code digest} + {@code status} for {@code (envId, ref)}. Returns true on a 2xx the
     * control plane response; false (with a WARN) when the channel is unavailable or the control plane
     * rejected the report — never throws so it cannot break the upload response.
     */
    public boolean report(String envId, String ref, String digest, String status) {
        return report(envId, ref, digest, status, null, null);
    }

    /**
     * Terminal ACTIVE/FAILED for local builds. Optional {@code error} stamps {@code build_error} on
     * FAILED (≤ 2048); optional {@code buildId} correlates the SSE channel {@code
     * oci-build:{envId}:{buildId}}.
     */
    public boolean report(String envId, String ref, String digest, String status,
                          String error, String buildId) {
        ChannelHttpClients clients = httpClients.getIfAvailable();
        if (clients == null) {
            log.info("[SQ-6] digest report skipped ref={} — secure channel not enabled; the load-oci-image "
                    + "command result is the canonical digest path.", ref);
            return false;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.warn("[SQ-6] digest report skipped ref={} — sqs.backend.base-url not set", ref);
            return false;
        }
        String effectiveEnv = (envId == null || envId.isBlank()) ? channelProps.getEnvId() : envId;
        String url = digestUrl(effectiveEnv);
        try {
            ObjectNode body = mapper.createObjectNode();
            // ITEM 4: the ref rides in the BODY (never the URL path) so a slashed Docker-Hub ref like
            // `library/postgres` can't break the control plane routing or trip the encoded-slash firewall.
            body.put("ref", ref == null ? "" : ref);
            body.put("digest", digest == null ? "" : digest);
            if (status != null && !status.isBlank()) {
                body.put("status", status);
            }
            // Optional terminal fields — omit when blank so pull reports stay unchanged on wire.
            if (error != null && !error.isBlank()) {
                body.put("error", error.length() <= ERROR_MAX_CHARS
                        ? error : error.substring(0, ERROR_MAX_CHARS));
            }
            if (buildId != null && !buildId.isBlank()) {
                body.put("buildId", buildId);
            }
            HttpResponse<String> resp = ChannelHttp.postJson(
                    clients.get(), url, channelProps.getBearer(), mapper.writeValueAsString(body), TIMEOUT);
            if (!ChannelHttp.isOk(resp.statusCode())) {
                log.warn("[SQ-6] digest report ref={} → BE {}: {}", ref, resp.statusCode(),
                        ChannelHttp.trim(resp.body()));
                return false;
            }
            log.info("[SQ-6] digest report ref={} env={} status={} → BE ok", ref, effectiveEnv, status);
            return true;
        } catch (Exception e) {
            log.warn("[SQ-6] digest report ref={} failed: {}", ref, e.toString());
            return false;
        }
    }

    /**
     * Builds the daemon-callback digest URL (ITEM 4): the per-env callback base ({@code {beBase} +
     * {@code /oci-images/digest}. The ref is not in the path — it travels in the request body — so a
     * slashed Docker-Hub ref is safe.
     */
    String digestUrl(String envId) {
        String base = backendBaseUrl == null ? "" : backendBaseUrl.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/be/v1/sqvm/environments/" + envId + "/oci-images/digest";
    }
}
