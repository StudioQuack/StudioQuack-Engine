package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
 * POSTs the boot-time stateful host-reconcile outcome to the control plane. The control plane uses
 * the report to align {@code env_stateful_instances} rows with the host: a {@code RUNNING}/{@code
 * STARTING} row whose container the daemon no longer sees stops blocking a fresh start; a row
 * matching an adopted container is confirmed with its live ids.
 */
@Slf4j
@Component
public class StatefulReconcileReporter {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final ObjectProvider<ChannelHttpClients> httpClients;
    private final SqsChannelProperties channelProps;
    private final ObjectMapper mapper;
    private final String backendBaseUrl;

    public StatefulReconcileReporter(ObjectProvider<ChannelHttpClients> httpClients,
                                     SqsChannelProperties channelProps,
                                     ObjectMapper mapper,
                                     @Value("${sqs.backend.base-url:}") String backendBaseUrl) {
        this.httpClients = httpClients;
        this.channelProps = channelProps;
        this.mapper = mapper;
        this.backendBaseUrl = backendBaseUrl;
    }

    /**
     * Reports one reconcile outcome. Returns true on a 2xx; false (with a WARN/INFO) otherwise —
     * never throws, so a report failure can never break daemon startup.
     */
    public boolean report(String envId, StatefulReconcileReport outcome) {
        if (outcome == null) {
            return false;
        }
        ChannelHttpClients clients = httpClients.getIfAvailable();
        if (clients == null || backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.info("[SQ-6] stateful reconcile report skipped — secure channel not enabled; the "
                    + "status command's statefulReconcile block remains the fallback path");
            return false;
        }
        String effectiveEnv = (envId == null || envId.isBlank()) ? channelProps.getEnvId() : envId;
        if (effectiveEnv == null || effectiveEnv.isBlank()) {
            log.info("[SQ-6] stateful reconcile report skipped — no environment id configured");
            return false;
        }
        try {
            ObjectNode body = mapper.createObjectNode();
            body.put("reportedAt", outcome.ranAt());
            body.put("runtimeAvailable", outcome.runtimeAvailable());
            body.put("adopted", outcome.adopted());
            body.put("reaped", outcome.reaped());
            body.put("reapedDuplicate", outcome.reapedDuplicate());
            body.put("ignored", outcome.ignored());
            ArrayNode containers = body.putArray("containers");
            for (StatefulReconcileReport.Entry e : outcome.entries()) {
                ObjectNode c = containers.addObject();
                c.put("instanceId", e.instanceId() == null ? "" : e.instanceId());
                c.put("containerId", e.containerId() == null ? "" : e.containerId());
                c.put("containerName", e.containerName() == null ? "" : e.containerName());
                c.put("imageRef", e.imageRef() == null ? "" : e.imageRef());
                c.put("state", e.state() == null ? "" : e.state());
                c.put("action", e.action() == null ? "" : e.action());
                c.put("reason", e.reason() == null ? "" : e.reason());
            }
            HttpResponse<String> resp = ChannelHttp.postJson(clients.get(), reconcileUrl(effectiveEnv),
                    channelProps.getBearer(), mapper.writeValueAsString(body), TIMEOUT);
            if (!ChannelHttp.isOk(resp.statusCode())) {
                log.warn("[SQ-6] stateful reconcile report env={} → BE {}: {}", effectiveEnv,
                        resp.statusCode(), ChannelHttp.trim(resp.body()));
                return false;
            }
            log.info("[SQ-6] stateful reconcile report env={} adopted={} reaped={} reapedDuplicate={} "
                            + "ignored={} → BE ok", effectiveEnv, outcome.adopted(), outcome.reaped(),
                    outcome.reapedDuplicate(), outcome.ignored());
            return true;
        } catch (Exception e) {
            log.warn("[SQ-6] stateful reconcile report env={} failed: {}", effectiveEnv, e.toString());
            return false;
        }
    }

    /** Per-env daemon-callback URL, same family as the digest report (body-carries-everything). */
    String reconcileUrl(String envId) {
        String base = backendBaseUrl == null ? "" : backendBaseUrl.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/be/v1/sqvm/environments/" + envId + "/stateful-instances/reconcile";
    }
}
