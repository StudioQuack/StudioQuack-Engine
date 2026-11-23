package com.studioquack.sqs.channel.config;

import com.studioquack.sqs.channel.DefaultHeartbeatResourceProvider;
import com.studioquack.sqs.channel.HeartbeatResourceProvider;
import com.studioquack.sqs.sandbox.egress.EgressProxyProbe;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Wires the advisory egress-proxy health into the heartbeat/status resource snapshot as the
 * {@code egressProxy} block, so a proxy outage is VISIBLE on the control plane (never a silent degrade): the
 * block carries {@code state} ({@code UP}/{@code DOWN}) and {@code reason} verbatim from
 * {@link EgressProxyProbe#snapshot()}. The block is omitted entirely when no proxy is configured (
 * {@link EgressProxyProbe.State#NOT_CONFIGURED} maps to a null view) — "absent" means "nothing to watch",
 * distinct from any health value.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class EgressProxyTelemetryWiring {

    public EgressProxyTelemetryWiring(ObjectProvider<HeartbeatResourceProvider> resourceProvider,
                                      ObjectProvider<EgressProxyProbe> proxyProbe) {
        HeartbeatResourceProvider provider = resourceProvider == null
                ? null : resourceProvider.getIfAvailable();
        EgressProxyProbe probe = proxyProbe == null ? null : proxyProbe.getIfAvailable();
        if (probe == null) {
            // No probe bean: the daemon cannot report proxy health — leave the block absent.
            return;
        }
        if (provider instanceof DefaultHeartbeatResourceProvider defaultProvider) {
            defaultProvider.setEgressProxyHealthProbe(EgressProxyProbe.heartbeatView(probe)::get);
            log.debug("telemetry: egressProxy health probe wired into {}", provider.getClass().getSimpleName());
        }
    }
}
