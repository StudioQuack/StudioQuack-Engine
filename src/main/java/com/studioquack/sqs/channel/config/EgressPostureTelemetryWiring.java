package com.studioquack.sqs.channel.config;

import com.studioquack.sqs.channel.DefaultHeartbeatResourceProvider;
import com.studioquack.sqs.channel.HeartbeatResourceProvider;
import com.studioquack.sqs.sandbox.egress.EgressReadiness;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Wires the daemon's live overlay-egress readiness into the heartbeat/status resource snapshot as the
 * {@code sandboxEgress} block, so the product can SHOW the enforcement posture instead of implying it. The
 * block carries {@code state} ({@code ENFORCED}/{@code DEGRADED}/{@code UNAVAILABLE}), {@code reason} and
 * {@code overlayIp} verbatim from {@link SandboxEgressManager#readiness()}.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class EgressPostureTelemetryWiring {

    public EgressPostureTelemetryWiring(ObjectProvider<HeartbeatResourceProvider> resourceProvider,
                                        ObjectProvider<SandboxEgressManager> egressManager) {
        HeartbeatResourceProvider provider = resourceProvider == null
                ? null : resourceProvider.getIfAvailable();
        SandboxEgressManager manager = egressManager == null ? null : egressManager.getIfAvailable();
        if (manager == null) {
            // No egress manager bean: the daemon cannot report a posture — leave the block absent.
            return;
        }
        if (provider instanceof DefaultHeartbeatResourceProvider defaultProvider) {
            defaultProvider.setEgressPostureProbe(() -> {
                EgressReadiness readiness = manager.readiness();
                return new String[]{readiness.state().name(), readiness.reason(), readiness.overlayIp()};
            });
            log.debug("telemetry: sandboxEgress posture probe wired into {}", provider.getClass().getSimpleName());
        }
    }
}
