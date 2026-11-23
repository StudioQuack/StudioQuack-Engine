package com.studioquack.sqs.channel.config;

import com.studioquack.sqs.channel.DefaultHeartbeatResourceProvider;
import com.studioquack.sqs.channel.HeartbeatResourceProvider;
import com.studioquack.sqs.channel.SecurityTelemetryAccumulator;
import com.studioquack.sqs.sandbox.egress.EgressReadiness;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the singleton {@link SecurityTelemetryAccumulator} and wires it into the heartbeat resource
 * provider (and the egress manager for boot-time DEGRADED catch-up). Channel-gated: without the channel there
 * is no heartbeat to drain into, so hooks that {@code ObjectProvider.getIfAvailable()} the accumulator simply
 * no-op.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class SecurityTelemetryWiring {

    @Bean
    @ConditionalOnMissingBean(SecurityTelemetryAccumulator.class)
    public SecurityTelemetryAccumulator securityTelemetryAccumulator(
            ObjectProvider<HeartbeatResourceProvider> resourceProvider,
            ObjectProvider<SandboxEgressManager> egressManager) {
        SecurityTelemetryAccumulator accumulator = new SecurityTelemetryAccumulator();

        HeartbeatResourceProvider provider = resourceProvider == null
                ? null : resourceProvider.getIfAvailable();
        if (provider instanceof DefaultHeartbeatResourceProvider defaultProvider) {
            defaultProvider.setSecurityTelemetryAccumulator(accumulator);
            log.debug("telemetry: securityTelemetry accumulator wired into {}",
                    provider.getClass().getSimpleName());
        }

        // Manager constructs before this bean exists, so the ctor-time DEGRADED warn cannot
        // record yet — catch up once here so boot posture is not lost. Later warn-path records
        // use the setter field directly.
        SandboxEgressManager manager = egressManager == null ? null : egressManager.getIfAvailable();
        if (manager != null) {
            manager.setSecurityTelemetryAccumulator(accumulator);
            EgressReadiness readiness = manager.readiness();
            if (readiness != null
                    && readiness.state() == EgressReadiness.State.DEGRADED
                    && readiness.reason() != null
                    && !readiness.reason().equals("overlay-egress disabled")) {
                accumulator.record("sandbox_egress_degraded");
            }
        }

        return accumulator;
    }
}
