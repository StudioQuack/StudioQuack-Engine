package com.studioquack.sqs.channel.config;

import com.studioquack.sqs.aimodel.AiModelProviderClient;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.DefaultHeartbeatResourceProvider;
import com.studioquack.sqs.channel.HeartbeatResourceProvider;
import com.studioquack.sqs.channel.MtlsIdentity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.net.CliNetAgentClient;
import com.studioquack.sqs.channel.net.NetAgentClient;
import com.studioquack.sqs.sandbox.queue.DurableJobQueue;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.nio.file.Path;

/**
 * Spring wiring for the daemon secure-channel client. {@link SqsChannelProperties} always binds so operators
 * can see/set the {@code sqs.channel.*} tree.
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(SqsChannelProperties.class)
public class ChannelConfig {

    /**
     * Per-host mTLS identity (RSA-4096 keypair + persisted leaf/CA). Enabled channels only.
     */
    @Bean
    @ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
    public MtlsIdentity channelMtlsIdentity(SqsChannelProperties props,
                                            com.studioquack.sqs.secrets.DaemonFileCustody fileCustody)
            throws Exception {
        return MtlsIdentity.loadOrCreate(Path.of(props.getDataDir()),
                fileCustody == null ? null : fileCustody.mtlsIdentityKey());
    }

    /**
     * Outbound HTTP clients. Trust is the JDK/OS store; the public backend host is an accepted certificate name
     * when the private-network hub is dialed by overlay IP.
     */
    @Bean
    @ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
    public ChannelHttpClients channelHttpClients(MtlsIdentity channelMtlsIdentity,
                                                 @org.springframework.beans.factory.annotation.Value("${sqs.backend.base-url:}")
                                                 String backendBaseUrl) {
        java.util.Set<String> aliasHosts = backendPublicHost(backendBaseUrl);
        if (!aliasHosts.isEmpty()) {
            log.info("channel: backend identity alias host(s) for the private-network hub: {}", aliasHosts);
        }
        return new ChannelHttpClients(channelMtlsIdentity, aliasHosts);
    }

    /**
     * Host of {@code sqs.backend.base-url}, or empty when unset/unparseable.
     */
    static java.util.Set<String> backendPublicHost(String backendBaseUrl) {
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            return java.util.Set.of();
        }
        try {
            String host = java.net.URI.create(backendBaseUrl.trim()).getHost();
            return host == null || host.isBlank() ? java.util.Set.of() : java.util.Set.of(host);
        } catch (IllegalArgumentException ex) {
            return java.util.Set.of();
        }
    }

    /**
     * The mesh data plane — the real {@code sq-net} CLI or none at all. Returning {@code null} is intentional:
     * Spring registers it as an absent bean, so {@code SqsChannelBootstrap} reads "no overlay".
     */
    @Bean
    @ConditionalOnMissingBean(NetAgentClient.class)
    @ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
    public NetAgentClient netAgentClient(SqsChannelProperties props, ObjectMapper mapper) {
        return selectDataPlane(props, mapper);
    }

    /**
     * Shared selection rule: CLI present → {@link CliNetAgentClient}, else {@code null}.
     */
    static NetAgentClient selectDataPlane(SqsChannelProperties props, ObjectMapper mapper) {
        String cli = props.getNet() == null ? "sq-net" : props.getNet().getCliPath();
        String iface = props.getNet() == null ? "sqnet0" : props.getNet().getInterfaceName();
        String socket = props.getNet() == null ? "" : props.getNet().getSocketPath();
        if (!CliNetAgentClient.available(cli)) {
            log.error("net: overlay is ENABLED but the sq-net CLI ({}) is not on PATH — the "
                            + "daemon has NO data plane (no-data-plane). Install sq-net-agent "
                            + "(the installer does this) and restart. os={}",
                    cli, System.getProperty("os.name"));
            return null;
        }
        log.info("net: real data plane enabled cli={} socket={} iface={}",
                cli, (socket == null || socket.isBlank()) ? "(default)" : socket, iface);
        return new CliNetAgentClient(cli, iface, mapper, socket);
    }

    /**
     * Shared host + live queue/warm-pool heartbeat resources.
     */
    @Bean
    @ConditionalOnMissingBean(HeartbeatResourceProvider.class)
    @ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
    public HeartbeatResourceProvider defaultHeartbeatResourceProvider(
            Environment environment,
            ObjectProvider<DurableJobQueue> jobQueue,
            ObjectProvider<WarmPool> warmPool,
            ObjectProvider<AiModelProviderClient> aiClient,
            ObjectProvider<com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe> gpuProbe) {
        String dataDir = environment.getProperty("SQS_DATA_DIR", "./data");
        DefaultHeartbeatResourceProvider provider = new DefaultHeartbeatResourceProvider(
                Path.of(dataDir == null || dataDir.isBlank() ? "./data" : dataDir),
                jobQueue.getIfAvailable(),
                warmPool.getIfAvailable());
        if (aiClient.getIfAvailable() != null) {
            provider.setLlmHealthProbe(() -> aiClient.getIfAvailable().healthProbe());
        }
        // Surface host GPU passthrough so the FE can render (or hide) the gpu cap field.
        if (gpuProbe.getIfAvailable() != null) {
            provider.setGpuPassthroughProbe(() -> gpuProbe.getIfAvailable().gpuPassthroughAvailable());
        }
        return provider;
    }
}
