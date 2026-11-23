package com.studioquack.sqs.channel.net;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Outbound HTTPS to a sibling {@code:8443}: trust store is ONLY the platform mesh CA; hostname verification is
 * CN/SAN {@code env-<targetEnvId>}; hybrid group enabled.
 */
@Slf4j
@Component
public final class MeshPeerHttp {

    private volatile String meshCaPem;
    private final ConcurrentHashMap<String, HttpClient> byEnv = new ConcurrentHashMap<>();
    private final NetTelemetry telemetry;

    public MeshPeerHttp() {
        this(null);
    }

    public MeshPeerHttp(NetTelemetry telemetry) {
        this.telemetry = telemetry;
    }

    public void installMeshCa(String pem) {
        if (pem == null || pem.isBlank()) {
            return;
        }
        if (pem.equals(meshCaPem)) {
            return;
        }
        this.meshCaPem = pem;
        byEnv.clear();
    }

    public String meshCaPem() {
        return meshCaPem;
    }

    /**
     * Drops the cached (pooling) client for a peer after a dial-phase TLS/transport failure so the next
     * {@link #clientFor} rebuilds with a fresh pool — a silently-dead pooled socket or a half-open overlay flow
     * would otherwise keep failing on the same cached connections.
     */
    public void invalidate(String targetEnvId) {
        byEnv.remove(targetEnvId == null ? "" : targetEnvId.trim());
    }

    public HttpClient clientFor(String targetEnvId) {
        String ca = meshCaPem;
        if (ca == null || ca.isBlank()) {
            throw new IllegalStateException("mesh peer HTTPS: mesh CA not installed (handshake first)");
        }
        String envId = targetEnvId == null ? "" : targetEnvId.trim();
        return byEnv.computeIfAbsent(envId, id -> build(ca, PeerAddressResolver.hostnameFor(id)));
    }

    private HttpClient build(String caPem, String expectedHostname) {
        try {
            SSLContext ctx = MeshTls.peerClientContext(caPem, expectedHostname);
            SSLParameters params = com.studioquack.common.security.tls.HybridTls.hybridParameters(ctx, true);
            params.setEndpointIdentificationAlgorithm(null);
            return HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .sslContext(ctx)
                    .sslParameters(params)
                    .build();
        } catch (Exception ex) {
            if (telemetry != null) {
                telemetry.noteTlsHandshakeError();
            }
            throw new IllegalStateException("mesh peer HTTPS: failed to build client for "
                    + expectedHostname, ex);
        }
    }
}
