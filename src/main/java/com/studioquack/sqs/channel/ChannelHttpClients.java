package com.studioquack.sqs.channel;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.net.http.HttpClient;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Set;

/**
 * Supplies the {@link HttpClient}s the channel uses, and rebuilds them after mTLS enrolment changes the client
 * certificate material. Trust is always the JDK/OS store.
 */
@Slf4j
public class ChannelHttpClients {

    private final MtlsIdentity identity;
    /**
     * Public backend host(s) the edge certificate may name instead of the dialed hub address.
     */
    private final Set<String> backendAliasHosts;

    private volatile HttpClient standard;
    private volatile HttpClient streaming;
    /**
     * TLS context backing both clients; exposed for raw HttpURLConnection streaming uploads.
     */
    private volatile SSLContext sslContext;

    public ChannelHttpClients(MtlsIdentity identity) {
        this(identity, Set.of());
    }

    /**
     * @param backendAliasHosts public backend host(s) accepted as certificate identity when dialing the
     * private-network hub by overlay IP
     */
    public ChannelHttpClients(MtlsIdentity identity, Set<String> backendAliasHosts) {
        this.identity = identity;
        this.backendAliasHosts = backendAliasHosts == null ? Set.of() : Set.copyOf(backendAliasHosts);
        rebuild();
    }

    /**
     * Standard client (connect timeout only; per-request read timeouts set per call).
     */
    public HttpClient get() {
        return standard;
    }

    /**
     * Streaming client (no request timeout) for SSE / long-lived reads.
     */
    public HttpClient streaming() {
        return streaming;
    }

    /**
     * TLS context backing the clients, for the raw-{@link java.net.HttpURLConnection} streaming upload path. Null
     * only after the unenrolled bootstrap fallback.
     */
    public SSLContext sslContext() {
        return sslContext;
    }

    /**
     * Hostname check for {@link javax.net.ssl.HttpsURLConnection}, which applies its own verifier in addition to
     * the trust manager. Accepts the dialed host or a configured backend alias.
     */
    public HostnameVerifier hostnameVerifier() {
        return this::verifyHostname;
    }

    private boolean verifyHostname(String host, SSLSession session) {
        try {
            Certificate[] chain = session.getPeerCertificates();
            if (chain == null || chain.length == 0 || !(chain[0] instanceof X509Certificate leaf)) {
                return false;
            }
            if (host != null && !host.isBlank()
                    && BackendIdentityTrustManager.leafNames(leaf, BackendIdentityTrustManager.normalise(host))) {
                return true;
            }
            for (String alias : backendAliasHosts) {
                if (BackendIdentityTrustManager.leafNames(leaf, alias)) {
                    return true;
                }
            }
            return false;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * Rebuilds both clients from the current TLS material. Call after enrolment persists the leaf so the next
     * request can answer a client-certificate request with the new leaf.
     */
    public final synchronized void refresh() {
        rebuild();
        log.debug("channel: HTTP clients (re)built — enrolled={}",
                identity != null && identity.isEnrolled());
    }

    private void rebuild() {
        try {
            SSLContext ctx = ChannelTls.build(identity, backendAliasHosts);
            this.sslContext = ctx;
            SSLParameters params = com.studioquack.common.security.tls.HybridTls.hybridParameters(ctx, true);
            // HttpClient forces dialed-host endpoint identification, which rejects the public
            // certificate on the overlay hub (dialed by IP). Name matching stays in
            // BackendIdentityTrustManager: dialed host OR the configured public backend host.
            params.setEndpointIdentificationAlgorithm(null);
            this.standard = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(15))
                    .sslContext(ctx)
                    .sslParameters(params)
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .build();
            this.streaming = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(15))
                    .sslContext(ctx)
                    .sslParameters(params)
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .build();
        } catch (Exception ex) {
            // Enrolled material must not degrade to a client that dropped the leaf key.
            boolean enrolled = identity != null && identity.isEnrolled();
            if (enrolled) {
                throw new IllegalStateException(
                        "channel: TLS context build failed with enrolled mTLS material"
                                + " — refusing default HttpClient fallback", ex);
            }
            log.warn("channel: TLS context build failed ({}); using default HttpClient", ex.toString());
            this.sslContext = null;
            this.standard = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
            this.streaming = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
        }
    }
}
