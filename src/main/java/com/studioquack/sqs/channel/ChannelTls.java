package com.studioquack.sqs.channel;

import com.studioquack.common.security.tls.HybridTls;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.util.Set;

/**
 * SSLContext for outbound HTTPS to the control plane. Trust is the JDK/OS default trust store.
 */
final class ChannelTls {

    private ChannelTls() {
    }

    static SSLContext build(MtlsIdentity identity) throws Exception {
        return build(identity, Set.of());
    }

    /**
     * @param backendAliasHosts public backend host(s) the edge certificate may name when the hub is dialed by
     * overlay IP; empty means dialed-host only
     */
    static SSLContext build(MtlsIdentity identity, Set<String> backendAliasHosts) throws Exception {
        KeyManager[] kms = identity == null ? null : identity.keyManagers();
        TrustManager[] tms = wrapWithBackendIdentity(platformTrustManagers(), backendAliasHosts);
        return HybridTls.newContext(kms, tms);
    }

    /**
     * Platform default trust store ({@code tmf.init(null)}), never a replacement pin.
     */
    private static TrustManager[] platformTrustManagers() throws Exception {
        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null);
        return tmf.getTrustManagers();
    }

    private static TrustManager[] wrapWithBackendIdentity(TrustManager[] tms, Set<String> aliasHosts) {
        TrustManager[] out = new TrustManager[tms.length];
        for (int i = 0; i < tms.length; i++) {
            out[i] = tms[i] instanceof X509TrustManager x509
                    ? new BackendIdentityTrustManager(x509, aliasHosts)
                    : tms[i];
        }
        return out;
    }
}
