package com.studioquack.sqs.channel.net;

import com.studioquack.common.security.tls.HybridTls;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.ByteArrayInputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Mesh-CA TLS for overlay legs: cert pin + hostname check, then {@link HybridTls} (BCJSSE,
 * {@code X25519MLKEM768} first, classic fallback). SunJSSE is not used for these contexts.
 */
public final class MeshTls {

    /**
     * IANA / BCJSSE name for the TLS 1.3 hybrid key-exchange group.
     */
    public static final String HYBRID_GROUP = HybridTls.HYBRID_GROUP;

    private MeshTls() {
    }

    /**
     * Groups offered on sockets we configure.
     */
    public static String[] offeredGroups() {
        return HybridTls.NAMED_GROUPS.clone();
    }

    /**
     * Server SSLContext: our leaf + key, optional client-trust store. {@code clientTrust} {@code null} → no
     * client-auth trust managers (plain TLS).
     */
    public static SSLContext serverContext(PrivateKey key, X509Certificate leaf,
                                           TrustManager[] clientTrust) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        char[] pw = "mesh".toCharArray();
        ks.setKeyEntry("listener", key, pw, new Certificate[]{leaf});
        // BCJSSE's own key managers — a SunJSSE key manager cannot present this EC leaf
        // (handshake_failure(40) on every peer call); see HybridTls.keyManagers.
        return HybridTls.newContext(HybridTls.keyManagers(ks, pw), clientTrust);
    }

    /**
     * Production {@code:8443} server context backed by the daemon's {@link ListenerKeyStore}. Unlike the
     * fixed-leaf variant above, the key manager re-reads the store on every TLS handshake, so a listener leaf
     * renewed by the periodic re-handshake (24-h TTL, re-signed by the BE on every handshake) is presented
     * immediately — no listener restart and no dropped in-flight peer call.
     */
    public static SSLContext serverContext(ListenerKeyStore keys,
                                           TrustManager[] clientTrust) {
        return HybridTls.newContext(new KeyManager[]{
                new com.studioquack.common.security.tls.RotatingKeyManager(
                        keys::leaf, keys::privateKey)}, clientTrust);
    }

    /**
     * Client SSLContext that trusts ONLY {@code caPem} and checks the peer leaf CN/SAN against
     * {@code expectedHostname} (e.g. {@code env-<envId>}). Default HTTPS endpoint identification is disabled — the
     * pin + name check are the identity, because we dial by overlay IP.
     */
    public static SSLContext peerClientContext(String caPem, String expectedHostname) throws Exception {
        X509Certificate ca = parseFirstCert(caPem);
        TrustManager[] tms = new TrustManager[]{new MeshCaTrust(ca, expectedHostname)};
        return HybridTls.newContext(null, tms);
    }

    public static X509Certificate parseFirstCert(String pem) throws Exception {
        List<X509Certificate> all = parseCerts(pem);
        if (all.isEmpty()) {
            throw new IllegalArgumentException("mesh-tls: PEM contains no X.509 certificate");
        }
        return all.getFirst();
    }

    public static List<X509Certificate> parseCerts(String pem) throws Exception {
        if (pem == null || pem.isBlank()) {
            throw new IllegalArgumentException("mesh-tls: empty PEM");
        }
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> loaded =
                cf.generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
        List<X509Certificate> out = new ArrayList<>();
        for (Certificate c : loaded) {
            if (c instanceof X509Certificate x) {
                out.add(x);
            }
        }
        return out;
    }

    /**
     * Leaf matches {@code expected} as CN or a DNS SAN.
     */
    static boolean hostnameMatches(X509Certificate leaf, String expected) {
        if (leaf == null || expected == null || expected.isBlank()) {
            return false;
        }
        String want = expected.trim().toLowerCase(Locale.ROOT);
        String dn = leaf.getSubjectX500Principal().getName();
        for (String part : dn.split(",")) {
            String p = part.trim();
            if (p.length() >= 3 && p.regionMatches(true, 0, "CN=", 0, 3)
                    && want.equals(p.substring(3).trim().toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        try {
            Collection<List<?>> sans = leaf.getSubjectAlternativeNames();
            if (sans != null) {
                for (List<?> san : sans) {
                    if (san != null && san.size() >= 2 && Integer.valueOf(2).equals(san.get(0))) {
                        String dns = String.valueOf(san.get(1)).trim().toLowerCase(Locale.ROOT);
                        if (want.equals(dns)) {
                            return true;
                        }
                    }
                }
            }
        } catch (Exception ignored) {

        }
        return false;
    }

    private static final class MeshCaTrust extends X509ExtendedTrustManager {
        private final X509Certificate ca;
        private final String expectedHostname;

        MeshCaTrust(X509Certificate ca, String expectedHostname) {
            this.ca = ca;
            this.expectedHostname = expectedHostname;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws java.security.cert.CertificateException {
            check(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws java.security.cert.CertificateException {
            check(chain);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
                throws java.security.cert.CertificateException {
            check(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
                throws java.security.cert.CertificateException {
            check(chain);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
                throws java.security.cert.CertificateException {
            check(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
                throws java.security.cert.CertificateException {
            check(chain);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[]{ca};
        }

        private void check(X509Certificate[] chain) throws java.security.cert.CertificateException {
            if (chain == null || chain.length == 0) {
                throw new java.security.cert.CertificateException("mesh-tls: empty peer chain");
            }
            X509Certificate leaf = chain[0];
            try {
                leaf.checkValidity();
                leaf.verify(ca.getPublicKey());
            } catch (Exception ex) {
                throw new java.security.cert.CertificateException("mesh-tls: peer not signed by mesh CA", ex);
            }
            if (expectedHostname != null && !expectedHostname.isBlank()
                    && !hostnameMatches(leaf, expectedHostname)) {
                throw new java.security.cert.CertificateException(
                        "mesh-tls: peer CN/SAN does not match " + expectedHostname);
            }
        }
    }
}
