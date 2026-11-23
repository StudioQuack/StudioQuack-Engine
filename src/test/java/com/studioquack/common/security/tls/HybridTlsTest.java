package com.studioquack.common.security.tls;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves BCJSSE actually negotiates {@code X25519MLKEM768} (SunJSSE accepts
 * the name and ignores it). Hybrid-only vs hybrid-only must succeed; classic
 * fallback must succeed; classic vs hybrid-only must fail.
 */
class HybridTlsTest {

    @Test
    void available_isTrue() {
        assertTrue(HybridTls.available(), "BCJSSE must expose X25519MLKEM768");
    }

    @Test
    void doesNotRegisterBcJsseGlobally() {
        HybridTls.newContext(null, null);
        assertTrue(Security.getProvider("BCJSSE") == null,
                "BCJSSE must stay scoped to our contexts — not Security.insertProviderAt");
    }

    @Test
    void hybridOnly_handshakeSucceeds() throws Exception {
        HandshakeResult r = handshake(
                new String[]{HybridTls.HYBRID_GROUP},
                new String[]{HybridTls.HYBRID_GROUP});
        assertTrue(r.ok, "hybrid-only peers must complete a TLS 1.3 handshake: " + r.error);
        // negotiatedGroup is best-effort; the hybrid-only success is the proof.
        assertNotNull(r.group);
    }

    @Test
    void classicClient_againstHybridPlusClassic_succeeds() throws Exception {
        HandshakeResult r = handshake(
                HybridTls.NAMED_GROUPS,
                new String[]{"x25519"});
        assertTrue(r.ok, "classic-only client must fall back against a hybrid+classic server: " + r.error);
    }

    @Test
    void classicClient_againstHybridOnly_fails() throws Exception {
        HandshakeResult r = handshake(
                new String[]{HybridTls.HYBRID_GROUP},
                new String[]{"x25519"});
        assertFalse(r.ok, "x25519-only client vs hybrid-only server must fail — otherwise the group is ignored");
    }

    private static HandshakeResult handshake(String[] serverGroups, String[] clientGroups) throws Exception {
        EphemeralPki pki = EphemeralPki.generate();
        SSLContext serverCtx = HybridTls.newContext(pki.keyManagers, null);
        SSLContext clientCtx = HybridTls.newContext(null, pki.trustManagers);

        try (SSLServerSocket server = (SSLServerSocket) serverCtx.getServerSocketFactory()
                .createServerSocket()) {
            server.bind(new InetSocketAddress("127.0.0.1", 0));
            SSLParameters sp = HybridTls.parameters(serverCtx, false, serverGroups);
            server.setSSLParameters(sp);
            int port = server.getLocalPort();
            AtomicReference<Exception> serverErr = new AtomicReference<>();
            Thread acceptor = new Thread(() -> {
                try (SSLSocket accepted = (SSLSocket) server.accept()) {
                    accepted.setSoTimeout(8_000);
                    accepted.startHandshake();
                } catch (Exception ex) {
                    serverErr.set(ex);
                }
            }, "hybrid-tls-server");
            acceptor.setDaemon(true);
            acceptor.start();

            Exception clientErr = null;
            String group = "unknown";
            try (SSLSocket client = (SSLSocket) clientCtx.getSocketFactory().createSocket()) {
                SSLParameters cp = HybridTls.parameters(clientCtx, true, clientGroups);
                client.setSSLParameters(cp);
                client.setSoTimeout(8_000);
                client.connect(new InetSocketAddress("127.0.0.1", port), 8_000);
                client.startHandshake();
                group = HybridTls.negotiatedGroup(client.getSession());
            } catch (java.io.IOException ex) {
                clientErr = ex;
            }
            acceptor.join(8_000);
            if (clientErr == null) {
                return new HandshakeResult(true, group, null);
            }
            return new HandshakeResult(false, group, clientErr.getClass().getSimpleName()
                    + ": " + clientErr.getMessage());
        }
    }

    private record HandshakeResult(boolean ok, String group, String error) {
    }

    /**
     * Self-signed RSA leaf used only for this handshake. Signing uses a local
     * BC provider instance — not {@code Security.addProvider}.
     */
    private static final class EphemeralPki {
        final javax.net.ssl.KeyManager[] keyManagers;
        final javax.net.ssl.TrustManager[] trustManagers;

        private EphemeralPki(javax.net.ssl.KeyManager[] keyManagers,
                             javax.net.ssl.TrustManager[] trustManagers) {
            this.keyManagers = keyManagers;
            this.trustManagers = trustManagers;
        }

        static EphemeralPki generate() throws Exception {
            BouncyCastleProvider bc = new BouncyCastleProvider();
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            KeyPair kp = kpg.generateKeyPair();
            Instant notBefore = Instant.now().minus(1, ChronoUnit.MINUTES);
            Instant notAfter = notBefore.plus(2, ChronoUnit.DAYS);
            JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                    new X500Name("CN=hybrid-tls-test"),
                    BigInteger.valueOf(System.nanoTime()),
                    Date.from(notBefore),
                    Date.from(notAfter),
                    new X500Name("CN=hybrid-tls-test"),
                    kp.getPublic());
            ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                    .setProvider(bc)
                    .build(kp.getPrivate());
            X509Certificate cert = new JcaX509CertificateConverter()
                    .setProvider(bc)
                    .getCertificate(builder.build(signer));

            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(null, null);
            char[] pw = "test".toCharArray();
            ks.setKeyEntry("leaf", kp.getPrivate(), pw, new X509Certificate[]{cert});
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, pw);

            KeyStore ts = KeyStore.getInstance("PKCS12");
            ts.load(null, null);
            ts.setCertificateEntry("leaf", cert);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            return new EphemeralPki(kmf.getKeyManagers(), tmf.getTrustManagers());
        }
    }
}
