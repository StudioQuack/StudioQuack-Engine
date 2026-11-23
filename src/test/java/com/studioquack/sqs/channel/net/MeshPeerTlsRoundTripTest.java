package com.studioquack.sqs.channel.net;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.studioquack.common.security.tls.HybridTls;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The daemon↔daemon mesh leg exactly as production wires it. */
class MeshPeerTlsRoundTripTest {

    private HttpsServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    record Pki(X509Certificate ca, String caPem, X509Certificate leaf, KeyPair leafKp, String host,
               KeyPair caKp) {}

    /** Pre- daemon: SunJSSE context, TLS 1.3 preferred, hybrid group list via SSLParameters. */
    private static SSLParameters legacySunParams(SSLContext ctx) {
        SSLParameters p = ctx.getDefaultSSLParameters();
        p.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
        p.setNamedGroups(new String[]{"X25519MLKEM768", "x25519", "secp256r1"});
        return p;
    }

    private static SSLContext legacySunServerContext(Pki pki) throws Exception {
        java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("l", pki.leafKp().getPrivate(), "m".toCharArray(),
                new java.security.cert.Certificate[]{pki.leaf()});
        javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(
                javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, "m".toCharArray());
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(kmf.getKeyManagers(), null, null);
        return ctx;
    }

    private static SSLContext legacySunClientContext(Pki pki) throws Exception {
        java.security.KeyStore ts = java.security.KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("ca", pki.ca());
        javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        // Old daemons pinned the mesh CA with a chain-only trust manager (identity = CA pin + CN check
        // done by the caller), no SAN/IP match — mirror that so the legacy leg dials by overlay IP.
        javax.net.ssl.X509TrustManager base = (javax.net.ssl.X509TrustManager) tmf.getTrustManagers()[0];
        javax.net.ssl.X509ExtendedTrustManager chainOnly = new javax.net.ssl.X509ExtendedTrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] c, String a, java.net.Socket s)
                    throws java.security.cert.CertificateException { base.checkClientTrusted(c, a); }
            @Override public void checkServerTrusted(X509Certificate[] c, String a, java.net.Socket s)
                    throws java.security.cert.CertificateException { base.checkServerTrusted(c, a); }
            @Override public void checkClientTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e)
                    throws java.security.cert.CertificateException { base.checkClientTrusted(c, a); }
            @Override public void checkServerTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e)
                    throws java.security.cert.CertificateException { base.checkServerTrusted(c, a); }
            @Override public void checkClientTrusted(X509Certificate[] c, String a)
                    throws java.security.cert.CertificateException { base.checkClientTrusted(c, a); }
            @Override public void checkServerTrusted(X509Certificate[] c, String a)
                    throws java.security.cert.CertificateException { base.checkServerTrusted(c, a); }
            @Override public X509Certificate[] getAcceptedIssuers() { return base.getAcceptedIssuers(); }
        };
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(null, new javax.net.ssl.TrustManager[]{chainOnly}, null);
        return ctx;
    }

    private int startServer(SSLContext serverCtx, java.util.function.Function<SSLContext, SSLParameters> params)
            throws Exception {
        server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(serverCtx) {
            @Override
            public void configure(HttpsParameters p) {
                p.setSSLParameters(params.apply(getSSLContext()));
            }
        });
        server.createContext("/", ex -> {
            byte[] body = "peer-ok".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, body.length);
            try (var out = ex.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        return server.getAddress().getPort();
    }

    private static String get(SSLContext clientCtx, SSLParameters cp, int port) throws Exception {
        cp.setEndpointIdentificationAlgorithm(null);
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5)).sslContext(clientCtx).sslParameters(cp).build();
        HttpResponse<String> r = client.send(
                HttpRequest.newBuilder(URI.create("https://127.0.0.1:" + port + "/sqs/v1/overlay/llm/chat"))
                        .timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode());
        return r.body();
    }

    /** Old (SunJSSE, TLS 1.3-first) daemon is the callee; current BCJSSE daemon calls it. */
    @Test
    void currentClient_againstLegacySunJsseListener() throws Exception {
        Pki pki = pki();
        int port = startServer(legacySunServerContext(pki), MeshPeerTlsRoundTripTest::legacySunParams);
        SSLContext clientCtx = MeshTls.peerClientContext(pki.caPem(), pki.host());
        assertEquals("peer-ok", get(clientCtx, HybridTls.hybridParameters(clientCtx, true), port));
    }

    /** Old (SunJSSE) daemon is the caller; current BCJSSE listener answers. */
    @Test
    void legacySunJsseClient_againstCurrentListener() throws Exception {
        Pki pki = pki();
        SSLContext serverCtx = MeshTls.serverContext(pki.leafKp().getPrivate(), pki.leaf(), null);
        int port = startServer(serverCtx, c -> HybridTls.hybridParameters(c, false));
        SSLContext clientCtx = legacySunClientContext(pki);
        assertEquals("peer-ok", get(clientCtx, legacySunParams(clientCtx), port));
    }

    @Test
    void meshPeerHttpReachesOverlayPeerListenerOverTls() throws Exception {
        Pki pki = pki();
        SSLContext serverCtx = MeshTls.serverContext(pki.leafKp().getPrivate(), pki.leaf(), null);
        int port = startServer(serverCtx, c -> HybridTls.hybridParameters(c, false));
        SSLContext clientCtx = MeshTls.peerClientContext(pki.caPem(), pki.host());
        assertEquals("peer-ok", get(clientCtx, HybridTls.hybridParameters(clientCtx, true), port));
    }

    private static Pki pki() throws Exception {
        BouncyCastleProvider bc = new BouncyCastleProvider();
        // Same as production: daemon ListenerKeyStore makes an EC key with the default JCA (SunEC),
        // the server MeshCertificateAuthority signs SHA256withECDSA.
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(new java.security.spec.ECGenParameterSpec("secp256r1"));
        KeyPair caKp = kpg.generateKeyPair();
        KeyPair leafKp = kpg.generateKeyPair();
        Instant nb = Instant.now().minus(1, ChronoUnit.MINUTES);

        JcaX509v3CertificateBuilder caB = new JcaX509v3CertificateBuilder(
                new X500Name("CN=sq-mesh-ca"), BigInteger.valueOf(1), Date.from(nb),
                Date.from(nb.plus(1, ChronoUnit.DAYS)), new X500Name("CN=sq-mesh-ca"), caKp.getPublic());
        caB.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        X509Certificate ca = new JcaX509CertificateConverter().setProvider(bc).getCertificate(
                caB.build(new JcaContentSignerBuilder("SHA256withECDSA").setProvider(bc).build(caKp.getPrivate())));

        String host = "env-peer1";
        JcaX509v3CertificateBuilder leafB = new JcaX509v3CertificateBuilder(
                new X500Name("CN=sq-mesh-ca"), BigInteger.valueOf(2), Date.from(nb),
                Date.from(nb.plus(1, ChronoUnit.DAYS)), new X500Name("CN=" + host), leafKp.getPublic());
        leafB.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, host)));
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withECDSA").setProvider(bc).build(caKp.getPrivate());
        X509Certificate leaf = new JcaX509CertificateConverter().setProvider(bc).getCertificate(leafB.build(signer));
        String caPem = "-----BEGIN CERTIFICATE-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(ca.getEncoded())
                + "\n-----END CERTIFICATE-----\n";
        return new Pki(ca, caPem, leaf, leafKp, host, caKp);
    }

    /** Re-signs the leaf over the SAME daemon key with a new serial — what the server does per handshake. */
    private static X509Certificate signLeaf(Pki pki, long serial, Instant notAfter) throws Exception {
        BouncyCastleProvider bc = new BouncyCastleProvider();
        Instant nb = Instant.now().minus(1, ChronoUnit.MINUTES);
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(
                new X500Name("CN=sq-mesh-ca"), BigInteger.valueOf(serial), Date.from(nb),
                Date.from(notAfter), new X500Name("CN=" + pki.host()), pki.leafKp().getPublic());
        b.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, pki.host())));
        return new JcaX509CertificateConverter().setProvider(bc).getCertificate(b.build(
                new JcaContentSignerBuilder("SHA256withECDSA").setProvider(bc)
                        .build(pki.caKp().getPrivate())));
    }

    /** Client context that records the served leaf while enforcing the MeshCaTrust checks. */
    private static SSLContext recordingClientContext(
            Pki pki, java.util.concurrent.atomic.AtomicReference<X509Certificate> served) {
        javax.net.ssl.X509ExtendedTrustManager tm = new javax.net.ssl.X509ExtendedTrustManager() {
            private void check(X509Certificate[] chain) throws java.security.cert.CertificateException {
                served.set(chain[0]);
                chain[0].checkValidity();
                try {
                    chain[0].verify(pki.ca().getPublicKey());
                } catch (Exception ex) {
                    throw new java.security.cert.CertificateException(ex);
                }
            }
            @Override public void checkClientTrusted(X509Certificate[] c, String a) { }
            @Override public void checkServerTrusted(X509Certificate[] c, String a)
                    throws java.security.cert.CertificateException { check(c); }
            @Override public void checkClientTrusted(X509Certificate[] c, String a, java.net.Socket s) { }
            @Override public void checkServerTrusted(X509Certificate[] c, String a, java.net.Socket s)
                    throws java.security.cert.CertificateException { check(c); }
            @Override public void checkClientTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) { }
            @Override public void checkServerTrusted(X509Certificate[] c, String a, javax.net.ssl.SSLEngine e)
                    throws java.security.cert.CertificateException { check(c); }
            @Override public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[]{pki.ca()};
            }
        };
        return HybridTls.newContext(null, new javax.net.ssl.TrustManager[]{tm});
    }

    /**
     * The 24-h listener leaf rotates under a RUNNING listener: the server re-signs it on every
     * re-handshake and the daemon {@code installLeaf}s it; the next TLS handshake must present the
     * renewed leaf with no listener restart. Regression for the ~1-day outage where the listener
     * presented the expired startup leaf forever and every cross-env call failed {@code
     * certificate_expired} → {@code Error_PeerUnreachable} until a daemon restart.
     */
    @Test
    void runningListenerPresentsTheRenewedLeafWithoutRestart() throws Exception {
        Pki pki = pki();
        ListenerKeyStore keys = ListenerKeyStore.of(pki.leafKp());
        keys.installLeaf(pki.leaf());
        int port = startServer(MeshTls.serverContext(keys, null),
                c -> HybridTls.hybridParameters(c, false));

        java.util.concurrent.atomic.AtomicReference<X509Certificate> served =
                new java.util.concurrent.atomic.AtomicReference<>();
        SSLContext c1 = recordingClientContext(pki, served);
        assertEquals("peer-ok", get(c1, HybridTls.hybridParameters(c1, true), port));
        assertEquals(BigInteger.valueOf(2), served.get().getSerialNumber(), "startup leaf");

        keys.installLeaf(signLeaf(pki, 3, Instant.now().plus(2, ChronoUnit.DAYS)));

        // A fresh client context (no resumable session) against the SAME running listener.
        SSLContext c2 = recordingClientContext(pki, served);
        assertEquals("peer-ok", get(c2, HybridTls.hybridParameters(c2, true), port));
        assertEquals(BigInteger.valueOf(3), served.get().getSerialNumber(),
                "the renewed leaf is presented without a listener restart");
    }
}
