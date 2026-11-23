package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.net.MeshTls;
import com.studioquack.sqs.channel.overlay.OverlayPeerListener;
import com.studioquack.sqs.channel.overlay.PeerCallAuthenticator;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link OverlayPeerListener} over TLS. A test CA signs a leaf whose CN and DNS SAN are {@code
 * env-peer-a}; {@link MeshTls#peerClientContext} must accept that name and reject any other.
 */
class OverlayPeerListenerTlsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PEER_HOST = "env-peer-a";

    @Test
    void tlsClient_withMatchingHostname_reachesExecuteRoute() throws Exception {
        MeshIdentity id = mintCaAndLeaf(PEER_HOST);
        SSLContext serverSsl = MeshTls.serverContext(id.leafKey.getPrivate(), id.leaf, null);
        com.studioquack.sqs.sandbox.helperchan.HelperExecuteService svc =
                mock(com.studioquack.sqs.sandbox.helperchan.HelperExecuteService.class);
        when(svc.executeForPeer(any(), any(), any(), any(), any())).thenReturn(Map.of("ok", true));
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, svc, PeerCallAuthenticator.allowAll(), null, MAPPER, serverSsl);
        listener.start();
        try {
            SSLContext clientSsl = MeshTls.peerClientContext(id.caPem, PEER_HOST);
            HttpResponse<String> resp = post(listener.boundPort(), clientSsl,
                    OverlayPeerListener.EXECUTE_PATH,
                    "{\"executionContext\":{\"envId\":\"env-peer-a\"},\"targetNodeId\":\"n1\"}");
            assertEquals(200, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals(Map.of("ok", true), body.get("result"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void tlsClient_withWrongHostname_failsHandshake() throws Exception {
        MeshIdentity id = mintCaAndLeaf(PEER_HOST);
        SSLContext serverSsl = MeshTls.serverContext(id.leafKey.getPrivate(), id.leaf, null);
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, mock(com.studioquack.sqs.sandbox.helperchan.HelperExecuteService.class),
                PeerCallAuthenticator.allowAll(), null, MAPPER, serverSsl);
        listener.start();
        try {
            SSLContext wrong = MeshTls.peerClientContext(id.caPem, "env-peer-wrong");
            assertThrows(Exception.class, () -> post(listener.boundPort(), wrong,
                    OverlayPeerListener.EXECUTE_PATH,
                    "{\"executionContext\":{\"envId\":\"x\"}}"));
        } finally {
            listener.stop();
        }
    }

    private static HttpResponse<String> post(int port, SSLContext ssl, String path, String body)
            throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create("https://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .sslContext(ssl)
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
    }

    /** Test CA + leaf (CN + DNS SAN = hostname), BouncyCastle — same style as reverse-listener tests. */
    private static MeshIdentity mintCaAndLeaf(String hostname) throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair caKey = kpg.generateKeyPair();
        KeyPair leafKey = kpg.generateKeyPair();
        X500Name caDn = new X500Name("CN=sq-mesh-test-ca");
        X500Name leafDn = new X500Name("CN=" + hostname);
        long now = System.currentTimeMillis();
        Date notBefore = new Date(now - 60_000L);
        Date notAfter = new Date(now + 3_600_000L);
        ContentSigner caSigner = new JcaContentSignerBuilder("SHA256withRSA").build(caKey.getPrivate());
        X509Certificate ca = new JcaX509CertificateConverter().getCertificate(
                new JcaX509v3CertificateBuilder(caDn, BigInteger.valueOf(now),
                        notBefore, notAfter, caDn, caKey.getPublic())
                        .build(caSigner));
        JcaX509v3CertificateBuilder leafBuilder = new JcaX509v3CertificateBuilder(
                caDn, BigInteger.valueOf(now + 1), notBefore, notAfter, leafDn, leafKey.getPublic());
        leafBuilder.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, hostname)));
        X509Certificate leaf = new JcaX509CertificateConverter().getCertificate(leafBuilder.build(caSigner));
        return new MeshIdentity(toPem(ca), leaf, leafKey);
    }

    private static String toPem(X509Certificate cert) throws Exception {
        String b64 = Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(cert.getEncoded());
        return "-----BEGIN CERTIFICATE-----\n" + b64 + "\n-----END CERTIFICATE-----\n";
    }

    private record MeshIdentity(String caPem, X509Certificate leaf, KeyPair leafKey) {
    }
}
