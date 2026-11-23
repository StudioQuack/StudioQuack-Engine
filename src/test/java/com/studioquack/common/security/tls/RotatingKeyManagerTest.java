package com.studioquack.common.security.tls;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

/**
 * {@link RotatingKeyManager}: the presented credential follows the leaf supplier across
 * renewals without rebuilding the SSLContext, and a missing leaf fails closed (no alias,
 * no chain) rather than presenting anything stale.
 */
class RotatingKeyManagerTest {

    private static KeyPair ec() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(new java.security.spec.ECGenParameterSpec("secp256r1"));
        return kpg.generateKeyPair();
    }

    private static X509Certificate selfSigned(KeyPair kp, String cn, long serial,
                                              Instant notAfter) throws Exception {
        BouncyCastleProvider bc = new BouncyCastleProvider();
        Instant nb = Instant.now().minus(1, ChronoUnit.MINUTES);
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(
                new X500Name("CN=" + cn), BigInteger.valueOf(serial), Date.from(nb),
                Date.from(notAfter), new X500Name("CN=" + cn), kp.getPublic());
        return new JcaX509CertificateConverter().setProvider(bc).getCertificate(
                b.build(new JcaContentSignerBuilder("SHA256withECDSA").setProvider(bc)
                        .build(kp.getPrivate())));
    }

    @Test
    void presentedLeafFollowsTheSupplierAcrossRenewals() throws Exception {
        KeyPair kp = ec();
        X509Certificate leafA = selfSigned(kp, "env-a", 1, Instant.now().plus(1, ChronoUnit.DAYS));
        X509Certificate leafB = selfSigned(kp, "env-a", 2, Instant.now().plus(2, ChronoUnit.DAYS));
        AtomicReference<X509Certificate> current = new AtomicReference<>(leafA);
        RotatingKeyManager km = new RotatingKeyManager(current::get, kp::getPrivate);

        // Chain/key are asserted through the store alias: alias SELECTION is BCJSSE's
        // handshake-time affair (proven end-to-end by MeshPeerTlsRoundTripTest). The key
        // manager may re-encode the key (PKCS8 round-trip), so keys compare by content.
        assertSame(leafA, km.getCertificateChain("leaf")[0], "initial leaf");
        assertArrayEquals(kp.getPrivate().getEncoded(), km.getPrivateKey("leaf").getEncoded(),
                "stable key");

        // Renewal installs a freshly parsed certificate object — the reference flip is the signal.
        current.set(leafB);
        assertSame(leafB, km.getCertificateChain("leaf")[0], "renewed leaf without a rebuild");
        assertArrayEquals(kp.getPrivate().getEncoded(), km.getPrivateKey("leaf").getEncoded(),
                "key unchanged across rotation");
    }

    @Test
    void missingLeafFailsClosed() throws Exception {
        KeyPair kp = ec();
        AtomicReference<X509Certificate> current = new AtomicReference<>(null);
        RotatingKeyManager km = new RotatingKeyManager(current::get, kp::getPrivate);

        assertNull(km.chooseEngineServerAlias("EC", null, null));
        assertNull(km.chooseServerAlias("EC", null, null));
        assertNull(km.getCertificateChain("leaf"));
        assertNull(km.getPrivateKey("leaf"));
    }
}
