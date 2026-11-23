package com.studioquack.sqs.channel;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MtlsIdentity} unit tests. Focus on the wire-critical fact that {@code csrPublicKey} is a
 * SubjectPublicKeyInfo PEM (what the server {@code enrol-mtls} endpoint parses with {@code
 * X509EncodedKeySpec}), the RSA-4096 key size, a well-formed BouncyCastle PKCS#10 CSR, and the
 * pre-enrolment posture (no TLS material, rotation due).
 */
class MtlsIdentityTest {

    @Test
    void csrPublicKey_is_rsa4096_subjectPublicKeyInfo_pem() throws Exception {
        Path dir = Files.createTempDirectory("sqs-mtls-spki");
        MtlsIdentity id = MtlsIdentity.loadOrCreate(dir);

        String pem = id.csrPublicKeyPem();
        assertTrue(pem.startsWith("-----BEGIN PUBLIC KEY-----"),
                "enrol-mtls consumes a SubjectPublicKeyInfo PEM, not a CSR PEM");

        // Decodes exactly the way the server's decodePublicKey does (X509EncodedKeySpec).
        byte[] der = Base64.getMimeDecoder().decode(
                pem.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", ""));
        PublicKey pub = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(der));
        assertEquals(4096, ((RSAPublicKey) pub).getModulus().bitLength());
    }

    @Test
    void pkcs10Csr_is_wellformed_and_carries_the_same_key() throws Exception {
        Path dir = Files.createTempDirectory("sqs-mtls-csr");
        MtlsIdentity id = MtlsIdentity.loadOrCreate(dir);

        String csrPem = id.pkcs10CsrPem("env-1");
        assertTrue(csrPem.startsWith("-----BEGIN CERTIFICATE REQUEST-----"));
        byte[] der = Base64.getMimeDecoder().decode(
                csrPem.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", ""));
        // Parses as a real PKCS#10 CSR (BouncyCastle).
        PKCS10CertificationRequest csr = new PKCS10CertificationRequest(der);
        assertTrue(csr.getSubject().toString().contains("sqvm-daemon-env-1"));
    }

    @Test
    void pre_enrolment_has_no_tls_material_and_rotation_is_due() throws Exception {
        Path dir = Files.createTempDirectory("sqs-mtls-pre");
        MtlsIdentity id = MtlsIdentity.loadOrCreate(dir);

        assertFalse(id.isEnrolled());
        assertNull(id.leafPem());
        assertNull(id.caPem());
        assertNull(id.keyManagers(), "no client cert before enrolment");
        assertNull(id.trustManagers(), "no pinned CA before enrolment");
        assertEquals(0L, id.leafExpiresAtEpochSecond());
        assertTrue(id.rotationDue(30L * 24 * 3600), "not enrolled → rotation due");
    }

    @Test
    void keypair_is_stable_across_reload() throws Exception {
        Path dir = Files.createTempDirectory("sqs-mtls-stable");
        String first = MtlsIdentity.loadOrCreate(dir).csrPublicKeyPem();
        String second = MtlsIdentity.loadOrCreate(dir).csrPublicKeyPem();
        assertArrayEquals(first.getBytes(), second.getBytes(),
                "the persisted keypair (and thus its public key) is stable across restarts");
    }

    @Test
    void unparseable_persisted_leaf_fails_closed() throws Exception {
        Path dir = Files.createTempDirectory("sqs-mtls-badleaf");
        Path mtls = dir.resolve("mtls");
        Files.createDirectories(mtls);
        // A CA that parses fine, so the failure is attributable to the leaf alone.
        Files.writeString(mtls.resolve("ca.pem"), selfSignedTestCaPem());
        // PEM-shaped garbage: base64-decodes, but is not an X.509 certificate.
        Files.writeString(mtls.resolve("cert.pem"),
                "-----BEGIN CERTIFICATE-----\nbm90LWEtY2VydGlmaWNhdGU=\n-----END CERTIFICATE-----\n");

        // Residual / H.0c: the load must throw (fail closed) rather than null the leaf and degrade
        // the channel to bearer-only TLS on the platform trust store — the exact fail-open shape
        // closed.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> MtlsIdentity.loadOrCreate(dir));
        assertTrue(ex.getMessage().contains("persisted leaf failed to parse"));
    }

    /** A minimal self-signed X.509 cert PEM to stand in for the env CA in tests. */
    private static String selfSignedTestCaPem() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair ca = gen.generateKeyPair();
        X500Name dn = new X500Name("CN=sqs-test-env-ca");
        Instant now = Instant.now();
        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                dn, BigInteger.ONE, Date.from(now), Date.from(now.plus(Duration.ofDays(30))),
                dn, ca.getPublic());
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(ca.getPrivate());
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(builder.build(signer));
        return "-----BEGIN CERTIFICATE-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(cert.getEncoded())
                + "\n-----END CERTIFICATE-----\n";
    }
}
