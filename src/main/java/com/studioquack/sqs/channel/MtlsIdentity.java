package com.studioquack.sqs.channel;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.Callable;

/**
 * Daemon-side per-host PKI material for the BE↔daemon mutual-TLS channel. The public half is the only thing
 * that leaves the host.
 */
@Slf4j
public final class MtlsIdentity {

    /**
     * RSA-4096 — same posture as the BE env CA + enrolment bearer hash.
     */
    private static final int RSA_KEY_SIZE = 4096;

    private static final String KEY_FILE = "identity.key";
    private static final String PUB_FILE = "identity.pub";
    private static final String LEAF_FILE = "cert.pem";
    private static final String CA_FILE = "ca.pem";

    private final Path mtlsDir;

    /**
     * Produces the keypair on FIRST USE rather than at construction. This bean is built by {@code ChannelConfig}
     * on the Spring main thread during context refresh, so anything it does eagerly runs before Tomcat binds and
     * before the health endpoint the installer polls can answer.
     */
    private final Callable<KeyPair> keyPairSource;
    private final Object keyLock = new Object();
    private volatile KeyPair keyPair;

    /**
     * BE-signed leaf PEM; null until enrolment persists a bundle.
     */
    private volatile String leafPem;
    /**
     * Env CA PEM; null until enrolment persists a bundle.
     */
    private volatile String caPem;
    private volatile X509Certificate leafCert;

    private MtlsIdentity(Path mtlsDir, Callable<KeyPair> keyPairSource, String leafPem, String caPem) {
        this.mtlsDir = mtlsDir;
        this.keyPairSource = keyPairSource;
        applyBundle(leafPem, caPem);
    }

    /**
     * Resolves the keypair once, blocking concurrent callers until it is ready.
     */
    private KeyPair keyPair() {
        KeyPair local = keyPair;
        if (local != null) {
            return local;
        }
        synchronized (keyLock) {
            if (keyPair == null) {
                try {
                    keyPair = keyPairSource.call();
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "channel/mtls: identity keypair unavailable: " + e.getMessage(), e);
                }
            }
            return keyPair;
        }
    }

    private PrivateKey privateKey() {
        return keyPair().getPrivate();
    }

    private PublicKey publicKey() {
        return keyPair().getPublic();
    }

    /**
     * Prepares the identity directory and loads any persisted leaf + CA. The RSA-4096 keypair is bound but NOT
     * materialised here — see {@link #keyPairSource} for why construction must stay off the critical path.
     */
    public static MtlsIdentity loadOrCreate(Path dataDir) throws Exception {
        return loadOrCreate(dataDir, null);
    }

    /**
     * As {@link #loadOrCreate(Path)} but routes the private key's at-rest custody through {@code sealedKeyStore}
     * (daemon-bound AES-256-GCM; {@code null} = legacy plaintext PEM). A legacy plaintext {@code identity.key}
     * still loads (the custody store passes it through) and is re-sealed once on load; when OS custody is
     * unavailable the first-boot keygen falls back to the plaintext-0600 write with a single WARN, so the daemon
     * always boots and enrols.
     */
    public static MtlsIdentity loadOrCreate(Path dataDir,
                                            com.studioquack.sqs.secrets.DaemonBoundSealedStore sealedKeyStore)
            throws Exception {
        Path dir = dataDir.resolve("mtls");
        Files.createDirectories(dir);
        Path keyPath = dir.resolve(KEY_FILE);

        Callable<KeyPair> source = Files.exists(keyPath)
                ? () -> loadKeyPair(keyPath, sealedKeyStore)
                : () -> createAndPersistKeyPair(dir, keyPath, sealedKeyStore);

        String leaf = readIfExists(dir.resolve(LEAF_FILE));
        String ca = readIfExists(dir.resolve(CA_FILE));
        return new MtlsIdentity(dir, source, leaf, ca);
    }

    /**
     * First-boot keygen. Bracketed by logs on purpose: this is the single slowest step in bringing a fresh daemon
     * up, and without a line before it a stall here is indistinguishable from a hang.
     */
    private static KeyPair createAndPersistKeyPair(Path dir, Path keyPath,
                                                   com.studioquack.sqs.secrets.DaemonBoundSealedStore sealed)
            throws Exception {
        log.info("channel/mtls: generating RSA-{} identity keypair (first boot; a 4096-bit prime "
                + "search can take a while on a slow host)", RSA_KEY_SIZE);
        long startNanos = System.nanoTime();
        KeyPair fresh = generateRsaKeyPair();
        persistPrivateKey(keyPath, fresh.getPrivate(), sealed);

        writeAtomic(dir.resolve(PUB_FILE), spkiPem(fresh.getPublic()).getBytes(StandardCharsets.UTF_8));
        log.info("channel/mtls: generated RSA-{} identity keypair at {} in {} ms",
                RSA_KEY_SIZE, keyPath, (System.nanoTime() - startNanos) / 1_000_000L);
        return fresh;
    }

    /**
     * The daemon public key as a {@code -----BEGIN PUBLIC KEY-----} SubjectPublicKeyInfo PEM — the exact shape the
     * BE {@code enrol-mtls} endpoint consumes as its {@code csrPublicKey} field.
     */
    public String csrPublicKeyPem() {
        return spkiPem(publicKey());
    }

    /**
     * A standards-compliant PKCS#10 CSR (SHA256withRSA self-signed proof of possession) built with BouncyCastle.
     * Not consumed by the current BE endpoint (which takes the SPKI PEM above) but kept ready for a future
     * CSR-signature-validating BE.
     */
    public String pkcs10CsrPem(String envId) throws Exception {
        X500Name subject = new X500Name("CN=sqvm-daemon-" + (envId == null ? "" : envId));
        JcaPKCS10CertificationRequestBuilder builder =
                new JcaPKCS10CertificationRequestBuilder(subject, publicKey());
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(privateKey());
        PKCS10CertificationRequest csr = builder.build(signer);
        return pemWrap("CERTIFICATE REQUEST", csr.getEncoded());
    }

    /**
     * Persists the BE-signed leaf + env CA atomically (write {@code.tmp} then rename so a crash mid-write can't
     * leave a half-rotated identity) and refreshes the in-memory bundle. Callers re-bind their TLS clients after.
     */
    public synchronized void persistSignedBundle(String leafPemIn, String caPemIn) throws Exception {
        if (leafPemIn == null || leafPemIn.isBlank() || caPemIn == null || caPemIn.isBlank()) {
            throw new IllegalArgumentException("persistSignedBundle: empty leaf/ca PEM");
        }
        writeAtomic(mtlsDir.resolve(LEAF_FILE), leafPemIn.getBytes(StandardCharsets.UTF_8));
        writeAtomic(mtlsDir.resolve(CA_FILE), caPemIn.getBytes(StandardCharsets.UTF_8));
        applyBundle(leafPemIn, caPemIn);
        log.info("channel/mtls: persisted BE-signed leaf + env CA (leaf expires {})",
                leafCert == null ? "?" : leafCert.getNotAfter());
    }

    /**
     * True once both the leaf and the env CA are present — i.e. mTLS-ready.
     */
    public boolean isEnrolled() {
        return leafPem != null && caPem != null && leafCert != null;
    }

    /**
     * PEM of the current BE-signed leaf, or {@code null} before enrolment.
     */
    public String leafPem() {
        return leafPem;
    }

    /**
     * Parsed current leaf, or {@code null} before enrolment. Feeds the reverse listener's
     * {@code RotatingKeyManager}: a renewed bundle installed by {@link #persistSignedBundle} is presented on the
     * next TLS handshake without a listener restart.
     */
    public X509Certificate leafCert() {
        return leafCert;
    }

    /**
     * Identity private key (stable across cert rotations — only the leaf rotates). Resolves the lazy keypair; safe
     * to call once enrolment has run (the key exists by then).
     */
    public PrivateKey identityPrivateKey() {
        return privateKey();
    }

    /**
     * PEM of the pinned env CA, or {@code null} before enrolment.
     */
    public String caPem() {
        return caPem;
    }

    /**
     * Leaf {@code NotAfter} as epoch-seconds, or 0 when no leaf is loaded.
     */
    public long leafExpiresAtEpochSecond() {
        X509Certificate cert = leafCert;
        return cert == null ? 0L : cert.getNotAfter().toInstant().getEpochSecond();
    }

    /**
     * True when the leaf should be re-issued: not enrolled yet, or remaining validity is below
     * {@code thresholdSeconds} (the hourly rotation check uses a &lt;30d threshold, matching the BE's
     * {@code rotateLeafIfDue}).
     */
    public boolean rotationDue(long thresholdSeconds) {
        if (!isEnrolled()) {
            return true;
        }
        long remaining = leafExpiresAtEpochSecond() - Instant.now().getEpochSecond();
        return remaining < thresholdSeconds;
    }

    /**
     * KeyManagers presenting the leaf + private key as the client certificate, or {@code null} before enrolment
     * (caller falls back to bearer-only TLS).
     */
    public KeyManager[] keyManagers() throws Exception {
        if (!isEnrolled()) {
            return null;
        }
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        char[] empty = new char[0];
        ks.setKeyEntry("leaf", privateKey(), empty, new X509Certificate[]{leafCert});
        // Consumed by BCJSSE contexts (ChannelTls, OverlayReverseListener): must be BCJSSE's own
        // key managers or an EC leaf is unselectable — see HybridTls.keyManagers.
        return com.studioquack.common.security.tls.HybridTls.keyManagers(ks, empty);
    }

    /**
     * TrustManagers pinning the env CA as the sole trust anchor, or {@code null} before enrolment. The daemon
     * rejects a BE presenting any chain that does not terminate at this CA.
     */
    public TrustManager[] trustManagers() throws Exception {
        if (caPem == null) {
            return null;
        }
        X509Certificate ca = decodeCert(caPem);
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("env-ca", ca);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        return tmf.getTrustManagers();
    }

    // Internals ---------------------------------------------------------

    private void applyBundle(String leafPemIn, String caPemIn) {
        this.leafPem = (leafPemIn == null || leafPemIn.isBlank()) ? null : leafPemIn;
        this.caPem = (caPemIn == null || caPemIn.isBlank()) ? null : caPemIn;
        this.leafCert = null;
        if (this.leafPem != null) {
            try {
                this.leafCert = decodeCert(this.leafPem);
            } catch (Exception ex) {
                // Residual: fail closed like the sibling keypair/CA
                // failures. A persisted leaf that cannot be parsed must abort the
                // load — never null it out and silently degrade the channel to
                // bearer-only TLS on the platform trust store.
                this.leafPem = null;
                throw new IllegalStateException(
                        "channel/mtls: persisted leaf failed to parse: " + ex, ex);
            }
        }
    }

    private static KeyPair generateRsaKeyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(RSA_KEY_SIZE);
        return gen.generateKeyPair();
    }

    private static KeyPair loadKeyPair(Path keyPath,
                                       com.studioquack.sqs.secrets.DaemonBoundSealedStore sealed)
            throws Exception {
        byte[] raw;
        if (sealed != null) {
            // Sealed blob: opened with the OS-held key + AAD (wrong engine ⇒ empty).
            // Plaintext PEM: passed through verbatim. An existing file that will not open is
            // fail-closed — never silently regenerate under a signed leaf that pins the old key.
            raw = sealed.load().orElseThrow(() -> new IllegalStateException(
                    "channel/mtls: identity key present at " + keyPath + " but could not be opened "
                            + "(daemon-bound custody unavailable, or the blob belongs to another "
                            + "engine) — refusing to improvise"));
            migrateKeyIfPlaintext(keyPath, sealed, raw);
        } else {
            raw = Files.readAllBytes(keyPath);
        }
        String pem = new String(raw, StandardCharsets.UTF_8);
        byte[] der = Base64.getMimeDecoder().decode(stripPem(pem));
        PrivateKey priv = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(der));
        // Recover the public key from the private key's modulus/exponent.
        java.security.interfaces.RSAPrivateCrtKey crt = (java.security.interfaces.RSAPrivateCrtKey) priv;
        PublicKey pub = KeyFactory.getInstance("RSA").generatePublic(
                new java.security.spec.RSAPublicKeySpec(crt.getModulus(), crt.getPublicExponent()));
        return new KeyPair(pub, priv);
    }

    /**
     * One-time, idempotent migration: a legacy plaintext PEM that just loaded is re-sealed in place. Best-effort —
     * a custody hiccup must never break a key load that already succeeded.
     */
    private static void migrateKeyIfPlaintext(Path keyPath,
                                              com.studioquack.sqs.secrets.DaemonBoundSealedStore sealed,
                                              byte[] rawPem) {
        if (sealed.sealedFilePresent() || !sealed.custodyAvailable()) {
            return;
        }
        try {
            sealed.save(rawPem);
            log.info("channel/mtls: migrated identity private key to daemon-bound sealed custody ({})",
                    keyPath.getFileName());
        } catch (Exception ex) {
            log.warn("channel/mtls: identity-key re-seal skipped ({}) — plaintext 0600 custody kept "
                    + "for now", ex.getMessage());
        }
    }

    private static void persistPrivateKey(Path keyPath, PrivateKey key,
                                          com.studioquack.sqs.secrets.DaemonBoundSealedStore sealed)
            throws Exception {
        byte[] pemBytes = pemWrap("PRIVATE KEY", key.getEncoded()).getBytes(StandardCharsets.UTF_8);
        if (sealed != null) {
            try {
                sealed.save(pemBytes);
                return;
            } catch (com.studioquack.sqs.secrets.DaemonBoundSealedStore.DaemonBoundCustodyException ex) {
                log.warn("channel/mtls: OS custody unavailable for the identity key ({}) — "
                        + "falling back to plaintext 0600 custody", ex.getMessage());
            }
        }
        writeAtomic(keyPath, pemBytes);
        try {
            Files.setPosixFilePermissions(keyPath, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX filesystem (Windows) — ACLs handled by the installer.
        }
    }

    private static String spkiPem(PublicKey pub) {
        return pemWrap("PUBLIC KEY", pub.getEncoded());
    }

    private static X509Certificate decodeCert(String pem) throws Exception {
        byte[] der = Base64.getMimeDecoder().decode(stripPem(pem));
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        return (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(der));
    }

    private static String pemWrap(String type, byte[] der) {
        String b64 = Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(der);
        return "-----BEGIN " + type + "-----\n" + b64 + "\n-----END " + type + "-----\n";
    }

    private static String stripPem(String pem) {
        return pem.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
    }

    private static String readIfExists(Path path) throws Exception {
        return Files.exists(path) ? Files.readString(path) : null;
    }

    private static void writeAtomic(Path path, byte[] data) throws Exception {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.write(tmp, data);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
    }
}
