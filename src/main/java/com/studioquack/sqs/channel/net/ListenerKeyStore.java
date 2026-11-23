package com.studioquack.sqs.channel.net;

import com.studioquack.sqs.secrets.DaemonBoundSealedStore;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Daemon-held EC P-256 keypair for the:8443 listener. The private key is sealed under
 * {@code <dataDir>/net/listener.key}; the CSR (CN + DNS SAN {@code env-<envId>}) rides handshake v2; the
 * BE-issued leaf is held in memory after each handshake.
 */
@Slf4j
public final class ListenerKeyStore {

    static final String KEY_FILE = "listener.key";
    static final String PUB_FILE = "listener.pub";

    private final PrivateKey privateKey;
    private final java.security.PublicKey publicKey;
    private volatile X509Certificate leaf;

    private ListenerKeyStore(KeyPair pair) {
        this.privateKey = pair.getPrivate();
        this.publicKey = pair.getPublic();
    }

    /**
     * Test seam: a store around an externally generated pair (no file I/O).
     */
    static ListenerKeyStore of(KeyPair pair) {
        return new ListenerKeyStore(pair);
    }

    /**
     * Loads the sealed (or legacy plaintext PKCS#8) key, or generates a new P-256 pair. {@code sealed} may be
     * {@code null} — then the file is 0600 PKCS#8 PEM (tests / no OS store).
     */
    public static ListenerKeyStore loadOrCreate(Path dataDir, String envId,
                                                DaemonBoundSealedStore sealed) throws Exception {
        Path dir = dataDir.resolve("net");
        Files.createDirectories(dir);
        Path keyPath = dir.resolve(KEY_FILE);
        KeyPair pair = load(keyPath, dir.resolve(PUB_FILE), sealed);
        if (pair == null) {
            pair = generate();
            persist(keyPath, dir.resolve(PUB_FILE), sealed, pair);
            log.info("channel/net: generated listener EC P-256 identity at {}", keyPath);
        }
        return new ListenerKeyStore(pair);
    }

    public PrivateKey privateKey() {
        return privateKey;
    }

    public X509Certificate leaf() {
        return leaf;
    }

    public void installLeaf(X509Certificate cert) {
        this.leaf = cert;
    }

    /**
     * PKCS#10 CSR: CN={@code hostname}, DNS SAN={@code hostname}.
     */
    public String csrPem(String hostname) throws Exception {
        String cn = hostname == null || hostname.isBlank() ? "env-unknown" : hostname.trim();
        X500Name subject = new X500Name("CN=" + cn);
        JcaPKCS10CertificationRequestBuilder builder =
                new JcaPKCS10CertificationRequestBuilder(subject, publicKey);
        // PKCS#10 has no addExtension — SAN rides the extensionRequest attribute.
        ExtensionsGenerator extGen = new ExtensionsGenerator();
        extGen.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, cn)));
        builder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extGen.generate());
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withECDSA").build(privateKey);
        PKCS10CertificationRequest csr = builder.build(signer);
        return pemWrap("CERTIFICATE REQUEST", csr.getEncoded());
    }

    private static KeyPair generate() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(new ECGenParameterSpec("secp256r1"), new SecureRandom());
        return kpg.generateKeyPair();
    }

    private static KeyPair load(Path keyPath, Path pubPath, DaemonBoundSealedStore sealed) {
        try {
            byte[] pkcs8 = null;
            if (sealed != null) {
                pkcs8 = sealed.load().orElse(null);
            }
            if (pkcs8 == null && Files.isRegularFile(keyPath)) {
                pkcs8 = decodePemOrDer(Files.readAllBytes(keyPath));
            }
            if (pkcs8 == null || pkcs8.length == 0 || !Files.isRegularFile(pubPath)) {
                return null;
            }
            KeyFactory kf = KeyFactory.getInstance("EC");
            PrivateKey priv = kf.generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
            byte[] spki = decodePemOrDer(Files.readAllBytes(pubPath));
            java.security.PublicKey pub = kf.generatePublic(new java.security.spec.X509EncodedKeySpec(spki));
            return new KeyPair(pub, priv);
        } catch (Exception ex) {
            log.warn("channel/net: listener key load failed ({}) — generating a new pair", ex.toString());
            return null;
        }
    }

    private static void persist(Path keyPath, Path pubPath, DaemonBoundSealedStore sealed, KeyPair pair)
            throws Exception {
        byte[] pkcs8 = pair.getPrivate().getEncoded();
        Files.writeString(pubPath, pemWrap("PUBLIC KEY", pair.getPublic().getEncoded()),
                StandardCharsets.US_ASCII);
        if (sealed != null) {
            try {
                sealed.save(pkcs8);
                return;
            } catch (Exception ex) {
                log.warn("channel/net: listener key seal failed ({}); writing 0600 PEM fallback",
                        ex.toString());
            }
        }
        byte[] pem = pemWrap("PRIVATE KEY", pkcs8).getBytes(StandardCharsets.US_ASCII);
        Path tmp = keyPath.resolveSibling(KEY_FILE + ".tmp");
        try {
            Files.createFile(tmp, java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
                    java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
        } catch (UnsupportedOperationException | java.nio.file.FileAlreadyExistsException ignored) {
            Files.deleteIfExists(tmp);
            Files.createFile(tmp);
        }
        Files.write(tmp, pem);
        Files.move(tmp, keyPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    static String pemWrap(String type, byte[] der) {
        String b64 = Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(der);
        return "-----BEGIN " + type + "-----\n" + b64 + "\n-----END " + type + "-----\n";
    }

    private static byte[] decodePemOrDer(byte[] raw) {
        String s = new String(raw, StandardCharsets.US_ASCII);
        int begin = s.indexOf("-----BEGIN");
        if (begin < 0) {
            return raw;
        }
        int start = s.indexOf('\n', begin);
        int end = s.indexOf("-----END");
        if (start < 0 || end < 0) {
            return raw;
        }
        String b64 = s.substring(start, end).replaceAll("\\s", "");
        return Base64.getDecoder().decode(b64);
    }
}
