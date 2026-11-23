package com.studioquack.common.security.envelope;

import javax.crypto.KeyAgreement;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.XECPrivateKey;
import java.security.spec.NamedParameterSpec;
import java.security.spec.XECPublicKeySpec;

/**
 * X25519 half of the hybrid KEM, using the JDK {@code XDH} provider.
 * Encapsulation is an ephemeral Diffie-Hellman: the peer public key is the ciphertext.
 */
public final class X25519KemPrimitive implements HybridKemPrimitive {

    /** Stable wire identifier — referenced by KemSchema and AAD. */
    public static final String NAME = "X25519";

    /** X25519 raw public-key length. */
    private static final int PUBLIC_KEY_LEN = 32;

    /** X25519 raw private-key length. */
    private static final int PRIVATE_KEY_LEN = 32;

    private static final SecureRandom RANDOM = new SecureRandom();

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public int publicKeyLength() {
        return PUBLIC_KEY_LEN;
    }

    @Override
    public int ciphertextLength() {
        // X25519 "ciphertext" is the encapsulator's ephemeral
        // public key — same length as a regular pubkey.
        return PUBLIC_KEY_LEN;
    }

    @Override
    public KemKeypair generateKeypair() {
        try {
            KeyPairGenerator gen = KeyPairGenerator.getInstance("XDH");
            gen.initialize(NamedParameterSpec.X25519, RANDOM);
            KeyPair kp = gen.generateKeyPair();
            byte[] pub = encodePublicKey(kp.getPublic());
            byte[] priv = encodePrivateKey(kp.getPrivate());
            return new KemKeypair(pub, priv);
        } catch (GeneralSecurityException ex) {
            throw new KemException(NAME, "keypair generation failed", ex);
        }
    }

    @Override
    public KemEncapsulation encapsulate(byte[] peerPublicKey) {
        if (peerPublicKey == null || peerPublicKey.length != PUBLIC_KEY_LEN) {
            throw new KemException(NAME,
                    "peer public key has wrong length " +
                            (peerPublicKey == null ? "null" : String.valueOf(peerPublicKey.length)),
                    null);
        }
        // Generate a fresh ephemeral X25519 keypair on the
        // encapsulator side. The ephemeral public key IS the
        // wire ciphertext; the shared secret is the ECDH point.
        KemKeypair ephemeral = generateKeypair();
        try {
            byte[] sharedSecret = computeSharedSecret(ephemeral.getPrivateKey(), peerPublicKey);
            return KemEncapsulation.builder()
                    .ciphertext(ephemeral.getPublicKey())
                    .sharedSecret(sharedSecret)
                    .build();
        } catch (GeneralSecurityException ex) {
            throw new KemException(NAME, "encapsulation failed", ex);
        } finally {
            // Zeroise the ephemeral private key — it has no
            // further purpose after the encapsulator has
            // computed the shared secret.
            zero(ephemeral.getPrivateKey());
        }
    }

    @Override
    public byte[] decapsulate(byte[] ciphertext, byte[] privateKey) {
        if (ciphertext == null || ciphertext.length != PUBLIC_KEY_LEN) {
            throw new KemException(NAME,
                    "ciphertext has wrong length " +
                            (ciphertext == null ? "null" : String.valueOf(ciphertext.length)),
                    null);
        }
        if (privateKey == null || privateKey.length != PRIVATE_KEY_LEN) {
            throw new KemException(NAME,
                    "private key has wrong length " +
                            (privateKey == null ? "null" : String.valueOf(privateKey.length)),
                    null);
        }
        try {
            return computeSharedSecret(privateKey, ciphertext);
        } catch (GeneralSecurityException ex) {
            throw new KemException(NAME, "decapsulation failed", ex);
        }
    }

    /**
     * Wraps the JDK XDH KeyAgreement so callers feed in raw
     * bytes and get back the 32-byte ECDH shared point.
     */
    private static byte[] computeSharedSecret(byte[] privateKeyBytes, byte[] peerPublicKeyBytes)
            throws GeneralSecurityException {
        KeyFactory factory = KeyFactory.getInstance("XDH");
        // Reconstruct the JDK PrivateKey from raw bytes via
        // PKCS8 DER wrapping. The JDK's only public path to a
        // raw X25519 private key goes through this round-trip
        // because the platform preserves the option to hide the
        // raw scalar from caller code in HSM-backed builds.
        PrivateKey priv = factory.generatePrivate(
                new java.security.spec.PKCS8EncodedKeySpec(wrapPkcs8X25519Private(privateKeyBytes)));
        PublicKey pub = factory.generatePublic(
                new XECPublicKeySpec(NamedParameterSpec.X25519, decodeLittleEndianU(peerPublicKeyBytes)));

        KeyAgreement ka = KeyAgreement.getInstance("XDH");
        ka.init(priv);
        ka.doPhase(pub, true);
        return ka.generateSecret();
    }

    /**
     * Encodes the JDK PublicKey as the raw 32-byte X25519
     * little-endian u-coordinate the wire format expects. JDK
     * exposes this via the X.509 SPKI form — we strip the ASN.1
     * prefix and keep the trailing 32 bytes.
     */
    private static byte[] encodePublicKey(PublicKey pub) throws GeneralSecurityException {
        byte[] encoded = pub.getEncoded(); // X.509 SPKI
        // The X25519 SPKI is a fixed-length ASN.1 wrapper around
        // 32 bytes of u-coordinate. JDK guarantees the trailing
        // 32 bytes are the raw key per RFC 8410.
        if (encoded.length < PUBLIC_KEY_LEN) {
            throw new GeneralSecurityException("Unexpected X25519 SPKI length: " + encoded.length);
        }
        byte[] out = new byte[PUBLIC_KEY_LEN];
        System.arraycopy(encoded, encoded.length - PUBLIC_KEY_LEN, out, 0, PUBLIC_KEY_LEN);
        return out;
    }

    /**
     * Encodes the JDK PrivateKey as the raw 32-byte X25519
     * scalar. Equivalent to inverse of {@link #wrapPkcs8X25519Private}.
     */
    private static byte[] encodePrivateKey(PrivateKey priv) throws GeneralSecurityException {
        if (priv instanceof XECPrivateKey xec) {
            return xec.getScalar()
                    .orElseThrow(() -> new GeneralSecurityException(
                            "JDK XDH provider hid the raw X25519 scalar — HSM-backed key not supported"));
        }
        throw new GeneralSecurityException(
                "Unexpected private-key type for X25519: " + priv.getClass().getName());
    }

    /**
     * Wraps a raw 32-byte X25519 scalar into the PKCS#8
     * structure the JDK KeyFactory expects. Constants come from
     * RFC 8410 §7.2.
     */
    private static byte[] wrapPkcs8X25519Private(byte[] raw) {
        // PKCS#8: SEQUENCE { version 0, AlgorithmIdentifier {oid 1.3.101.110}, OCTET STRING { OCTET STRING raw } }
        byte[] prefix = new byte[]{
                0x30, 0x2e,               // SEQUENCE (46 bytes total)
                0x02, 0x01, 0x00,         // INTEGER 0 (version)
                0x30, 0x05,               // SEQUENCE (5)
                0x06, 0x03, 0x2b, 0x65, 0x6e, // OID 1.3.101.110 (X25519)
                0x04, 0x22,               // OCTET STRING (34)
                0x04, 0x20                // OCTET STRING (32) — the scalar
        };
        byte[] out = new byte[prefix.length + raw.length];
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        System.arraycopy(raw, 0, out, prefix.length, raw.length);
        return out;
    }

    /**
     * Decode a 32-byte little-endian u-coordinate into the
     * BigInteger that {@link XECPublicKeySpec} expects. We
     * MUST mask the high bit of the last byte per RFC 7748 §5.
     */
    private static BigInteger decodeLittleEndianU(byte[] u) {
        byte[] copy = u.clone();
        // Clear the most-significant bit of the last byte (per
        // RFC 7748 §5: "implementations MUST mask off the most
        // significant bit of the final byte").
        copy[copy.length - 1] &= (byte) 0x7f;
        // The wire format is little-endian; BigInteger expects
        // big-endian, so reverse the byte order.
        byte[] beU = new byte[copy.length];
        for (int i = 0; i < copy.length; i++) {
            beU[i] = copy[copy.length - 1 - i];
        }
        return new BigInteger(1, beU);
    }

    private static void zero(byte[] b) {
        if (b == null) return;
        for (int i = 0; i < b.length; i++) b[i] = 0;
    }
}
