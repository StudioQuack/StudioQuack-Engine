package com.studioquack.common.security.envelope;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;

/**
 * Combines hybrid KEM shared secrets into the 32-byte IKM the envelope uses.
 * Primitive outputs and the environment id are bound into HKDF so both sides derive the same key.
 */
public final class HybridKemEnrolment {

    private HybridKemEnrolment() {}

    /**
     * Length of the derived IKM in bytes — fixed at 32 to match
     * the envelope's HKDF input expectation.
     */
    public static final int IKM_BYTE_LEN = 32;

    /**
     * Stable label woven into the transcript so an attacker
     * cannot use the same primitive outputs to forge an IKM
     * for an unrelated context (e.g. session-key derivation).
     */
    public static final String TRANSCRIPT_LABEL = "sqvm/secenv/v1/hybrid-kem";

    /**
     * Canonical primitive ordering for hybrid_x25519_mlkem768.
     * Order MUST match the daemon side; declaration here is the
     * single source of truth.
     */
    public static final List<String> SCHEMA_X25519_MLKEM768_ORDER = List.of(
            X25519KemPrimitive.NAME,
            MlKem768Primitive.NAME);

    /**
     * Primitives for {@code schema}. Only {@link KemSchema#HYBRID_X25519_MLKEM768} is implemented.
     */
    public static List<HybridKemPrimitive> primitivesFor(KemSchema schema) {
        Objects.requireNonNull(schema, "schema");
        if (schema == KemSchema.HYBRID_X25519_MLKEM768) {
            return List.of(new X25519KemPrimitive(), new MlKem768Primitive());
        }
        throw new IllegalArgumentException(
                "Hybrid primitive list not implemented for schema " + schema
                        + " (only HYBRID_X25519_MLKEM768 is supported in v1.0)");
    }

    /**
     * Builds the canonical transcript and derives the 32-byte IKM.
     * Encapsulation order must match {@link #primitivesFor(KemSchema)}.
     *
     * @param encapsulations per-primitive results in {@link #primitivesFor(KemSchema)} order
     * @param envId          environment id bound into the derivation
     */
    public static byte[] combineToIkm(List<KemEncapsulation> encapsulations,
                                      List<HybridKemPrimitive> primitives,
                                      String envId) {
        Objects.requireNonNull(encapsulations, "encapsulations");
        Objects.requireNonNull(primitives, "primitives");
        if (encapsulations.size() != primitives.size()) {
            throw new IllegalArgumentException(
                    "transcript order mismatch: " + encapsulations.size()
                            + " encapsulations vs " + primitives.size() + " primitives");
        }
        byte[] transcript = buildTranscript(primitives, encapsulations);
        byte[] salt = EnvelopeCrypto.buildAad(
                EnvelopeCrypto.WIRE_VERSION,
                KemSchema.HYBRID_X25519_MLKEM768,
                Direction.BE_TO_AGENT,
                envId == null ? "" : envId,
                "0");
        byte[] info = ("sqvm/secenv/v1|hybrid|envId=" + (envId == null ? "" : envId))
                .getBytes(StandardCharsets.UTF_8);
        try {
            byte[] prk = hmacSha256(salt, transcript);
            try {
                return hkdfExpand(prk, info, IKM_BYTE_LEN);
            } finally {
                zero(prk);
            }
        } finally {
            zero(transcript);
        }
    }

    /** Serialises the transcript so both sides feed HKDF the same bytes. */
    private static byte[] buildTranscript(List<HybridKemPrimitive> primitives,
                                          List<KemEncapsulation> encapsulations) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(2048);
        byte[] label = TRANSCRIPT_LABEL.getBytes(StandardCharsets.UTF_8);
        out.write(label, 0, label.length);
        if (primitives.size() > 0xFF) {
            throw new IllegalStateException("transcript: too many primitives " + primitives.size());
        }
        out.write((byte) primitives.size());
        for (int i = 0; i < primitives.size(); i++) {
            HybridKemPrimitive p = primitives.get(i);
            KemEncapsulation enc = encapsulations.get(i);
            byte[] nameBytes = p.name().getBytes(StandardCharsets.UTF_8);
            if (nameBytes.length > 0xFF) {
                throw new IllegalStateException("transcript: primitive name too long " + p.name());
            }
            out.write((byte) nameBytes.length);
            out.write(nameBytes, 0, nameBytes.length);
            byte[] ct = enc.getCiphertext() == null ? new byte[0] : enc.getCiphertext();
            byte[] ss = enc.getSharedSecret() == null ? new byte[0] : enc.getSharedSecret();
            appendUint32Be(out, ct.length);
            out.write(ct, 0, ct.length);
            appendUint32Be(out, ss.length);
            out.write(ss, 0, ss.length);
        }
        return out.toByteArray();
    }

    private static void appendUint32Be(ByteArrayOutputStream out, int v) {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(v);
        out.write(buf.array(), 0, 4);
    }

    private static byte[] hmacSha256(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("HmacSHA256 unavailable", ex);
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException("HMAC init failed", ex);
        }
    }

    private static byte[] hkdfExpand(byte[] prk, byte[] info, int outLen) {
        int sha = 32;
        int n = (outLen + sha - 1) / sha;
        if (n > 255) {
            throw new IllegalStateException("hkdf: outLen too large " + outLen);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(n * sha);
        byte[] t = new byte[0];
        for (int i = 1; i <= n; i++) {
            byte[] input = new byte[t.length + info.length + 1];
            System.arraycopy(t, 0, input, 0, t.length);
            System.arraycopy(info, 0, input, t.length, info.length);
            input[input.length - 1] = (byte) i;
            t = hmacSha256(prk, input);
            out.write(t, 0, t.length);
        }
        byte[] full = out.toByteArray();
        byte[] result = new byte[outLen];
        System.arraycopy(full, 0, result, 0, outLen);
        zero(full);
        zero(t);
        return result;
    }

    private static void zero(byte[] b) {
        if (b == null) return;
        for (int i = 0; i < b.length; i++) b[i] = 0;
    }
}
