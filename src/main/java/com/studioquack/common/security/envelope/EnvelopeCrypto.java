package com.studioquack.common.security.envelope;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;

/**
 * AES-256-GCM and HKDF-SHA256 envelope crypto.
 * Callers pass the IKM. Each envelope gets its own salt, nonce, and AAD bound to direction, environment, and job.
 */
public final class EnvelopeCrypto {

    /** Wire version emitted by this implementation. */
    public static final int WIRE_VERSION = 1;

    /** Algorithm identifier emitted in {@link SecureEnvelope#getAlg()}. */
    public static final String ALG_AES256GCM_HKDF_SHA256 = "AES-256-GCM-HKDF-SHA256";

    /** Length of the AES-GCM nonce in bytes. RFC 5116 §5.1 recommends 96 bits. */
    private static final int GCM_NONCE_LEN = 12;

    /** Length of the AES-GCM authentication tag in bits (16 bytes). */
    private static final int GCM_TAG_BITS = 128;

    /** Length of the HKDF salt in bytes. Matches the daemon side. */
    private static final int HKDF_SALT_LEN = 16;

    /** Length of the derived AES key in bytes (AES-256). */
    private static final int AES_KEY_LEN = 32;

    /** SHA-256 output length in bytes — used by HKDF expand block sizing. */
    private static final int SHA256_LEN = 32;

    /** Non-blocking CSPRNG for salt + nonce generation. */
    private static final SecureRandom RANDOM = new SecureRandom();

    private EnvelopeCrypto() {
        // util — never instantiated
    }

    /**
     * Recomputes the preshared-hash IKM the way both sides agree on
     * it: SHA-256 of the UTF-8 bytes of the enrolment bearer.
     *
     * <p>Returns {@code null} for a blank bearer so callers can
     * short-circuit and ship/accept the legacy plaintext path
     * without an extra null check.
     */
    public static byte[] ikmFromBearer(String bearer) {
        if (bearer == null || bearer.isEmpty()) return null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(bearer.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }

    /** Wire prefix for heartbeat and overlay command authenticity. */
    public static final String COMMAND_MAC_ALG = "sqcmd1";

    private static final byte[] COMMAND_MAC_INFO =
            "sq-cmd-mac-v1".getBytes(StandardCharsets.UTF_8);

    /**
     * HMAC over {@code envId|commandId|type|issuedAt|payload} using a key
     * derived from the enrolment IKM. Bind the env so a captured MAC
     * cannot be replayed onto another daemon.
     */
    public static String macCommand(byte[] ikm, String envId, String commandId, String type,
                                    String issuedAt, String payload) {
        if (ikm == null || ikm.length == 0) {
            throw new IllegalArgumentException("macCommand: ikm required");
        }
        try {
            byte[] key = hmacSha256(COMMAND_MAC_INFO, ikm);
            try {
                byte[] digest = hmacSha256(key,
                        commandMacMessage(envId, commandId, type, issuedAt, payload));
                return COMMAND_MAC_ALG + "."
                        + Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
            } finally {
                zero(key);
            }
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("command MAC failed", e);
        }
    }

    /** Constant-time verify. Blank {@code mac} or missing IKM is a miss. */
    public static boolean verifyCommandMac(byte[] ikm, String envId, String commandId, String type,
                                           String issuedAt, String payload, String mac) {
        if (mac == null || mac.isBlank() || ikm == null || ikm.length == 0) {
            return false;
        }
        String expected = macCommand(ikm, envId, commandId, type, issuedAt, payload);
        return constantTimeEquals(
                expected.getBytes(StandardCharsets.US_ASCII),
                mac.getBytes(StandardCharsets.US_ASCII));
    }

    private static byte[] commandMacMessage(String envId, String commandId, String type,
                                            String issuedAt, String payload) {
        String msg = n(envId) + "\n" + n(commandId) + "\n" + n(type) + "\n"
                + n(issuedAt) + "\n" + n(payload);
        return msg.getBytes(StandardCharsets.UTF_8);
    }

    private static String n(String s) {
        return s == null ? "" : s;
    }

    /**
     * Convenience wrapper around {@link #seal} for the common case
     * where the inner payload is a structured object that can be
     * marshalled to JSON.
     *
     * @param ikm     32-byte preshared key material
     *                (see {@link #ikmFromBearer(String)})
     * @param dir     direction binding (see {@link Direction})
     * @param envId   target env id — woven into AAD
     * @param jobKey  job correlation tag — woven into AAD; pass
     *                {@code "0"} for non-job-bound payloads
     * @param object  the JSON-serialisable inner object
     * @param mapper  Jackson mapper used to serialise the inner
     */
    public static SecureEnvelope sealJson(byte[] ikm, Direction dir, String envId,
                                          String jobKey, Object object, ObjectMapper mapper)
            throws GeneralSecurityException, IOException {
        Objects.requireNonNull(mapper, "mapper");
        byte[] body = mapper.writeValueAsBytes(object);
        return seal(ikm, dir, envId, jobKey, body);
    }

    /**
     * Builds an envelope around plaintext using the AES-256-GCM +
     * HKDF-SHA256 v1 contract. Salt + nonce are freshly generated
     * per call from {@link SecureRandom} so the same IKM never
     * produces the same ciphertext twice (defence against any
     * deterministic-output side channel).
     */
    public static SecureEnvelope seal(byte[] ikm, Direction dir, String envId,
                                      String jobKey, byte[] plaintext)
            throws GeneralSecurityException {
        if (ikm == null || ikm.length == 0) {
            throw new IllegalArgumentException("seal: ikm must not be empty");
        }
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(plaintext, "plaintext");
        String safeEnv = envId == null ? "" : envId;
        String safeJob = jobKey == null ? "" : jobKey;

        byte[] salt = new byte[HKDF_SALT_LEN];
        RANDOM.nextBytes(salt);
        byte[] nonce = new byte[GCM_NONCE_LEN];
        RANDOM.nextBytes(nonce);

        KemSchema schema = KemSchema.PRESHARED_HASH;
        byte[] aad = buildAad(WIRE_VERSION, schema, dir, safeEnv, safeJob);
        byte[] key = deriveKey(ikm, salt, dir, safeEnv, safeJob);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE,
                    new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(GCM_TAG_BITS, nonce));
            cipher.updateAAD(aad);
            byte[] ciphertext = cipher.doFinal(plaintext);
            return SecureEnvelope.builder()
                    .v(WIRE_VERSION)
                    .alg(ALG_AES256GCM_HKDF_SHA256)
                    .kemSchema(schema.name())
                    .dir(dir.name())
                    .envId(safeEnv)
                    .jobKey(safeJob)
                    .salt(b64(salt))
                    .nonce(b64(nonce))
                    .aad(b64(aad))
                    .ciphertext(b64(ciphertext))
                    .build();
        } finally {
            // Best-effort key zeroisation. Java byte[]s are not
            // guaranteed to be reused immediately, but clearing
            // helps when the JIT hoists this stack frame's array
            // into a long-lived buffer pool.
            zero(key);
        }
    }

    /**
     * Verifies the envelope contract and returns the plaintext.
     * Caller decides how to deserialise the inner payload.
     *
     * <p>{@code expectedDir} / {@code expectedEnvId} /
     * {@code expectedJobKey} are the receiver's view of the
     * channel. Mismatches are loud errors so a tampered envelope
     * cannot trick a receiver into opening it under a different
     * (job, dir, env) tuple than the sender intended. Pass empty
     * strings for envId/jobKey to skip the corresponding bind
     * check (e.g. when the receiver is the FE and the channel is
     * not job-scoped).
     */
    public static byte[] open(byte[] ikm, SecureEnvelope env,
                              Direction expectedDir, String expectedEnvId, String expectedJobKey)
            throws GeneralSecurityException {
        if (ikm == null || ikm.length == 0) {
            throw new IllegalArgumentException("open: ikm must not be empty");
        }
        Objects.requireNonNull(env, "env");
        Objects.requireNonNull(expectedDir, "expectedDir");

        if (env.getV() != WIRE_VERSION) {
            throw new GeneralSecurityException("envelope: unsupported version " + env.getV());
        }
        if (!ALG_AES256GCM_HKDF_SHA256.equals(env.getAlg())) {
            throw new GeneralSecurityException("envelope: unsupported alg " + env.getAlg());
        }
        KemSchema parsedSchema;
        try {
            parsedSchema = KemSchema.valueOf(env.getKemSchema());
        } catch (IllegalArgumentException ex) {
            throw new GeneralSecurityException("envelope: unknown kemSchema " + env.getKemSchema());
        }
        if (parsedSchema != KemSchema.PRESHARED_HASH) {
            throw new GeneralSecurityException(
                    "envelope: unsupported kemSchema " + parsedSchema + " (v1 expects PRESHARED_HASH)");
        }
        Direction parsedDir;
        try {
            parsedDir = Direction.valueOf(env.getDir());
        } catch (IllegalArgumentException ex) {
            throw new GeneralSecurityException("envelope: unknown direction " + env.getDir());
        }
        if (parsedDir != expectedDir) {
            throw new GeneralSecurityException(
                    "envelope: direction mismatch (got " + parsedDir + " want " + expectedDir + ")");
        }
        if (expectedEnvId != null && !expectedEnvId.isEmpty()
                && !expectedEnvId.equals(env.getEnvId())) {
            throw new GeneralSecurityException("envelope: envId mismatch");
        }
        if (expectedJobKey != null && !expectedJobKey.isEmpty()
                && !expectedJobKey.equals(env.getJobKey())) {
            throw new GeneralSecurityException("envelope: jobKey mismatch");
        }

        byte[] salt = unb64(env.getSalt(), "salt");
        byte[] nonce = unb64(env.getNonce(), "nonce");
        byte[] aadOnWire = unb64(env.getAad(), "aad");
        byte[] ct = unb64(env.getCiphertext(), "ciphertext");

        byte[] expectedAad = buildAad(env.getV(), parsedSchema, parsedDir,
                env.getEnvId() == null ? "" : env.getEnvId(),
                env.getJobKey() == null ? "" : env.getJobKey());
        if (!constantTimeEquals(aadOnWire, expectedAad)) {
            throw new GeneralSecurityException(
                    "envelope: AAD mismatch (canonical recompute differs)");
        }

        byte[] key = deriveKey(ikm, salt, parsedDir,
                env.getEnvId() == null ? "" : env.getEnvId(),
                env.getJobKey() == null ? "" : env.getJobKey());
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(GCM_TAG_BITS, nonce));
            cipher.updateAAD(expectedAad);
            return cipher.doFinal(ct);
        } finally {
            zero(key);
        }
    }

    /**
     * Returns the canonical AAD bytes. Layout MUST match the
     * agent-source {@code BuildAAD} exactly:
     *
     * <pre>{@code
     *   [u8       version           ]
     *   [u8       kemSchemaOrdinal  ]
     *   [u8       directionOrdinal  ]
     *   [u32-be   envIdLen          ]
     *   [bytes    envIdUtf8         ]
     *   [u32-be   jobKeyLen         ]
     *   [bytes    jobKeyUtf8        ]
     * }</pre>
     */
    public static byte[] buildAad(int version, KemSchema schema, Direction dir,
                                  String envId, String jobKey) {
        byte[] envBytes = envId == null ? new byte[0] : envId.getBytes(StandardCharsets.UTF_8);
        byte[] jobBytes = jobKey == null ? new byte[0] : jobKey.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(3 + 4 + envBytes.length + 4 + jobBytes.length);
        out.write((byte) version);
        out.write((byte) schema.ordinal());
        out.write((byte) dir.ordinal());
        appendUint32Be(out, envBytes.length);
        out.write(envBytes, 0, envBytes.length);
        appendUint32Be(out, jobBytes.length);
        out.write(jobBytes, 0, jobBytes.length);
        return out.toByteArray();
    }

    /**
     * HKDF-Extract-then-Expand (RFC 5869) with the per-envelope salt
     * and a direction, environment, and job binding.
     */
    private static byte[] deriveKey(byte[] ikm, byte[] salt, Direction dir,
                                    String envId, String jobKey) throws GeneralSecurityException {
        String info = "sqvm/secenv/v1|" + dir.name() + "|envId=" + envId + "|jobKey=" + jobKey;
        byte[] prk = hmacSha256(salt, ikm);
        try {
            return hkdfExpand(prk, info.getBytes(StandardCharsets.UTF_8), AES_KEY_LEN);
        } finally {
            zero(prk);
        }
    }

    /**
     * HKDF-Expand inner loop. For the 32-byte AES key path only one
     * round runs; the loop generalises in case a future caller asks
     * for a 64-byte split key (encrypt + mac).
     */
    private static byte[] hkdfExpand(byte[] prk, byte[] info, int outLen)
            throws GeneralSecurityException {
        int n = (outLen + SHA256_LEN - 1) / SHA256_LEN;
        if (n > 255) {
            throw new GeneralSecurityException("hkdf: outLen too large " + outLen);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(n * SHA256_LEN);
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

    private static byte[] hmacSha256(byte[] key, byte[] data) throws GeneralSecurityException {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (NoSuchAlgorithmException ex) {
            // HmacSHA256 is mandated by the JCA spec.
            throw new IllegalStateException("HmacSHA256 unavailable", ex);
        }
    }

    private static void appendUint32Be(ByteArrayOutputStream out, int v) {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(v);
        byte[] bytes = buf.array();
        out.write(bytes, 0, bytes.length);
    }

    private static String b64(byte[] b) {
        return Base64.getEncoder().encodeToString(b);
    }

    private static byte[] unb64(String s, String field) throws GeneralSecurityException {
        if (s == null || s.isEmpty()) {
            throw new GeneralSecurityException("envelope: " + field + " missing");
        }
        try {
            return Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException ex) {
            throw new GeneralSecurityException("envelope: " + field + " not valid base64", ex);
        }
    }

    /**
     * Constant-time equality check. We could use
     * {@link java.security.MessageDigest#isEqual(byte[], byte[])}
     * directly, but routing through a tiny private helper keeps the
     * intent explicit at every callsite (this is a security-
     * sensitive comparison, not a generic equals).
     */
    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        return MessageDigest.isEqual(a, b);
    }

    private static void zero(byte[] b) {
        if (b == null) return;
        for (int i = 0; i < b.length; i++) b[i] = 0;
    }
}
