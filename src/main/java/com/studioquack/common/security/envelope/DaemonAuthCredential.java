package com.studioquack.common.security.envelope;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Objects;

/**
 * Channel credential derived from the hybrid-enrolment {@code kemIkm}.
 * Both sides present it as {@code Authorization: Bearer …} on daemon-to-control-plane calls.
 * The wire value is {@code sqikm1.} plus a base64url HMAC-SHA256 of the IKM bound to the environment id.
 */
public final class DaemonAuthCredential {

    /** Wire prefix. Distinct from the installer bearer. */
    public static final String PREFIX = "sqikm1.";

    private static final String INFO_PREFIX = "studioquack.daemon-auth.v1|";

    private DaemonAuthCredential() {
    }

    /** True when {@code presented} starts with the kemIkm credential prefix. */
    public static boolean looksLike(String presented) {
        return presented != null && presented.startsWith(PREFIX);
    }

    /**
     * Derives the channel credential from raw 32-byte {@code kemIkm}.
     *
     * @throws IllegalArgumentException when ikm/envId are unusable
     */
    public static String fromKemIkm(byte[] kemIkm, String envId) {
        Objects.requireNonNull(kemIkm, "kemIkm");
        if (kemIkm.length == 0) {
            throw new IllegalArgumentException("DaemonAuthCredential: empty kemIkm");
        }
        if (envId == null || envId.isBlank()) {
            throw new IllegalArgumentException("DaemonAuthCredential: blank envId");
        }
        byte[] mac = hmacSha256(kemIkm, (INFO_PREFIX + envId).getBytes(StandardCharsets.UTF_8));
        return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(mac);
    }

    /**
     * Derives the channel credential from the env-row form of {@code kemIkm}
     * (base64-url, no padding — same encoding {@code HybridKemEnrolmentService} persists).
     *
     * @return the credential, or {@code null} when {@code kemIkmB64} is blank/malformed
     */
    public static String fromKemIkmB64(String kemIkmB64, String envId) {
        byte[] ikm = decodeIkm(kemIkmB64);
        if (ikm == null) {
            return null;
        }
        try {
            return fromKemIkm(ikm, envId);
        } finally {
            zero(ikm);
        }
    }

    /**
     * Constant-time verify of a presented credential against the stored kemIkm.
     * Fail-closed: blank/malformed inputs return {@code false}.
     */
    public static boolean matches(String presented, String kemIkmB64, String envId) {
        if (presented == null || presented.isBlank() || !looksLike(presented)) {
            return false;
        }
        String expected = fromKemIkmB64(kemIkmB64, envId);
        if (expected == null) {
            return false;
        }
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                presented.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] decodeIkm(String kemIkmB64) {
        if (kemIkmB64 == null || kemIkmB64.isBlank()) {
            return null;
        }
        try {
            byte[] ikm = Base64.getUrlDecoder().decode(kemIkmB64);
            return ikm.length == 0 ? null : ikm;
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static byte[] hmacSha256(byte[] key, byte[] message) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(message);
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException("HmacSHA256 unavailable", ex);
        }
    }

    private static void zero(byte[] buf) {
        if (buf != null) {
            java.util.Arrays.fill(buf, (byte) 0);
        }
    }
}
