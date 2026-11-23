package com.studioquack.common.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;

/**
 * Mints and hashes the one-shot enrolment bearer.
 * The plaintext is returned once; only the SHA-256 digest is stored.
 */
public final class EnrollmentBearerUtil {

    /** Prefix on a freshly minted enrolment bearer. */
    public static final String BEARER_PREFIX = "sqvm_";

    private static final int BEARER_BYTES = 32;
    private static final SecureRandom RANDOM = new SecureRandom();

    private EnrollmentBearerUtil() {
        // util — never instantiated
    }

    /**
     * Mints a fresh one-shot enrolment bearer. The plaintext is returned
     * exactly once; only the SHA-256 of the bearer should ever be
     * persisted ({@code environments.enrollment_bearer_hash}).
     */
    public static String mintBearer() {
        byte[] entropy = new byte[BEARER_BYTES];
        RANDOM.nextBytes(entropy);
        return BEARER_PREFIX + HexFormat.of().formatHex(entropy);
    }

    /** SHA-256 of the bearer, base64url without padding, for at-rest storage. */
    public static String sha256(String bearer) {
        if (bearer == null) return null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(bearer.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }
}
