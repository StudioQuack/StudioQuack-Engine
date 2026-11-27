package com.studioquack.common.dto.webhook;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wire format parser, generator, and validator for engine webhook API keys.
 *
 * <p>Canonical wire format:
 * <pre>{@code
 * sqwh_<8-char-base62-keyId>_<43-char-base64url-256bit-secret>
 * }</pre>
 *
 * <p>The {@code sqwh_} prefix is structurally disjoint from general engine API keys
 * ({@code sqk_}) so a webhook key can never authenticate on non-webhook endpoints and
 * log/audit scrubbers can reliably redact any {@code sqwh_[A-Za-z0-9_-]+} token.
 */
public final class WebhookKeyFormat {

    /** Wire prefix for provider webhook keys (disjoint from {@code sqk_}). */
    public static final String WIRE_PREFIX = "sqwh_";

    /** Stable control-plane reference id prefix stored in workflow/node JSON. */
    public static final String REF_ID_PREFIX = "whk_";

    /** Non-secret keyId pattern: exactly 8 base62 characters. */
    public static final Pattern KEY_ID_PATTERN = Pattern.compile("^[A-Za-z0-9]{8}$");

    /**
     * Full wire key pattern: {@code sqwh_<8-char-base62>_<base64url-secret>}
     * (secret >= 32 chars; standard 256-bit base64url without padding is 43 chars).
     */
    public static final Pattern FULL_KEY_PATTERN =
            Pattern.compile("^sqwh_([A-Za-z0-9]{8})_([A-Za-z0-9_-]{32,128})$");

    /** Broad scrubbing pattern used by redactors and request log filters. */
    public static final Pattern REDACT_PATTERN = Pattern.compile("sqwh_[A-Za-z0-9_-]+");

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final char[] BASE62 =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
    private static final int KEY_ID_LENGTH = 8;
    private static final int SECRET_BYTES = 32;

    private WebhookKeyFormat() {
    }

    /** Parsed components of a valid {@code sqwh_<keyId>_<secret>} wire token. */
    public record ParsedWebhookKey(String keyId, String secret) {
    }

    /** Parse a candidate full wire key into its {@code keyId} and {@code secret}. */
    public static Optional<ParsedWebhookKey> parse(String rawKey) {
        if (rawKey == null || rawKey.isBlank()) {
            return Optional.empty();
        }
        Matcher matcher = FULL_KEY_PATTERN.matcher(rawKey.trim());
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return Optional.of(new ParsedWebhookKey(matcher.group(1), matcher.group(2)));
    }

    /** True when the candidate string starts with the {@code sqwh_} wire prefix. */
    public static boolean hasWebhookKeyPrefix(String value) {
        return value != null && value.trim().startsWith(WIRE_PREFIX);
    }

    /** True when the candidate string is a valid non-secret 8-char base62 {@code keyId}. */
    public static boolean isValidKeyId(String keyId) {
        return keyId != null && KEY_ID_PATTERN.matcher(keyId.trim()).matches();
    }

    /** Assemble a canonical wire key from a keyId and secret. */
    public static String format(String keyId, String secret) {
        return WIRE_PREFIX + keyId + "_" + secret;
    }

    /**
     * Safe 4-character non-secret prefix for structured security logs.
     * Never returns the full secret or anything beyond 4 chars of {@code keyId}.
     */
    public static String safeKeyIdPrefix(String keyId) {
        if (keyId == null || keyId.isBlank()) {
            return "unknown";
        }
        String trimmed = keyId.trim();
        return trimmed.length() <= 4 ? trimmed : trimmed.substring(0, 4);
    }

    /** Generate a random 8-character base62 non-secret {@code keyId}. */
    public static String generateKeyId() {
        char[] chars = new char[KEY_ID_LENGTH];
        for (int i = 0; i < KEY_ID_LENGTH; i++) {
            chars[i] = BASE62[RANDOM.nextInt(BASE62.length)];
        }
        return new String(chars);
    }

    /** Generate a 256-bit cryptographically random base64url secret (43 chars, no padding). */
    public static String generateSecret() {
        byte[] raw = new byte[SECRET_BYTES];
        RANDOM.nextBytes(raw);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
    }

    /** Generate a new {@code whk_<24-hex>} control-plane reference id. */
    public static String generateRefId() {
        byte[] bytes = new byte[12];
        RANDOM.nextBytes(bytes);
        char[] out = new char[24];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            out[i * 2] = HEX[v >>> 4];
            out[i * 2 + 1] = HEX[v & 0x0F];
        }
        return REF_ID_PREFIX + new String(out);
    }
}
