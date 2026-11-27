package com.studioquack.sqs.webhook;

import com.studioquack.common.dto.webhook.WebhookKeyFormat;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Optional;

/**
 * Engine-local zero-knowledge webhook key store.
 *
 * <p>Generates 256-bit webhook secrets locally, persists only their SHA-256 digests, and verifies
 * presented secrets in constant time using {@link MessageDigest#isEqual(byte[], byte[])}. Plaintext
 * {@code rawKey} and {@code secret} values are NEVER logged and NEVER persisted.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DaemonWebhookKeyStore {

    public static final String AUTH_MODE_URL_KEY = "URL_KEY";
    public static final String AUTH_MODE_HEADER_KEY = "HEADER_KEY";
    public static final String AUTH_MODE_KEY_ID_ONLY = "KEY_ID_ONLY";

    private static final int MAX_KEY_ID_ATTEMPTS = 8;

    private final DaemonWebhookKeyRepository repository;

    /** One-time key material returned immediately to the sealed control-plane response. */
    public record CreatedKey(String refId, String keyId, String rawKey) {
    }

    /** Constant-time verification verdict for an inbound webhook request. */
    public record VerificationResult(
            boolean valid,
            String refId,
            String keyId,
            boolean allowKeyIdRoute,
            String failureReason) {

        public static VerificationResult ok(String refId, String keyId, boolean allowKeyIdRoute) {
            return new VerificationResult(true, refId, keyId, allowKeyIdRoute, null);
        }

        public static VerificationResult denied(String reason) {
            return new VerificationResult(false, null, null, false, reason);
        }
    }

    /**
     * Generates a fresh {@code sqwh_<8-char-base62>_<43-char-base64url>} webhook key and stores
     * only its non-secret metadata + SHA-256 digest under {@code refId}.
     */
    @Transactional
    public CreatedKey create(String refId, String name, boolean allowKeyIdRoute) {
        String normalizedRefId = requireRefId(refId);
        if (repository.existsById(normalizedRefId)) {
            throw new IllegalStateException("webhook key refId already exists: " + normalizedRefId);
        }
        String keyId = allocateUniqueKeyId();
        String secret = WebhookKeyFormat.generateSecret();
        String digestHex = sha256Hex(secret);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        DaemonWebhookKeyEntity entity = DaemonWebhookKeyEntity.builder()
                .refId(normalizedRefId)
                .keyId(keyId)
                .secretSha256(digestHex)
                .name(StringUtils.trimToNull(name))
                .allowKeyIdRoute(allowKeyIdRoute)
                .status(DaemonWebhookKeyEntity.STATUS_ACTIVE)
                .createdAt(now)
                .build();
        repository.save(entity);

        log.info("webhook-key.created refId={} keyPrefix={} allowKeyIdRoute={}",
                normalizedRefId, WebhookKeyFormat.safeKeyIdPrefix(keyId), allowKeyIdRoute);
        return new CreatedKey(normalizedRefId, keyId, WebhookKeyFormat.format(keyId, secret));
    }

    /**
     * Atomically replaces the {@code (keyId, secret)} pair for an active {@code refId} so bound
     * workflows keep referencing the same {@code $webhookKeyRef} without re-editing nodes.
     */
    @Transactional
    public CreatedKey rotate(String refId, String previousKeyId) {
        String normalizedRefId = requireRefId(refId);
        DaemonWebhookKeyEntity entity = repository.findById(normalizedRefId)
                .orElseThrow(() -> new IllegalArgumentException("unknown webhook key refId: " + normalizedRefId));
        if (!DaemonWebhookKeyEntity.STATUS_ACTIVE.equalsIgnoreCase(entity.getStatus())) {
            throw new IllegalStateException("cannot rotate revoked webhook key refId: " + normalizedRefId);
        }
        String prev = StringUtils.trimToNull(previousKeyId);
        if (prev != null && !prev.equals(entity.getKeyId())) {
            log.info("webhook-key.rotate previousKeyId mismatch refId={} expectedPrefix={} gotPrefix={}",
                    normalizedRefId,
                    WebhookKeyFormat.safeKeyIdPrefix(entity.getKeyId()),
                    WebhookKeyFormat.safeKeyIdPrefix(prev));
        }

        String newKeyId = allocateUniqueKeyId();
        String newSecret = WebhookKeyFormat.generateSecret();
        entity.setKeyId(newKeyId);
        entity.setSecretSha256(sha256Hex(newSecret));
        entity.setRotatedAt(OffsetDateTime.now(ZoneOffset.UTC));
        repository.save(entity);

        log.info("webhook-key.rotated refId={} newKeyPrefix={}",
                normalizedRefId, WebhookKeyFormat.safeKeyIdPrefix(newKeyId));
        return new CreatedKey(normalizedRefId, newKeyId, WebhookKeyFormat.format(newKeyId, newSecret));
    }

    /**
     * Idempotently marks {@code refId} as {@code REVOKED} and wipes its stored secret digest.
     * Succeeds cleanly when already revoked or absent.
     */
    @Transactional
    public boolean revoke(String refId, String keyId) {
        String normalizedRefId = StringUtils.trimToNull(refId);
        String normalizedKeyId = StringUtils.trimToNull(keyId);
        Optional<DaemonWebhookKeyEntity> found = Optional.empty();
        if (normalizedRefId != null) {
            found = repository.findById(normalizedRefId);
        }
        if (found.isEmpty() && normalizedKeyId != null) {
            found = repository.findByKeyId(normalizedKeyId);
        }
        if (found.isEmpty()) {
            log.info("webhook-key.revoke.noop refId={} keyPrefix={} (absent)",
                    normalizedRefId, WebhookKeyFormat.safeKeyIdPrefix(normalizedKeyId));
            return true;
        }
        DaemonWebhookKeyEntity entity = found.get();
        entity.setStatus(DaemonWebhookKeyEntity.STATUS_REVOKED);
        entity.setSecretSha256(null);
        if (entity.getRevokedAt() == null) {
            entity.setRevokedAt(OffsetDateTime.now(ZoneOffset.UTC));
        }
        repository.save(entity);
        log.info("webhook-key.revoked refId={} keyPrefix={}",
                entity.getRefId(), WebhookKeyFormat.safeKeyIdPrefix(entity.getKeyId()));
        return true;
    }

    /**
     * Constant-time verification of an inbound webhook presentation against local key custody.
     *
     * <p>When {@code authMode} is {@code KEY_ID_ONLY}, succeeds iff the key is {@code ACTIVE} and
     * {@code allowKeyIdRoute == true}. For {@code URL_KEY} / {@code HEADER_KEY}, requires a
     * non-blank {@code presentedSecret} whose SHA-256 digest equals the stored digest under
     * {@link MessageDigest#isEqual(byte[], byte[])}.
     */
    @Transactional(readOnly = true)
    public VerificationResult verify(String refId, String keyId, String authMode, String presentedSecret) {
        String normalizedRefId = StringUtils.trimToNull(refId);
        String normalizedKeyId = StringUtils.trimToNull(keyId);
        Optional<DaemonWebhookKeyEntity> found = Optional.empty();
        if (normalizedRefId != null) {
            found = repository.findById(normalizedRefId);
        } else if (normalizedKeyId != null) {
            found = repository.findByKeyId(normalizedKeyId);
        }
        if (found.isEmpty()) {
            return VerificationResult.denied("unknown_webhook_key");
        }
        DaemonWebhookKeyEntity entity = found.get();
        if (!DaemonWebhookKeyEntity.STATUS_ACTIVE.equalsIgnoreCase(entity.getStatus())) {
            return VerificationResult.denied("revoked_webhook_key");
        }
        if (normalizedKeyId != null && !normalizedKeyId.equals(entity.getKeyId())) {
            return VerificationResult.denied("key_id_mismatch");
        }
        if (normalizedRefId != null && !normalizedRefId.equals(entity.getRefId())) {
            return VerificationResult.denied("ref_id_mismatch");
        }

        if (AUTH_MODE_KEY_ID_ONLY.equalsIgnoreCase(StringUtils.trimToEmpty(authMode))) {
            if (!entity.isAllowKeyIdRoute()) {
                return VerificationResult.denied("key_id_route_not_allowed");
            }
            return VerificationResult.ok(entity.getRefId(), entity.getKeyId(), true);
        }

        if (StringUtils.isBlank(presentedSecret) || StringUtils.isBlank(entity.getSecretSha256())) {
            return VerificationResult.denied("invalid_webhook_secret");
        }
        byte[] expectedDigest;
        try {
            expectedDigest = HexFormat.of().parseHex(entity.getSecretSha256());
        } catch (IllegalArgumentException badHex) {
            return VerificationResult.denied("corrupt_key_digest");
        }
        byte[] presentedDigest = sha256Bytes(presentedSecret);
        if (!MessageDigest.isEqual(expectedDigest, presentedDigest)) {
            return VerificationResult.denied("invalid_webhook_secret");
        }
        return VerificationResult.ok(entity.getRefId(), entity.getKeyId(), entity.isAllowKeyIdRoute());
    }

    @Transactional(readOnly = true)
    public Optional<DaemonWebhookKeyEntity> findByRefId(String refId) {
        return StringUtils.isBlank(refId) ? Optional.empty() : repository.findById(refId.trim());
    }

    private String allocateUniqueKeyId() {
        for (int attempt = 0; attempt < MAX_KEY_ID_ATTEMPTS; attempt++) {
            String candidate = WebhookKeyFormat.generateKeyId();
            if (repository.findByKeyId(candidate).isEmpty()) {
                return candidate;
            }
        }
        throw new IllegalStateException("failed to allocate unique webhook keyId after " + MAX_KEY_ID_ATTEMPTS + " attempts");
    }

    private static String requireRefId(String refId) {
        String trimmed = StringUtils.trimToNull(refId);
        if (trimmed == null) {
            throw new IllegalArgumentException("refId is required");
        }
        return trimmed;
    }

    public static byte[] sha256Bytes(String secret) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(secret.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public static String sha256Hex(String secret) {
        return HexFormat.of().formatHex(sha256Bytes(secret));
    }
}
