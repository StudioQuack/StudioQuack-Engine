package com.studioquack.sqs.webhook;

import com.studioquack.common.dto.webhook.EngineApiKeyScope;
import com.studioquack.common.dto.webhook.WebhookKeyFormat;
import com.studioquack.common.dto.webhook.WebhookKeyRefHelper;
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
 * Engine-local zero-knowledge API key store covering both {@link EngineApiKeyScope#ENGINE_WIDE}
 * and {@link EngineApiKeyScope#NODE_SCOPED} keys.
 *
 * <p>Generates 256-bit secrets locally ({@code sqk_<8-char-base62-keyId>_<43-char-base64url-secret>}),
 * persists only their SHA-256 digests alongside scope and status metadata, and verifies presented
 * secrets in constant time using {@link MessageDigest#isEqual(byte[], byte[])}. Plaintext
 * {@code rawKey} and {@code secret} values are NEVER logged and NEVER persisted.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DaemonWebhookKeyStore {

    public static final String AUTH_MODE_URL_KEY = "URL_KEY";
    public static final String AUTH_MODE_HEADER_KEY = "HEADER_KEY";

    private static final int MAX_KEY_ID_ATTEMPTS = 8;

    private final DaemonWebhookKeyRepository repository;

    /** One-time key material returned immediately to the sealed control-plane response. */
    public record CreatedKey(String refId, String keyId, String rawKey) {
    }

    /** Constant-time verification verdict for an inbound API / webhook key presentation. */
    public record VerificationResult(
            boolean valid,
            String refId,
            String keyId,
            EngineApiKeyScope keyScope,
            String failureReason) {

        public static VerificationResult ok(String refId,
                                            String keyId,
                                            EngineApiKeyScope keyScope) {
            return new VerificationResult(true, refId, keyId, keyScope, null);
        }

        public static VerificationResult denied(String reason) {
            return new VerificationResult(false, null, null, null, reason);
        }
    }

    /**
     * Generates a fresh {@code sqk_<8-char-base62>_<43-char-base64url>} engine API key and stores
     * only its non-secret metadata + SHA-256 digest under {@code refId}.
     */
    @Transactional
    public CreatedKey create(String refId,
                             String name,
                             EngineApiKeyScope keyScope) {
        String normalizedRefId = requireRefId(refId);
        if (repository.existsById(normalizedRefId)) {
            throw new IllegalStateException("engine API key refId already exists: " + normalizedRefId);
        }
        EngineApiKeyScope resolvedScope = keyScope == null ? EngineApiKeyScope.NODE_SCOPED : keyScope;

        String keyId = allocateUniqueKeyId();
        String secret = WebhookKeyFormat.generateSecret();
        String digestHex = sha256Hex(secret);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        DaemonWebhookKeyEntity entity = DaemonWebhookKeyEntity.builder()
                .refId(normalizedRefId)
                .keyId(keyId)
                .secretSha256(digestHex)
                .name(StringUtils.trimToNull(name))
                .keyScope(resolvedScope)
                .status(DaemonWebhookKeyEntity.STATUS_ACTIVE)
                .createdAt(now)
                .build();
        repository.save(entity);

        log.info("engine-api-key.created refId={} keyPrefix={} scope={}",
                normalizedRefId, WebhookKeyFormat.safeKeyIdPrefix(keyId), resolvedScope);
        return new CreatedKey(normalizedRefId, keyId, WebhookKeyFormat.format(keyId, secret));
    }

    /**
     * Atomically replaces the {@code (keyId, secret)} pair for an active {@code refId} while
     * preserving its {@code keyScope}, so bound workflows keep referencing the same {@code refId}
     * without re-editing nodes.
     */
    @Transactional
    public CreatedKey rotate(String refId, String previousKeyId) {
        String normalizedRefId = requireRefId(refId);
        DaemonWebhookKeyEntity entity = repository.findById(normalizedRefId)
                .orElseThrow(() -> new IllegalArgumentException("unknown engine API key refId: " + normalizedRefId));
        if (!DaemonWebhookKeyEntity.STATUS_ACTIVE.equalsIgnoreCase(entity.getStatus())) {
            throw new IllegalStateException("cannot rotate revoked engine API key refId: " + normalizedRefId);
        }
        String prev = StringUtils.trimToNull(previousKeyId);
        if (prev != null && !prev.equals(entity.getKeyId())) {
            log.info("engine-api-key.rotate previousKeyId mismatch refId={} expectedPrefix={} gotPrefix={}",
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

        log.info("engine-api-key.rotated refId={} newKeyPrefix={} scope={}",
                normalizedRefId, WebhookKeyFormat.safeKeyIdPrefix(newKeyId), entity.effectiveKeyScope());
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
            log.info("engine-api-key.revoke.noop refId={} keyPrefix={} (absent)",
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
        log.info("engine-api-key.revoked refId={} keyPrefix={} scope={}",
                entity.getRefId(), WebhookKeyFormat.safeKeyIdPrefix(entity.getKeyId()),
                entity.effectiveKeyScope());
        return true;
    }

    /**
     * Constant-time secret verification against local key custody with strict scope enforcement.
     *
     * <p>Verifies that the key exists, {@code status == ACTIVE}, {@code keyId}/{@code refId} match
     * when supplied, {@code entity.effectiveKeyScope() == expectedScope}, and the SHA-256 digest
     * of {@code presentedSecret} equals the stored digest under
     * {@link MessageDigest#isEqual(byte[], byte[])}.
     */
    @Transactional(readOnly = true)
    public VerificationResult verifySecret(String refId,
                                           String keyId,
                                           String presentedSecret,
                                           EngineApiKeyScope expectedScope) {
        if (expectedScope == null) {
            return VerificationResult.denied("missing_expected_scope");
        }
        Optional<DaemonWebhookKeyEntity> resolved = resolveAndCheckIdentity(refId, keyId);
        if (resolved.isEmpty()) {
            return deniedReasonForIdentity(refId, keyId);
        }
        DaemonWebhookKeyEntity entity = resolved.get();
        if (entity.effectiveKeyScope() != expectedScope) {
            return VerificationResult.denied("key_scope_mismatch");
        }
        if (StringUtils.isBlank(presentedSecret) || StringUtils.isBlank(entity.getSecretSha256())) {
            return VerificationResult.denied("invalid_api_secret");
        }
        byte[] expectedDigest;
        try {
            expectedDigest = HexFormat.of().parseHex(entity.getSecretSha256());
        } catch (IllegalArgumentException badHex) {
            return VerificationResult.denied("corrupt_key_digest");
        }
        byte[] presentedDigest = sha256Bytes(presentedSecret);
        if (!MessageDigest.isEqual(expectedDigest, presentedDigest)) {
            return VerificationResult.denied("invalid_api_secret");
        }
        return VerificationResult.ok(
                entity.getRefId(), entity.getKeyId(), entity.effectiveKeyScope());
    }

    /**
     * Verifies an inbound API / webhook key presentation ({@code URL_KEY} or {@code HEADER_KEY})
     * by requiring a valid non-blank {@code presentedSecret} matching the stored SHA-256 digest
     * in constant time while strictly enforcing {@code expectedScope}.
     */
    @Transactional(readOnly = true)
    public VerificationResult verify(String refId,
                                     String keyId,
                                     String authMode,
                                     String presentedSecret,
                                     EngineApiKeyScope expectedScope) {
        return verifySecret(refId, keyId, presentedSecret, expectedScope);
    }

    @Transactional(readOnly = true)
    public Optional<DaemonWebhookKeyEntity> findByRefId(String refId) {
        return StringUtils.isBlank(refId) ? Optional.empty() : repository.findById(refId.trim());
    }

    private Optional<DaemonWebhookKeyEntity> resolveAndCheckIdentity(String refId, String keyId) {
        String normalizedRefId = StringUtils.trimToNull(refId);
        String normalizedKeyId = StringUtils.trimToNull(keyId);
        Optional<DaemonWebhookKeyEntity> found = Optional.empty();
        if (normalizedRefId != null) {
            found = repository.findById(normalizedRefId);
        } else if (normalizedKeyId != null) {
            found = repository.findByKeyId(normalizedKeyId);
        }
        if (found.isEmpty()) {
            return Optional.empty();
        }
        DaemonWebhookKeyEntity entity = found.get();
        if (!DaemonWebhookKeyEntity.STATUS_ACTIVE.equalsIgnoreCase(entity.getStatus())) {
            return Optional.empty();
        }
        if (normalizedKeyId != null && !normalizedKeyId.equals(entity.getKeyId())) {
            return Optional.empty();
        }
        if (normalizedRefId != null && !normalizedRefId.equals(entity.getRefId())) {
            return Optional.empty();
        }
        return Optional.of(entity);
    }

    private VerificationResult deniedReasonForIdentity(String refId, String keyId) {
        String normalizedRefId = StringUtils.trimToNull(refId);
        String normalizedKeyId = StringUtils.trimToNull(keyId);
        Optional<DaemonWebhookKeyEntity> found = Optional.empty();
        if (normalizedRefId != null) {
            found = repository.findById(normalizedRefId);
        } else if (normalizedKeyId != null) {
            found = repository.findByKeyId(normalizedKeyId);
        }
        if (found.isEmpty()) {
            return VerificationResult.denied("unknown_api_key");
        }
        DaemonWebhookKeyEntity entity = found.get();
        if (!DaemonWebhookKeyEntity.STATUS_ACTIVE.equalsIgnoreCase(entity.getStatus())) {
            return VerificationResult.denied("revoked_api_key");
        }
        if (normalizedKeyId != null && !normalizedKeyId.equals(entity.getKeyId())) {
            return VerificationResult.denied("key_id_mismatch");
        }
        return VerificationResult.denied("ref_id_mismatch");
    }

    private String allocateUniqueKeyId() {
        for (int attempt = 0; attempt < MAX_KEY_ID_ATTEMPTS; attempt++) {
            String candidate = WebhookKeyFormat.generateKeyId();
            if (repository.findByKeyId(candidate).isEmpty()) {
                return candidate;
            }
        }
        throw new IllegalStateException("failed to allocate unique engine API keyId after " + MAX_KEY_ID_ATTEMPTS + " attempts");
    }

    private static String requireRefId(String refId) {
        String trimmed = StringUtils.trimToNull(refId);
        if (trimmed == null || !WebhookKeyRefHelper.isValidRefId(trimmed)) {
            throw new IllegalArgumentException("valid refId is required");
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
