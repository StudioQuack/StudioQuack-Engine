package com.studioquack.sqs.webhook;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * Daemon-local zero-knowledge webhook key custody record.
 *
 * <p>Only the SHA-256 digest of the 256-bit webhook secret is persisted (never the plaintext secret).
 * On H2 the backing database file is additionally AES-encrypted via keychain-managed credentials;
 * on managed Postgres deployments the table lives in the daemon's isolated database.
 */
@Entity
@Table(name = "daemon_webhook_keys", indexes = {
        @Index(name = "idx_daemon_whk_key_id", columnList = "key_id", unique = true)
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DaemonWebhookKeyEntity {

    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_REVOKED = "REVOKED";

    /** Control-plane reference id ({@code whk_<24hex>}). Primary key. */
    @Id
    @Column(name = "ref_id", length = 64, nullable = false)
    private String refId;

    /** Non-secret 8-char base62 lookup identifier embedded in {@code sqwh_<keyId>_<secret>}. */
    @Column(name = "key_id", length = 16, nullable = false, unique = true)
    private String keyId;

    /** Lowercase hex SHA-256 digest of the secret (64 chars). Wiped to {@code null} on revoke. */
    @Column(name = "secret_sha256", length = 64)
    private String secretSha256;

    /** Optional human-readable key label (non-secret). */
    @Column(name = "name", length = 128)
    private String name;

    /** When {@code true}, permits {@code KEY_ID_ONLY} routing where node code verifies provider auth. */
    @Column(name = "allow_key_id_route", nullable = false)
    private boolean allowKeyIdRoute;

    /** {@link #STATUS_ACTIVE} or {@link #STATUS_REVOKED}. */
    @Column(name = "status", length = 32, nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "rotated_at")
    private OffsetDateTime rotatedAt;

    @Column(name = "revoked_at")
    private OffsetDateTime revokedAt;
}
