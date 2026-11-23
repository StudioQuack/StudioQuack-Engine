package com.studioquack.sqs.secrets.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * Client-side inventory of the OS-store secrets THIS daemon committed. Secret plaintext lives in
 * the host OS keychain/libsecret under the {@code <servicePrefix>.secret} namespace (see
 * {@code SecretCommandHandlers}).
 */
@Entity
@Table(name = "client_owned_secret")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ClientOwnedSecretEntity {

    /** The {@code sec_<24hex>} id (never the versioned wire ref, never plaintext). */
    @Id
    @Column(name = "secret_id", length = 64, nullable = false)
    private String secretId;

    /** Highest version this daemon has committed for {@link #secretId} (1-based). */
    @Column(name = "max_version", nullable = false)
    private int maxVersion;

    /** When we last committed/rotated this secret; informational (audit / newest-first purge). */
    @Column(name = "committed_at", nullable = false)
    private OffsetDateTime committedAt;
}
