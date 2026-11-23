package com.studioquack.sqs.files;

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
 * One file held by the daemon, whatever its origin. Every file the client touches lands here — a
 * Duckbot attachment, a webhook upload relayed as base64, a node's deploy-time {@code file}
 * parameter, and any single parameter value that outgrew the 1 MB inline budget.
 */
@Entity
@Table(name = "client_file", indexes = {
        // Cleanup is always "everything for this run" or "everything for this workflow's deployment".
        @Index(name = "idx_client_file_scope", columnList = "scope, scope_key"),
        @Index(name = "idx_client_file_created", columnList = "created_at")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClientFileEntity {

    /** The opaque handle, without the {@code sq-file:} prefix that appears in parameter values. */
    @Id
    @Column(name = "id", length = 64, nullable = false)
    private String id;

    /** {@code RUN} or {@code DEPLOY} — decides when the row is reclaimed. */
    @Column(name = "scope", length = 16, nullable = false)
    private String scope;

    /** The {@code workflowExecutionId} for RUN scope, the {@code workflowId} for DEPLOY scope. */
    @Column(name = "scope_key", length = 190, nullable = false)
    private String scopeKey;

    /** Sanitisation happens on write, not read: this value is later. */
    @Column(name = "file_name", length = 255, nullable = false)
    private String fileName;

    @Column(name = "mime_type", length = 190)
    private String mimeType;

    /** Plaintext length in bytes, recorded before sealing so limits can be enforced without decrypting. */
    @Column(name = "size_bytes", nullable = false)
    private long sizeBytes;

    /** The AES-GCM envelope as JSON text, exactly as {@code execution_context} stores its blobs. */
    @Column(name = "content_envelope", columnDefinition = "text")
    private String contentEnvelope;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;
}
