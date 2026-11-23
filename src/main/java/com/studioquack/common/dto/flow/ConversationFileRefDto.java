package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pointer to a chat attachment. Metadata rides on the message; bytes are
 * fetched separately, except on the inbound start-run payload where they travel inline.
 * {@code kind} is {@code image}, {@code document}, {@code audio}, or {@code video}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConversationFileRefDto {

    /** Stored file id (hex). Required to resolve the blob. */
    private String fileId;

    /** Original filename (best-effort) — surfaced as the download filename. */
    private String name;

    /** RFC 1521 media type, e.g. {@code image/png}, {@code application/pdf}. */
    private String mimeType;

    /** File size in bytes (after base64 decode for assistant files). */
    private long size;

    /**
     * One of {@code image | document | audio | video}. Drives the chat
     * renderer choice. Defaults to {@code document} when the producer has
     * no better hint than the MIME type.
     */
    private String kind;

    /**
     * ISO-8601 timestamp string. Kept as String instead of Instant because
     * ExecutionContext is serialized through the engine and rehydrated in several
     * places with local ObjectMapper instances that do not all register
     * jackson-datatype-jsr310. String keeps the DTO transport-safe.
     */
    private String uploadedAt;

    /**
     * Base64 file bytes, present only on the inbound start-run payload.
     * Ingest copies them into the sealed file store and clears this field before the context is snapshotted.
     */
    private String contentBase64;

    /** Convenience for the pre-existing five-field construction, which is the storage-side shape. */
    public ConversationFileRefDto(String fileId, String name, String mimeType, long size,
                                  String kind, String uploadedAt) {
        this(fileId, name, mimeType, size, kind, uploadedAt, null);
    }
}
