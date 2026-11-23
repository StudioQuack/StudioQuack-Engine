package com.studioquack.sqs.files;

import java.util.Optional;

/**
 * Pulls a conversation attachment's bytes back from the control plane when the {@code start-run}
 * push omitted the inline content because the file was above the daemon-inline ceiling. The daemon
 * cannot read backend stored files directly, so this rides the enrolled secure channel with the
 * daemon bearer.
 */
public interface BackendConversationFileClient {

    /** The file's bytes and metadata, or empty when the fetch is unavailable, unauthorised or missing. */
    Optional<FetchedFile> fetch(String workflowExecutionId, String conversationId, String fileId);

    /** A fetched file: server-resolved name / MIME plus the raw content. */
    record FetchedFile(String name, String mimeType, byte[] content) { }
}
