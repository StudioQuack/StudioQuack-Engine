package com.studioquack.sqs.docling;

import com.studioquack.common.dto.flow.ProcessContextRef;

/**
 * A {@code docling-converter} workflow node dispatches through the single {@link DoclingDelegate},
 * which delegates the actual document→markdown conversion to the one {@code DoclingBackend} bean
 * active on this deployment. Each tier runs its OWN local Docling side-car (a stateful Podman
 * instance managed by {@link DoclingContainer} through the daemon's stateful-instance control
 * plane).
 */
public interface DoclingBackend {

    /**
     * @param bytes Raw document bytes (never {@code null}; may be empty)
     * @param filename
     * @param mimeType Original media type; blank → the implementation substitutes
     * {@code application/octet-stream}
     * @param ref
     * @return The converted markdown/text (may be {@code null}/empty when the source was empty)
     * @throws Exception On any conversion failure; {@link DoclingDelegate} routes it to the error gate
     * via {@code DispatchResultCoordinator.deliverFailure} ({@code DOCLING_FAILED}) rather than
     * letting it dead-letter the async job
     */
    String convert(byte[] bytes, String filename, String mimeType, ProcessContextRef ref) throws Exception;

    /**
     * Converts with the node's {@code output_format} ({@code markdown}/{@code text}) so the side-car
     * is asked for the matching {@code to_formats}.
     */
    default String convert(byte[] bytes, String filename, String mimeType, ProcessContextRef ref,
                           String outputFormat) throws Exception {
        return convert(bytes, filename, mimeType, ref);
    }
}
