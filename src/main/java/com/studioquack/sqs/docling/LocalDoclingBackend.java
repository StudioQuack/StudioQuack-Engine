package com.studioquack.sqs.docling;

import com.studioquack.common.dto.flow.ProcessContextRef;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Document bytes are converted by the local Docling side-car and stay on this host. Whether a
 * Docling node may run at all is a runtime decision, not a boot one: the per-environment toggle
 * arrives with the entitlement snapshot and is checked in {@code DoclingDelegate}.
 */
@Slf4j
@Component
public class LocalDoclingBackend implements DoclingBackend {

    private final DoclingClient doclingClient;
    private final DoclingContainer doclingContainer;
    private final DoclingImagePreprocessor imagePreprocessor;

    public LocalDoclingBackend(DoclingClient doclingClient,
                               DoclingContainer doclingContainer,
                               DoclingImagePreprocessor imagePreprocessor) {
        this.doclingClient = doclingClient;
        this.doclingContainer = doclingContainer;
        this.imagePreprocessor = imagePreprocessor;
        log.info("docling: local side-car backend active — data stays in-env, target={}",
                doclingClient.submitUrl());
    }

    @Override
    public String convert(byte[] bytes, String filename, String mimeType, ProcessContextRef ref) {
        return convert(bytes, filename, mimeType, ref, null);
    }

    @Override
    public String convert(byte[] bytes, String filename, String mimeType, ProcessContextRef ref,
                          String outputFormat) {
        // Brought up lazily: the first conversion in an environment may include an image pull, and
        // a failure here surfaces as a named node error rather than an empty result.
        doclingContainer.ensureRunning();
        // Downscale oversized bitmaps before POST — OCR cost scales with pixels; preprocessor is
        // non-throwing so a bad image still reaches convert with the original bytes.
        DoclingImagePreprocessor.PreparedDocument prepared =
                imagePreprocessor.prepare(bytes, filename, mimeType);
        // Data-local: the conversion runs against the in-env side-car; nothing is sent to the control
        // plane.
        return doclingClient.convert(prepared.bytes(), prepared.filename(), prepared.mimeType(),
                outputFormat);
    }
}
