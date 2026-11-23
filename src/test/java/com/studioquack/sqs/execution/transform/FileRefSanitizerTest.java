package com.studioquack.sqs.execution.transform;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Deny-path for transform bake: ContentBlocks with an {@code sq-file:} handle must never
 * carry inline {@code data}/{@code content}/{@code base64} into JOLT or edge-script pre-step.
 */
class FileRefSanitizerTest {

    @Test
    void stripInlineFileBytes_drops_payload_keys_keeps_sq_file_ref() {
        String ref = "sq-file:deny-path-1";
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "document");
        block.put("mime_type", "application/pdf");
        block.put("source", "base64");
        block.put("name", "report.pdf");
        block.put("fileId", ref);
        block.put("data", "JVBERi0xLjQK" + "A".repeat(64));
        block.put("content", "should-go");
        block.put("base64", "should-go-too");

        Object out = FileRefSanitizer.stripInlineFileBytes(block);

        @SuppressWarnings("unchecked")
        Map<String, Object> got = (Map<String, Object>) out;
        assertEquals(ref, got.get("fileId"));
        assertEquals("sq-file", got.get("source"));
        assertEquals("report.pdf", got.get("name"));
        assertEquals("application/pdf", got.get("mime_type"));
        assertNull(got.get("data"));
        assertNull(got.get("content"));
        assertNull(got.get("base64"));
        assertFalse(got.containsKey("b64"));
        assertFalse(got.containsKey("bytes"));
    }

    /**
     * The parallel-branch voice defect: a DuckBot voice clip can arrive as {@code source=sq-file}
     * with inline base64 but NO {@code fileId} handle. Treating the source label alone as a ref
     * stripped {@code data} in place and left the block with neither bytes nor a handle — the
     * transformed branch saw an empty voice while the unwired branch kept the content.
     */
    @Test
    void stripInlineFileBytes_keeps_inline_bytes_when_source_sq_file_has_no_handle() {
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "audio");
        block.put("mime_type", "audio/webm");
        block.put("source", "sq-file");
        block.put("name", "user prompt by voice command.webm");
        block.put("data", "GkXfowE" + "A".repeat(64));

        Object out = FileRefSanitizer.stripInlineFileBytes(block);

        @SuppressWarnings("unchecked")
        Map<String, Object> got = (Map<String, Object>) out;
        assertEquals("GkXfowE" + "A".repeat(64), got.get("data"),
                "handle-less sq-file block keeps its inline bytes — they are the only copy");
        assertEquals("sq-file", got.get("source"));
        assertEquals("user prompt by voice command.webm", got.get("name"));
    }

    /** A handle hiding under a payload alias still counts as a ref: promote + strip. */
    @Test
    void stripInlineFileBytes_promotes_handle_hiding_under_data_key() {
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "document");
        block.put("name", "report.pdf");
        block.put("data", "sq-file:handle-under-data");

        Object out = FileRefSanitizer.stripInlineFileBytes(block);

        @SuppressWarnings("unchecked")
        Map<String, Object> got = (Map<String, Object>) out;
        assertEquals("sq-file:handle-under-data", got.get("fileId"));
        assertEquals("sq-file", got.get("source"));
        assertNull(got.get("data"));
    }
}
