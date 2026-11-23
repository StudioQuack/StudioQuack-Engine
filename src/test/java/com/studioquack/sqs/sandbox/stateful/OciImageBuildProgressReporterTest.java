package com.studioquack.sqs.sandbox.stateful;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OciImageBuildProgressReporter} helpers (URL composition + daemon-side
 * truncation). HTTP transport is not mocked; best-effort POST path stays integration.
 */
class OciImageBuildProgressReporterTest {

    @Test
    void buildProgressUrlStripsTrailingSlashAndUsesDaemonCallbackPath() {
        assertEquals(
                "https://be.example.com/be/v1/sqvm/environments/env-9/oci-images/build-progress",
                OciImageBuildProgressReporter.buildProgressUrl("https://be.example.com/", "env-9"));
        assertEquals(
                "https://be.example.com/be/v1/sqvm/environments/env-9/oci-images/build-progress",
                OciImageBuildProgressReporter.buildProgressUrl("https://be.example.com", "env-9"));
    }

    @Test
    void truncateCapsLogChunkAt64KiB() {
        String huge = "x".repeat(OciImageBuildProgressReporter.LOG_CHUNK_MAX_CHARS + 50);
        String capped = OciImageBuildProgressReporter.truncate(
                huge, OciImageBuildProgressReporter.LOG_CHUNK_MAX_CHARS);
        assertEquals(OciImageBuildProgressReporter.LOG_CHUNK_MAX_CHARS, capped.length());
        assertEquals(65_536, OciImageBuildProgressReporter.LOG_CHUNK_MAX_CHARS);
    }

    @Test
    void truncateCapsMessageAt512AndDropsBlank() {
        String msg = "m".repeat(600);
        String capped = OciImageBuildProgressReporter.truncate(
                msg, OciImageBuildProgressReporter.MESSAGE_MAX_CHARS);
        assertEquals(512, capped.length());
        assertNull(OciImageBuildProgressReporter.truncate(null, 512));
        assertNull(OciImageBuildProgressReporter.truncate("   ", 512));
        assertTrue(OciImageBuildProgressReporter.truncate("ok", 512).equals("ok"));
    }
}
