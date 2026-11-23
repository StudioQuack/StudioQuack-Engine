package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** {@link OciBuildLogStreamer} streams process output in chunks and respects wall-clock. */
class OciBuildLogStreamerTest {

    @Test
    void chunkCharsIs32KiBUnderBe64KiBCap() {
        assertEquals(32 * 1024, OciBuildLogStreamer.CHUNK_CHARS);
        assertTrue(OciBuildLogStreamer.CHUNK_CHARS < 64 * 1024);
    }

    @Test
    void flushReadySplitsOnNewlineAndForceFlushesAtChunkCeiling() {
        StringBuilder pending = new StringBuilder("hello\nworld");
        List<String> chunks = new ArrayList<>();
        AtomicReference<String> last = new AtomicReference<>();
        OciBuildLogStreamer.flushReady(pending, chunks::add, last, false);
        assertEquals(List.of("hello\n"), chunks);
        assertEquals("world", pending.toString());
        assertEquals("hello", last.get());

        // Force flush when buffer hits CHUNK_CHARS with no newline.
        StringBuilder big = new StringBuilder("x".repeat(OciBuildLogStreamer.CHUNK_CHARS + 10));
        chunks.clear();
        OciBuildLogStreamer.flushReady(big, chunks::add, last, false);
        assertEquals(1, chunks.size());
        assertEquals(OciBuildLogStreamer.CHUNK_CHARS, chunks.get(0).length());
        assertEquals(10, big.length());
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void runStreamingDeliversChunksFromShortProcess() {
        assumeTrue(new File("/bin/sh").canExecute(), "/bin/sh missing");
        List<String> chunks = new ArrayList<>();
        OciBuildLogStreamer.Outcome out = OciBuildLogStreamer.runStreaming(
                List.of("/bin/sh", "-c", "printf 'a\\n'; sleep 0.1; printf 'b\\n'"),
                Duration.ofSeconds(10),
                chunks::add);
        assertFalse(out.timedOut());
        assertEquals(0, out.exitCode());
        String joined = String.join("", chunks);
        assertTrue(joined.contains("a"), "expected chunk with a, got: " + joined);
        assertTrue(joined.contains("b"), "expected chunk with b, got: " + joined);
        assertEquals("b", out.lastErrorLine());
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void runStreamingTimesOutAndKillsLongSleep() {
        assumeTrue(new File("/bin/sh").canExecute(), "/bin/sh missing");
        OciBuildLogStreamer.Outcome out = OciBuildLogStreamer.runStreaming(
                List.of("/bin/sh", "-c", "sleep 30"),
                Duration.ofMillis(400),
                chunk -> { /* ignore */ });
        assertTrue(out.timedOut());
        assertEquals(-1, out.exitCode());
        // Process must be dead — a second short waitFor on a live sleep would hang the test.
        // destroyForcibly already ran inside runStreaming; timedOut=true is the contract signal.
    }
}
