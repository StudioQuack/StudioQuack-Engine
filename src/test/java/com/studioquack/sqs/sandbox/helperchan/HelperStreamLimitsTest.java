package com.studioquack.sqs.sandbox.helperchan;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HelperStreamLimitsTest {

    @Test
    void shortDeltaStaysOneFrame() {
        assertEquals(List.of("hello"), HelperStreamLimits.splitDelta("hello"));
        assertEquals(List.of(""), HelperStreamLimits.splitDelta(""));
        assertEquals(List.of(""), HelperStreamLimits.splitDelta(null));
    }

    @Test
    void oversizeDeltaIsChunkedNotTruncated() {
        String huge = "x".repeat(HelperStreamLimits.MAX_DELTA_BYTES * 2 + 100);
        List<String> chunks = HelperStreamLimits.splitDelta(huge);
        assertEquals(3, chunks.size());
        // Every frame fits the transport unit and the concatenation is the exact input.
        for (String chunk : chunks) {
            assertTrue(chunk.getBytes(StandardCharsets.UTF_8).length <= HelperStreamLimits.MAX_DELTA_BYTES);
        }
        assertEquals(huge, String.join("", chunks));
    }

    @Test
    void chunkingNeverSplitsAMultiByteCodePoint() {
        // 'é' is 2 UTF-8 bytes: place it straddling the frame boundary.
        String prefix = "a".repeat(HelperStreamLimits.MAX_DELTA_BYTES - 1);
        String input = prefix + "é" + "tail";
        List<String> chunks = HelperStreamLimits.splitDelta(input);
        assertEquals(2, chunks.size());
        for (String chunk : chunks) {
            assertTrue(chunk.getBytes(StandardCharsets.UTF_8).length <= HelperStreamLimits.MAX_DELTA_BYTES);
        }
        assertEquals(input, String.join("", chunks));
    }

    @Test
    void chunkingNeverSplitsASurrogatePair() {
        // Emoji (4 UTF-8 bytes, 2 chars) straddling the boundary must stay in one frame.
        String prefix = "a".repeat(HelperStreamLimits.MAX_DELTA_BYTES - 2);
        String input = prefix + "😀" + "tail";
        List<String> chunks = HelperStreamLimits.splitDelta(input);
        assertEquals(2, chunks.size());
        for (String chunk : chunks) {
            assertTrue(chunk.getBytes(StandardCharsets.UTF_8).length <= HelperStreamLimits.MAX_DELTA_BYTES);
            // No half-surrogate at either cut edge.
            assertFalse(chunk.isEmpty() || Character.isHighSurrogate(chunk.charAt(chunk.length() - 1)));
            assertFalse(chunk.isEmpty() || Character.isLowSurrogate(chunk.charAt(0)));
        }
        assertEquals(input, String.join("", chunks));
    }
}
