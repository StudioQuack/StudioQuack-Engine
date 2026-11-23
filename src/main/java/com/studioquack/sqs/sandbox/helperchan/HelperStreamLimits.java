package com.studioquack.sqs.sandbox.helperchan;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Bounds for helper-channel SSE / emit-partial frames. The queue cap is a catastrophic DoS guard
 * (a hostile sandbox must not pin unbounded daemon memory), sized orders of magnitude beyond any
 * real turn so it never fires in practice.
 */
public final class HelperStreamLimits {

    /** Max UTF-8 bytes of a single wire frame; larger deltas are split into chunks. */
    public static final int MAX_DELTA_BYTES = 256 * 1024;

    /**
     * Max queued partial frames per parent execute/stream job; drop-oldest past this.
     * Catastrophic guard only (~87 min of a fully stalled 50 tok/s stream).
     */
    public static final int MAX_QUEUED_FRAMES = 262_144;

    private HelperStreamLimits() {
    }

    /**
     * Splits {@code delta} into frames of at most {@link #MAX_DELTA_BYTES} UTF-8 bytes, cutting only
     * at code-point boundaries. Content is preserved exactly and in order; the receiver appends the
     * chunks by seq.
     */
    public static List<String> splitDelta(String delta) {
        List<String> out = new ArrayList<>();
        if (delta == null || delta.isEmpty()) {
            out.add("");
            return out;
        }
        int start = 0;
        int bytes = 0;
        for (int i = 0; i < delta.length(); i++) {
            char c = delta.charAt(i);
            if (Character.isLowSurrogate(c) && Character.isHighSurrogate(delta.charAt(i - 1))) {
                // Counted atomically with its high surrogate — a cut therefore never
                // lands inside a surrogate pair.
                continue;
            }
            final int charBytes;
            if (Character.isHighSurrogate(c)
                    && i + 1 < delta.length() && Character.isLowSurrogate(delta.charAt(i + 1))) {
                charBytes = 4;
            } else if (c < 0x80) {
                charBytes = 1;
            } else if (c < 0x800) {
                charBytes = 2;
            } else {
                charBytes = 3;
            }
            if (bytes + charBytes > MAX_DELTA_BYTES && i > start) {
                out.add(delta.substring(start, i));
                start = i;
                bytes = 0;
            }
            bytes += charBytes;
        }
        out.add(delta.substring(start));
        return out;
    }
}
