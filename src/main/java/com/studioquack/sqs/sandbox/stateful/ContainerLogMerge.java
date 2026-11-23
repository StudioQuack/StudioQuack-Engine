package com.studioquack.sqs.sandbox.stateful;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Rebuilds a container's interleaved console from {@code podman logs --timestamps}. Podman replays
 * the container's stdout on its own stdout and the container's stderr on its own stderr, so
 * reading only one stream silently drops the other — a JVM that reports its failure on stderr
 * looked like an empty log.
 */
public final class ContainerLogMerge {

    private ContainerLogMerge() {
    }

    private record Line(Instant at, String text) {
    }

    /**
     * Merges podman's stdout and stderr (both captured with {@code --timestamps}) into one text in
     * time order. Ties keep stdout first.
     */
    public static String merge(String stdout, String stderr) {
        List<Line> out = parse(stdout);
        List<Line> err = parse(stderr);
        StringBuilder sb = new StringBuilder();
        int i = 0;
        int j = 0;
        while (i < out.size() || j < err.size()) {
            boolean takeOut = j >= err.size()
                    || (i < out.size() && !out.get(i).at().isAfter(err.get(j).at()));
            Line next = takeOut ? out.get(i++) : err.get(j++);
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(next.text());
        }
        return sb.toString();
    }

    /** The line without its leading {@code --timestamps} prefix; unchanged when it has none. */
    public static String stripTimestamp(String line) {
        if (line == null) {
            return "";
        }
        int sp = line.indexOf(' ');
        if (sp <= 0) {
            return line;
        }
        try {
            OffsetDateTime.parse(line.substring(0, sp));
            return line.substring(sp + 1);
        } catch (Exception notATimestamp) {
            return line;
        }
    }

    private static List<Line> parse(String text) {
        List<Line> lines = new ArrayList<>();
        if (text == null || text.isBlank()) {
            return lines;
        }
        Instant last = Instant.EPOCH;
        for (String raw : text.split("\n")) {
            String line = raw.endsWith("\r") ? raw.substring(0, raw.length() - 1) : raw;
            if (line.isEmpty()) {
                continue;
            }
            int sp = line.indexOf(' ');
            if (sp > 0) {
                try {
                    last = OffsetDateTime.parse(line.substring(0, sp)).toInstant();
                } catch (Exception notATimestamp) {
                    // continuation line — keep the previous instant
                }
            }
            lines.add(new Line(last, line));
        }
        return lines;
    }
}
