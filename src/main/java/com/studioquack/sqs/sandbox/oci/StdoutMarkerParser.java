package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the legacy {@code portValue:<port>:::result:<value>} stdout markers. This is the fallback
 * per-port output channel for when the modern helper-channel POST does not arrive — e.g. a {@code
 * --network none} job (no socket back to the daemon), a warm slot on a runtime whose loopback
 * can't reach the host, or the operator-pinned strict loopback bind.
 */
public final class StdoutMarkerParser {

    private static final String PORT_PREFIX = "portValue:";
    private static final String RESULT_SEP = ":::result:";
    private static final String EVENT_PREFIX = "sqEvent:::";
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final TypeReference<Map<String, Object>> EVENT_TYPE = new TypeReference<>() {
    };

    private StdoutMarkerParser() {
    }

    /**
     * Scans {@code stdout} for the marker lines and returns a {@code port -> rawValue} map (empty
     * when no markers are present). Values that legitimately contain newlines are JSON-encoded by the
     * SDK before printing, so the line scan recovers the full payload without false splits.
     */
    public static Map<String, String> parse(String stdout) {
        Map<String, String> out = new LinkedHashMap<>();
        if (stdout == null || stdout.isEmpty()) {
            return out;
        }
        for (String rawLine : stdout.split("\n", -1)) {
            // Trim a trailing CR for runtimes that emit \r\n through stdout.
            String line = rawLine.endsWith("\r") ? rawLine.substring(0, rawLine.length() - 1) : rawLine;
            if (!line.startsWith(PORT_PREFIX)) {
                continue;
            }
            String rest = line.substring(PORT_PREFIX.length());
            int idx = rest.indexOf(RESULT_SEP);
            if (idx < 0) {
                continue;
            }
            String port = rest.substring(0, idx).trim();
            if (port.isEmpty()) {
                continue;
            }
            out.put(port, rest.substring(idx + RESULT_SEP.length()));
        }
        return out;
    }

    /**
     * Scans {@code stdout} for structured event marker lines and returns valid JSON-object events in
     * arrival order. Malformed event markers are ignored so ordinary sandbox output cannot fail a run.
     */
    public static List<Map<String, Object>> events(String stdout) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (stdout == null || stdout.isEmpty()) {
            return out;
        }
        for (String rawLine : stdout.split("\n", -1)) {
            String line = rawLine.endsWith("\r") ? rawLine.substring(0, rawLine.length() - 1) : rawLine;
            if (!line.startsWith(EVENT_PREFIX)) {
                continue;
            }
            try {
                Map<String, Object> event = MAPPER.readValue(line.substring(EVENT_PREFIX.length()), EVENT_TYPE);
                if (event != null) {
                    out.add(event);
                }
            } catch (Exception ignored) {
                // Ignore bad markers from user code; valid events later in stdout should still flow.
            }
        }
        return out;
    }
}
