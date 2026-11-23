package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Builds helper-channel SSE frames matching the in-sandbox SDK {@code _iter_sse} contract: {@code
 * event: partial|complete|error|done} plus JSON {@code data}. A serialization failure must never
 * silently become {@code {}} — that turns a multi-MB image-bearing {@code complete} frame into an
 * empty map and the AI agent then publishes an empty {@code main_output} with no error.
 */
@Slf4j
public final class HelperSse {

    public static final String CONTENT_TYPE = "text/event-stream";

    /** Depth guard for the sanitizer walk (agent envelopes nest a few levels). */
    private static final int MAX_SANITIZE_DEPTH = 32;

    private HelperSse() {
    }

    /**
     * Content-negotiation: {@code llm/*} streams only when Accept lists {@code text/event-stream}
     * (case-insensitive, may be one of several types). Absent / blank Accept is buffered — the Python
     * helper reads one JSON body.
     */
    public static boolean acceptsEventStream(String accept) {
        if (accept == null || accept.isBlank()) {
            return false;
        }
        return accept.toLowerCase(Locale.ROOT).contains(CONTENT_TYPE);
    }

    /** One SSE event. {@code data} is serialized JSON (objects/lists/scalars). */
    public static byte[] frame(ObjectMapper mapper, String event, Object data) {
        String json;
        try {
            json = mapper.writeValueAsString(data == null ? "" : data);
        } catch (Exception e) {
            // Loud, shape-only: event + payload type + Jackson reason class. Never the payload.
            log.warn("helper-sse: frame serialization failed event={} dataType={} err={} — retrying sanitized",
                    event, data == null ? "null" : data.getClass().getSimpleName(),
                    e.getClass().getSimpleName());
            try {
                json = mapper.writeValueAsString(sanitize(data, 0));
            } catch (Exception e2) {
                log.warn("helper-sse: sanitized serialization still failed event={} err={} — emitting {{}}",
                        event, e2.getClass().getSimpleName());
                json = "{}";
            }
        }
        StringBuilder sb = new StringBuilder(16 + event.length() + json.length());
        sb.append("event: ").append(event).append('\n');
        sb.append("data: ").append(json).append('\n');
        sb.append('\n');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Structural copy that keeps every JSON-safe value and replaces anything else
     * (POJO without serializers, raw byte streams, …) with a type marker, so one bad
     * leaf can never take down the whole frame.
     */
    private static Object sanitize(Object value, int depth) {
        if (value == null || depth > MAX_SANITIZE_DEPTH) {
            return null;
        }
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.put(String.valueOf(e.getKey()), sanitize(e.getValue(), depth + 1));
            }
            return out;
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object item : list) {
                out.add(sanitize(item, depth + 1));
            }
            return out;
        }
        if (value instanceof byte[] bytes) {
            return "<bytes:" + bytes.length + ">";
        }
        return "<unserializable:" + value.getClass().getSimpleName() + ">";
    }
}
