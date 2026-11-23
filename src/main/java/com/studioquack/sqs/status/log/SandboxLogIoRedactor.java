package com.studioquack.sqs.status.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deep-copies a sandbox-log I/O payload and strips oversized ContentBlock {@code data} so the log
 * path never mutates the live execution context. Extracted from {@code ScriptDispatchDelegate} so
 * the redact → {@link SandboxLogStore} → {@link SandboxLogRowMapper} chain is unit-testable
 * without reflection.
 */
@Slf4j
public final class SandboxLogIoRedactor {

    /**
     * Cap an inline {@code data} payload at the point where a single log POST would trip the control
     * plane body limit. The old 64 KB cap stripped a legitimate multi-MB base64 payload down to "
     * {@code <omitted N chars>}", which reached Duck Tales with no bytes to expand and no fileId to
     * download.
     */
    static final int MAX_INLINE_DATA_CHARS = 18_000_000;

    private SandboxLogIoRedactor() {
    }

    /**
     * Which I/O column is being copied onto the sandbox-log row. {@link #OUTPUT} may promote
     * {@code unfilteredValue} (the pre-schema-filter envelope the node produced).
     */
    public enum IoLane { INPUT, OUTPUT }

    /** Deep-copy the consumer's executed input — never promote producer {@code unfilteredValue}. */
    public static Object redactInput(ObjectMapper objectMapper, Object payload) {
        return redact(objectMapper, payload, IoLane.INPUT);
    }

    /** Deep-copy the node's produced output — promote {@code unfilteredValue} when present. */
    public static Object redactOutput(ObjectMapper objectMapper, Object payload) {
        return redact(objectMapper, payload, IoLane.OUTPUT);
    }

    /**
     * {@code null} stays {@code null}; a missing mapper returns the original reference. Never turns a
     * non-null payload into {@code null}.
     */
    static Object redact(ObjectMapper objectMapper, Object payload, IoLane lane) {
        if (payload == null || objectMapper == null) {
            return payload;
        }
        IoLane side = lane == null ? IoLane.OUTPUT : lane;
        // Output: schema-filter may have emptied {@code value} while {@code unfilteredValue}
        // still holds what the node produced. Input: {@code value} is already post-transform,
        // and the inherited producer envelope is dropped rather than shipped alongside it.
        Object projected = side == IoLane.OUTPUT ? projectUnfilteredIo(payload) : stripUnfilteredIo(payload);
        try {
            Object copy = objectMapper.convertValue(projected, Object.class);
            stripDataFields(copy);
            stripUnfilteredKey(copy);
            return copy;
        } catch (Exception e) {
            // Do not log the payload — it may hold PHI / file bytes. Class+message only.
            log.debug("sandbox-log[redact]: convertValue failed ({}); using defensive copy", e.toString());
            return fallbackCopy(objectMapper, projected, side);
        }
    }

    /**
     * Replaces each {@link InOutValueDto#getValue value} with
     * {@link InOutValueDto#getUnfilteredValue unfilteredValue} when the latter is present. The
     * internal field is then cleared so the log feed does not ship a second copy.
     */
    static Object projectUnfilteredIo(Object payload) {
        if (payload instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object item : list) {
                out.add(projectUnfilteredIo(item));
            }
            return out;
        }
        if (payload instanceof InOutValueDto io) {
            return projectDto(io);
        }
        if (payload instanceof Map<?, ?> map && map.containsKey("unfilteredValue")) {
            return projectMap(map);
        }
        return payload;
    }

    /**
     * Drops the {@code unfilteredValue} an inbound DTO inherited from the producer's fan-out, keeping
     * {@code value} — the post-transform payload the node executed with. The field is dropped on a
     * COPY: {@code DoclingDelegate.wireValue} still reads the live inbound envelope to recover a file
     * payload the producer's schema filter emptied.
     */
    static Object stripUnfilteredIo(Object payload) {
        if (payload instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object item : list) {
                out.add(stripUnfilteredIo(item));
            }
            return out;
        }
        if (payload instanceof InOutValueDto io) {
            if (io.getUnfilteredValue() == null) {
                return io;
            }
            InOutValueDto copy = io.clone();
            copy.setUnfilteredValue(null);
            return copy;
        }
        if (payload instanceof Map<?, ?> map && map.containsKey("unfilteredValue")) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                String key = String.valueOf(e.getKey());
                if (!"unfilteredValue".equals(key)) {
                    copy.put(key, e.getValue());
                }
            }
            return copy;
        }
        return payload;
    }

    private static InOutValueDto projectDto(InOutValueDto io) {
        InOutValueDto copy = io.clone();
        if (copy.getUnfilteredValue() != null) {
            copy.setValue(copy.getUnfilteredValue());
            copy.setUnfilteredValue(null);
        }
        return copy;
    }

    private static Map<String, Object> projectMap(Map<?, ?> map) {
        Map<String, Object> copy = new LinkedHashMap<>();
        Object unfiltered = map.get("unfilteredValue");
        for (Map.Entry<?, ?> e : map.entrySet()) {
            String key = String.valueOf(e.getKey());
            if ("unfilteredValue".equals(key)) {
                continue;
            }
            copy.put(key, e.getValue());
        }
        if (unfiltered != null) {
            copy.put("value", unfiltered);
        }
        return copy;
    }

    /** Structural copy (or truncated marker) so the COMPLETED-row column stays parseable JSON. */
    private static Object fallbackCopy(ObjectMapper objectMapper, Object payload, IoLane lane) {
        try {
            return structuralCopy(objectMapper, payload, newIdentitySet(), lane);
        } catch (Exception e) {
            log.debug("sandbox-log[redact]: defensive copy failed ({}); using truncated marker", e.toString());
            return truncatedMarker(estimateBytes(payload));
        }
    }

    private static Object structuralCopy(ObjectMapper mapper, Object node, Set<Object> seen, IoLane lane) {
        if (node == null) {
            return null;
        }
        if (node instanceof String || node instanceof Number || node instanceof Boolean
                || node instanceof Character) {
            return node;
        }
        if (node instanceof byte[] bytes) {
            return bytes.clone();
        }
        if (!seen.add(node)) {
            return truncatedMarker(0);
        }
        if (node instanceof InOutValueDto io) {
            return copyInOut(mapper, io, seen, lane);
        }
        if (node instanceof Map<?, ?> map) {
            Map<Object, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                Object k = e.getKey();
                Object v = e.getValue();
                if ("data".equals(k) && v instanceof String s && s.length() > MAX_INLINE_DATA_CHARS) {
                    copy.put(k, "<omitted " + s.length() + " chars>");
                } else {
                    copy.put(k, structuralCopy(mapper, v, seen, lane));
                }
            }
            return copy;
        }
        if (node instanceof Collection<?> col) {
            List<Object> copy = new ArrayList<>(col.size());
            for (Object el : col) {
                copy.add(structuralCopy(mapper, el, seen, lane));
            }
            return copy;
        }
        if (node.getClass().isArray()) {
            int len = Array.getLength(node);
            List<Object> copy = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                copy.add(structuralCopy(mapper, Array.get(node, i), seen, lane));
            }
            return copy;
        }
        try {
            Object converted = mapper.convertValue(node, Object.class);
            stripDataFields(converted);
            return converted;
        } catch (Exception e) {
            return truncatedMarker(estimateBytes(node));
        }
    }

    private static Map<String, Object> copyInOut(ObjectMapper mapper, InOutValueDto io, Set<Object> seen,
                                                 IoLane lane) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("elementInstanceKey", io.getElementInstanceKey());
        m.put("gateway", io.isGateway());
        m.put("targetPortId", io.getTargetPortId());
        m.put("targetPortValue", io.getTargetPortValue());
        m.put("targetNodeId", io.getTargetNodeId());
        m.put("nodeId", io.getNodeId());
        m.put("portId", io.getPortId());
        m.put("portValue", io.getPortValue());
        m.put("sourcePortType", io.getSourcePortType());
        m.put("sourceNodeLabel", io.getSourceNodeLabel());
        // Output: the pre-schema-filter envelope is what the node produced.
        // Input: {@code value} is the post-transform payload the node executed with.
        Object displayed = (lane == IoLane.OUTPUT && io.getUnfilteredValue() != null)
                ? io.getUnfilteredValue() : io.getValue();
        m.put("value", structuralCopy(mapper, displayed, seen, lane));
        return m;
    }

    /** Same shape {@link SandboxLogStore} emits when a spill cannot store a ref. */
    static Map<String, Object> truncatedMarker(int originalBytes) {
        Map<String, Object> marker = new LinkedHashMap<>();
        marker.put("truncated", true);
        marker.put("originalBytes", originalBytes);
        return marker;
    }

    /** Size only — never the body. Used when the last-resort marker has no spilled bytes. */
    private static int estimateBytes(Object node) {
        if (node == null) {
            return 0;
        }
        if (node instanceof String s) {
            return s.length();
        }
        if (node instanceof byte[] b) {
            return b.length;
        }
        if (node instanceof Map<?, ?> map) {
            int n = 0;
            for (Object v : map.values()) {
                n += estimateBytes(v);
            }
            return n;
        }
        if (node instanceof Collection<?> col) {
            int n = 0;
            for (Object v : col) {
                n += estimateBytes(v);
            }
            return n;
        }
        if (node instanceof InOutValueDto io) {
            Object displayed = io.getUnfilteredValue() != null ? io.getUnfilteredValue() : io.getValue();
            return estimateBytes(displayed);
        }
        return 0;
    }

    /** Drop the internal harvest field so Duct Tape never renders a second copy. */
    @SuppressWarnings("unchecked")
    static void stripUnfilteredKey(Object node) {
        if (node instanceof Map<?, ?> map) {
            ((Map<Object, Object>) map).remove("unfilteredValue");
            for (Object value : map.values()) {
                stripUnfilteredKey(value);
            }
        } else if (node instanceof List<?> list) {
            for (Object element : list) {
                stripUnfilteredKey(element);
            }
        }
    }

    @SuppressWarnings("unchecked")
    static void stripDataFields(Object node) {
        if (node instanceof Map<?, ?> map) {
            Map<Object, Object> m = (Map<Object, Object>) map;
            Object data = m.get("data");
            if (data instanceof String s && s.length() > MAX_INLINE_DATA_CHARS) {
                m.put("data", "<omitted " + s.length() + " chars>");
            }
            for (Object value : m.values()) {
                stripDataFields(value);
            }
        } else if (node instanceof List<?> list) {
            for (Object element : list) {
                stripDataFields(element);
            }
        }
    }

    private static Set<Object> newIdentitySet() {
        return java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    }
}
