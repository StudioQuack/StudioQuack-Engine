package com.studioquack.sqs.execution.transform;

import com.studioquack.sqs.files.ClientFileStore;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Keeps file ContentBlocks REF-only across transform paths (JOLT today; edge-script
 * pre-step next). Inline base64 aliases are stripped when an {@code sq-file:} handle
 * is already present — bytes materialise later at sandbox bake / Docling / Duckbot.
 *
 * <p>Shared so JOLT and the edge-transform workspace cannot drift.
 */
public final class FileRefSanitizer {

    /** Inline file payload aliases — ContentBlocks / LLM helpers disagree on the key name. */
    private static final Set<String> INLINE_FILE_PAYLOAD_KEYS =
            Set.of("data", "content", "base64", "b64", "bytes");

    private FileRefSanitizer() {
    }

    /**
     * Walk maps/lists and strip inline file payloads from ContentBlocks that already carry an
     * {@code sq-file:} ref (or {@code source=sq-file}). Mutates in place.
     */
    @SuppressWarnings("unchecked")
    public static Object stripInlineFileBytes(Object node) {
        if (node instanceof Map<?, ?>) {
            Map<String, Object> map = (Map<String, Object>) node;
            for (Map.Entry<String, Object> e : map.entrySet()) {
                Object stripped = stripInlineFileBytes(e.getValue());
                if (stripped != e.getValue()) {
                    e.setValue(stripped);
                }
            }
            if (isFileRefContentBlock(map)) {
                stripFilePayloadKeys(map);
            }
            return map;
        }
        if (node instanceof List<?>) {
            List<Object> list = (List<Object>) node;
            for (int i = 0; i < list.size(); i++) {
                Object stripped = stripInlineFileBytes(list.get(i));
                if (stripped != list.get(i)) {
                    list.set(i, stripped);
                }
            }
            return list;
        }
        return node;
    }

    /**
     * True when the map is a ContentBlock addressed by an {@code sq-file:} handle. A bare
     * {@code source=sq-file} WITHOUT a resolvable handle does NOT qualify: the inline bytes are
     * then the only copy (a voice clip the ingest left handle-less arrives exactly like that),
     * and stripping them would empty the block for every downstream consumer.
     */
    private static boolean isFileRefContentBlock(Map<String, Object> map) {
        if (ClientFileStore.isRef(map.get("fileId"))) {
            return true;
        }
        // Wire mistake: payload key holds the handle instead of base64.
        for (String key : INLINE_FILE_PAYLOAD_KEYS) {
            if (ClientFileStore.isRef(map.get(key))) {
                return true;
            }
        }
        return false;
    }

    /** Drop inline bytes; keep / promote the {@code sq-file:} handle under {@code fileId}. */
    private static void stripFilePayloadKeys(Map<String, Object> map) {
        if (!ClientFileStore.isRef(map.get("fileId"))) {
            for (String key : INLINE_FILE_PAYLOAD_KEYS) {
                Object v = map.get(key);
                if (ClientFileStore.isRef(v)) {
                    map.put("fileId", v);
                    break;
                }
            }
        }
        for (String key : INLINE_FILE_PAYLOAD_KEYS) {
            map.remove(key);
        }
        map.put("source", "sq-file");
    }
}
