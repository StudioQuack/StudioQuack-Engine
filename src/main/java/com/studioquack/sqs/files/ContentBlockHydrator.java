package com.studioquack.sqs.files;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Expands sealed ContentBlocks that carry only an {@code sq-file:} {@code fileId} into the
 * agent-facing shape {@code { kind, mime_type, source: "base64", data, name }} on a private
 * per-dispatch sandbox context map. Duckbot start must never persist multi-MB {@code data} into
 * the sealed execution_context store — Jackson's string limit and sandbox-log push both break.
 */
@Slf4j
@Component
public class ContentBlockHydrator {

    /** Agent envelopes nest files under {@code inputFiles} / {@code messages[].content[]}; cap the walk. */
    private static final int MAX_WALK_DEPTH = 32;

    private final ClientFileStore clientFileStore;

    public ContentBlockHydrator(ClientFileStore clientFileStore) {
        this.clientFileStore = clientFileStore;
    }

    /** Walk {@code trees} in place and fill missing {@code data} from a {@code sq-file:} handle. */
    public void hydrate(Object... trees) {
        if (trees == null) {
            return;
        }
        Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Object tree : trees) {
            walk(tree, visited, 0);
        }
    }

    @SuppressWarnings("unchecked")
    private void walk(Object node, Set<Object> visited, int depth) {
        if (node == null || depth > MAX_WALK_DEPTH) {
            return;
        }
        if (node instanceof Map<?, ?> map) {
            if (!visited.add(map)) {
                return;
            }
            maybeHydrateBlock((Map<Object, Object>) map);
            for (Object value : map.values()) {
                walk(value, visited, depth + 1);
            }
        } else if (node instanceof List<?> list) {
            if (!visited.add(list)) {
                return;
            }
            for (Object element : list) {
                walk(element, visited, depth + 1);
            }
        }
    }

    /**
     * Hydratable when {@code fileId} <em>or</em> {@code source} is {@code sq-file:<id>}.
     * The bare token {@code sq-file} is a kind/source marker, not a handle.
     */
    private void maybeHydrateBlock(Map<Object, Object> map) {
        Object handle = resolveHandle(map);
        if (handle == null) {
            return;
        }
        // Later hops still need the handle even when only {@code source} carried it.
        if (!ClientFileStore.isRef(map.get("fileId"))) {
            map.put("fileId", handle);
        }
        Object existing = map.get("data");
        if (existing instanceof String s && !s.isBlank()) {
            return; // already hydrated
        }
        var stored = clientFileStore.resolve(handle);
        if (stored.isEmpty()) {
            log.warn("content-block hydrate: handle {} could not be resolved; leaving without data",
                    handle);
            return;
        }
        ClientFileStore.StoredFile file = stored.get();
        map.put("data", Base64.getEncoder().encodeToString(file.content()));
        map.put("source", "base64");
        if (!(map.get("name") instanceof String n) || n.isBlank()) {
            if (file.fileName() != null && !file.fileName().isBlank()) {
                map.put("name", file.fileName());
            }
        }
        if (!(map.get("mime_type") instanceof String m) || m.isBlank()) {
            if (file.mimeType() != null && !file.mimeType().isBlank()) {
                map.put("mime_type", file.mimeType());
            }
        }
    }

    /** Prefer {@code fileId}; fall back to {@code source} when that string is itself a store handle. */
    private static Object resolveHandle(Map<Object, Object> map) {
        Object fileId = map.get("fileId");
        if (ClientFileStore.isRef(fileId)) {
            return fileId;
        }
        Object source = map.get("source");
        return ClientFileStore.isRef(source) ? source : null;
    }
}
