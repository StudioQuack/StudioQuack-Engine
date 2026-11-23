package com.studioquack.sqs.files;

import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.oci.ContainerBootstrap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns {@code sq-file:} handles into real files inside the sandbox. Runs at dispatch, immediately
 * after inline secret resolution and before the context is baked into the script source — the
 * script must see the final value, and the bake is what freezes it.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SandboxFileMaterializer {

    private final ClientFileStore clientFileStore;

    /**
     * Replaces handles in {@code trees} (in place) and returns the files to plant.
     *
     * <p>Deduplicated by file id: the same handle routinely appears in both the node's parameters and
     * its port inputs, and sending the bytes twice would double the payload for no benefit.
     */
    public List<SandboxJobRequest.AttachedFile> materialize(Object... trees) {
        // id -> attached file, so a handle seen twice resolves and travels once.
        Map<String, SandboxJobRequest.AttachedFile> byId = new LinkedHashMap<>();
        // in-sandbox name -> the id that claimed it, to detect a genuine collision.
        Map<String, String> claimedNames = new LinkedHashMap<>();

        for (Object tree : trees) {
            walk(tree, byId, claimedNames);
        }
        if (!byId.isEmpty()) {
            log.debug("sandbox.files.materialised count={}", byId.size());
        }
        return new ArrayList<>(byId.values());
    }

    @SuppressWarnings("unchecked")
    private void walk(Object node,
                      Map<String, SandboxJobRequest.AttachedFile> byId,
                      Map<String, String> claimedNames) {
        if (node instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : ((Map<Object, Object>) map).entrySet()) {
                Object value = entry.getValue();
                String path = resolveToPath(value, byId, claimedNames);
                if (path != null) {
                    ((Map<Object, Object>) map).put(entry.getKey(), path);
                } else {
                    walk(value, byId, claimedNames);
                }
            }
        } else if (node instanceof List<?> list) {
            List<Object> items = (List<Object>) list;
            for (int i = 0; i < items.size(); i++) {
                Object value = items.get(i);
                String path = resolveToPath(value, byId, claimedNames);
                if (path != null) {
                    trySet(items, i, path);
                } else {
                    walk(value, byId, claimedNames);
                }
            }
        }
    }

    /** @return the in-sandbox path when {@code value} was a resolvable handle / file item, else {@code null}. */
    private String resolveToPath(Object value,
                                 Map<String, SandboxJobRequest.AttachedFile> byId,
                                 Map<String, String> claimedNames) {
        // Plant bytes for the sandbox but do NOT replace the map — agents read name/data from the payload
        // (path substitution would strip them).
        if (value instanceof Map<?, ?> map) {
            plantFileItem(map, byId, claimedNames);
            return null;
        }
        if (!ClientFileStore.isRef(value)) {
            return null;
        }
        return resolveHandleToPath((String) value, byId, claimedNames);
    }

    /** Side-effect plant for a `{ name, data }` / `{ fileId }` item; leaves the map in the tree. */
    private void plantFileItem(Map<?, ?> map,
                               Map<String, SandboxJobRequest.AttachedFile> byId,
                               Map<String, String> claimedNames) {
        Object fileId = map.get("fileId");
        if (ClientFileStore.isRef(fileId)) {
            resolveHandleToPath((String) fileId, byId, claimedNames);
            return;
        }
        Object data = map.get("data");
        if (!(data instanceof String b64) || b64.isBlank() || ClientFileStore.isRef(b64)) {
            return;
        }
        try {
            byte[] bytes = java.util.Base64.getDecoder().decode(b64);
            String preferred = map.get("name") instanceof String n && !n.isBlank() ? n : "file";
            String id = "inline-" + Integer.toHexString(java.util.Arrays.hashCode(bytes));
            if (byId.containsKey(id)) {
                return;
            }
            String name = uniqueName(preferred, id, claimedNames);
            byId.put(id, new SandboxJobRequest.AttachedFile(name, bytes));
        } catch (IllegalArgumentException e) {
            log.debug("sandbox.files: skip inline item (not base64): {}", e.getClass().getSimpleName());
        }
    }

    private String resolveHandleToPath(String handle,
                                       Map<String, SandboxJobRequest.AttachedFile> byId,
                                       Map<String, String> claimedNames) {
        String id = handle.substring(ClientFileStore.REF_PREFIX.length());
        SandboxJobRequest.AttachedFile already = byId.get(id);
        if (already != null) {
            return ContainerBootstrap.FILES_DIR + "/" + already.name();
        }
        var resolved = clientFileStore.resolve(handle);
        if (resolved.isEmpty()) {
            log.warn("sandbox.files: handle {} could not be resolved; leaving it unsubstituted", handle);
            return null;
        }
        String name = uniqueName(resolved.get().fileName(), id, claimedNames);
        byId.put(id, new SandboxJobRequest.AttachedFile(name, resolved.get().content()));
        return ContainerBootstrap.FILES_DIR + "/" + name;
    }

    /**
     * Keeps the author's filename when it is free, and disambiguates only on a real clash.
     *
     * <p>Two different files can legitimately share a basename — the same document attached to two
     * parameters, say — and writing both to one path would silently give the node the wrong content.
     */
    private String uniqueName(String preferred, String id, Map<String, String> claimedNames) {
        String name = ClientFileStore.safeName(preferred);
        String owner = claimedNames.get(name);
        if (owner == null) {
            claimedNames.put(name, id);
            return name;
        }
        if (owner.equals(id)) {
            return name;
        }
        String disambiguated = id.substring(0, Math.min(8, id.length())) + "-" + name;
        claimedNames.put(disambiguated, id);
        return disambiguated;
    }

    /** {@code List.of(...)} results are immutable; a value we cannot substitute is left as the handle. */
    private void trySet(List<Object> items, int index, String path) {
        try {
            items.set(index, path);
        } catch (UnsupportedOperationException immutable) {
            log.warn("sandbox.files: could not substitute into an immutable list; "
                    + "the handle stays and the node will report it");
        }
    }
}
