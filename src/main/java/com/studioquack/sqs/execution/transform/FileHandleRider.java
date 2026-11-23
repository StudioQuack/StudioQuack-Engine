package com.studioquack.sqs.execution.transform;

import com.studioquack.sqs.files.ClientFileStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Restores a resolvable {@code sq-file:} handle onto a rebuilt file ContentBlock.
 *
 * <p>Authors leaf-map {@code name}/{@code data}/{@code mime_type}/{@code kind}/{@code source}
 * and never {@code fileId}. JOLT therefore rebuilds a ContentBlock without the transport
 * handle; {@link com.studioquack.common.utils.flow.PortSchemaFilter} then drops an
 * undeclared {@code fileId} from map-shaped values (AI Agent {@code inputFiles} children
 * omit it). Hydrate can fill {@code data} only when {@code fileId} or a handle-valued
 * {@code source} is present — so the handle must ride through both hops.
 *
 * <p>This is the single recovery path. Do not also inject implicit {@code fileId} JOLT
 * rows; do not copy or log bytes.
 */
@Slf4j
public final class FileHandleRider {

    /** Same cap as {@code ContentBlockHydrator} — agent envelopes nest, they do not go deep. */
    private static final int MAX_WALK_DEPTH = 32;

    /** Source tokens that mark a ContentBlock, as opposed to a contact {@code {name, email}}. */
    private static final Set<String> SOURCE_KIND_TOKENS = Set.of("sq-file", "base64", "url");

    private FileHandleRider() {
    }

    /**
     * Walk {@code sourceTree} for ContentBlocks that already hold a resolvable handle, then
     * copy {@code fileId} onto file-shaped maps in {@code targetTree} that lack one.
     * Mutates {@code targetTree} in place (same contract as {@link FileRefSanitizer}).
     *
     * @return {@code targetTree} (null-safe); never invents a handle
     */
    public static Object rideHandles(Object sourceTree, Object targetTree) {
        if (targetTree == null) {
            return null;
        }
        if (sourceTree == null) {
            return targetTree;
        }
        List<Candidate> candidates = new ArrayList<>();
        collect(sourceTree, candidates, Collections.newSetFromMap(new IdentityHashMap<>()), 0);
        if (candidates.isEmpty()) {
            return targetTree;
        }
        int attached = apply(targetTree, candidates, Collections.newSetFromMap(new IdentityHashMap<>()), 0);
        log.debug("file-handle-rider.done attached={} candidates={}", attached, candidates.size());
        return targetTree;
    }

    private static void collect(Object node, List<Candidate> candidates, Set<Object> visited, int depth) {
        if (node == null || depth > MAX_WALK_DEPTH) {
            return;
        }
        if (node instanceof Map<?, ?> map) {
            if (!visited.add(map)) {
                return;
            }
            String handle = resolveHandle(map);
            if (handle != null) {
                candidates.add(new Candidate(blockName(map), handle));
            }
            for (Object value : map.values()) {
                collect(value, candidates, visited, depth + 1);
            }
            return;
        }
        if (node instanceof List<?> list) {
            if (!visited.add(list)) {
                return;
            }
            for (Object element : list) {
                collect(element, candidates, visited, depth + 1);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static int apply(Object node, List<Candidate> candidates, Set<Object> visited, int depth) {
        if (node == null || depth > MAX_WALK_DEPTH) {
            return 0;
        }
        int attached = 0;
        if (node instanceof Map<?, ?> map) {
            if (!visited.add(map)) {
                return 0;
            }
            if (resolveHandle(map) == null && isFileShaped(map)) {
                boolean namePresent = blockName(map) != null;
                Candidate match = match(blockName(map), candidates);
                if (match != null) {
                    ((Map<String, Object>) map).put("fileId", match.handle);
                    attached++;
                    log.debug("file-handle-rider.attached candidates={} namePresent={}",
                            candidates.size(), namePresent);
                } else if (isAmbiguous(blockName(map), candidates)) {
                    log.warn("file-handle-rider.skip candidates={} namePresent={}",
                            candidates.size(), namePresent);
                }
            }
            for (Object value : map.values()) {
                attached += apply(value, candidates, visited, depth + 1);
            }
            return attached;
        }
        if (node instanceof List<?> list) {
            if (!visited.add(list)) {
                return 0;
            }
            for (Object element : list) {
                attached += apply(element, candidates, visited, depth + 1);
            }
        }
        return attached;
    }

    /**
     * Unique {@code name} equality wins. A nameless target may take the sole collected
     * source. Several sources and no unique name match → skip (do not guess). One source
     * attached to many same/empty-name targets is intentional (same file).
     */
    private static Candidate match(String targetName, List<Candidate> candidates) {
        if (targetName != null) {
            Candidate hit = null;
            for (Candidate c : candidates) {
                if (targetName.equals(c.name)) {
                    if (hit != null) {
                        return null;
                    }
                    hit = c;
                }
            }
            return hit;
        }
        return candidates.size() == 1 ? candidates.get(0) : null;
    }

    /** True when we had something to attach but refused to guess. */
    private static boolean isAmbiguous(String targetName, List<Candidate> candidates) {
        if (targetName != null) {
            int hits = 0;
            for (Candidate c : candidates) {
                if (targetName.equals(c.name)) {
                    hits++;
                }
            }
            return hits > 1;
        }
        return candidates.size() > 1;
    }

    /**
     * File-shaped TARGET: at least two ContentBlock signals among
     * {@code name}, {@code mime_type}/{@code mimeType}, {@code kind}, {@code data}, {@code source}
     * AND a marker that says this block is a FILE rather than any other ContentBlock
     * ({@code mime_type}/{@code mimeType}, {@code kind = file}, or an {@code sq-file} source).
     * {@code source} counts only as {@code sq-file}/{@code base64}/{@code url} or a store ref.
     *
     * <p>The file marker is what keeps a plain text block out. An agent envelope's
     * {@code {kind: text, data: …}} clears the two-signal bar and carries no name, so it would
     * otherwise take the sole collected handle and let hydrate overwrite its text with file
     * bytes. {@code {name, mime_type}} is still enough (the JOLT-only leaf case);
     * {@code {name, email}} was never a file.
     *
     * <p>A block whose {@code data} is already populated needs no handle: its bytes are present,
     * and attaching one only invites hydrate to replace them.
     */
    private static boolean isFileShaped(Map<?, ?> map) {
        if (hasInlineData(map)) {
            return false;
        }
        boolean fileMarker = map.get("mime_type") != null
                || map.get("mimeType") != null
                || (map.get("kind") instanceof String kind && "file".equalsIgnoreCase(kind.trim()))
                || isStoreSourceMarker(map.get("source"));
        if (!fileMarker) {
            return false;
        }
        int signals = 0;
        if (map.get("name") != null) {
            signals++;
        }
        if (map.get("mime_type") != null || map.get("mimeType") != null) {
            signals++;
        }
        if (map.get("kind") != null) {
            signals++;
        }
        if (map.get("data") != null) {
            signals++;
        }
        if (isSourceSignal(map.get("source"))) {
            signals++;
        }
        return signals >= 2;
    }

    /** True when the block already carries bytes — there is nothing for a handle to fetch. */
    private static boolean hasInlineData(Map<?, ?> map) {
        Object data = map.get("data");
        if (data instanceof String s) {
            return !s.isBlank();
        }
        return data != null;
    }

    /** {@code sq-file} (bare marker or a store ref) — the file-store family, not base64/url. */
    private static boolean isStoreSourceMarker(Object source) {
        if (ClientFileStore.isRef(source)) {
            return true;
        }
        return source instanceof String s && "sq-file".equals(s);
    }

    private static boolean isSourceSignal(Object source) {
        if (ClientFileStore.isRef(source)) {
            return true;
        }
        return source instanceof String s && SOURCE_KIND_TOKENS.contains(s);
    }

    /**
     * Prefer {@code fileId}; fall back to a handle-valued {@code source} — same promotion
     * {@code ContentBlockHydrator} uses. Bare {@code source=sq-file} is a kind marker, not a handle.
     */
    private static String resolveHandle(Map<?, ?> map) {
        Object fileId = map.get("fileId");
        if (ClientFileStore.isRef(fileId)) {
            return (String) fileId;
        }
        Object source = map.get("source");
        return ClientFileStore.isRef(source) ? (String) source : null;
    }

    private static String blockName(Map<?, ?> map) {
        Object name = map.get("name");
        if (name instanceof String s && !s.isBlank()) {
            return s;
        }
        return null;
    }

    /** Shape-only: name for matching + the handle string. Never holds {@code data}. */
    private record Candidate(String name, String handle) {
    }
}
