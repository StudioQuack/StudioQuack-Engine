package com.studioquack.sqs.files;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Moves the inline bytes of a file ContentBlock into the daemon's file store, leaving a
 * {@code sq-file:} handle on the block. A field becomes a file because its declared schema type is
 * {@code file} (it arrives as a ContentBlock with {@code name}/{@code mime_type}/{@code data}),
 * never because some arbitrary text happened to be large.
 */
@Slf4j
@Service
public class ParameterSpiller {

    private final ClientFileStore clientFileStore;

    /**
     * One constructor is also what Spring needs to autowire the bean without an explicit
     * {@code @Autowired}.
     */
    public ParameterSpiller(ClientFileStore clientFileStore) {
        this.clientFileStore = clientFileStore;
    }

    /**
     * Decoded-bytes ceiling above which a file ContentBlock's inline base64 moves to the store.
     * Matches the backend's {@code studioquack.workflow.output-inline-threshold-bytes} so a file is
     * judged the same on both sides of the wire and does not oscillate between inline and spilled.
     */
    @Value("${sqs.files.inline-threshold-bytes:1048576}")
    private int inlineThresholdBytes;

    /**
     * @param portValue           the output port, used only for diagnostics
     * @param workflowExecutionId RUN scope key, so the file is reclaimed when the run ends
     * @return the value with any oversized file ContentBlock's bytes behind a {@code sq-file:}
     *         handle; a non-file value is returned unchanged
     */
    public Object spillIfOversized(Object value, String portValue, String workflowExecutionId) {
        // A non-positive threshold means "disabled", never "spill everything". Getting that backwards
        // would route every value on every port through the file store on a single mis-set property.
        if (inlineThresholdBytes <= 0 || value == null || ClientFileStore.isRef(value)) {
            return value;
        }
        // Only ContentBlocks (file-typed content) spill — file by file, keeping each file's name, media
        // type and identity inline.
        Object perFile = spillContentBlocks(value, workflowExecutionId,
                Collections.newSetFromMap(new IdentityHashMap<>()));
        return perFile != null ? perFile : value;
    }

    /**
     * Spills the inline bytes of every oversized ContentBlock in {@code value}, leaving the block's
     * metadata ({@code name}, {@code mime_type}, {@code kind}) in the context and its content in the
     * file store behind a {@code fileId} handle.
     *
     * @return The rewritten value, or null when nothing here is a file worth spilling this way (the
     * caller then falls back to spilling the whole value)
     */
    private Object spillContentBlocks(Object value, String workflowExecutionId, Set<Object> visited) {
        if (value instanceof Map<?, ?> map) {
            if (!visited.add(map)) {
                return null;
            }
            Map<String, Object> spilled = spillOneBlock(map, workflowExecutionId);
            if (spilled != null) {
                return spilled;
            }
            // Not itself a file block — nested envelopes (e.g. an LLM node's
            // {result: {content, output_files: [...]}}) still carry file blocks deeper down.
            // Recurse so multi-MB image bytes never ride the transport inline.
            Map<String, Object> rewritten = null;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object child = spillContentBlocks(entry.getValue(), workflowExecutionId, visited);
                if (child != null) {
                    if (rewritten == null) {
                        rewritten = new LinkedHashMap<>();
                        for (Map.Entry<?, ?> original : map.entrySet()) {
                            rewritten.put(String.valueOf(original.getKey()), original.getValue());
                        }
                    }
                    rewritten.put(String.valueOf(entry.getKey()), child);
                }
            }
            return rewritten;
        }
        if (value instanceof List<?> list && !list.isEmpty()) {
            if (!visited.add(list)) {
                return null;
            }
            List<Object> out = new ArrayList<>(list.size());
            boolean spilledAny = false;
            for (Object element : list) {
                Object spilled = spillContentBlocks(element, workflowExecutionId, visited);
                if (spilled != null) {
                    spilledAny = true;
                    out.add(spilled);
                } else {
                    out.add(element);
                }
            }
            return spilledAny ? out : null;
        }
        return null;
    }

    /**
     * One ContentBlock with oversized inline base64 → the same block with its bytes in the store.
     * Returns null when the block is not a file, is already a handle, or is small enough to stay inline.
     */
    private Map<String, Object> spillOneBlock(Map<?, ?> block, String workflowExecutionId) {
        Object rawData = block.get("data");
        if (!(rawData instanceof String data) || data.isBlank() || ClientFileStore.isRef(data)) {
            return null;
        }
        String name = firstNonBlank(block.get("name"));
        String mime = firstNonBlank(block.get("mime_type"), block.get("mimeType"));
        // File-shaped: name/mime, or an sq-file: handle on fileId/source — never a bare "sq-file" token.
        if (name == null && mime == null
                && !ClientFileStore.isRef(block.get("fileId"))
                && !ClientFileStore.isRef(block.get("source"))) {
            return null;
        }
        // Judge the decoded size, which is what the store holds and what the budget is about.
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(data);
        } catch (IllegalArgumentException notBase64) {
            return null;
        }
        if (decoded.length <= inlineThresholdBytes) {
            return null;
        }
        try {
            String ref = clientFileStore.store(ClientFileStore.SCOPE_RUN, workflowExecutionId,
                    name == null ? "attachment" : name,
                    mime == null ? "application/octet-stream" : mime,
                    decoded);
            if (ref == null || ref.isBlank()) {
                return null;
            }
            Map<String, Object> rewritten = new LinkedHashMap<>();
            block.forEach((k, v) -> rewritten.put(String.valueOf(k), v));
            rewritten.remove("data");
            rewritten.put("fileId", ref);
            rewritten.put("source", "sq-file");
            log.info("parameter.spilled_file name={} bytes={} run={}", name, decoded.length, workflowExecutionId);
            return rewritten;
        } catch (RuntimeException e) {
            // Above the store's ceiling / store unavailable: keeping it inline is worse for the
            // context but still correct, whereas dropping the file would lose the node's output.
            log.warn("parameter.spill_file_failed name={} bytes={}: {}", name, decoded.length, e.getMessage());
            return null;
        }
    }

    private static String firstNonBlank(Object... candidates) {
        for (Object candidate : candidates) {
            if (candidate instanceof String s && !s.isBlank()) {
                return s;
            }
        }
        return null;
    }
}
