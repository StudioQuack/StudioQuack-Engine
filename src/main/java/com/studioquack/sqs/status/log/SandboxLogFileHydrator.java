package com.studioquack.sqs.status.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.files.ClientFileStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Extracted from {@code ChannelSandboxLogCallback} so the live per-node mirror, the terminal BATCH
 * flush ({@code RunLogBatchFlusher}) and the on-demand pull ({@code RunLogsCommandHandler}) all
 * hydrate identically. Unresolvable handles pass through unchanged: a missing file must never
 * break a log.
 */
@Slf4j
@Component
public class SandboxLogFileHydrator {

    private final ObjectMapper mapper;
    private final ObjectProvider<ClientFileStore> clientFileStore;

    public SandboxLogFileHydrator(ObjectMapper mapper, ObjectProvider<ClientFileStore> clientFileStore) {
        this.mapper = mapper;
        this.clientFileStore = clientFileStore;
    }

    /**
     * Returns a deep copy of {@code payload} with every {@code sq-file:} handle hydrated to inline
     * base64, and any I/O-spill marker ({@code sqFileRef}) replaced by the original JSON object.
     * Returns the original reference on any fault (best-effort, never throws).
     */
    public Object hydrate(Object payload) {
        ClientFileStore store = clientFileStore == null ? null : clientFileStore.getIfAvailable();
        if (payload == null || store == null) {
            return payload;
        }
        try {
            Object copy = mapper.convertValue(payload, Object.class);
            return hydrateNode(copy, store);
        } catch (Exception e) {
            log.debug("sandbox-log[hydrate]: file-data hydration skipped ({})", e.toString());
            return payload;
        }
    }

    @SuppressWarnings("unchecked")
    private Object hydrateNode(Object node, ClientFileStore store) {
        if (node instanceof Map<?, ?> map) {
            Map<Object, Object> m = (Map<Object, Object>) map;
            Object spillRef = m.get("sqFileRef");
            if (spillRef instanceof String ref && ClientFileStore.isRef(ref)) {
                Optional<ClientFileStore.StoredFile> spilled = store.resolve(ref);
                if (spilled.isPresent()) {
                    try {
                        Object restored = mapper.readValue(spilled.get().content(), Object.class);
                        return hydrateNode(restored, store);
                    } catch (Exception e) {
                        log.debug("sandbox-log[hydrate]: IO spill restore failed ({})", e.toString());
                    }
                }
                return node;
            }
            Object fileId = m.get("fileId");
            if (fileId instanceof String ref && ClientFileStore.isRef(ref) && !m.containsKey("data")) {
                Optional<ClientFileStore.StoredFile> stored = store.resolve(ref);
                if (stored.isPresent()) {
                    ClientFileStore.StoredFile file = stored.get();
                    m.put("data", Base64.getEncoder().encodeToString(file.content()));
                    m.put("source", "base64");
                    if (m.get("name") == null && file.fileName() != null) {
                        m.put("name", file.fileName());
                    }
                    if (m.get("mime_type") == null && file.mimeType() != null) {
                        m.put("mime_type", file.mimeType());
                    }
                }
            }
            for (var entry : m.entrySet()) {
                Object replacement = hydrateNode(entry.getValue(), store);
                if (replacement != entry.getValue()) {
                    entry.setValue(replacement);
                }
            }
            return m;
        } else if (node instanceof List<?> list) {
            List<Object> values = (List<Object>) list;
            for (int i = 0; i < values.size(); i++) {
                Object replacement = hydrateNode(values.get(i), store);
                if (replacement != values.get(i)) {
                    values.set(i, replacement);
                }
            }
            return values;
        }
        return node;
    }
}
