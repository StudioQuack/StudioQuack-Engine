package com.studioquack.sqs.aimodel;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The daemon's record of which provider models are LOADED into memory. Local model servers with
 * Just-In-Time loading (LM Studio and compatible) answer a chat request for an unloaded model by
 * loading it on the fly — evicting whatever was loaded before.
 */
@Component
public class AiModelLoadState {

    /** Loaded provider model ids (exact strings the provider's chat path expects). */
    private final Set<String> loaded = ConcurrentHashMap.newKeySet();

    /** Records a successful load (idempotent). */
    public void markLoaded(String model) {
        if (model != null && !model.isBlank()) {
            loaded.add(model);
        }
    }

    /** Records a successful unload (idempotent). */
    public void markUnloaded(String model) {
        if (model != null) {
            loaded.remove(model);
        }
    }

    /** True when {@code model} is currently recorded as loaded. */
    public boolean isLoaded(String model) {
        return model != null && loaded.contains(model);
    }

    /** Sorted immutable snapshot for list annotation / diagnostics. */
    public List<String> snapshot() {
        List<String> out = new ArrayList<>(loaded);
        Collections.sort(out);
        return Collections.unmodifiableList(out);
    }

    /** Drops every record (provider server changed — see class doc). */
    public void clear() {
        loaded.clear();
    }

    /**
     * Replaces the whole record with the provider's own answer (reconciliation). Called when a
     * managed provider reports its in-memory models — the provider is ground truth, so anything
     * it does not report is dropped (it covers TTL evictions and out-of-band unloads too).
     */
    public void replaceAll(java.util.Collection<String> modelIds) {
        loaded.clear();
        if (modelIds == null) {
            return;
        }
        for (String id : modelIds) {
            markLoaded(id);
        }
    }
}
