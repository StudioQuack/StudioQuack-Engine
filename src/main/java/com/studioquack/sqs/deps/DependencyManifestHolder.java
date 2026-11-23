package com.studioquack.sqs.deps;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Seeded empty at boot; a pushed {@code set-deps} command swaps in the full manifest
 * (add/remove/preload are always full-list pushes). The manifest is the authoritative list of
 * libraries this environment's sandbox should have available.
 */
@Slf4j
@Component
public class DependencyManifestHolder {

    private final AtomicReference<List<DependencyEntry>> current = new AtomicReference<>(List.of());

    /** The live manifest (never null; empty until the BE pushes one). */
    public List<DependencyEntry> current() {
        return current.get();
    }

    /** Replace the live manifest with a BE-pushed one (null coerced to empty). */
    public void apply(List<DependencyEntry> manifest) {
        List<DependencyEntry> next = manifest == null ? List.of() : List.copyOf(manifest);
        current.set(next);
        log.info("deps: applied manifest with {} entr{}", next.size(), next.size() == 1 ? "y" : "ies");
    }
}
