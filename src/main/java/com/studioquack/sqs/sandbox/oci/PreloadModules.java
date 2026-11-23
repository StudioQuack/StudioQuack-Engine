package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.deps.DependencyEntry;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.SandboxLanguage;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The import names a language runtime loads before the user script, from the live dependency
 * manifest ({@code preload=true} rows). One source for every consumer: the warm-pool runners
 * ({@link PythonRunner}, {@link NodeRunner}) load them at warm time, and the per-exec {@code
 * SQ_PRELOAD_MODULES} hint lets the helper preamble load them on a fresh interpreter.
 */
public final class PreloadModules {

    private PreloadModules() {
    }

    /** Comma-joined, de-duplicated import names for {@code lang}; empty when there are none. */
    public static String csvFor(DependencyManifestHolder depsHolder, SandboxLanguage lang) {
        if (depsHolder == null || lang == null) {
            return "";
        }
        String bucket = HardeningArgs.depBucket(lang);
        Set<String> modules = new LinkedHashSet<>();
        for (DependencyEntry entry : depsHolder.current()) {
            if (entry == null || !Boolean.TRUE.equals(entry.preload())) {
                continue;
            }
            SandboxLanguage entryLang = SandboxLanguage.fromWire(entry.language());
            if (entryLang == null || !bucket.equals(HardeningArgs.depBucket(entryLang))) {
                continue;
            }
            if (entry.preloadModules() != null && !entry.preloadModules().isEmpty()) {
                entry.preloadModules().forEach(m -> {
                    if (m != null && !m.isBlank()) {
                        modules.add(m.trim());
                    }
                });
            } else if (entry.name() != null && !entry.name().isBlank()) {
                modules.add(entry.name().trim());
            }
        }
        return String.join(",", modules);
    }
}
