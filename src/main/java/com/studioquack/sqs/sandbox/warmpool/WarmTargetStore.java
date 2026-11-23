package com.studioquack.sqs.sandbox.warmpool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * File-backed persistence for the operator's image-keyed warm-pool targets. A warm target the
 * operator sets from the environment Runtime UI must survive a daemon restart, otherwise the warm
 * containers silently vanish and the pool falls back to cold-start on the next boot.
 */
@Slf4j
@Component
public class WarmTargetStore {

    /**
     * Former language defaults the daemon no longer ships (end-of-life / unpatched Node lines),
     * keyed to the language they were the default for. A persisted target still on one would keep
     * warming an image no job dispatches to, so it moves to the language's current default on load.
     */
    private static final Map<String, SandboxLanguage> RETIRED_DEFAULTS = Map.of(
            "node:20-slim", SandboxLanguage.JAVASCRIPT,
            "node:22-slim", SandboxLanguage.TYPESCRIPT);

    private final Path file;
    private final ObjectMapper objectMapper;

    public WarmTargetStore(
            @Value("${sqs.sandbox.warmpool.state-path:${SQS_DATA_DIR:./data}/warm-targets.json}") String path,
            ObjectMapper objectMapper) {
        this.file = Path.of(path);
        this.objectMapper = objectMapper;
    }

    /**
     * Loads the persisted entries, or an empty list when none exist / the file is unreadable.
     * Accepts both the v2 {@code entries} array and the legacy language→target flat map.
     */
    public List<WarmTargetEntry> load() {
        if (!Files.isRegularFile(file)) {
            return List.of();
        }
        try {
            JsonNode root = objectMapper.readTree(Files.readAllBytes(file));
            if (root == null || root.isNull()) {
                return List.of();
            }
            if (root.has("entries") && root.get("entries").isArray()) {
                List<WarmTargetEntry> entries = parseEntries(root.get("entries"));
                log.info("warmpool.targets.loaded path={} entries={}", file, entries.size());
                return entries;
            }
            // LEGACY: flat string→int object {"python":6}
            if (root.isObject()) {
                List<WarmTargetEntry> migrated = migrateLegacy(root);
                log.info("warmpool.targets.legacy-migrated path={} entries={} (language keys → image refs)",
                        file, migrated.size());
                return migrated;
            }
            log.warn("warmpool.targets.load-unrecognised path={} — starting with no persisted targets", file);
            return List.of();
        } catch (IOException | RuntimeException e) {
            log.warn("warmpool.targets.load-failed path={}: {} — starting with no persisted targets",
                    file, e.getMessage());
            return List.of();
        }
    }

    /** Persists the full entry list in v2 shape (best-effort); creates the parent dir if missing. */
    public void save(List<WarmTargetEntry> entries) {
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            ObjectNode root = objectMapper.createObjectNode();
            root.put("version", 2);
            ArrayNode arr = root.putArray("entries");
            List<WarmTargetEntry> list = entries == null ? List.of() : entries;
            for (WarmTargetEntry e : list) {
                if (e == null || e.imageRef() == null || e.imageRef().isBlank()) {
                    continue;
                }
                ObjectNode n = arr.addObject();
                n.put("imageRef", e.imageRef());
                if (e.language() == null || e.language().isBlank()) {
                    n.putNull("language");
                } else {
                    n.put("language", e.language());
                }
                n.put("target", Math.max(0, e.target()));
                WarmTargetEntry.Caps caps = e.caps();
                if (caps == null || caps.isEmpty()) {
                    n.putNull("caps");
                } else {
                    ObjectNode c = n.putObject("caps");
                    if (caps.cpu() != null) {
                        c.put("cpu", caps.cpu());
                    }
                    if (caps.ramMb() != null) {
                        c.put("ramMb", caps.ramMb());
                    }
                    if (caps.diskMb() != null) {
                        c.put("diskMb", caps.diskMb());
                    }
                    if (caps.gpu() != null) {
                        c.put("gpu", caps.gpu());
                    }
                }
            }
            Files.write(file, objectMapper.writeValueAsBytes(root));
            log.debug("warmpool.targets.saved path={} entries={}", file, arr.size());
        } catch (IOException | RuntimeException e) {
            log.warn("warmpool.targets.save-failed path={}: {} — target applied in-memory only",
                    file, e.getMessage());
        }
    }

    private List<WarmTargetEntry> parseEntries(JsonNode arr) {
        List<WarmTargetEntry> out = new ArrayList<>();
        for (JsonNode n : arr) {
            if (n == null || !n.isObject()) {
                continue;
            }
            String imageRef = text(n, "imageRef");
            if (imageRef.isBlank()) {
                imageRef = text(n, "image");
            }
            if (imageRef.isBlank()) {
                continue;
            }
            String language = text(n, "language");
            if (language.isBlank()) {
                language = null;
            }
            int target = n.has("target") && n.get("target").isNumber()
                    ? Math.max(0, n.get("target").asInt()) : 0;
            WarmTargetEntry.Caps caps = parseCaps(n.get("caps"));
            out.add(new WarmTargetEntry(currentDefaultIfRetired(imageRef, language), language, target, caps));
        }
        return out;
    }

    /**
     * {@code imageRef} unless it is a retired default of {@code language} (or carries no language),
     * in which case the language's current default. Any other image is the operator's own choice.
     */
    private static String currentDefaultIfRetired(String imageRef, String language) {
        SandboxLanguage retiredFor = RETIRED_DEFAULTS.get(imageRef.toLowerCase(java.util.Locale.ROOT));
        if (retiredFor == null || (language != null && SandboxLanguage.fromWire(language) != retiredFor)) {
            return imageRef;
        }
        log.info("warmpool.targets.retired-default image={} → {} ({})",
                imageRef, retiredFor.defaultImage(), retiredFor.wire());
        return retiredFor.defaultImage();
    }

    /**
     * Legacy {@code {"python":6}} → image-keyed entries. Known languages map to their
     * {@link SandboxLanguage#defaultImage()}; unknown keys are treated as image refs (forward-tolerant).
     */
    private List<WarmTargetEntry> migrateLegacy(JsonNode root) {
        List<WarmTargetEntry> out = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> f = fields.next();
            String key = f.getKey();
            JsonNode v = f.getValue();
            if (key == null || key.isBlank() || v == null || !v.isNumber()) {
                continue;
            }
            // Skip the v2 "version" field if someone mixed shapes.
            if ("version".equals(key) || "entries".equals(key)) {
                continue;
            }
            int target = Math.max(0, v.asInt());
            SandboxLanguage lang = SandboxLanguage.fromWire(key);
            if (lang != null && lang.defaultImage() != null) {
                out.add(new WarmTargetEntry(lang.defaultImage(), lang.wire(), target, null));
            } else {
                out.add(new WarmTargetEntry(key.trim(), null, target, null));
            }
        }
        return out;
    }

    private static WarmTargetEntry.Caps parseCaps(JsonNode node) {
        if (node == null || node.isNull() || !node.isObject()) {
            return null;
        }
        Double cpu = null;
        Integer ramMb = null;
        Integer diskMb = null;
        Integer gpu = null;
        if (node.has("cpu") && node.get("cpu").isNumber()) {
            cpu = node.get("cpu").asDouble();
        }
        if (node.has("ramMb") && node.get("ramMb").isNumber()) {
            ramMb = node.get("ramMb").asInt();
        }
        if (node.has("diskMb") && node.get("diskMb").isNumber()) {
            diskMb = node.get("diskMb").asInt();
        }
        if (node.has("gpu") && node.get("gpu").isNumber()) {
            gpu = node.get("gpu").asInt();
        }
        WarmTargetEntry.Caps caps = new WarmTargetEntry.Caps(cpu, ramMb, diskMb, gpu);
        return caps.isEmpty() ? null : caps;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull() || !v.isTextual()) {
            return "";
        }
        return v.asText().trim();
    }
}
