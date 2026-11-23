package com.studioquack.sqs.sandbox.warmpool;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link WarmTargetStore} must persist per-image caps so a daemon restart keeps them. Handler-level
 * coverage lives in {@code SetWarmTargetCommandHandlerTest}; this pins the store alone (save → new
 * store on same path → load).
 */
class WarmTargetStoreCapsRoundTripTest {

    @TempDir
    Path tempDir;

    @Test
    void save_then_load_preserves_per_image_caps() {
        Path file = tempDir.resolve("warm-targets.json");
        ObjectMapper mapper = new ObjectMapper();
        WarmTargetStore writer = new WarmTargetStore(file.toString(), mapper);

        String imageRef = "registry.example/acme/app:1.2";
        writer.save(List.of(new WarmTargetEntry(
                imageRef, null, 2, new WarmTargetEntry.Caps(1.5, 256, 128))));

        WarmTargetStore reader = new WarmTargetStore(file.toString(), mapper);
        List<WarmTargetEntry> loaded = reader.load();

        assertEquals(1, loaded.size());
        WarmTargetEntry e = loaded.get(0);
        assertEquals(imageRef, e.imageRef());
        assertNull(e.language());
        assertEquals(2, e.target());
        assertNotNull(e.caps());
        assertEquals(1.5, e.caps().cpu());
        assertEquals(256, e.caps().ramMb());
        assertEquals(128, e.caps().diskMb());
        assertNull(e.caps().gpu(), "no gpu in the cap set → gpu stays unset (not zero)");
    }

    @Test
    void save_then_load_preserves_gpu_cap() {
        Path file = tempDir.resolve("warm-targets-gpu.json");
        ObjectMapper mapper = new ObjectMapper();
        WarmTargetStore writer = new WarmTargetStore(file.toString(), mapper);

        String imageRef = "registry.example/acme/gpu-app:2.0";
        writer.save(List.of(new WarmTargetEntry(
                imageRef, null, 1, new WarmTargetEntry.Caps(2.0, 512, 256, 1))));

        WarmTargetStore reader = new WarmTargetStore(file.toString(), mapper);
        List<WarmTargetEntry> loaded = reader.load();

        assertEquals(1, loaded.size());
        WarmTargetEntry e = loaded.get(0);
        assertEquals(imageRef, e.imageRef());
        assertNotNull(e.caps());
        assertEquals(1, e.caps().gpu(), "D110 — the gpu cap must survive a daemon restart");
        assertEquals(2.0, e.caps().cpu());
    }
}
