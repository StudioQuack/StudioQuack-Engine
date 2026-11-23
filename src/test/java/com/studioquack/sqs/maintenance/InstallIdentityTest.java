package com.studioquack.sqs.maintenance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link InstallIdentity} persists a UUID under {@code channel/install-identity} and never
 * overwrites an existing non-blank id.
 */
class InstallIdentityTest {

    @TempDir
    Path dataDir;

    @Test
    void loadOrCreate_createsUuid_whenMissing() throws Exception {
        String id = InstallIdentity.loadOrCreate(dataDir);

        assertFalse(id.isBlank());
        Path file = dataDir.resolve("channel").resolve("install-identity");
        assertTrue(Files.isRegularFile(file));
        assertEquals(id, Files.readString(file, StandardCharsets.UTF_8).trim());
    }

    @Test
    void loadOrCreate_returnsExisting_neverOverwrites() throws Exception {
        Path file = dataDir.resolve("channel").resolve("install-identity");
        Files.createDirectories(file.getParent());
        String existing = "11111111-2222-3333-4444-555555555555";
        Files.writeString(file, existing + "\n", StandardCharsets.UTF_8);

        String first = InstallIdentity.loadOrCreate(dataDir);
        String second = InstallIdentity.loadOrCreate(dataDir);

        assertEquals(existing, first);
        assertEquals(existing, second);
        assertEquals(existing, Files.readString(file, StandardCharsets.UTF_8).trim());
    }

    @Test
    void loadOrCreate_replacesBlank_withNewUuid() throws Exception {
        Path file = dataDir.resolve("channel").resolve("install-identity");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "   \n", StandardCharsets.UTF_8);

        String id = InstallIdentity.loadOrCreate(dataDir);

        assertFalse(id.isBlank());
        assertEquals(id, Files.readString(file, StandardCharsets.UTF_8).trim());
    }
}
