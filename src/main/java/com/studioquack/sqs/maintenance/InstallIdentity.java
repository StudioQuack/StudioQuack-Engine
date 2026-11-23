package com.studioquack.sqs.maintenance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Stored at {@code {SQS_DATA_DIR}/channel/install-identity}. Uninstall already clears
 * {@code data/channel/}, so a reinstall gets a new UUID without changing uninstall scripts; normal
 * restarts keep the same id.
 */
public final class InstallIdentity {

    /** Relative path under the data dir (single line, trimmed). */
    public static final String RELATIVE_PATH = "channel/install-identity";

    private InstallIdentity() {
    }

    /**
     * Loads the install UUID from {@code dataDir/channel/install-identity}, or creates one when the
     * file is missing or blank. An existing non-blank id is never overwritten.
     *
     * @param dataDir
     */
    public static String loadOrCreate(Path dataDir) {
        Path file = dataDir.resolve("channel").resolve("install-identity");
        try {
            if (Files.isRegularFile(file)) {
                String existing = Files.readString(file, StandardCharsets.UTF_8).trim();
                if (!existing.isBlank()) {
                    return existing;
                }
            }
            Files.createDirectories(file.getParent());
            // Re-read after creating parents in case another thread claimed first.
            if (Files.isRegularFile(file)) {
                String existing = Files.readString(file, StandardCharsets.UTF_8).trim();
                if (!existing.isBlank()) {
                    return existing;
                }
            }
            String id = UUID.randomUUID().toString();
            Files.writeString(file, id, StandardCharsets.UTF_8);
            return id;
        } catch (IOException e) {
            throw new MaintenanceService.MaintenanceException(
                    "install-identity: cannot load or create at " + file + ": " + e.getMessage(), e);
        }
    }
}
