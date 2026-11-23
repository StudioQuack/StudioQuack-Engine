package com.studioquack.sqs.maintenance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MaintenanceService} log-tailing and install-identity isolation. Proves log selection is
 * keyed by install identity (dir claim + sidecars), fail closed, and skips symlinks.
 */
class MaintenanceServiceTest {

    @TempDir
    Path root;

    /** A log location OUTSIDE the data dir, where cross-install refusal still applies. */
    @TempDir
    Path externalRoot;

    private MaintenanceProperties props;
    private MaintenanceService service;
    private Path logDir;

    @BeforeEach
    void setUp() throws Exception {
        logDir = Files.createDirectories(root.resolve("logs"));

        props = new MaintenanceProperties();
        props.setLogDir(logDir.toString());

        service = new MaintenanceService(root.toString(), props);
    }

    private Path writeFile(Path dir, String name, String content, long ageHours) throws Exception {
        Path p = dir.resolve(name);
        Files.writeString(p, content);
        Files.setLastModifiedTime(p, FileTime.from(Instant.now().minus(ageHours, ChronoUnit.HOURS)));
        return p;
    }

    // Install-identity isolation

    @Test
    void tailLogs_cleanInstall_tailsNewestEligible() throws Exception {
        writeFile(logDir, "older.log", "older-content", 5);
        writeFile(logDir, "newer.log", "newer-content", 1);

        MaintenanceService.LogsResult result = service.tailLogs(100, 64_000L);

        assertEquals("daemon-log-file", result.source());
        assertEquals("newer.log", result.filename());
        assertTrue(result.content().contains("newer-content"));
    }

    @Test
    void tailLogs_skipsNewerForeignSidecar_selectsOlderOwn() throws Exception {
        String installId = InstallIdentity.loadOrCreate(root);
        Path own = writeFile(logDir, "own.log", "own-content", 10);
        Files.writeString(logDir.resolve("own.log.sq-install-id"), installId, StandardCharsets.UTF_8);
        writeFile(logDir, "foreign.log", "foreign-secret", 1); // newer by mtime
        Files.writeString(logDir.resolve("foreign.log.sq-install-id"),
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", StandardCharsets.UTF_8);
        // Claim dir for current install so no-sidecar eligibility is not in play for foreign.
        Files.writeString(logDir.resolve(MaintenanceService.LOG_DIR_INSTALL_MARKER),
                installId, StandardCharsets.UTF_8);
        Files.setLastModifiedTime(own, FileTime.from(Instant.now().minus(10, ChronoUnit.HOURS)));

        MaintenanceService.LogsResult result = service.tailLogs(100, 64_000L);

        assertEquals("own.log", result.filename());
        assertTrue(result.content().contains("own-content"));
        assertFalse(result.content().contains("foreign-secret"));
    }

    @Test
    void tailLogs_refuses_whenOnlyForeignFilesExist() throws Exception {
        String installId = InstallIdentity.loadOrCreate(root);
        Files.writeString(logDir.resolve(MaintenanceService.LOG_DIR_INSTALL_MARKER),
                installId, StandardCharsets.UTF_8);
        writeFile(logDir, "foreign.log", "foreign-secret", 1);
        Files.writeString(logDir.resolve("foreign.log.sq-install-id"),
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", StandardCharsets.UTF_8);

        MaintenanceService.MaintenanceException ex = assertThrows(
                MaintenanceService.MaintenanceException.class, () -> service.tailLogs(100, 64_000L));
        assertEquals(MaintenanceService.REFUSE_NO_ELIGIBLE_LOGS, ex.getMessage());
    }

    @Test
    void tailLogs_refuses_whenSidecarBlank() throws Exception {
        String installId = InstallIdentity.loadOrCreate(root);
        Files.writeString(logDir.resolve(MaintenanceService.LOG_DIR_INSTALL_MARKER),
                installId, StandardCharsets.UTF_8);
        writeFile(logDir, "mystery.log", "mystery", 1);
        Files.writeString(logDir.resolve("mystery.log.sq-install-id"), "  \n", StandardCharsets.UTF_8);

        MaintenanceService.MaintenanceException ex = assertThrows(
                MaintenanceService.MaintenanceException.class, () -> service.tailLogs(100, 64_000L));
        assertEquals(MaintenanceService.REFUSE_NO_ELIGIBLE_LOGS, ex.getMessage());
    }

    @Test
    void tailLogs_refuses_whenDirClaimMismatch_onAnExternalLogDir() throws Exception {
        // An operator-chosen dir OUTSIDE the data dir carries no ownership guarantee — another
        // install's logs really can live there — so a foreign marker stays fail-closed.
        Path external = Files.createDirectories(externalRoot.resolve("shared-logs"));
        props.setLogDir(external.toString());
        writeFile(external, "app.log", "hello", 1);
        Files.writeString(external.resolve(MaintenanceService.LOG_DIR_INSTALL_MARKER),
                "ffffffff-0000-1111-2222-333333333333", StandardCharsets.UTF_8);

        MaintenanceService.MaintenanceException ex = assertThrows(
                MaintenanceService.MaintenanceException.class, () -> service.tailLogs(100, 64_000L));
        assertEquals(MaintenanceService.REFUSE_DIR_CLAIM_MISMATCH, ex.getMessage());
    }

    @Test
    void logs_rebindTheMarker_whenTheLogDirLivesUnderThisInstallsDataDir() throws Exception {
        // The installer default is {SQS_DATA_DIR}/logs, and a reinstall purges data/channel (new
        // install id) while KEEPING data/logs and its old marker. That mismatch is a previous
        // generation of this same install, so the dir re-binds instead of locking the operator out.
        writeFile(logDir, "app.log", "hello", 1);
        Path marker = logDir.resolve(MaintenanceService.LOG_DIR_INSTALL_MARKER);
        Files.writeString(marker, "ffffffff-0000-1111-2222-333333333333", StandardCharsets.UTF_8);

        String tail = service.tailLogs(100, 64_000L).content();

        assertTrue(tail.contains("hello"));
        assertEquals(InstallIdentity.loadOrCreate(root),
                Files.readString(marker, StandardCharsets.UTF_8).trim());
    }

    @Test
    void tailLogs_refuses_whenDirMarkerBlank() throws Exception {
        writeFile(logDir, "app.log", "hello", 1);
        Files.writeString(logDir.resolve(MaintenanceService.LOG_DIR_INSTALL_MARKER),
                "   \n", StandardCharsets.UTF_8);

        MaintenanceService.MaintenanceException ex = assertThrows(
                MaintenanceService.MaintenanceException.class, () -> service.tailLogs(100, 64_000L));
        assertEquals(MaintenanceService.REFUSE_DIR_MARKER_UNIDENTIFIABLE, ex.getMessage());
    }

    @Test
    void tailLogs_emptyDir_returnsEmptyContent() {
        MaintenanceService.LogsResult result = service.tailLogs(100, 64_000L);

        assertEquals("daemon-log-file", result.source());
        assertEquals("", result.content());
        assertEquals(0, result.lineCount());
    }

    @Test
    void tailLogs_skipsSymlinkLogFiles() throws Exception {
        writeFile(logDir, "app.log", "keep", 5);
        Path outside = externalRoot.resolve("secret.log");
        Files.writeString(outside, "do-not-tail");
        Path link = logDir.resolve("escape.log");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (Exception ex) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "symlinks not available: " + ex);
        }
        MaintenanceService.LogsResult result = service.tailLogs(100, 64_000L);
        assertEquals("app.log", result.filename());
        assertEquals("keep", result.content());
        assertTrue(Files.exists(outside));
        assertTrue(Files.exists(link, java.nio.file.LinkOption.NOFOLLOW_LINKS));
    }
}
