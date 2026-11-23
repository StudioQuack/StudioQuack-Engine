package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Seccomp baseline path, POSIX modes, and tamper detection. */
class SeccompProfileTest {

    @TempDir
    Path dir;

    @Test
    void writes_under_the_data_dir_with_owner_only_modes() throws Exception {
        SeccompProfile.configure(dir);
        String path = SeccompProfile.profilePath(false);
        assertNotNull(path);
        Path file = Path.of(path);
        assertEquals(dir.resolve("seccomp").resolve("seccomp-baseline.json").toAbsolutePath().normalize(),
                file.toAbsolutePath().normalize());
        assertTrue(file.startsWith(dir.toAbsolutePath().normalize()));
        if (supportsPosix()) {
            assertEquals(PosixFilePermissions.fromString("rwx------"),
                    Files.getPosixFilePermissions(file.getParent()));
            assertEquals(PosixFilePermissions.fromString("rw-------"),
                    Files.getPosixFilePermissions(file));
        }
    }

    @Test
    void tampered_bytes_are_not_trusted_and_required_refuses_an_unwritable_dir() throws Exception {
        SeccompProfile.configure(dir);
        String path = SeccompProfile.profilePath(false);
        assertNotNull(path);
        Files.writeString(Path.of(path), "{}\n");
        assertFalse(SeccompProfile.contentMatches(Path.of(path)));

        Path notADirectory = dir.resolve("not-a-dir");
        Files.writeString(notADirectory, "x");
        SeccompProfile.configure(notADirectory);
        assertNull(SeccompProfile.profilePath(false));
        assertThrows(IllegalStateException.class, () -> SeccompProfile.profilePath(true));
    }

    private static boolean supportsPosix() {
        return !System.getProperty("os.name", "").toLowerCase().contains("win");
    }
}
