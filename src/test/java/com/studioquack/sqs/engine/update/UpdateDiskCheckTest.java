package com.studioquack.sqs.engine.update;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An update on a nearly full disk took the engine offline: the staged jar fit, the new build could
 * not write its database at boot. The preflight refuses first, with the numbers the operator needs.
 */
class UpdateDiskCheckTest {

    private static final long MB = 1024L * 1024;
    private static final long JAR = 90 * MB;

    @Test
    void fitsWithTheStagedJarAndTheHeadroom() {
        assertNull(UpdateDiskCheck.shortfall(JAR, JAR + UpdateDiskCheck.HEADROOM_BYTES, "C:", 50_000 * MB, "C:"));
    }

    @Test
    void refusesWhenTheJarVolumeCannotHoldTheStagedJarPlusHeadroom() {
        String why = UpdateDiskCheck.shortfall(JAR, 1500 * MB, "OS (C:)", 1500 * MB, "OS (C:)");

        assertTrue(why.startsWith("not enough free disk space to update: 1.5 GB free on OS (C:)"), why);
        assertTrue(why.contains("about 2.1 GB needed"), why);
    }

    @Test
    void refusesWhenASeparateDataVolumeIsFull() {
        String why = UpdateDiskCheck.shortfall(JAR, 100_000 * MB, "/", 300 * MB, "/data (/dev/sdb1)");

        assertTrue(why.contains("0.3 GB free on /data (/dev/sdb1)"), why);
    }

    @Test
    void messageIsAsciiOnly() {
        String why = UpdateDiskCheck.shortfall(JAR, 0, "C:", 0, "C:");

        assertTrue(why.chars().allMatch(c -> c >= 0x20 && c < 0x7F), why);
    }

    @Test
    void measuresARealJarOnARealVolume(@TempDir Path dir) throws Exception {
        Path jar = Files.write(dir.resolve("sqs.jar"), new byte[1024]);

        String why = UpdateDiskCheck.shortfall(jar, dir);

        // The build machine has more than 2 GB free; a real verdict either way must not throw.
        assertTrue(why == null || why.startsWith("not enough free disk space"), String.valueOf(why));
    }
}
