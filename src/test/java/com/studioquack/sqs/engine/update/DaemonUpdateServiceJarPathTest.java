package com.studioquack.sqs.engine.update;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The update writes the download beside the running boot jar, so the jar must resolve to a real
 * file path. 2026-11-04: the code source of a Spring Boot 3 fat jar reads {@code
 * jar:nested:/…/sqs.jar/!BOOT-INF/classes/!/}; resolved as-is it became a read-only nested-jar path
 * and every "Update daemon" aborted with UnsupportedOperationException.
 */
class DaemonUpdateServiceJarPathTest {

    @Test
    void nestedBootJarCodeSource_resolvesToTheJarFileOnDisk() {
        Path jar = DaemonUpdateService.jarPathFrom(
                "jar:nested:/opt/studioquack/sqs/sqs.jar/!BOOT-INF/classes/!/", "");

        assertEquals(Path.of("/opt/studioquack/sqs/sqs.jar"), jar);
        assertEquals("file", jar.getFileSystem().provider().getScheme());
    }

    @Test
    void escapedSpacesInTheInstallPath_areDecoded() {
        Path jar = DaemonUpdateService.jarPathFrom(
                "jar:nested:/Users/a%20b/StudioQuack/sqs.jar/!BOOT-INF/classes/!/", "");

        assertEquals(Path.of("/Users/a b/StudioQuack/sqs.jar"), jar);
    }

    @Test
    void plainFileJar_andClassPathFallback() {
        assertEquals(Path.of("/srv/sqs.jar"), DaemonUpdateService.jarPathFrom("file:/srv/sqs.jar", ""));
        assertEquals(Path.of("/srv/sqs.jar"),
                DaemonUpdateService.jarPathFrom("file:/work/target/classes/", "/srv/sqs.jar"));
    }

    @Test
    void notRunningFromAJar_isRefused() {
        assertThrows(IllegalStateException.class,
                () -> DaemonUpdateService.jarPathFrom("file:/work/target/classes/", "/work/target/classes"));
    }
}
