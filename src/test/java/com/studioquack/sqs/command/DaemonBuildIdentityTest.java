package com.studioquack.sqs.command;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The boot jar path from Spring Boot's nested code-source location. A Windows daemon reported
 * {@code InvalidPathException: Illegal char <:> … nested:/C:/Users/…} instead of its jar sha, and on
 * macOS / Linux the {@code nested:} prefix made the path miss silently.
 */
class DaemonBuildIdentityTest {

    @Test
    void nestedLocationResolvesToTheJarOnDisk() throws Exception {
        Path dir = Files.createTempDirectory("sq build id");
        Path jar = Files.writeString(dir.resolve("sqs-client-daemon-1.0.0.jar"), "jar");
        String location = "nested:" + jar.toUri().getPath() + "/!BOOT-INF/classes/!/";

        Path resolved = DaemonBuildIdentity.jarPathOf(location);

        assertEquals(jar.toRealPath(), resolved.toRealPath());
        assertTrue(Files.isRegularFile(resolved));
    }

    @Test
    void windowsDrivePathDoesNotThrow() throws Exception {
        Path resolved = DaemonBuildIdentity.jarPathOf(
                "nested:/C:/Users/knylm/AppData/Local/StudioQuack/sqs/sqs-client-daemon-1.0.0.jar/!BOOT-INF/classes/!/");

        assertEquals("sqs-client-daemon-1.0.0.jar", resolved.getFileName().toString());
    }

    @Test
    void plainFileLocationStillWorks() throws Exception {
        Path resolved = DaemonBuildIdentity.jarPathOf("file:/opt/sqs/sqs-client-daemon-1.0.0.jar!/BOOT-INF/classes!/");

        assertEquals("sqs-client-daemon-1.0.0.jar", resolved.getFileName().toString());
    }
}
