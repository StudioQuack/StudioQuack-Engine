package com.studioquack.sqs.sandbox.oci;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;

/**
 * Materialises the RN-05 seccomp baseline (a classpath resource) to a real on-disk file so the
 * container CLI can reference it via {@code --security-opt seccomp=<path>}. Podman can only point
 * {@code seccomp=} at a real file, never a jar entry, so we copy the resource out once and cache
 * the path.
 */
@Slf4j
public final class SeccompProfile {

    private static final String RESOURCE = "/sandbox/seccomp-baseline.json";
    private static volatile Path dataDir;
    private static volatile byte[] expected;

    private SeccompProfile() {
    }

    /** Bind the daemon data directory. Called once at startup; tests call it with a temp dir. */
    public static void configure(Path dir) {
        dataDir = dir == null ? null : dir.toAbsolutePath().normalize();
        expected = null;
    }

    /**
     * @return The absolute path of the materialised baseline, or {@code null} when it could not b.
     * @throws IllegalStateException When {@code required} and the baseline cannot be trusted.
     */
    public static String profilePath(boolean required) {
        try {
            Path file = materialise(required);
            if (file == null) {
                return null;
            }
            if (!matches(file)) {
                // Another local user can replace the file between the move and this read.
                String msg = "seccomp baseline at " + file + " does not match the classpath resource";
                if (required) {
                    throw new IllegalStateException(msg);
                }
                log.warn("seccomp: {} — omitting --security-opt seccomp; runtime default applies", msg);
                return null;
            }
            return file.toAbsolutePath().toString();
        } catch (IllegalStateException e) {
            if (required) {
                throw e;
            }
            log.warn("seccomp: {} — omitting --security-opt seccomp; runtime default applies", e.getMessage());
            return null;
        }
    }

    /** Optional seccomp: mismatch or a missing data dir omits the flag instead of failing the run. */
    public static String profilePath() {
        return profilePath(false);
    }

    private static Path materialise(boolean required) {
        byte[] bytes = expectedBytes();
        if (bytes == null) {
            fail(required, "baseline resource " + RESOURCE + " not found on classpath");
            return null;
        }
        Path root = dataDir;
        if (root == null) {
            fail(required, "data directory is not configured");
            return null;
        }
        try {
            Path dir = root.resolve("seccomp");
            Files.createDirectories(dir);
            posix(dir, "rwx------");
            Path profile = dir.resolve("seccomp-baseline.json");
            if (Files.isRegularFile(profile) && matches(profile, bytes)) {
                return profile;
            }
            Path tmp = dir.resolve("seccomp-baseline.json.tmp");
            Files.write(tmp, bytes);
            posix(tmp, "rw-------");
            try {
                Files.move(tmp, profile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, profile, StandardCopyOption.REPLACE_EXISTING);
            }
            posix(profile, "rw-------");
            // NOTE: seccomp=<path> REPLACES the runtime's default profile (no stacking).
            log.info("seccomp: baseline materialised to {} (replaces the runtime default profile).", profile);
            return profile;
        } catch (Exception e) {
            fail(required, "could not materialise the baseline (" + e.getClass().getSimpleName() + ")");
            return null;
        }
    }

    private static void fail(boolean required, String message) {
        if (required) {
            throw new IllegalStateException("seccomp: " + message);
        }
        log.warn("seccomp: {} — omitting --security-opt seccomp; runtime default applies", message);
    }

    /** True when {@code file} still holds the classpath baseline. Package-visible for tamper tests. */
    static boolean contentMatches(Path file) {
        return matches(file);
    }

    private static boolean matches(Path file) {
        byte[] bytes = expectedBytes();
        return bytes != null && matches(file, bytes);
    }

    private static boolean matches(Path file, byte[] bytes) {
        try {
            return Arrays.equals(bytes, Files.readAllBytes(file));
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] expectedBytes() {
        byte[] cached = expected;
        if (cached != null) {
            return cached;
        }
        synchronized (SeccompProfile.class) {
            if (expected != null) {
                return expected;
            }
            try (InputStream in = SeccompProfile.class.getResourceAsStream(RESOURCE)) {
                if (in == null) {
                    return null;
                }
                expected = in.readAllBytes();
                return expected;
            } catch (Exception e) {
                return null;
            }
        }
    }

    private static void posix(Path path, String mode) {
        try {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(mode));
        } catch (UnsupportedOperationException ignore) {
            // non-POSIX: skip. The file is still under the daemon data directory.
        } catch (Exception ignore) {
            // best-effort; the re-read compare is what refuses a tampered file
        }
    }
}
