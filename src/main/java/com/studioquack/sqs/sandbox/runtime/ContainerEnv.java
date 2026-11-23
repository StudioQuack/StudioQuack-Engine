package com.studioquack.sqs.sandbox.runtime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Container environment values must not appear in the podman argv. {@code /proc/<pid>/cmdline} is
 * world-readable and is what process auditing records.
 */
public final class ContainerEnv {

    /**
     * Env names that change how the podman <em>client</em> resolves binaries,
     * libraries, or its API socket. Never placed in the client environment.
     */
    static final Set<String> CLIENT_CONTROL = Set.of(
            "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_DEBUG", "LD_PROFILE", "LD_ORIGIN_PATH",
            "DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH", "DYLD_FRAMEWORK_PATH",
            "DYLD_FALLBACK_LIBRARY_PATH", "DYLD_FALLBACK_FRAMEWORK_PATH",
            "PATH", "HOME", "USER", "LOGNAME", "SHELL", "TMPDIR", "TMP", "TEMP",
            "XDG_RUNTIME_DIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME",
            "DBUS_SESSION_BUS_ADDRESS", "SSH_AUTH_SOCK");

    /** Argv plus the child-process environment podman will read for bare {@code -e NAME}. */
    public record Prepared(List<String> argv, Map<String, String> childEnv, Path envFile) {
    }

    private ContainerEnv() {
    }

    /** Appends {@code -e NAME} for every entry. Values are not placed in argv. */
    public static void appendNameOnly(List<String> argv, Map<String, String> env) {
        if (env == null || env.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> e : env.entrySet()) {
            if (e.getKey() == null || e.getKey().isBlank()) {
                continue;
            }
            argv.add("-e");
            argv.add(e.getKey());
        }
    }

    /**
     * Splits {@code containerEnv} into the child-process environment and, when a
     * name would retarget the podman client, a 0600 env-file. {@code argv} is
     * the name-only form from {@link #appendNameOnly}; client-control names are
     * removed from it and replaced with {@code --env-file}.
     */
    public static Prepared prepare(List<String> argv, Map<String, String> containerEnv, Path dataDir) {
        Map<String, String> child = new LinkedHashMap<>();
        Map<String, String> file = new LinkedHashMap<>();
        if (containerEnv != null) {
            for (Map.Entry<String, String> e : containerEnv.entrySet()) {
                if (e.getKey() == null || e.getKey().isBlank()) {
                    continue;
                }
                String value = e.getValue() == null ? "" : e.getValue();
                if (CLIENT_CONTROL.contains(e.getKey())) {
                    file.put(e.getKey(), value);
                } else {
                    child.put(e.getKey(), value);
                }
            }
        }
        List<String> out = stripClientControlFlags(argv, file.keySet());
        Path envFile = null;
        if (!file.isEmpty()) {
            if (dataDir == null) {
                throw new IllegalStateException("container env-file requires the daemon data directory");
            }
            envFile = writeEnvFile(dataDir, file);
            if (out.size() >= 2) {
                out.add(2, envFile.toAbsolutePath().toString());
                out.add(2, "--env-file");
            } else {
                out.add("--env-file");
                out.add(envFile.toAbsolutePath().toString());
            }
        }
        return new Prepared(List.copyOf(out), Map.copyOf(child), envFile);
    }

    /** Daemon data directory: {@code sqs.dataDir} property, else {@code SQS_DATA_DIR}, else {@code ./data}. */
    public static Path dataDir() {
        String configured = System.getProperty("sqs.dataDir");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("SQS_DATA_DIR");
        }
        if (configured == null || configured.isBlank()) {
            configured = "./data";
        }
        return Path.of(configured);
    }

    public static void deleteQuietly(Path file) {
        if (file == null) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignore) {
            // best-effort; the file is mode 0600 under the data dir
        }
    }

    private static List<String> stripClientControlFlags(List<String> argv, Set<String> names) {
        if (argv == null || argv.isEmpty() || names.isEmpty()) {
            return argv == null ? List.of() : new ArrayList<>(argv);
        }
        List<String> out = new ArrayList<>(argv.size());
        for (int i = 0; i < argv.size(); i++) {
            String token = argv.get(i);
            if ("-e".equals(token) && i + 1 < argv.size()) {
                String next = argv.get(i + 1);
                int eq = next.indexOf('=');
                String name = eq < 0 ? next : next.substring(0, eq);
                if (names.contains(name)) {
                    i++;
                    continue;
                }
            }
            out.add(token);
        }
        return out;
    }

    /**
     * {@code KEY=VALUE} lines, mode 0600, under {@code <dataDir>/container-env}
     * (directory 0700). Values never appear in the returned path.
     */
    private static Path writeEnvFile(Path dataDir, Map<String, String> vars) {
        StringBuilder body = new StringBuilder();
        for (Map.Entry<String, String> e : vars.entrySet()) {
            if (e.getKey().indexOf('=') >= 0 || e.getKey().indexOf('\n') >= 0 || e.getKey().indexOf('\0') >= 0
                    || e.getValue().indexOf('\n') >= 0 || e.getValue().indexOf('\r') >= 0
                    || e.getValue().indexOf('\0') >= 0) {
                throw new IllegalStateException(
                        "container env " + e.getKey() + " cannot be passed to the sandbox");
            }
            body.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        try {
            Path dir = dataDir.toAbsolutePath().normalize().resolve("container-env");
            Files.createDirectories(dir);
            posix(dir, "rwx------");
            Path tmp = dir.resolve("env-" + UUID.randomUUID() + ".tmp");
            Files.writeString(tmp, body.toString(), StandardCharsets.UTF_8);
            posix(tmp, "rw-------");
            Path dest = dir.resolve("env-" + UUID.randomUUID() + ".env");
            try {
                Files.move(tmp, dest, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING);
            }
            posix(dest, "rw-------");
            return dest;
        } catch (IOException e) {
            throw new IllegalStateException("could not write the container env-file: " + e.getClass().getSimpleName());
        }
    }

    private static void posix(Path path, String mode) {
        try {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(mode));
        } catch (UnsupportedOperationException ignore) {
            // non-POSIX (Windows): the directory is still under the daemon data dir
        } catch (IOException ignore) {
            // best-effort; the subsequent re-read of seccomp is a different path
        }
    }
}
