package com.studioquack.sqs.sandbox.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * CLI-based OCI runtime detection. Pure static helpers so unit tests can drive them without a
 * Spring context; {@link ContainerRuntime} owns the cached, injected instance.
 */
public final class OciRuntimeDetector {

    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(3);
    /**
     * {@code podman info} walks storage, network and runtime versions; on a freshly booted host,
     * while the daemon's own dependencies are still starting, it routinely exceeds
     * {@link #PROBE_TIMEOUT}. It runs only when the runtime is (re-)detected, not per dispatch.
     */
    private static final Duration INFO_TIMEOUT = Duration.ofSeconds(20);

    private OciRuntimeDetector() {
    }

    /** Resolved CLI: the binary path + kind, or {@code null} when no container CLI is on PATH. */
    public record ResolvedCli(String cliPath, OciRuntimeKind kind) {
    }

    /**
     * Walks the Podman-first preference list and returns the first CLI on PATH, honouring an
     * explicit {@code want} override. Returns {@code null} when nothing is installed.
     */
    public static ResolvedCli resolveCli(OciRuntimeKind want) {
        if (want != null) {
            String path = CliRunner.lookPath(want.cli());
            return path == null ? null : new ResolvedCli(path, want);
        }
        for (OciRuntimeKind k : OciRuntimeKind.preferenceOrder()) {
            String path = CliRunner.lookPath(k.cli());
            if (path != null) {
                return new ResolvedCli(path, k);
            }
        }
        return null;
    }

    /**
     * Cheap daemon-liveness probe. Uses {@code <cli> version --format {{.Server.Version}}} which
     * deliberately hits the daemon and exits non-zero when it is unreachable ({@code info} exits 0
     * even when the daemon is down, so it is unsafe as a liveness probe — Go's comment).
     */
    public static String probeDaemon(String cliPath) {
        CliRunner.Result r = CliRunner.run(
                List.of(cliPath, "version", "--format", "{{.Server.Version}}"), PROBE_TIMEOUT);
        if (!r.ok()) {
            return null;
        }
        String v = r.stdout().trim();
        return v.isEmpty() ? "unknown" : v;
    }

    /**
     * Detects the low-level runtimes the resolved CLI advertises, sorted gVisor-first → default →
     * alphabetical (mirrors {@code sortDetected}). Merges a PATH-only {@code runsc} fallback when the
     * daemon doesn't advertise gVisor yet.
     */
    public static List<DetectedOciRuntime> detectRuntimes(String cliPath, OciRuntimeKind kind,
                                                          ObjectMapper mapper) {
        List<DetectedOciRuntime> out;
        try {
            out = detectPodmanRuntimes(cliPath, mapper);
        } catch (Exception e) {
            out = new ArrayList<>();
        }
        List<DetectedOciRuntime> pathRunsc = mergePathRunsc(out);
        out.addAll(pathRunsc);
        out.sort(sortComparator());
        return out;
    }

    private static List<DetectedOciRuntime> detectPodmanRuntimes(String cliPath, ObjectMapper mapper) {
        CliRunner.Result r = CliRunner.run(List.of(cliPath, "info", "--format", "json"), INFO_TIMEOUT);
        return mergeRuntimes(r.ok() ? r.stdout() : null, containersConfRuntimes(), mapper);
    }

    /**
     * Runtimes from {@code podman info} JSON (null when the call failed) plus the containers.conf
     * registrations. The registrations are merged even without {@code podman info}: a missed
     * runsc pins the daemon below the best tier until it restarts, and on a host that puts
     * stateful instances on the shared kernel.
     */
    static List<DetectedOciRuntime> mergeRuntimes(String podmanInfoJson, Map<String, String> confRuntimes,
                                                  ObjectMapper mapper) {
        List<DetectedOciRuntime> out = new ArrayList<>();
        String def = "";
        Map<String, Boolean> seen = new LinkedHashMap<>();
        try {
            JsonNode host = podmanInfoJson == null ? mapper.missingNode()
                    : mapper.readTree(podmanInfoJson).path("host");
            def = host.path("ociRuntime").path("name").asText("");
            if (!def.isEmpty()) {
                out.add(DetectedOciRuntime.of(def, def, "podman"));
                seen.put(def, true);
            }
            JsonNode runtimes = host.path("ociRuntimes");
            String defaultName = def;
            runtimes.fieldNames().forEachRemaining(name -> {
                if (!seen.containsKey(name)) {
                    out.add(DetectedOciRuntime.of(name, defaultName, "podman"));
                    seen.put(name, true);
                }
            });
        } catch (Exception ignore) {
            // malformed podman info → config-layer registrations only
        }
        // Podman info reports only the DEFAULT runtime (host.ociRuntime) — the [engine.runtimes] table
        // from containers.conf is never advertised, so a runsc registered there (the VM bootstrap writes
        // /etc/containers/containers.conf) would otherwise surface only via the PATH fallback, whose
        // source="path" the tier check deliberately distrusts, and a gVisor-equipped host is mis-detected
        // as intermediate. Merge the config-layer registrations directly, with source="podman".
        String defaultName = def;
        confRuntimes.forEach((name, bin) -> {
            if (!seen.containsKey(name) && runtimeBinaryResolves(name, bin)) {
                out.add(DetectedOciRuntime.of(name, defaultName, "podman"));
                seen.put(name, true);
            }
        });
        return out;
    }

    private static final Pattern TOML_SECTION = Pattern.compile("^\\s*\\[([^]]+)]\\s*(?:#.*)?$");
    private static final Pattern TOML_KEY = Pattern.compile("^\\s*([A-Za-z0-9_.-]+)\\s*=\\s*(.*)$");
    private static final Pattern QUOTED = Pattern.compile("\"([^\"]+)\"");

    /**
     * Registered runtime name → first configured binary path, read from the containers.conf
     * layers in podman's merge order ({@code /usr/share} → {@code /etc} →
     * {@code /etc/containers/containers.conf.d/*.conf} → {@code $XDG_CONFIG_HOME} /
     * {@code ~/.config}). Only the {@code [engine.runtimes]} table is read; a later layer
     * re-defining the same key wins, mirroring podman's key-wise map merge.
     */
    static Map<String, String> containersConfRuntimes() {
        Map<String, String> runtimes = new LinkedHashMap<>();
        List<Path> layers = new ArrayList<>();
        layers.add(Path.of("/usr/share/containers/containers.conf"));
        layers.add(Path.of("/etc/containers/containers.conf"));
        Path dropIns = Path.of("/etc/containers/containers.conf.d");
        try (Stream<Path> stream = Files.isDirectory(dropIns) ? Files.list(dropIns) : Stream.empty()) {
            stream.filter(p -> p.getFileName().toString().endsWith(".conf")).sorted()
                    .forEach(layers::add);
        } catch (IOException ignore) {
            // unreadable drop-in dir — the remaining layers still apply
        }
        String xdg = System.getenv("XDG_CONFIG_HOME");
        String home = System.getProperty("user.home");
        if (xdg != null && !xdg.isBlank()) {
            layers.add(Path.of(xdg, "containers", "containers.conf"));
        } else if (home != null && !home.isBlank()) {
            layers.add(Path.of(home, ".config", "containers", "containers.conf"));
        }
        for (Path layer : layers) {
            parseRuntimesLayer(layer, runtimes);
        }
        return runtimes;
    }

    /** Reads one containers.conf layer, collecting {@code [engine.runtimes]} key → binary path. */
    static void parseRuntimesLayer(Path layer, Map<String, String> runtimes) {
        List<String> lines;
        try {
            if (layer == null || !Files.isRegularFile(layer) || !Files.isReadable(layer)) {
                return;
            }
            lines = Files.readAllLines(layer);
        } catch (IOException | RuntimeException ex) {
            return;
        }
        boolean inRuntimes = false;
        for (int i = 0; i < lines.size(); i++) {
            Matcher section = TOML_SECTION.matcher(lines.get(i));
            if (section.matches()) {
                inRuntimes = "engine.runtimes".equals(section.group(1).trim());
                continue;
            }
            if (!inRuntimes) {
                continue;
            }
            Matcher key = TOML_KEY.matcher(lines.get(i));
            if (!key.matches()) {
                continue;
            }
            String value = key.group(2);
            // TOML arrays may span lines (the bootstrap writes runsc = [\n  "/path",\n])
            while (value.contains("[") && !value.contains("]") && i + 1 < lines.size()) {
                value += " " + lines.get(++i);
            }
            Matcher quoted = QUOTED.matcher(value);
            runtimes.put(key.group(1), quoted.find() ? quoted.group(1) : "");
        }
    }

    /**
     * A registered runtime counts only when its binary resolves: the configured absolute path is
     * executable, or the name is on PATH. A dangling registration must not claim the best tier.
     */
    private static boolean runtimeBinaryResolves(String name, String configuredPath) {
        if (configuredPath != null && configuredPath.startsWith("/")) {
            return Files.isExecutable(Path.of(configuredPath));
        }
        return CliRunner.lookPath(name) != null;
    }

    /** Surfaces {@code runsc} on PATH when the daemon doesn't advertise gVisor yet (informational). */
    private static List<DetectedOciRuntime> mergePathRunsc(List<DetectedOciRuntime> canonical) {
        if (CliRunner.lookPath("runsc") == null) {
            return List.of();
        }
        for (DetectedOciRuntime c : canonical) {
            if ("runsc".equalsIgnoreCase(c.name())) {
                return List.of();
            }
        }
        return List.of(new DetectedOciRuntime("runsc",
                "gVisor (runsc — not registered with container daemon)", true, false, "path"));
    }

    private static Comparator<DetectedOciRuntime> sortComparator() {
        return (a, b) -> {
            if (a.gvisor() != b.gvisor()) {
                return a.gvisor() ? -1 : 1; // gVisor first
            }
            if (a.isDefault() != b.isDefault()) {
                return a.isDefault() ? -1 : 1; // then the daemon default
            }
            return a.name().compareTo(b.name());
        };
    }
}
