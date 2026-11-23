package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

/**
 * Host capability probe: does this engine's container runtime actually offer GPU passthrough to
 * containers? The answer gates the per-image warm-pool {@code gpu} cap end-to-end: the daemon
 * refuses a gpu cap it cannot honour (a cap that fails every warm create is worse than no option),
 * and the FE renders the gpu field only when this reports {@code true}.
 */
@Slf4j
@Component
public class GpuCapabilityProbe {

    /** Cache window for the probe result; the subprocess is far too expensive for per-call use. */
    private static final long CACHE_TTL_MS = 60_000L;
    private static final Duration OP_TIMEOUT = Duration.ofSeconds(10);

    /** CDI spec directories scanned (in-machine via ssh, or on the host for native podman). */
    private static final List<String> CDI_DIRS = List.of("/etc/cdi", "/var/run/cdi");
    /** Every GPU vendor class podman/CDI knows ends in this substring (nvidia.com/gpu, …). */
    private static final String GPU_CLASS_MARKER = "com/gpu";

    private final ContainerRuntime runtime;
    private final ObjectMapper mapper;
    /** Native-mode spec dirs; a bean ctor seam so tests never touch the host's real /etc. */
    private final List<Path> nativeCdiDirs;

    private volatile Boolean cached;
    private volatile long cachedAtMs;

    @org.springframework.beans.factory.annotation.Autowired
    public GpuCapabilityProbe(ContainerRuntime runtime, ObjectMapper mapper) {
        this(runtime, mapper, CDI_DIRS.stream().map(Path::of).toList());
    }

    /** Test seam: pin the native-mode CDI directories (the machine path stays CLI-driven). */
    public GpuCapabilityProbe(ContainerRuntime runtime, ObjectMapper mapper, List<Path> nativeCdiDirs) {
        this.runtime = runtime;
        this.mapper = mapper;
        this.nativeCdiDirs = nativeCdiDirs == null ? List.of() : List.copyOf(nativeCdiDirs);
    }

    /** True when the runtime can honour a {@code --gpus} cap. Cached; fail-closed on any error. */
    public boolean gpuPassthroughAvailable() {
        Boolean hit = cached;
        if (hit != null && System.currentTimeMillis() - cachedAtMs < CACHE_TTL_MS) {
            return hit;
        }
        boolean fresh = probeSafely();
        Boolean previous = cached;
        cached = fresh;
        cachedAtMs = System.currentTimeMillis();
        if (previous == null || previous != fresh) {
            // One line per TRANSITION — this feeds the FE's decision to render the gpu field.
            log.info("sandbox.gpu-passthrough available={} (probed via container runtime CDI specs)", fresh);
        }
        return fresh;
    }

    /** Drops the cached answer (operator "Re-detect" flows straight into a re-probe). */
    public void invalidate() {
        cached = null;
    }

    private boolean probeSafely() {
        try {
            return probe();
        } catch (Exception e) {
            log.debug("sandbox.gpu-passthrough probe failed (reporting unavailable): {}", e.toString());
            return false;
        }
    }

    private boolean probe() {
        if (runtime == null || !runtime.available() || runtime.cli() == null) {
            return false;
        }
        String cli = runtime.cli();
        String machine = runningMachine(cli);
        if (machine != null) {
            return probeInsideMachine(cli, machine);
        }
        return probeNative();
    }

    /**
     * The name of a RUNNING podman machine, or {@code null} when podman is native (no machines
     * configured / none running / the subcommand unanswered — a native Linux podman answers with
     * {@code []}).
     */
    private String runningMachine(String cli) {
        CliRunner.Result r = CliRunner.run(List.of(cli, "machine", "list", "--format", "json"), OP_TIMEOUT);
        if (!r.ok() || r.stdout() == null || r.stdout().isBlank()) {
            return null;
        }
        try {
            JsonNode arr = mapper.readTree(r.stdout());
            if (!arr.isArray()) {
                return null;
            }
            String firstNamed = null;
            for (JsonNode m : arr) {
                String name = m.path("Name").asText("").trim();
                if (name.isEmpty()) {
                    continue;
                }
                if (m.path("Running").asBoolean(false)) {
                    return name;
                }
                if (firstNamed == null) {
                    firstNamed = name;
                }
            }
            // Machines exist but none report Running — the runtime answered, so the stopped
            // default machine is still the place containers WOULD run; probe it anyway.
            return firstNamed;
        } catch (Exception e) {
            return null;
        }
    }

    /** Machine-backed probe: the CDI specs that count live INSIDE the machine VM. */
    private boolean probeInsideMachine(String cli, String machine) {
        // No double quotes anywhere: the argv crosses the Win32 command-line round-trip on
        // Windows hosts, which shatters quote-bearing elements (see ShExec). The marker is a
        // fixed space-free constant, so grep needs no quoting.
        String script = "grep -rl " + GPU_CLASS_MARKER + " " + String.join(" ", CDI_DIRS)
                + " 2>/dev/null | head -n 1";
        CliRunner.Result r = CliRunner.run(
                List.of(cli, "machine", "ssh", machine, "--", "sh", "-c", script), OP_TIMEOUT);
        return r.ok() && r.stdout() != null && !r.stdout().isBlank();
    }

    /** Native probe: read the host's own CDI dirs for a spec advertising a GPU vendor class. */
    private boolean probeNative() {
        for (Path dir : nativeCdiDirs) {
            if (dir == null || !Files.isDirectory(dir)) {
                continue;
            }
            try (Stream<Path> files = Files.list(dir)) {
                for (Path f : files.filter(Files::isRegularFile).toList()) {
                    try {
                        if (Files.readString(f, StandardCharsets.UTF_8).contains(GPU_CLASS_MARKER)) {
                            return true;
                        }
                    } catch (IOException | RuntimeException unreadable) {
                        log.debug("sandbox.gpu-passthrough: unreadable CDI spec {} (skipped)", f);
                    }
                }
            } catch (IOException | RuntimeException e) {
                log.debug("sandbox.gpu-passthrough: CDI dir {} not listable (skipped): {}", dir, e.toString());
            }
        }
        return false;
    }
}
