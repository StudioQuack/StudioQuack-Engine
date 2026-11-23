package com.studioquack.sqs.sandbox.warmpool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.egress.StubSandboxEgressManager;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import com.studioquack.sqs.sandbox.runtime.CapabilityReport;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.runtime.OciRuntimeKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OciWarmer#warm} must emit podman argv that carries the WarmSpec caps ({@code
 * --memory}/{@code --memory-swap}, {@code --cpus}, {@code /sandbox} tmpfs sized by {@code diskMb}).
 * Uses a fake CLI script (same ReflectionTestUtils pin pattern as {@code
 * ContainerRuntimeIsolationEnforceTest}) so the test never needs a live Podman.
 */
class OciWarmerCapsArgvTest {

    @TempDir
    Path tempDir;

    @Test
    void warm_emits_memory_cpus_and_sandbox_tmpfs_from_warm_spec_caps() throws Exception {
        Path log = tempDir.resolve("argv.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);

        OciWarmer warmer = new OciWarmer(
                runtime,
                "",
                null,
                false,
                120,
                null,
                false,
                10,
                SandboxOwnership.of("owner-caps", "boot-caps"),
                image -> "python", // script-mode path; broker/runner disabled above
                0.0,
                0);

        WarmSlot slot = warmer.warm(new Warmer.WarmSpec(
                "python:3.11-slim", "python", 1.5, 256, 128));

        assertNotNull(slot);
        assertTrue(slot.hasContainer());

        List<String> runArgv = firstRunArgv(log);
        assertTrue(pair(runArgv, "--memory", "256m") >= 0, "ramMb → --memory: " + runArgv);
        assertTrue(pair(runArgv, "--memory-swap", "256m") >= 0, "swap disabled: " + runArgv);
        assertTrue(pair(runArgv, "--cpus", "1.5") >= 0, "cpus → --cpus: " + runArgv);
        assertTrue(pair(runArgv, "--tmpfs",
                        "/sandbox:rw,nosuid,nodev,size=128m,mode=1777") >= 0,
                "diskMb → /sandbox tmpfs: " + runArgv);
        assertTrue(pair(runArgv, "--gpus", "1") < 0 && !runArgv.contains("--gpus"),
                "no gpu in the spec → no --gpus flag: " + runArgv);
    }

    /* Custom (entrypoint) images: ENTRYPOINT is the per-job exec argv */

    @Test
    void warm_of_a_custom_image_clears_its_entrypoint_so_the_slot_idles_on_sleep() throws Exception {
        // The parked container's main process must be the hardening `sleep`, not the image's
        // ENTRYPOINT: a one-shot app would exit at create and --rm would delete the slot before
        // any dispatch could exec into it ("no such container").
        Path log = tempDir.resolve("argv-entrypoint.log");
        Path fakeCli = installFakePodmanWithEntrypoint(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);

        OciWarmer warmer = new OciWarmer(
                runtime, "", null, false, 120, null, false, 10,
                SandboxOwnership.of("owner-caps", "boot-caps"),
                image -> null, // not a known script-mode default → custom (entrypoint) image
                0.0, 0);

        WarmSlot slot = warmer.warm(new Warmer.WarmSpec("localhost/sq-local/node-x:1", null, 0, 0, 0));

        assertTrue(slot.isEntrypointImage(), "custom image slot records the entrypoint for the per-job exec");
        List<String> runArgv = firstRunArgv(log);
        int entrypoint = pair(runArgv, "--entrypoint", "");
        assertTrue(entrypoint >= 0, "custom-image warm must reset ENTRYPOINT: " + runArgv);
        assertTrue(entrypoint < runArgv.indexOf("localhost/sq-local/node-x:1"),
                "--entrypoint \"\" precedes the image: " + runArgv);
    }

    @Test
    void warm_of_a_script_mode_image_keeps_the_entrypoint_flag_out() throws Exception {
        Path log = tempDir.resolve("argv-script-mode.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);

        OciWarmer warmer = new OciWarmer(
                runtime, "", null, false, 120, null, false, 10,
                SandboxOwnership.of("owner-caps", "boot-caps"),
                image -> "python",
                0.0, 0);

        warmer.warm(new Warmer.WarmSpec("python:3.11-slim", "python", 0, 0, 0));

        assertFalse(firstRunArgv(log).contains("--entrypoint"),
                "script-mode images run their own Cmd/bootstrap — no ENTRYPOINT reset");
    }

    /* Gpu cap: probe-gated, shared flag shape */

    @Test
    void warm_emits_gpus_flag_when_host_reports_passthrough() throws Exception {
        Path log = tempDir.resolve("argv-gpu.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);
        com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe probe = probeWithCdiSpec(runtime, true);

        OciWarmer warmer = new OciWarmer(
                runtime, "", null, false, 120, null, false, 10,
                SandboxOwnership.of("owner-caps", "boot-caps"),
                image -> "python", 0.0, 0, null, probe);

        WarmSlot slot = warmer.warm(new Warmer.WarmSpec(
                "python:3.11-slim", "python", 1.5, 256, 128, 2));

        assertNotNull(slot);
        List<String> runArgv = firstRunArgv(log);
        assertTrue(pair(runArgv, "--gpus", "2") >= 0,
                "D110 — gpu cap → --gpus 2 (same flag shape as the stateful path): " + runArgv);
        // And the caps still ride alongside.
        assertTrue(pair(runArgv, "--memory", "256m") >= 0, "ramMb → --memory: " + runArgv);
        assertTrue(pair(runArgv, "--cpus", "1.5") >= 0, "cpus → --cpus: " + runArgv);
    }

    @Test
    void warm_refuses_gpu_cap_when_host_has_no_passthrough() throws Exception {
        Path log = tempDir.resolve("argv-gpu-refused.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);
        com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe probe = probeWithCdiSpec(runtime, false);

        OciWarmer warmer = new OciWarmer(
                runtime, "", null, false, 120, null, false, 10,
                SandboxOwnership.of("owner-caps", "boot-caps"),
                image -> "python", 0.0, 0, null, probe);

        Warmer.WarmException ex = assertThrows(Warmer.WarmException.class, () ->
                warmer.warm(new Warmer.WarmSpec("python:3.11-slim", "python", 1.0, 256, 128, 1)));

        assertTrue(ex.getMessage().contains("no GPU passthrough"), ex.getMessage());
        assertFalse(Files.isRegularFile(log) && Files.readString(log).contains("run\t"),
                "a refused gpu cap must never reach podman run");
    }

    @Test
    void warm_refuses_gpu_cap_when_probe_absent() throws Exception {
        // Null probe (channel-less/test wiring) = capability unknown = fail closed.
        Path log = tempDir.resolve("argv-gpu-noprobe.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);

        OciWarmer warmer = new OciWarmer(
                runtime, "", null, false, 120, null, false, 10,
                SandboxOwnership.of("owner-caps", "boot-caps"),
                image -> "python", 0.0, 0);

        assertThrows(Warmer.WarmException.class, () ->
                warmer.warm(new Warmer.WarmSpec("python:3.11-slim", "python", 1.0, 256, 128, 1)));
    }

    /** A probe backed by a temp CDI dir; {@code withGpuSpec} plants an nvidia vendor spec. */
    private com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe probeWithCdiSpec(
            ContainerRuntime runtime, boolean withGpuSpec) throws Exception {
        Path cdi = tempDir.resolve(withGpuSpec ? "cdi-gpu" : "cdi-empty");
        Files.createDirectories(cdi);
        if (withGpuSpec) {
            Files.writeString(cdi.resolve("nvidia.yaml"),
                    "cdiVersion: 0.5.0\nkind: nvidia.com/gpu\ndevices: []\n");
        }
        // The fake CLI answers `machine list` with empty stdout → the probe treats the runtime as
        // NATIVE podman and scans the injected CDI dirs — the seam under test.
        return new com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe(
                runtime, new ObjectMapper(), List.of(cdi));
    }


    @Test
    void warm_refuses_when_egress_unavailable_without_spawn() throws Exception {
        Path log = tempDir.resolve("refuse.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);

        OciWarmer warmer = new OciWarmer(
                runtime, "", null, false, 120, null, false, 10,
                SandboxOwnership.of("owner-caps", "boot-caps"),
                image -> "python", 0.0, 0,
                StubSandboxEgressManager.unavailable("overlay-down"));

        Warmer.WarmException ex = assertThrows(Warmer.WarmException.class, () ->
                warmer.warm(new Warmer.WarmSpec("python:3.11-slim", "python", 1.0, 256, 128)));

        assertEquals("sandbox egress unavailable: overlay-down", ex.getMessage());
        assertFalse(Files.isRegularFile(log) && Files.readString(log).contains("run"),
                "UNAVAILABLE must not spawn a warm container");
    }

    @Test
    void warm_proceeds_when_egress_enforced() throws Exception {
        Path log = tempDir.resolve("enforced.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);

        OciWarmer warmer = new OciWarmer(
                runtime, "", null, false, 120, null, false, 10,
                SandboxOwnership.of("owner-caps", "boot-caps"),
                image -> "python", 0.0, 0,
                StubSandboxEgressManager.enforced());

        WarmSlot slot = warmer.warm(new Warmer.WarmSpec("python:3.11-slim", "python", 1.0, 256, 128));
        assertNotNull(slot);
        assertTrue(slot.hasContainer());
    }

    /**
     * Fake podman for a CUSTOM image: additionally answers {@code inspect --type image} with an
     * Entrypoint+Cmd pair, the way the built seed image ({@code ENTRYPOINT ["java", ...]}) does.
     */
    private static Path installFakePodmanWithEntrypoint(Path dir, Path log) throws Exception {
        Path cli = dir.resolve("fake-podman-ep");
        String script = """
                #!/bin/sh
                LOG="%s"
                sub="$1"
                {
                  printf '%%s' "$1"
                  shift
                  for a in "$@"; do printf '\\t%%s' "$a"; done
                  printf '\\n'
                } >> "$LOG"
                case "$sub" in
                  run) printf 'warmciddeadbeef\\n'; exit 0 ;;
                  inspect) printf '["java","-cp","/app","Index"]|null\\n'; exit 0 ;;
                  network|rm|exec|ps) exit 0 ;;
                  *) exit 0 ;;
                esac
                """.formatted(log.toAbsolutePath());
        Files.writeString(cli, script, StandardCharsets.UTF_8);
        assertTrue(cli.toFile().setExecutable(true), "fake CLI must be executable");
        return cli;
    }

    /** Fake podman: records every invocation; {@code run} prints a container id. */
    private static Path installFakePodman(Path dir, Path log) throws Exception {
        Path cli = dir.resolve("fake-podman");
        String script = """
                #!/bin/sh
                LOG="%s"
                sub="$1"
                {
                  printf '%%s' "$1"
                  shift
                  for a in "$@"; do printf '\\t%%s' "$a"; done
                  printf '\\n'
                } >> "$LOG"
                case "$sub" in
                  run) printf 'warmciddeadbeef\\n'; exit 0 ;;
                  network|rm|inspect|exec|ps) exit 0 ;;
                  *) exit 0 ;;
                esac
                """.formatted(log.toAbsolutePath());
        Files.writeString(cli, script, StandardCharsets.UTF_8);
        assertTrue(cli.toFile().setExecutable(true), "fake CLI must be executable");
        return cli;
    }

    @SuppressWarnings("unchecked")
    private static ContainerRuntime pinnedRuntime(Path fakeCli) {
        ContainerRuntime rt = new ContainerRuntime(new ObjectMapper(), "auto", false);
        CapabilityReport pinned = new CapabilityReport(
                true, OciRuntimeKind.PODMAN, fakeCli.toAbsolutePath().toString(),
                "5.0.0", List.of(), false, true, null,
                IsolationTier.INTERMEDIATE, IsolationTier.INTERMEDIATE, "", "");
        ((AtomicReference<CapabilityReport>) ReflectionTestUtils.getField(rt, "report")).set(pinned);
        return rt;
    }

    private static List<String> firstRunArgv(Path log) throws Exception {
        assertTrue(Files.isRegularFile(log), "fake CLI never ran");
        for (String line : Files.readAllLines(log, StandardCharsets.UTF_8)) {
            if (line.startsWith("run\t") || line.equals("run")) {
                return Arrays.asList(line.split("\t", -1));
            }
        }
        throw new AssertionError("no podman run invocation recorded in " + Files.readString(log));
    }

    private static int pair(List<String> argv, String a, String b) {
        for (int i = 0; i + 1 < argv.size(); i++) {
            if (argv.get(i).equals(a) && argv.get(i + 1).equals(b)) {
                return i;
            }
        }
        return -1;
    }
}
