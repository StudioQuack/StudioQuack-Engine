package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.runtime.CapabilityReport;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.runtime.OciRuntimeKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link GpuCapabilityProbe} must read the RUNTIME's truth (CDI specs visible to podman), never
 * infer from the OS: machine-backed podman is probed inside the machine VM ({@code podman machine
 * ssh}), native podman via the host CDI dirs. Also pins the cache: the probe result is reused
 * inside its TTL so the heartbeat/status surface never pays a subprocess per call.
 */
class GpuCapabilityProbeTest {

    @TempDir
    Path tempDir;

    @Test
    void machineBackedHost_probesInsideTheMachineVm() throws Exception {
        Path log = tempDir.resolve("probe.log");
        // machine list → one running machine; machine ssh → a CDI spec path (GPU advertised).
        Path cli = installFakePodman(tempDir, log, """
                case "$sub" in
                  machine)
                    if [ "$1" = "list" ]; then
                      printf '[{"Name":"podman-machine-default","Running":true}]\\n'; exit 0
                    fi
                    if [ "$1" = "ssh" ]; then
                      printf '/etc/cdi/nvidia.yaml\\n'; exit 0
                    fi
                    exit 0 ;;
                  *) exit 0 ;;
                esac
                """);
        GpuCapabilityProbe probe = new GpuCapabilityProbe(pinnedRuntime(cli), new ObjectMapper());

        assertTrue(probe.gpuPassthroughAvailable(), "a CDI gpu spec inside the machine = passthrough");
        String transcript = Files.readString(log, StandardCharsets.UTF_8);
        assertTrue(transcript.contains("machine\tlist"), "must enumerate machines: " + transcript);
        assertTrue(transcript.contains("machine\tssh"), "must probe INSIDE the machine: " + transcript);
    }

    @Test
    void machineBackedHost_withoutCdiSpecs_reportsNoPassthrough() throws Exception {
        Path log = tempDir.resolve("probe-none.log");
        Path cli = installFakePodman(tempDir, log, """
                case "$sub" in
                  machine)
                    if [ "$1" = "list" ]; then
                      printf '[{"Name":"podman-machine-default","Running":true}]\\n'; exit 0
                    fi
                    exit 0 ;;
                  *) exit 0 ;;
                esac
                """);
        GpuCapabilityProbe probe = new GpuCapabilityProbe(pinnedRuntime(cli), new ObjectMapper());

        assertFalse(probe.gpuPassthroughAvailable(),
                "applehv-style machine with no CDI gpu specs → no passthrough");
    }

    @Test
    void nativeHost_scansTheHostCdiDirs() throws Exception {
        Path log = tempDir.resolve("probe-native.log");
        // Native podman: machine list answers with an empty array.
        Path cli = installFakePodman(tempDir, log, """
                case "$sub" in
                  machine) printf '[]\\n'; exit 0 ;;
                  *) exit 0 ;;
                esac
                """);
        Path cdi = tempDir.resolve("cdi");
        Files.createDirectories(cdi);
        Files.writeString(cdi.resolve("nvidia.yaml"), "kind: nvidia.com/gpu\n");

        GpuCapabilityProbe probe = new GpuCapabilityProbe(
                pinnedRuntime(cli), new ObjectMapper(), List.of(cdi));

        assertTrue(probe.gpuPassthroughAvailable(), "host CDI spec with a .com/gpu class → true");
    }

    @Test
    void nativeHost_withoutSpecs_reportsNoPassthrough_andCaches() throws Exception {
        Path log = tempDir.resolve("probe-native-empty.log");
        Path cli = installFakePodman(tempDir, log, """
                case "$sub" in
                  machine) printf '[]\\n'; exit 0 ;;
                  *) exit 0 ;;
                esac
                """);
        Path cdi = tempDir.resolve("cdi-empty");
        Files.createDirectories(cdi);

        GpuCapabilityProbe probe = new GpuCapabilityProbe(
                pinnedRuntime(cli), new ObjectMapper(), List.of(cdi));

        assertFalse(probe.gpuPassthroughAvailable());
        assertFalse(probe.gpuPassthroughAvailable());
        long machineListCalls = Files.readAllLines(log, StandardCharsets.UTF_8).stream()
                .filter(l -> l.contains("machine")).count();
        assertEquals(1, machineListCalls,
                "the cached answer must be reused inside the TTL (hot-path discipline)");
    }

    @Test
    void noRuntime_reportsNoPassthrough() {
        // Null runtime = channel-less/test wiring: fail closed, never a guessed yes.
        GpuCapabilityProbe probe = new GpuCapabilityProbe(null, new ObjectMapper(), List.of());
        assertFalse(probe.gpuPassthroughAvailable());
    }

    /* ---------------- fixtures ---------------- */

    /** Fake podman: logs every invocation, then runs the caller-supplied case body. */
    private static Path installFakePodman(Path dir, Path log, String caseBody) throws Exception {
        Path cli = dir.resolve("fake-podman-" + log.getFileName());
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
                %s
                """.formatted(log.toAbsolutePath(), caseBody);
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
}
