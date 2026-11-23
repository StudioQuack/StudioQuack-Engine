package com.studioquack.sqs.sandbox.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * podman info never advertises the {@code [engine.runtimes]} table — the VM bootstrap
 * registers runsc via {@code /etc/containers/containers.conf}, and the detector must honour
 * that registration (source "podman") or a gVisor-equipped host is mis-detected as
 * intermediate and the enforced isolation floor refuses every sandboxed node.
 */
class OciRuntimeDetectorTest {

    @TempDir
    Path tmp;

    @Test
    void runtimesSectionIsParsedIncludingMultiLineArrays() throws Exception {
        Path conf = tmp.resolve("containers.conf");
        Files.writeString(conf, """
                [engine]
                runtime = "crun"

                [engine.runtimes]
                runsc = [
                  "/usr/local/bin/runsc",
                ]
                crun = ["/usr/bin/crun"]

                [network]
                network_backend = "netavark"
                """);
        Map<String, String> out = new LinkedHashMap<>();
        OciRuntimeDetector.parseRuntimesLayer(conf, out);
        assertEquals("/usr/local/bin/runsc", out.get("runsc"),
                "the bootstrap's multi-line array form must resolve to its binary path");
        assertEquals("/usr/bin/crun", out.get("crun"));
        assertFalse(out.containsKey("runtime"), "keys outside [engine.runtimes] are ignored");
        assertFalse(out.containsKey("network_backend"));
    }

    @Test
    void laterLayerOverridesSameKey() throws Exception {
        Path base = tmp.resolve("base.conf");
        Path over = tmp.resolve("over.conf");
        Files.writeString(base, "[engine.runtimes]\nrunsc = [\"/old/runsc\"]\n");
        Files.writeString(over, "[engine.runtimes]\nrunsc = [\"/new/runsc\"]\n");
        Map<String, String> out = new LinkedHashMap<>();
        OciRuntimeDetector.parseRuntimesLayer(base, out);
        OciRuntimeDetector.parseRuntimesLayer(over, out);
        assertEquals("/new/runsc", out.get("runsc"), "podman merges the map key-wise, later layer wins");
    }

    @Test
    void commentsAndWhitespaceAreTolerated() throws Exception {
        Path conf = tmp.resolve("c.conf");
        Files.writeString(conf, """
                # gVisor registration
                [engine.runtimes]   # runtimes table
                  runsc   =   [  "/usr/local/bin/runsc" ]  # pinned
                """);
        Map<String, String> out = new LinkedHashMap<>();
        OciRuntimeDetector.parseRuntimesLayer(conf, out);
        assertEquals("/usr/local/bin/runsc", out.get("runsc"));
    }

    @Test
    void missingLayerIsSkipped() {
        Map<String, String> out = new LinkedHashMap<>();
        OciRuntimeDetector.parseRuntimesLayer(tmp.resolve("absent.conf"), out);
        assertTrue(out.isEmpty());
    }

    /**
     * A cold first {@code podman info} on a freshly booted host can time out. The registration
     * must still count, or the daemon stays below the best tier until it restarts.
     */
    @Test
    void registeredRunscCountsEvenWhenPodmanInfoFailed() throws Exception {
        Path runsc = Files.createFile(tmp.resolve("runsc"));
        assertTrue(runsc.toFile().setExecutable(true));

        List<DetectedOciRuntime> out = OciRuntimeDetector.mergeRuntimes(
                null, Map.of("runsc", runsc.toString()), new ObjectMapper());

        assertEquals(1, out.size());
        assertEquals("runsc", out.get(0).name());
        assertTrue(out.get(0).gvisor());
        assertEquals("podman", out.get(0).source(), "a config registration is trusted, unlike the PATH fallback");
    }

    @Test
    void danglingRegistrationNeverClaimsGvisor() {
        List<DetectedOciRuntime> out = OciRuntimeDetector.mergeRuntimes(
                null, Map.of("runsc", tmp.resolve("absent-runsc").toString()), new ObjectMapper());
        assertTrue(out.isEmpty());
    }
}
