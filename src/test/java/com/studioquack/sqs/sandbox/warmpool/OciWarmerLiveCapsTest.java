package com.studioquack.sqs.sandbox.warmpool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live verification — proves per-image warm caps against the LIVE container spec (not the argv
 * builder), and that a gpu cap on a host without passthrough is refused rather than attempted.
 * Gated with {@link Assumptions#assumeTrue} on a live runtime, so it self-skips where no Podman
 * exists (CI) and runs wherever one does.
 */
class OciWarmerLiveCapsTest {

    private static final String IMAGE = "python:3.11-slim";

    private final ObjectMapper mapper = new ObjectMapper();
    private ContainerRuntime runtime;
    private GpuCapabilityProbe probe;
    private OciWarmer warmer;

    @BeforeEach
    void setUp() {
        runtime = new ContainerRuntime(mapper, "auto", false);
        Assumptions.assumeTrue(runtime.available(),
                "no container runtime on PATH — skipping live caps verification");
        CliRunner.Result info = CliRunner.run(List.of(runtime.cli(), "info"), Duration.ofSeconds(15));
        Assumptions.assumeTrue(info.ok(), "runtime CLI present but daemon unreachable — skipping");
        probe = new GpuCapabilityProbe(runtime, mapper);
        // Broker/runner off, egress gate off (null manager): this test isolates the cap plumbing.
        warmer = new OciWarmer(runtime, "", null, false, 120, null, false, 10,
                SandboxOwnership.of("live-caps-test", "boot-live-caps"),
                image -> IMAGE.equals(image) ? "python" : null, 0.0, 0, null, probe);
    }

    private JsonNode inspect(String containerId) throws Exception {
        CliRunner.Result r = CliRunner.run(
                List.of(runtime.cli(), "inspect", containerId), Duration.ofSeconds(15));
        Assumptions.assumeTrue(r.ok(), "inspect failed: " + r.stderr());
        JsonNode root = mapper.readTree(r.stdout());
        return root.isArray() && !root.isEmpty() ? root.get(0) : root;
    }

    @Test
    void liveContainerSpecCarriesThePerImageCaps() throws Exception {
        WarmSlot slot = null;
        try {
            slot = warmer.warm(new Warmer.WarmSpec(IMAGE, "python", 1.5, 256, 128, 0));
            JsonNode hostConfig = inspect(slot.containerId()).path("HostConfig");

            assertEquals(256L * 1024 * 1024, hostConfig.path("Memory").asLong(),
                    "ramMb=256 must land as --memory 256m on the LIVE container");
            assertEquals(256L * 1024 * 1024, hostConfig.path("MemorySwap").asLong(),
                    "swap stays disabled (memory-swap == memory)");
            assertEquals(1_500_000_000L, hostConfig.path("NanoCpus").asLong(),
                    "cpus=1.5 must land as 1.5 NanoCpus on the LIVE container");
            JsonNode tmpfs = hostConfig.path("Tmpfs");
            String sandboxTmpfs = tmpfs == null ? "" : tmpfs.path("/sandbox").asText("");
            assertTrue(sandboxTmpfs.contains("size=128m"),
                    "diskMb=128 must size the /sandbox tmpfs on the LIVE container: " + tmpfs);
        } finally {
            if (slot != null) {
                warmer.cool(slot);
            }
        }
    }

    @Test
    void gpuCap_agreesWithTheHostProbe() throws Exception {
        boolean supported = probe.gpuPassthroughAvailable();
        if (!supported) {
            // e.g. podman-machine (applehv): refused, and nothing is attempted.
            Warmer.WarmException ex = assertThrows(Warmer.WarmException.class, () ->
                    warmer.warm(new Warmer.WarmSpec(IMAGE, "python", 1.0, 256, 128, 1)));
            assertTrue(ex.getMessage().contains("no GPU passthrough"), ex.getMessage());
            return;
        }
        WarmSlot slot = null;
        try {
            slot = warmer.warm(new Warmer.WarmSpec(IMAGE, "python", 1.0, 256, 128, 1));
            // --gpus resolves through CDI; the live spec exposes the granted devices.
            JsonNode hostConfig = inspect(slot.containerId()).path("HostConfig");
            JsonNode devices = hostConfig.path("Devices");
            assertTrue(devices != null && !devices.isEmpty(),
                    "gpu cap on a passthrough host must attach devices on the LIVE container");
        } finally {
            if (slot != null) {
                warmer.cool(slot);
            }
        }
    }
}
