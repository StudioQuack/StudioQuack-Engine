package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.sandbox.warmpool.WarmSlot;
import com.studioquack.sqs.sandbox.warmpool.WarmTargetEntry;
import com.studioquack.sqs.sandbox.warmpool.WarmTargetStore;
import com.studioquack.sqs.sandbox.warmpool.Warmer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SetWarmTargetCommandHandlerTest {

    private static final String PYTHON_IMG = "python:3.11-slim";

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private WarmTargetStore tempStore() {
        return new WarmTargetStore(tempDir.resolve("warm-targets.json").toString(), mapper);
    }

    private SetWarmTargetCommandHandler handler(WarmPool pool, WarmTargetStore store) {
        return new SetWarmTargetCommandHandler(mapper, pool, store, new MockEnvironment());
    }

    @Test
    void appliesWarmTargetLiveAndPersists() throws Exception {
        WarmPool pool = new WarmPool(new FakeWarmer(), Map.of());
        WarmTargetStore store = tempStore();
        SetWarmTargetCommandHandler handler = handler(pool, store);

        DaemonCommandOutcome outcome = handler.handle(cmd("{\"language\":\"python\",\"target\":2}"));

        assertTrue(outcome.succeeded());
        assertEquals(2, pool.target(PYTHON_IMG));
        assertEquals(2, pool.size(PYTHON_IMG), "handler runs reconcile immediately");
        JsonNode body = mapper.readTree(outcome.output());
        assertEquals(PYTHON_IMG, body.get("imageRef").asText());
        assertEquals("python", body.get("language").asText());
        assertEquals(2, body.get("target").asInt());
        assertTrue(body.get("persisted").asBoolean());
        assertTrue(body.get("persistence").asText().contains("persisted"));
        assertEquals("fake", body.path("warmPool").path("warmerKind").asText());
        // The target was written through to the store so it survives a daemon restart.
        List<WarmTargetEntry> loaded = store.load();
        assertEquals(1, loaded.size());
        assertEquals(PYTHON_IMG, loaded.get(0).imageRef());
        assertEquals("python", loaded.get(0).language());
        assertEquals(2, loaded.get(0).target());
    }

    @Test
    void appliesImageKeyedTargetWithCaps() throws Exception {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = new WarmPool(warmer, Map.of());
        WarmTargetStore store = tempStore();
        SetWarmTargetCommandHandler handler = handler(pool, store);

        String imageRef = "registry.example/acme/app:1.2";
        DaemonCommandOutcome outcome = handler.handle(cmd(
                "{\"imageRef\":\"" + imageRef + "\",\"target\":2,"
                        + "\"caps\":{\"cpu\":1.5,\"ramMb\":256,\"diskMb\":128}}"));

        assertTrue(outcome.succeeded());
        assertEquals(2, pool.target(imageRef));
        WarmTargetEntry.Caps caps = pool.capsFor(imageRef);
        assertNotNull(caps);
        assertEquals(1.5, caps.cpu());
        assertEquals(256, caps.ramMb());
        assertEquals(128, caps.diskMb());

        // Reconcile warms to target — every WarmSpec must carry the registered caps.
        assertEquals(2, warmer.specs.size());
        for (Warmer.WarmSpec spec : warmer.specs) {
            assertEquals(imageRef, spec.imageRef());
            assertEquals(1.5, spec.cpus());
            assertEquals(256, spec.ramMb());
            assertEquals(128, spec.diskMb());
        }

        JsonNode body = mapper.readTree(outcome.output());
        assertEquals(imageRef, body.get("imageRef").asText());
        assertTrue(body.get("language").isNull());
        assertEquals(1.5, body.path("caps").path("cpu").asDouble());

        // WarmTargetStore save→load round-trip: daemon restart keeps the caps.
        List<WarmTargetEntry> loaded = store.load();
        WarmTargetEntry entry = loaded.stream()
                .filter(e -> imageRef.equals(e.imageRef()))
                .findFirst()
                .orElseThrow();
        assertNull(entry.language());
        assertEquals(2, entry.target());
        assertNotNull(entry.caps());
        assertEquals(1.5, entry.caps().cpu());
        assertEquals(256, entry.caps().ramMb());
        assertEquals(128, entry.caps().diskMb());
    }

    @Test
    void rejectsPresentButNonPositiveCapFields() {
        SetWarmTargetCommandHandler handler =
                handler(new WarmPool(new FakeWarmer(), Map.of()), tempStore());

        DaemonCommandOutcome zeroCpu = handler.handle(cmd(
                "{\"imageRef\":\"registry.example/acme/app:1\",\"target\":1,"
                        + "\"caps\":{\"cpu\":0,\"ramMb\":256}}"));
        assertFalse(zeroCpu.succeeded());
        assertTrue(zeroCpu.error().contains("caps.cpu must be positive"), zeroCpu.error());

        DaemonCommandOutcome negRam = handler.handle(cmd(
                "{\"imageRef\":\"registry.example/acme/app:1\",\"target\":1,"
                        + "\"caps\":{\"ramMb\":-1}}"));
        assertFalse(negRam.succeeded());
        assertTrue(negRam.error().contains("caps.ramMb must be positive"), negRam.error());

        DaemonCommandOutcome zeroDisk = handler.handle(cmd(
                "{\"imageRef\":\"registry.example/acme/app:1\",\"target\":1,"
                        + "\"caps\":{\"diskMb\":0}}"));
        assertFalse(zeroDisk.succeeded());
        assertTrue(zeroDisk.error().contains("caps.diskMb must be positive"), zeroDisk.error());

        DaemonCommandOutcome zeroGpu = handler.handle(cmd(
                "{\"imageRef\":\"registry.example/acme/app:1\",\"target\":1,"
                        + "\"caps\":{\"gpu\":0}}"));
        assertFalse(zeroGpu.succeeded());
        assertTrue(zeroGpu.error().contains("caps.gpu must be positive"), zeroGpu.error());
    }

    /* Gpu cap gated on the host capability probe */

    /** A probe stub pinned to a fixed answer (the real probe is CLI/FS-driven). */
    private static com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe probeAnswering(boolean answer) {
        return new com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe(null, new ObjectMapper(),
                java.util.List.of()) {
            @Override
            public boolean gpuPassthroughAvailable() {
                return answer;
            }
        };
    }

    @Test
    void appliesGpuCapWhenHostReportsPassthrough() throws Exception {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = new WarmPool(warmer, Map.of());
        WarmTargetStore store = tempStore();
        SetWarmTargetCommandHandler handler =
                new SetWarmTargetCommandHandler(mapper, pool, store, new MockEnvironment(),
                        probeAnswering(true));

        String imageRef = "registry.example/acme/gpu-app:2.0";
        DaemonCommandOutcome outcome = handler.handle(cmd(
                "{\"imageRef\":\"" + imageRef + "\",\"target\":1,"
                        + "\"caps\":{\"cpu\":2.0,\"ramMb\":512,\"gpu\":1}}"));

        assertTrue(outcome.succeeded(), outcome.error());
        WarmTargetEntry.Caps caps = pool.capsFor(imageRef);
        assertNotNull(caps);
        assertEquals(1, caps.gpu());
        // The WarmSpec handed to the warmer carries gpu.
        assertEquals(1, warmer.specs.size());
        assertEquals(1, warmer.specs.get(0).gpu());
        // Ack echoes the gpu cap.
        JsonNode body = mapper.readTree(outcome.output());
        assertEquals(1, body.path("caps").path("gpu").asInt());
        // Persisted: a daemon restart keeps the gpu cap.
        List<WarmTargetEntry> loaded = store.load();
        assertEquals(1, loaded.get(0).caps().gpu());
    }

    @Test
    void refusesGpuCapWhenHostLacksPassthrough() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = new WarmPool(warmer, Map.of());
        WarmTargetStore store = tempStore();
        SetWarmTargetCommandHandler handler =
                new SetWarmTargetCommandHandler(mapper, pool, store, new MockEnvironment(),
                        probeAnswering(false));

        DaemonCommandOutcome outcome = handler.handle(cmd(
                "{\"imageRef\":\"registry.example/acme/gpu-app:2.0\",\"target\":1,"
                        + "\"caps\":{\"gpu\":1}}"));

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("no GPU passthrough"), outcome.error());
        // Fail-closed: nothing applied, nothing persisted, nothing warmed.
        assertEquals(0, pool.targets().size());
        assertTrue(warmer.specs.isEmpty());
        assertTrue(store.load().isEmpty(), "a refused gpu cap must never reach the store");
    }

    @Test
    void refusesGpuCapWhenProbeAbsent() {
        // The 4-arg ctor (null probe) = capability unknown = refuse (fail closed).
        WarmPool pool = new WarmPool(new FakeWarmer(), Map.of());
        SetWarmTargetCommandHandler handler = handler(pool, tempStore());

        DaemonCommandOutcome outcome = handler.handle(cmd(
                "{\"imageRef\":\"registry.example/acme/gpu-app:2.0\",\"target\":1,"
                        + "\"caps\":{\"gpu\":2}}"));

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("no GPU passthrough"), outcome.error());
    }


    @Test
    void legacyLanguageWithoutDefaultImageFailsClearly() {
        SetWarmTargetCommandHandler handler = handler(new WarmPool(new FakeWarmer(), Map.of()), tempStore());

        DaemonCommandOutcome outcome = handler.handle(cmd("{\"language\":\"ruby\",\"target\":1}"));

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("imageRef"),
                "error should tell the operator to pass imageRef: " + outcome.error());
    }

    @Test
    void legacyStoreFileIsMigratedToImageKeys() throws Exception {
        Path file = tempDir.resolve("warm-targets.json");
        Files.writeString(file, "{\"python\":3}");
        WarmTargetStore store = new WarmTargetStore(file.toString(), mapper);

        List<WarmTargetEntry> loaded = store.load();

        assertEquals(1, loaded.size());
        assertEquals(PYTHON_IMG, loaded.get(0).imageRef());
        assertEquals("python", loaded.get(0).language());
        assertEquals(3, loaded.get(0).target());
    }

    @Test
    void rejectsEmptyPayload() {
        SetWarmTargetCommandHandler handler =
                handler(new WarmPool(new FakeWarmer(), Map.of()), tempStore());

        DaemonCommandOutcome outcome = handler.handle(cmd(null));

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("empty payload"));
    }

    private static DaemonCommand cmd(String payload) {
        return new DaemonCommand("c1", "set-warm-target", "2026-07-14T00:00:00Z", payload);
    }

    private static final class FakeWarmer implements Warmer {
        private final AtomicInteger next = new AtomicInteger();
        /** WarmSpecs handed to the warmer (reconcile / on-demand), in call order. */
        final java.util.List<WarmSpec> specs = new java.util.ArrayList<>();

        @Override
        public String kind() {
            return "fake";
        }

        @Override
        public WarmSlot warm(WarmSpec spec) {
            specs.add(spec);
            int n = next.incrementAndGet();
            return new WarmSlot(spec.language(), "w_" + n, "c" + n, kind(),
                    spec.imageRef(), Instant.now());
        }

        @Override
        public void cool(WarmSlot slot) {
            // no-op
        }

        @Override
        public boolean alive(WarmSlot slot) {
            return true;
        }
    }
}
