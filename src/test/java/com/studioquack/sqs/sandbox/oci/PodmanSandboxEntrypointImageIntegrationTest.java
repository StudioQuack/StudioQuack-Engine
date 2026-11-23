package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.secrets.NoopSecretKeyStore;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.warmpool.NoopWarmer;
import com.studioquack.sqs.sandbox.warmpool.OciWarmer;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.sandbox.warmpool.WarmSlot;
import com.studioquack.sqs.sandbox.warmpool.WarmTargetEntry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test — the acceptance proof for the image-keyed warm pool against a REAL container
 * runtime (gated with {@link Assumptions#assumeTrue} exactly like {@link
 * PodmanSandboxIntegrationTest}, so it self-skips where no runtime exists)
 */
class PodmanSandboxEntrypointImageIntegrationTest {

    /** Fully-qualified local tag — short names can hit registry resolution; localhost/ never does. */
    private static final String IMG = "localhost/sq-s16-it-entrypoint:latest";

    /** The marker the ENTRYPOINT app emits on its output port. */
    private static final String ENTRYPOINT_RESULT = "entrypoint-ok";
    /** What the job's (ignored) script WOULD print if the bootstrap wrongly ran — must never appear. */
    private static final String BOOTSTRAP_RESULT = "bootstrap-ran";

    private static final String APP_PY = """
            import base64, json, os, sys
            node = "?"
            try:
                ctx = json.loads(base64.b64decode(os.environ.get("SQ_CONTEXT", "")).decode("utf-8"))
                node = ctx.get("currentNodeId") or "?"
            except Exception:
                pass
            print("entrypoint-ran node=%s" % node)
            print("portValue:answer:::result:ENTRYPOINT_RESULT")
            sys.exit(0)
            """.replace("ENTRYPOINT_RESULT", ENTRYPOINT_RESULT);

    private static final String DOCKERFILE = """
            FROM python:3.11-slim
            COPY main.py /app/main.py
            ENTRYPOINT ["python3", "/app/main.py"]
            """;

    private static boolean imageReady;

    private ContainerRuntime runtime;
    private HelperRequestRouter helper;
    private ObjectMapper mapper;

    @BeforeAll
    static void buildImage() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ContainerRuntime rt = new ContainerRuntime(m, "auto", false);
        Assumptions.assumeTrue(rt.available(),
                "no container runtime on PATH (podman/docker/nerdctl) — skipping live-container test");
        CliRunner.Result info = CliRunner.run(List.of(rt.cli(), "info"), Duration.ofSeconds(15));
        Assumptions.assumeTrue(info.ok(), "`" + rt.cli() + " info` failed — daemon unreachable; skipping");

        Path dir = Files.createTempDirectory("sq-s16-it");
        Files.writeString(dir.resolve("main.py"), APP_PY);
        Files.writeString(dir.resolve("Dockerfile"), DOCKERFILE);
        CliRunner.Result build = CliRunner.run(
                List.of(rt.cli(), "build", "-t", IMG, dir.toString()), Duration.ofSeconds(300));
        Assumptions.assumeTrue(build.ok(),
                "fixture image build failed — skipping: " + build.stderr());
        imageReady = true;
    }

    @AfterAll
    static void removeImage() {
        if (imageReady) {
            CliRunner.run(List.of("podman", "rmi", "-f", IMG), Duration.ofSeconds(30)); // best-effort
        }
    }

    @BeforeEach
    void setUp() {
        Assumptions.assumeTrue(imageReady, "fixture image unavailable");
        mapper = new ObjectMapper();
        runtime = new ContainerRuntime(mapper, "auto", false);
        helper = new HelperRequestRouter(mapper,
                new com.studioquack.sqs.aimodel.AiModelProviderProxy(
                        new com.studioquack.sqs.aimodel.AiModelProviderHolder(), mapper), null);
    }

    private PodmanSandboxExecution sandbox(WarmPool pool) {
        return new PodmanSandboxExecution(runtime,
                new SecretResolver(new NoopSecretKeyStore()),
                new Redactor(),
                helper,
                pool,
                new MockEnvironment(),
                new com.studioquack.sqs.deps.DependencyManifestHolder(),
                SandboxOwnership.unowned(),
                new com.studioquack.sqs.runtime.DispatchCancellationRegistry());
    }

    /** A python-language job pinned to the custom image, carrying a script that must be IGNORED. */
    private SandboxJobRequest entrypointJob(String jobId) throws Exception {
        String contextBase64 = Base64.getEncoder().encodeToString(mapper.writeValueAsBytes(
                Map.of("currentNodeId", jobId + "-node", "workflowExecutionId", "it-wfe")));
        return SandboxJobRequest.builder(jobId, SandboxLanguage.PYTHON,
                        "print('portValue:answer:::result:" + BOOTSTRAP_RESULT + "')\n")
                .ociImageRef(IMG)
                .contextBase64(contextBase64)
                .limits(SandboxLimits.of(256, 120_000))
                .build();
    }

    @Test
    void custom_image_cold_path_runs_the_image_entrypoint_not_the_language_bootstrap() throws Exception {
        // NoopWarmer pool → always the cold path; the custom image must still be honoured.
        PodmanSandboxExecution sandbox = sandbox(new WarmPool(new NoopWarmer(), Map.of()));

        SandboxJobResponse resp = sandbox.execute(entrypointJob("it-ep-cold"));

        assertTrue(resp.ok(), "entrypoint job succeeded (stderr=" + resp.stderr() + ")");
        assertEquals(ENTRYPOINT_RESULT, resp.outputVariables().get("answer"),
                "output came from the image's entrypoint");
        assertTrue(resp.stdout().contains("entrypoint-ran node=it-ep-cold-node"),
                "SQ_CONTEXT reached the app (currentNodeId round-tripped): " + resp.stdout());
        assertFalse(resp.stdout().contains(BOOTSTRAP_RESULT),
                "the node's stored script was ignored, not executed (D57)");
        assertNotNull(resp.containerId(), "cold dispatch created a container of the custom image");
    }

    @Test
    void custom_image_is_served_from_a_warm_slot_and_the_slot_carries_the_caps() throws Exception {
        OciWarmer warmer = new OciWarmer(runtime, "");
        WarmPool pool = new WarmPool(warmer, Map.of(), Runnable::run, SandboxOwnership.unowned());
        try {
            pool.setTarget(IMG, null, 1, new WarmTargetEntry.Caps(1.0, 256, 128));
            pool.reconcile();
            assertEquals(1, pool.size(IMG), "one warm slot for the custom image");

            WarmPool.LanguageStatus row = pool.status().stream()
                    .filter(r -> IMG.equals(r.imageRef())).findFirst().orElseThrow();
            assertEquals(1, row.preBuilt());
            String warmContainerId = row.slots().get(0).containerId();
            assertNotNull(warmContainerId);

            // The warm container was created WITH the entry's caps.
            CliRunner.Result insp = CliRunner.run(List.of(runtime.cli(), "inspect", "-f",
                    "{{.HostConfig.Memory}}|{{.HostConfig.NanoCpus}}|{{json .HostConfig.Tmpfs}}",
                    warmContainerId), Duration.ofSeconds(15));
            assertTrue(insp.ok(), "inspect warm slot: " + insp.stderr());
            String[] parts = insp.stdout().trim().split("\\|", -1);
            assertEquals("268435456", parts[0], "--memory 256m applied (256 * 1024 * 1024 bytes)");
            assertEquals("1000000000", parts[1], "--cpus 1 applied (1e9 nano-cpus)");
            assertTrue(parts.length > 2 && parts[2].contains("size=128m"),
                    "disc cap wired to the /sandbox tmpfs size: " + insp.stdout());

            // The dispatch is served by THIS pre-existing warm slot.
            SandboxJobResponse resp = sandbox(pool).execute(entrypointJob("it-ep-warm"));

            assertTrue(resp.ok(), "warm-hit entrypoint job succeeded (stderr=" + resp.stderr() + ")");
            assertEquals(ENTRYPOINT_RESULT, resp.outputVariables().get("answer"));
            assertEquals(warmContainerId, resp.containerId(),
                    "the response's container id IS the pre-existing warm slot's id");
        } finally {
            pool.drain(Duration.ZERO);
        }
    }

    @Test
    void warm_hit_dispatch_overhead_is_measured_against_the_150ms_budget() throws Exception {
        OciWarmer warmer = new OciWarmer(runtime, "");
        ExecutorService refill = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "s16-it-refill");
            t.setDaemon(true);
            return t;
        });
        WarmPool pool = new WarmPool(warmer, Map.of(IMG, 2), refill, SandboxOwnership.unowned());
        String rawContainer = null;
        try {
            pool.reconcile();
            assertEquals(2, pool.size(IMG), "reservoir at target");
            PodmanSandboxExecution sandbox = sandbox(pool);

            // (1) In-JVM acquire cost (pop + liveness probe + lease), refill overlapping async.
            List<Long> acquireMs = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                long t0 = System.nanoTime();
                Optional<WarmSlot> slot = pool.acquire(IMG);
                acquireMs.add((System.nanoTime() - t0) / 1_000_000L);
                slot.ifPresent(s -> {
                    try {
                        Thread.sleep(150); // let the async refill land before the destroy
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    pool.release(s);
                });
                Thread.sleep(400);
            }

            // (2) Full warm-hit dispatch wall time of the trivial entrypoint job (includes the
            // python interpreter startup — the "user code" share of the exec).
            List<Long> warmHitMs = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                SandboxJobResponse resp = sandbox.execute(entrypointJob("it-ep-bench-" + i));
                assertTrue(resp.ok(), "bench dispatch " + i + " ok (stderr=" + resp.stderr() + ")");
                warmHitMs.add(resp.wallMs());
                Thread.sleep(400);
            }

            // (3) Raw exec transport: `podman exec` of a no-op into a long-lived container — the
            // budget-relevant overhead floor (no interpreter, no app).
            CliRunner.Result raw = CliRunner.run(List.of(runtime.cli(), "run", "-d",
                    "python:3.11-slim", "sleep", "300"), Duration.ofSeconds(120));
            Assumptions.assumeTrue(raw.ok(), "raw transport container failed to start");
            rawContainer = raw.stdout().trim();
            List<Long> execNoopMs = new ArrayList<>();
            for (int i = 0; i < 15; i++) {
                long t0 = System.nanoTime();
                CliRunner.run(List.of(runtime.cli(), "exec", "-i", rawContainer, "sh", "-c", "true"),
                        Duration.ofSeconds(15));
                execNoopMs.add((System.nanoTime() - t0) / 1_000_000L);
            }

            long acquireMed = median(acquireMs);
            long execMed = median(execNoopMs);
            long hitMed = median(warmHitMs);
            System.out.println("[S16-bench] warmAcquireMs median=" + acquireMed
                    + " p95=" + p95(acquireMs)
                    + " | execNoopMs median=" + execMed + " p95=" + p95(execNoopMs)
                    + " | warmHitWallMs median=" + hitMed + " p95=" + p95(warmHitMs)
                    + " (wall includes python startup; budget 150ms excludes user code)");

            assertTrue(acquireMed < 150, "in-JVM warm acquire within budget: " + acquireMed + "ms");
            assertTrue(execMed < 150, "raw exec transport within budget: " + execMed + "ms");
        } finally {
            pool.drain(Duration.ZERO);
            refill.shutdownNow();
            if (rawContainer != null && !rawContainer.isBlank()) {
                CliRunner.run(List.of(runtime.cli(), "rm", "-f", rawContainer), Duration.ofSeconds(15));
            }
        }
    }

    private static long median(List<Long> values) {
        List<Long> sorted = values.stream().sorted().toList();
        return sorted.get(sorted.size() / 2);
    }

    private static long p95(List<Long> values) {
        List<Long> sorted = values.stream().sorted().toList();
        return sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * 0.95) - 1));
    }
}
