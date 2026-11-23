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
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test — runs a REAL container job through {@link PodmanSandboxExecution} against the
 * detected runtime (Docker on this build host; Podman in prod). GATED with {@link
 * Assumptions#assumeTrue} on a live runtime + a successful {@code info} probe, so it self-skips
 * where no container runtime exists (CI without Docker) while still running everywhere it can.
 */
class PodmanSandboxIntegrationTest {

    private ContainerRuntime runtime;
    private HelperRequestRouter helper;
    private PodmanSandboxExecution sandbox;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper();
        runtime = new ContainerRuntime(mapper, "auto", false);
        Assumptions.assumeTrue(runtime.available(),
                "no container runtime on PATH (podman/docker/nerdctl) — skipping live-container test");
        CliRunner.Result info = CliRunner.run(List.of(runtime.cli(), "info"), Duration.ofSeconds(15));
        Assumptions.assumeTrue(info.ok(), "`" + runtime.cli() + " info` failed — daemon unreachable; skipping");

        helper = new HelperRequestRouter(mapper,
                new com.studioquack.sqs.aimodel.AiModelProviderProxy(
                        new com.studioquack.sqs.aimodel.AiModelProviderHolder(), mapper), null);
        sandbox = new PodmanSandboxExecution(runtime,
                new SecretResolver(new NoopSecretKeyStore()),
                new Redactor(),
                helper,
                new WarmPool(new NoopWarmer(), Map.of()),
                new MockEnvironment(),
                new com.studioquack.sqs.deps.DependencyManifestHolder(),
                SandboxOwnership.unowned(),
                new com.studioquack.sqs.runtime.DispatchCancellationRegistry());
    }

    @Test
    void runs_a_real_python_job_and_returns_stdout_and_exit_code() {
        SandboxJobRequest job = SandboxJobRequest.builder("it-py-1", SandboxLanguage.PYTHON,
                        "print('hello-from-sandbox')\n")
                .limits(SandboxLimits.of(256, 120_000)) // generous wall to allow a first-run image pull
                .build();

        SandboxJobResponse resp = sandbox.execute(job);

        assertTrue(resp.ok(), "job succeeded (stderr=" + resp.stderr() + ")");
        assertEquals(0, resp.exitCode());
        assertTrue(resp.stdout().contains("hello-from-sandbox"), "stdout captured: " + resp.stdout());
        assertEquals("hello-from-sandbox", ((String) resp.outputVariables().get("$stdout")).trim());
        assertEquals(0, resp.outputVariables().get("$exitCode"));
        // This dispatch always misses the warm pool (NoopWarmer, see setUp), so this proves the
        // cold path now creates a real container to exec into (never null, unlike the old single
        // `run --rm` cold branch where the container WAS the job and nothing else execed into it).
        assertTrue(resp.containerId() != null && !resp.containerId().isBlank(),
                "cold dispatch created a container to exec the job into");
        System.out.println("[integration] ran python job via runtime=" + runtime.kind()
                + " tier=" + runtime.effectiveTier().wireId() + " wallMs=" + resp.wallMs());
    }

    @Test
    void per_port_output_is_parsed_from_the_stdout_marker_fallback() {
        // A raw job printing the legacy marker — no helper channel needed (proves the fallback path).
        SandboxJobRequest job = SandboxJobRequest.builder("it-py-2", SandboxLanguage.PYTHON,
                        "print('portValue:answer:::result:42')\n")
                .limits(SandboxLimits.of(256, 120_000))
                .build();

        SandboxJobResponse resp = sandbox.execute(job);

        assertTrue(resp.ok(), "job succeeded (stderr=" + resp.stderr() + ")");
        assertEquals("42", resp.outputVariables().get("answer"), "per-port output parsed from marker");
    }

    @Test
    void a_non_zero_exit_is_reported_as_a_typed_failure() {
        SandboxJobRequest job = SandboxJobRequest.builder("it-py-3", SandboxLanguage.PYTHON,
                        "import sys\nsys.stderr.write('boom')\nsys.exit(3)\n")
                .limits(SandboxLimits.of(256, 120_000))
                .build();

        SandboxJobResponse resp = sandbox.execute(job);

        assertTrue(resp.failed(), "non-zero exit is a failure");
        assertEquals(3, resp.exitCode());
        assertEquals(SandboxJobResponse.ERROR_SANDBOX_FAILED, resp.errorCode());
    }
}
