package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.egress.StubSandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.CapabilityReport;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.runtime.OciRuntimeKind;
import com.studioquack.sqs.sandbox.secrets.SecretResolution;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Language gate: an entrypoint (custom-image) job carries NO script language — a build-mode node
 * (Script toggle off) persists no {@code templateScript} at all. The sandbox must route such a job
 * to its image (cold create with the entrypoint cleared) instead of failing "unresolved language",
 * which only the script path may raise.
 */
class PodmanSandboxEntrypointLanguageGateTest {

    @TempDir
    Path tempDir;

    @Test
    void null_language_without_an_image_ref_still_fails_unresolved_language() throws Exception {
        PodmanSandboxExecution exec = execution(pinnedRuntime(installFakePodman(tempDir, "plain.log", false)));

        SandboxJobResponse resp = exec.execute(
                SandboxJobRequest.builder("j-no-lang", null, "").build());

        assertFalse(resp.ok());
        assertEquals("unresolved language", resp.stderr());
    }

    @Test
    void null_language_with_a_custom_image_runs_the_entrypoint_cold_path() throws Exception {
        Path log = tempDir.resolve("entrypoint.log");
        ContainerRuntime runtime = pinnedRuntime(installFakePodman(tempDir, "entrypoint.log", true));
        PodmanSandboxExecution exec = execution(runtime);

        SandboxJobResponse resp = exec.execute(
                SandboxJobRequest.builder("j-entrypoint", null, "")
                        .ociImageRef("localhost/sq-local/node-test-emulator:1")
                        .egress(EgressPolicy.NONE)
                        .build());

        assertTrue(resp.ok(), "entrypoint job with no script language must run: " + resp.errorMessage()
                + " / " + resp.stderr());
        assertEquals("cidcold", resp.containerId());
        // The create carried --entrypoint "" (idle sleep) and the exec used the recorded entrypoint.
        String logText = Files.readString(log, StandardCharsets.UTF_8);
        assertTrue(logText.contains("run\t") && logText.contains("--entrypoint"),
                "cold create must reset the image entrypoint: " + logText);
    }

    private static PodmanSandboxExecution execution(ContainerRuntime runtime) {
        WarmPool pool = mock(WarmPool.class);
        when(pool.acquire(any(), any(), any())).thenReturn(Optional.empty());
        SecretResolver secrets = mock(SecretResolver.class);
        when(secrets.resolveToEnv(any())).thenReturn(new SecretResolution(null, 0, List.of()));
        return new PodmanSandboxExecution(
                runtime,
                secrets,
                mock(Redactor.class),
                mock(HelperRequestRouter.class),
                pool,
                new MockEnvironment(),
                mock(DependencyManifestHolder.class),
                SandboxOwnership.unowned(),
                new com.studioquack.sqs.runtime.DispatchCancellationRegistry(),
                StubSandboxEgressManager.enforced());
    }

    /**
     * Fake podman: {@code run} prints a container id, {@code inspect} answers the entrypoint probe
     * (when {@code withEntrypoint}), {@code exec} prints a marker and exits 0.
     */
    private static Path installFakePodman(Path dir, String logName, boolean withEntrypoint) throws Exception {
        Path log = dir.resolve(logName);
        Path cli = dir.resolve("fake-podman-" + logName);
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
                  run) printf 'cidcold\\n'; exit 0 ;;
                  inspect) %s ; exit 0 ;;
                  exec) printf 'entrypoint-ran\\n'; exit 0 ;;
                  *) exit 0 ;;
                esac
                """.formatted(log.toAbsolutePath(), withEntrypoint
                ? "printf '[\"java\",\"-cp\",\"/app\",\"Index\"]|null\\n'"
                : "true");
        Files.writeString(cli, script, StandardCharsets.UTF_8);
        assertTrue(cli.toFile().setExecutable(true));
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
