package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.egress.StubSandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.CapabilityReport;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.runtime.OciRuntimeKind;
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

/** Dispatch fail-closed when overlay egress is UNAVAILABLE and the job would get internet. */
class PodmanSandboxEgressGateTest {

    @TempDir
    Path tempDir;

    @Test
    void unavailable_plus_open_refuses_infra_and_never_runs_container() throws Exception {
        Path log = tempDir.resolve("argv.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);
        WarmPool pool = mock(WarmPool.class);
        when(pool.acquire(any(), any(), any())).thenReturn(Optional.empty());

        PodmanSandboxExecution exec = execution(runtime, pool,
                StubSandboxEgressManager.unavailable("overlay-down"));

        SandboxJobResponse resp = exec.execute(
                SandboxJobRequest.builder("j-open", SandboxLanguage.PYTHON, "print(1)\n")
                        .egress(EgressPolicy.OPEN)
                        .build());

        assertFalse(resp.ok());
        assertEquals(SandboxJobResponse.ERROR_SANDBOX_FAILED, resp.errorCode());
        assertEquals(SandboxJobResponse.FailureKind.INFRA, resp.failureKind());
        assertEquals("sandbox egress unavailable: overlay-down", resp.errorMessage());
        assertFalse(Files.isRegularFile(log) && Files.readString(log).contains("run"),
                "no podman run must be attempted when egress is UNAVAILABLE");
    }

    @Test
    void unavailable_plus_none_is_not_refused_by_egress_gate() throws Exception {
        Path log = tempDir.resolve("argv-none.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);
        WarmPool pool = mock(WarmPool.class);
        when(pool.acquire(any(), any(), any())).thenReturn(Optional.empty());

        PodmanSandboxExecution exec = execution(runtime, pool,
                StubSandboxEgressManager.unavailable("overlay-down"));

        SandboxJobResponse resp = exec.execute(
                SandboxJobRequest.builder("j-none", SandboxLanguage.PYTHON, "print(1)\n")
                        .egress(EgressPolicy.NONE)
                        .build());

        // Gate must not emit the egress INFRA refusal; later mock/secret failures are fine.
        assertFalse("sandbox egress unavailable: overlay-down".equals(resp.errorMessage()),
                "NONE egress must pass the D90 gate; got: " + resp.errorMessage());
    }

    @Test
    void enforced_allows_past_egress_gate() throws Exception {
        assertNotEgressRefusal(StubSandboxEgressManager.enforced(), EgressPolicy.OPEN);
    }

    @Test
    void degraded_without_opt_out_refuses_open_egress() throws Exception {
        // DEGRADED means "no rules on this host"; a networked job must not start.
        Path log = tempDir.resolve("argv-degraded.log");
        Path fakeCli = installFakePodman(tempDir, log);
        WarmPool pool = mock(WarmPool.class);
        when(pool.acquire(any(), any(), any())).thenReturn(Optional.empty());
        PodmanSandboxExecution exec = execution(pinnedRuntime(fakeCli), pool,
                StubSandboxEgressManager.degraded());
        SandboxJobResponse resp = exec.execute(
                SandboxJobRequest.builder("j-degraded", SandboxLanguage.PYTHON, "print(1)\n")
                        .egress(EgressPolicy.OPEN)
                        .build());
        assertEquals(SandboxJobResponse.ERROR_SANDBOX_FAILED, resp.errorCode());
        assertTrue(resp.errorMessage().contains("not enforced on this host"),
                "expected D-06 refusal; got: " + resp.errorMessage());
        assertTrue(resp.errorMessage().contains("overlay-egress=off"));
        assertFalse(Files.isRegularFile(log) && Files.readString(log).contains("run"),
                "no podman run must be attempted on a DEGRADED host without opt-out");
    }

    @Test
    void degraded_with_operator_opt_out_allows_past_egress_gate() throws Exception {
        assertNotEgressRefusal(StubSandboxEgressManager.degradedOptOut(), EgressPolicy.OPEN);
    }

    private void assertNotEgressRefusal(StubSandboxEgressManager mgr, EgressPolicy egress)
            throws Exception {
        Path log = tempDir.resolve("argv-" + mgr.readiness().state() + ".log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);
        WarmPool pool = mock(WarmPool.class);
        when(pool.acquire(any(), any(), any())).thenReturn(Optional.empty());

        PodmanSandboxExecution exec = execution(runtime, pool, mgr);
        SandboxJobResponse resp = exec.execute(
                SandboxJobRequest.builder("j-ok", SandboxLanguage.PYTHON, "print(1)\n")
                        .egress(egress)
                        .build());

        assertFalse(resp.errorMessage() != null
                        && resp.errorMessage().startsWith("sandbox egress unavailable:"),
                "ENFORCED/DEGRADED must not refuse; got: " + resp.errorMessage());
    }

    private static PodmanSandboxExecution execution(ContainerRuntime runtime, WarmPool pool,
                                                    StubSandboxEgressManager egress) {
        return new PodmanSandboxExecution(
                runtime,
                mock(SecretResolver.class),
                mock(Redactor.class),
                mock(HelperRequestRouter.class),
                pool,
                new MockEnvironment(),
                mock(DependencyManifestHolder.class),
                SandboxOwnership.unowned(),
                new com.studioquack.sqs.runtime.DispatchCancellationRegistry(),
                egress);
    }

    private static Path installFakePodman(Path dir, Path log) throws Exception {
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
                case "$sub" in
                  run) printf 'cid\\n'; exit 0 ;;
                  *) exit 0 ;;
                esac
                """.formatted(log.toAbsolutePath());
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
