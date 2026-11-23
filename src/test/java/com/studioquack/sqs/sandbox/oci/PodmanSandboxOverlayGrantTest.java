package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.egress.EgressReadiness;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.ExecResult;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.Platform;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import com.studioquack.sqs.sandbox.runtime.CapabilityReport;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.runtime.OciRuntimeKind;
import com.studioquack.sqs.sandbox.secrets.SecretResolution;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.sandbox.warmpool.WarmSlot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Network Access Control, caller side, on the dispatch path: a job container on an engine that
 * holds a resolvable outbound grant gets its ACCEPT before the job runs and loses it before the
 * slot returns to the pool; on an engine without one the dispatch neither inspects the
 * container nor programs a rule.
 */
class PodmanSandboxOverlayGrantTest {

    private static final List<SandboxNet> NETS = List.of(
            new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"));
    private static final String BRIDGE_IP = "10.89.0.5";

    @TempDir
    Path tempDir;

    @Test
    void grantedEngine_programsTheGrantForTheRun_andClearsItBeforeRelease() throws Exception {
        Path podmanLog = tempDir.resolve("podman.log");
        List<String> perbox = new CopyOnWriteArrayList<>();
        NetworkRuleStore store = NetworkRuleStore.inMemory();
        OverlayPeerDirectory directory = new OverlayPeerDirectory();
        SandboxEgressManager egress = enforcedManager(perbox, podmanLog, store, directory);
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9"));
        WarmPool pool = poolWithWarmSlot();
        AtomicBoolean clearedBeforeRelease = new AtomicBoolean();
        doAnswer(inv -> {
            clearedBeforeRelease.set(perbox.stream().anyMatch(c -> c.startsWith("-D ")));
            return null;
        }).when(pool).release(any());

        execution(installFakePodman(podmanLog), pool, egress).execute(openJob());

        assertTrue(podmanRan(podmanLog, "exec"), "the job must have run");
        assertEquals(3, perbox.size(), "check + insert at start, one delete at teardown: " + perbox);
        String insert = perbox.get(1);
        assertTrue(insert.startsWith("-A ") && insert.contains(BRIDGE_IP)
                        && insert.contains("100.64.1.9/32") && insert.contains("2222")
                        && insert.endsWith("[before exec]"),
                "the grant is in place before the job runs: " + perbox);
        assertTrue(perbox.get(2).startsWith("-D ") && perbox.get(2).contains("100.64.1.9/32")
                        && perbox.get(2).endsWith("[after exec]"),
                "and retracted after it: " + perbox);
        assertTrue(clearedBeforeRelease.get(), "the slot must not return to the pool still granted");
    }

    @Test
    void engineWithoutGrants_neverInspectsTheContainer_norProgramsARule() throws Exception {
        Path podmanLog = tempDir.resolve("podman-nogrant.log");
        List<String> perbox = new CopyOnWriteArrayList<>();
        SandboxEgressManager egress = enforcedManager(perbox, podmanLog,
                NetworkRuleStore.inMemory(), new OverlayPeerDirectory());

        execution(installFakePodman(podmanLog), poolWithWarmSlot(), egress).execute(openJob());

        assertTrue(podmanRan(podmanLog, "exec"), "the job must have run");
        assertFalse(podmanRan(podmanLog, "inspect"), "zero added cost: no bridge-IP lookup");
        assertTrue(perbox.isEmpty(), "and no per-container rule: " + perbox);
    }

    /** ENFORCED manager whose host exec records, after initialization, every PERBOX rule op and whether the job ran yet. */
    private static SandboxEgressManager enforcedManager(List<String> perbox, Path podmanLog,
                                                        NetworkRuleStore store,
                                                        OverlayPeerDirectory directory) {
        SandboxEgressManager.CommandExec exec = (argv, timeout) -> {
            if (argv.size() > 2 && OverlayEgressRules.CHAIN_PERBOX.equals(argv.get(2))
                    && !"-N".equals(argv.get(1))) {
                perbox.add(String.join(" ", argv.subList(1, argv.size()))
                        + (podmanRan(podmanLog, "exec") ? " [after exec]" : " [before exec]"));
            }
            return argv.contains("-C") ? new ExecResult(false, 1, "Bad rule") : new ExecResult(true, 0, "");
        };
        SandboxEgressManager manager = SandboxEgressManager.forTest("auto", true, true, "sqnet0",
                Platform.supported(), "/usr/bin/podman", exec, cli -> NETS, directory, store);
        manager.overlayUp("100.64.1.7/32");
        assertEquals(EgressReadiness.State.ENFORCED, manager.readiness().state());
        perbox.clear();
        return manager;
    }

    private static WarmPool poolWithWarmSlot() {
        WarmPool pool = mock(WarmPool.class);
        when(pool.acquire(any(), any(), any())).thenReturn(Optional.of(
                new WarmSlot("python", "tok", "cid-1", "warm", null, Instant.now())));
        return pool;
    }

    private static SandboxJobRequest openJob() {
        return SandboxJobRequest.builder("j-grant", SandboxLanguage.PYTHON, "print(1)\n")
                .egress(EgressPolicy.OPEN)
                .build();
    }

    private static PodmanSandboxExecution execution(ContainerRuntime runtime, WarmPool pool,
                                                    SandboxEgressManager egress) {
        SecretResolver secrets = mock(SecretResolver.class);
        when(secrets.resolveToEnv(any())).thenReturn(new SecretResolution(null, 0, List.of()));
        return new PodmanSandboxExecution(
                runtime,
                secrets,
                new Redactor(),
                mock(HelperRequestRouter.class),
                pool,
                new MockEnvironment(),
                mock(DependencyManifestHolder.class),
                SandboxOwnership.unowned(),
                new com.studioquack.sqs.runtime.DispatchCancellationRegistry(),
                egress);
    }

    private static boolean podmanRan(Path log, String subcommand) {
        try {
            return Files.isRegularFile(log) && Files.readAllLines(log).stream()
                    .anyMatch(l -> l.equals(subcommand) || l.startsWith(subcommand + "\t"));
        } catch (Exception e) {
            return false;
        }
    }

    /** Fake podman: logs every call; {@code inspect} answers the container's bridge IP. */
    private ContainerRuntime installFakePodman(Path log) throws Exception {
        Path cli = tempDir.resolve("fake-podman-" + log.getFileName());
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
                  inspect) printf '%s\\n'; exit 0 ;;
                  *) exit 0 ;;
                esac
                """.formatted(log.toAbsolutePath(), BRIDGE_IP);
        Files.writeString(cli, script, StandardCharsets.UTF_8);
        assertTrue(cli.toFile().setExecutable(true));
        return pinnedRuntime(cli);
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
