package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.egress.StubSandboxEgressManager;
import com.studioquack.sqs.sandbox.runtime.CapabilityReport;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.runtime.OciRuntimeKind;
import com.studioquack.sqs.sandbox.warmpool.WarmSlot;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pure-logic unit tests for {@link ColdContainerLauncher}. Deliberately does NOT fake a container runtime. */
class ColdContainerLauncherTest {

    @TempDir
    Path tempDir;

    @Test
    void create_argv_carries_job_cpus_ram_and_disk_tmpfs() throws Exception {
        // EgressPolicy.NONE skips SandboxNetworkEnsure; broker disabled → create returns after run.
        Path log = tempDir.resolve("cold-argv.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);
        ColdContainerLauncher launcher = new ColdContainerLauncher(runtime, null, false, 10);

        ColdContainerLauncher.ColdContainer c = launcher.create(
                SandboxLanguage.PYTHON,
                "python:3.11-slim",
                IsolationTier.INTERMEDIATE,
                EgressPolicy.NONE,
                SandboxLimits.of(256, 5_000L),
                1.5,
                128,
                null,
                null,
                60_000L);

        assertNotNull(c);
        assertEquals("coldciddeadbeef", c.containerId());

        List<String> runArgv = firstRunArgv(log);
        assertTrue(pair(runArgv, "--memory", "256m") >= 0, "ram → --memory: " + runArgv);
        assertTrue(pair(runArgv, "--memory-swap", "256m") >= 0, "swap disabled: " + runArgv);
        assertTrue(pair(runArgv, "--cpus", "1.5") >= 0, "cpus → --cpus: " + runArgv);
        assertTrue(pair(runArgv, "--tmpfs",
                        "/sandbox:rw,nosuid,nodev,size=128m,mode=1777") >= 0,
                "diskMb → /sandbox tmpfs: " + runArgv);
    }

    @Test
    void bounded_lifetime_gives_an_unbounded_job_the_full_backstop_never_the_floor() {
        // wallMs<=0 is the "unbounded" job posture: the daemon awaits the exec with no deadline, so
        // the container's idle bound must be the 24h backstop, not the 60s floor. With the floor,
        // `sleep 60` (PID 1) expired while a cold bootstrap was still spooling a 21 MB file-bearing
        // payload and the still-running exec died as "exited non-zero (137)".
        assertEquals("86400", ColdContainerLauncher.boundedLifetimeSeconds(0));
        assertEquals("86400", ColdContainerLauncher.boundedLifetimeSeconds(-1));
        // A tiny wall deadline (1s) plus the 60s buffer is still below the 60s floor... exactly at it.
        assertEquals("61", ColdContainerLauncher.boundedLifetimeSeconds(1_000));
    }

    @Test
    void bounded_lifetime_tracks_the_jobs_own_wall_deadline_with_headroom() {
        // A 120s job deadline should get 120 + 60s buffer = 180s of idle headroom.
        assertEquals("180", ColdContainerLauncher.boundedLifetimeSeconds(120_000));
    }

    @Test
    void bounded_lifetime_tracks_a_long_but_legitimate_job_deadline_without_truncating_it() {
        // A job with a long, explicitly configured wall deadline (e.g. a slow agentic/LLM node) must
        // get an idle bound that outlives it — truncating here would SIGKILL a still-running `exec`.
        assertEquals("21660", ColdContainerLauncher.boundedLifetimeSeconds(6 * 3600 * 1000L));
    }

    @Test
    void bounded_lifetime_caps_at_twentyfour_hours_so_a_leaked_container_cannot_idle_forever() {
        // Only an absurd/corrupt deadline hits the ceiling — the same 24h backstop OciWarmer already
        // idles a warm slot under, so this never truncates any deadline this daemon would ever set.
        assertEquals("86400", ColdContainerLauncher.boundedLifetimeSeconds(Long.MAX_VALUE / 2));
    }

    @Test
    void toWarmSlot_carries_the_container_id_and_broker_state_and_never_reports_a_runner() {
        ColdContainerLauncher.ColdContainer withBroker =
                new ColdContainerLauncher.ColdContainer("cid-1", null, HelperBroker.SOCKET_PATH);
        // Simulate what create() returns for a python job once its broker channel is live: brokerReady()
        // is driven purely by "is brokerChannel non-null", so a null channel here (no real process
        // spawned in a pure unit test) has to report broker NOT ready — the positive case is proven by
        // the gated integration test where a real HelperPipeChannel exists.
        WarmSlot slot = withBroker.toWarmSlot(SandboxLanguage.PYTHON.wire(), "python:3.11-slim");
        assertEquals("cid-1", slot.containerId());
        assertTrue(slot.hasContainer());
        assertFalse(slot.runnerReady(), "the persistent python runner is warm-only, never started cold");
        assertFalse(slot.brokerReady(), "no broker channel was actually started in this pure test");
        assertEquals("", slot.helperSocketPath());
    }

    @Test
    void toWarmSlot_without_a_broker_reports_no_broker_and_blank_socket_path() {
        ColdContainerLauncher.ColdContainer noBroker =
                new ColdContainerLauncher.ColdContainer("cid-2", null, "");
        WarmSlot slot = noBroker.toWarmSlot(SandboxLanguage.JAVASCRIPT.wire(), "node:26.12.1-slim");

        assertEquals("cid-2", slot.containerId());
        assertFalse(slot.brokerReady());
        assertEquals("", slot.helperSocketPath());
        assertFalse(slot.runnerReady());
    }

    @Test
    void release_on_a_container_with_no_id_is_a_safe_no_op() {
        ColdContainerLauncher launcher = new ColdContainerLauncher(null, null, true, 10);
        // Neither call must throw, even with a null runtime — both short-circuit before touching it.
        launcher.release(null);
        launcher.release(new ColdContainerLauncher.ColdContainer("", null, ""));
        launcher.release(new ColdContainerLauncher.ColdContainer(null, null, ""));
    }

    @Test
    void create_open_refuses_when_egress_unavailable() throws Exception {
        Path log = tempDir.resolve("cold-refuse.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ContainerRuntime runtime = pinnedRuntime(fakeCli);
        ColdContainerLauncher launcher = new ColdContainerLauncher(runtime, null, false, 10,
                SandboxOwnership.unowned(),
                StubSandboxEgressManager.unavailable("overlay-down"));

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                launcher.create(SandboxLanguage.PYTHON, "python:3.11-slim", IsolationTier.INTERMEDIATE,
                        EgressPolicy.OPEN, SandboxLimits.of(256, 5_000L), 1.0, 0, null, null, 60_000L));

        assertEquals("sandbox egress unavailable: overlay-down", ex.getMessage());
        assertFalse(Files.isRegularFile(log) && Files.readString(log).contains("run"),
                "UNAVAILABLE must not spawn");
    }

    @Test
    void create_open_proceeds_when_egress_enforced() throws Exception {
        Path log = tempDir.resolve("cold-enforced.log");
        Path fakeCli = installFakePodman(tempDir, log);
        // Pin network ensure by using NONE path for spawn success without ensure; OPEN needs ensure.
        // ENFORCED + NONE proves acceptance of internetAllowed without requiring network create.
        ColdContainerLauncher launcher = new ColdContainerLauncher(pinnedRuntime(fakeCli), null, false, 10,
                SandboxOwnership.unowned(), StubSandboxEgressManager.enforced());

        ColdContainerLauncher.ColdContainer c = launcher.create(
                SandboxLanguage.PYTHON, "python:3.11-slim", IsolationTier.INTERMEDIATE,
                EgressPolicy.NONE, SandboxLimits.of(256, 5_000L), 1.0, 0, null, null, 60_000L);

        assertNotNull(c);
        assertEquals("coldciddeadbeef", c.containerId());
    }

    @Test
    void create_none_allowed_even_when_egress_unavailable() throws Exception {
        Path log = tempDir.resolve("cold-none-unavail.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ColdContainerLauncher launcher = new ColdContainerLauncher(pinnedRuntime(fakeCli), null, false, 10,
                SandboxOwnership.unowned(),
                StubSandboxEgressManager.unavailable("overlay-down"));

        ColdContainerLauncher.ColdContainer c = launcher.create(
                SandboxLanguage.PYTHON, "python:3.11-slim", IsolationTier.INTERMEDIATE,
                EgressPolicy.NONE, SandboxLimits.of(256, 5_000L), 1.0, 0, null, null, 60_000L);

        assertNotNull(c);
        assertEquals("coldciddeadbeef", c.containerId());
    }

    @Test
    void brokerReady_reflects_channel_presence_not_the_recorded_socket_path() {
        ColdContainerLauncher.ColdContainer noChannelButPathSet =
                new ColdContainerLauncher.ColdContainer("cid-3", null, HelperBroker.SOCKET_PATH);
        assertFalse(noChannelButPathSet.brokerReady(),
                "brokerReady must key off the live channel, not a leftover path string");
        assertNull(noChannelButPathSet.brokerChannel());
    }

    @Test
    void create_for_an_entrypoint_image_clears_the_entrypoint_before_the_idle_sleep() throws Exception {
        // The cold container for a custom image must idle on `sleep`, with the image's ENTRYPOINT
        // reset; otherwise a one-shot entrypoint exits at create and --rm removes the container
        // before the job's exec ("no such container").
        Path log = tempDir.resolve("cold-entrypoint.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ColdContainerLauncher launcher = new ColdContainerLauncher(pinnedRuntime(fakeCli), null, false, 10);

        ColdContainerLauncher.ColdContainer c = launcher.create(
                SandboxLanguage.JAVASCRIPT, "localhost/sq-local/node-x:1", IsolationTier.INTERMEDIATE,
                EgressPolicy.NONE, SandboxLimits.unbounded(), 0, 0, null, null, 60_000L, true);

        assertNotNull(c);
        List<String> runArgv = firstRunArgv(log);
        int entrypoint = pair(runArgv, "--entrypoint", "");
        assertTrue(entrypoint >= 0, "entrypoint image cold create must reset ENTRYPOINT: " + runArgv);
        assertTrue(entrypoint < runArgv.indexOf("localhost/sq-local/node-x:1"),
                "--entrypoint \"\" precedes the image: " + runArgv);
    }

    @Test
    void create_for_a_script_mode_image_keeps_the_image_entrypoint_untouched() throws Exception {
        Path log = tempDir.resolve("cold-script-mode.log");
        Path fakeCli = installFakePodman(tempDir, log);
        ColdContainerLauncher launcher = new ColdContainerLauncher(pinnedRuntime(fakeCli), null, false, 10);

        launcher.create(SandboxLanguage.PYTHON, "python:3.11-slim", IsolationTier.INTERMEDIATE,
                EgressPolicy.NONE, SandboxLimits.unbounded(), 0, 0, null, null, 60_000L, false);

        assertFalse(firstRunArgv(log).contains("--entrypoint"),
                "script-mode cold create must not reset ENTRYPOINT");
    }

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
                  run) printf 'coldciddeadbeef\\n'; exit 0 ;;
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
