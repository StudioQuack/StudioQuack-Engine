package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.egress.StubSandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.oci.CgroupCaps;
import com.studioquack.sqs.sandbox.oci.CgroupCapsTestSupport;
import com.studioquack.sqs.sandbox.runtime.CapabilityReport;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.runtime.OciRuntimeKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stateful lifecycle podman flow against a deterministic {@link FakeOciCli}, so it runs with NO
 * real container runtime (the live path is a runtime-gated integration concern). Pins the FIXED
 * result shapes + the routing registry the dispatch path reads.
 */
class StatefulInstanceManagerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private FakeOciCli cli;
    private StatefulRuntimeProperties props;
    private StatefulInstanceManager manager;

    @TempDir
    Path blobDir;

    /** Container State.Status the fake inspect reports; flip per-test. */
    private String inspectState = "running";
    /** Top-level {@code RestartCount} from {@code podman inspect}. */
    private int restartCount;
    /** stderr returned by {@code podman logs} — the dead container's own boot output. */
    private String containerLogs = "";
    private boolean runFails;
    /** When true, a {@code run} carrying {@code --pids-limit} fails the way crun does without a pids controller. */
    private boolean crunLacksPidsController;
    /** When true, the helper-broker plant {@code podman exec} returns non-zero. */
    private boolean plantFails;
    /** Stdout of {@code podman image inspect --format '{{json .Config.Labels}}'}. Empty = no labels. */
    private String imageLabelsJson = "";
    /** When true, image inspect returns non-zero (start must still succeed, not capable). */
    private boolean imageInspectFails;
    /** When true, the app-readiness probe {@code podman exec} exits non-zero (listener never up). */
    private boolean appProbeFails;
    /** Exit code the failed probe returns: 1 = deadline reached, anything else = broken probe. */
    private int appProbeExit = 1;
    /**
     * First {@code podman run} keeps the historical id so existing single-start assertions stay
     * pinned. Later runs (image-replace successors) get a distinct suffix so in-flight accounting
     * cannot collapse old and successor onto one key.
     */
    private final AtomicInteger runIdSeq = new AtomicInteger();

    @BeforeEach
    void setUp() {
        cli = new FakeOciCli();
        props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        cli.responder = this::respond;
        // Broker disabled in this unit suite — planting uses a real ProcessBuilder against the CLI
        // path and would race FakeOciCli; lifecycle/routing coverage does not need the broker.
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                null, false, 10);
    }

    private OciCli.Result respond(List<String> argv) {
        String sub = argv.size() > 1 ? argv.get(1) : "";
        return switch (sub) {
            case "run" -> crunLacksPidsController && argv.contains("--pids-limit")
                    ? new OciCli.Result(127, "", "Error: crun: open `pids.max` for writing: No such file or "
                            + "directory: OCI runtime attempted to invoke a command that was not found", false)
                    : runFails
                    ? new OciCli.Result(125, "", "container startup failed", false)
                    : new OciCli.Result(0, nextRunContainerId() + "\n", "", false);
            case "load" -> new OciCli.Result(0, "Loaded image: myenv/pg:1 sha256:deadbeefcafe0011", "", false);
            case "logs" -> new OciCli.Result(0, "", containerLogs, false);
            case "volume" -> // `volume exists` returns non-zero (absent) → create; `volume create` ok.
                    argv.contains("exists") ? new OciCli.Result(1, "", "no such volume", false)
                            : new OciCli.Result(0, "", "", false);
            case "inspect" -> new OciCli.Result(0,
                    "[{\"Id\":\"container-abc123def456\",\"OCIRuntime\":\"crun\",\"RestartCount\":" + restartCount
                            + ",\"State\":{\"Status\":\"" + inspectState
                            + "\",\"ExitCode\":" + ("exited".equals(inspectState) ? "137" : "0")
                            + ",\"OOMKilled\":" + ("exited".equals(inspectState) ? "true" : "false")
                            + ",\"Health\":{\"Status\":\"healthy\"}}}]", "", false);
            case "image" -> {
                if (argv.contains("inspect") && imageInspectFails) {
                    yield new OciCli.Result(125, "", "image inspect failed", false);
                }
                // Labels format vs digest format: Labels JSON when the format mentions Labels.
                String fmt = "";
                int fi = argv.indexOf("--format");
                if (fi >= 0 && fi + 1 < argv.size()) {
                    fmt = argv.get(fi + 1);
                }
                if (fmt.contains("Labels") || fmt.contains("json")) {
                    yield new OciCli.Result(0, imageLabelsJson, "", false);
                }
                yield new OciCli.Result(0, "sha256:deadbeefcafe0011", "", false);
            }
            case "exec" -> {
                // Plant script writes the broker via base64; fail that path when plantFails is set.
                String script = argv.isEmpty() ? "" : argv.get(argv.size() - 1);
                if (plantFails && script.contains("base64 -d")) {
                    yield new OciCli.Result(127, "", "python3: not found", false);
                }
                // The cold-start readiness probe rides stdin (sh -s) — the Windows-safe transport.
                String stdin = new String(cli.lastStdin, StandardCharsets.UTF_8);
                if (appProbeFails && stdin.contains("SQ_END")) {
                    yield new OciCli.Result(appProbeExit, "", "listener never came up", false);
                }
                yield new OciCli.Result(0, "", "", false);
            }
            default -> new OciCli.Result(0, "", "", false);
        };
    }

    /** Count of recorded argv lists that include the {@code run} subcommand. */
    private long runCallCount() {
        return cli.calls.stream().filter(argv -> argv.contains("run")).count();
    }

    /** First run stays {@code container-abc123def456}; later runs append {@code -2}, {@code -3}, … */
    private String nextRunContainerId() {
        int n = runIdSeq.incrementAndGet();
        return n == 1 ? "container-abc123def456" : "container-abc123def456-" + n;
    }

    /** {@code --name} token of the most recent recorded {@code podman run}, or null. */
    private String lastRunContainerName() {
        List<String> run = null;
        for (List<String> argv : cli.calls) {
            if (argv.contains("run")) {
                run = argv;
            }
        }
        if (run == null) {
            return null;
        }
        int idx = run.indexOf("--name");
        return idx >= 0 && idx + 1 < run.size() ? run.get(idx + 1) : null;
    }

    @Test
    void start_refuses_when_egress_unavailable() {
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                null, false, 10, StubSandboxEgressManager.unavailable("overlay-down"));

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertEquals("sandbox egress unavailable: overlay-down", r.message());
        assertNull(cli.firstCallWith("run"), "UNAVAILABLE must not spawn");
    }

    @Test
    void start_retries_without_the_cgroup_cap_crun_cannot_open() {
        crunLacksPidsController = true;
        try {
            StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

            assertEquals(StatefulStatus.RUNNING, r.status());
            assertEquals(2, runCallCount(), "one refused start, one retry without the pids cap");
            List<String> retried = cli.calls.stream().filter(argv -> argv.contains("run"))
                    .reduce((first, second) -> second).orElseThrow();
            assertFalse(retried.contains("--pids-limit"));
            assertFalse(CgroupCaps.applies(CgroupCaps.Controller.PIDS));
        } finally {
            CgroupCapsTestSupport.reset();
        }
    }

    @Test
    void start_does_not_retry_an_ordinary_run_failure() {
        runFails = true;

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertEquals(1, runCallCount());
        assertTrue(CgroupCaps.applies(CgroupCaps.Controller.PIDS));
    }

    @Test
    void start_proceeds_when_egress_enforced() {
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                null, false, 10, StubSandboxEgressManager.enforced());

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, r.status());
        assertNotNull(cli.firstCallWith("run"));
    }

    @Test
    void start_fails_loudly_when_app_listener_never_comes_up() {
        appProbeFails = true;

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertTrue(r.message() != null && r.message().contains("app listener not ready"),
                "gate failure must name the cause, was: " + r.message());
        // The failed container is reaped so the next cold start is not blocked by a stale name.
        assertTrue(cli.sawCallContaining("rm", "-f"), "failed start must remove the container");
    }

    @Test
    void start_names_container_death_when_the_probe_is_killed_mid_boot() {
        appProbeFails = true;
        inspectState = "exited"; // container died (e.g. cgroup OOM under a tight memory cap)

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertTrue(r.message() != null && r.message().contains("exited during boot")
                        && r.message().contains("OOM"),
                "a dead container must be named, was: " + r.message());
    }

    @Test
    void start_carries_the_dead_containers_last_output_line_before_removing_it() {
        appProbeFails = true;
        inspectState = "exited";
        containerLogs = "booting\nStudioQuackDispatch: bind failed on 0.0.0.0:8080/invoke (BindException)\n";

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertTrue(r.message() != null && r.message().contains("bind failed on 0.0.0.0:8080"),
                "the app's own last line must survive the container removal, was: " + r.message());
        assertTrue(cli.sawCallContaining("logs", "--tail"), "boot output must be read before rm");
        String kept = manager.byInstanceId(r.instanceId()).map(StatefulInstance::lastBootOutput).orElse(null);
        assertTrue(kept != null && kept.contains("bind failed on 0.0.0.0:8080"),
                "stateful-instance-logs serves this once the container is gone, was: " + kept);
    }

    @Test
    void start_runs_readiness_probe_before_reporting_running() {
        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, r.status());
        // The probe script is piped via sh -s (stdin): a quote-bearing sh -c argv element would
        // be shattered by the Win32 command-line round-trip on Windows hosts (see ShExec).
        assertTrue(cli.calls.stream().anyMatch(argv -> argv.contains("exec") && argv.contains("-s")),
                "start must gate on the in-container app listener via the stdin transport");
        assertTrue(cli.stdins.stream()
                        .map(b -> new String(b, StandardCharsets.UTF_8))
                        .anyMatch(s -> s.contains("SQ_END")),
                "the probe script rides stdin");
    }

    @Test
    void start_survives_a_broken_readiness_probe() {
        // Exit != 1 means the PROBE is broken (syntax error, wedged exec, wall-clock timeout),
        // not the app — the gate fails open and the launcher request becomes the real test.
        appProbeFails = true;
        appProbeExit = 2;

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, r.status(),
                "a broken probe must not brick an otherwise healthy start");
    }

    @Test
    void crash_looping_container_is_failed_not_reported_running() {
        // The probe exec is killed (137) because the app died; --restart unless-stopped has
        // already re-run it, so the state reads "running" — the restart count is the truth.
        appProbeFails = true;
        appProbeExit = 137;
        restartCount = 3;
        containerLogs = "StudioQuackDispatch: bind failed on 0.0.0.0:8080/invoke (BindException: x)\n";

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertTrue(r.message() != null && r.message().contains("restarted 3x")
                && r.message().contains("runtime=crun")
                && r.message().contains("bind failed"), r.message());
    }

    @Test
    @SuppressWarnings("unchecked")
    void start_refuses_below_the_enforced_isolation_floor() {
        // Tier "best" is enforced, but this host's runsc went undetected — the instance must not
        // fall back to the shared kernel the way an unchecked stateful start used to.
        ContainerRuntime rt = new ContainerRuntime(mapper, "best", true);
        CapabilityReport noGvisor = new CapabilityReport(true, OciRuntimeKind.PODMAN, "/usr/bin/podman",
                "5.0.0", List.of(), false, true, IsolationTier.BEST, IsolationTier.INTERMEDIATE,
                IsolationTier.INTERMEDIATE, "", "");
        ((AtomicReference<CapabilityReport>) ReflectionTestUtils.getField(rt, "report")).set(noGvisor);
        manager = new StatefulInstanceManager(cli, mapper, rt, props, new OciImageBlobStore(props),
                null, false, 10);

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.FAILED, r.status());
        assertTrue(r.message() != null && r.message().contains("isolation floor"), r.message());
        assertNull(cli.firstCallWith("run"), "a refused start must not spawn a container");
    }

    @Test
    void readiness_script_skips_the_gate_when_no_probe_tool_exists() {
        String script = StatefulInstanceManager.appReadinessScript(60);

        // No-tool images exit 0 (gate degrades to a no-op; the launcher reports that case).
        assertTrue(script.contains("else"), "script must have the no-tool fallback branch");
        assertTrue(script.contains("python3") && script.contains("curl") && script.contains("wget"),
                "probe must try python3 → curl → wget");
        assertTrue(script.contains("+ 60"), "timeout is baked into the deadline");
    }

    @Test
    void start_records_ids_reports_running_and_is_routable() {
        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true,
                new StatefulCaps(2, 1024, 4096, null));

        assertEquals(StatefulStatus.RUNNING, r.status());
        assertNotNull(r.instanceId(), "start records an instanceId");
        assertEquals("container-abc123def456", r.containerId(), "start records the container id");
        assertNotNull(r.health(), "health captured from inspect");

        // A `podman run` NOT using --rm, named, with the volume, was issued.
        List<String> run = cli.firstCallWith("run");
        assertNotNull(run);
        assertFalse(run.contains("--rm"), "stateful run is never --rm");
        assertTrue(run.contains("--name"));

        // Routable by (env + imageRef) → live container.
        assertTrue(manager.resolveRunning("env-1", "myenv/pg:1").isPresent(), "live instance is routable");
        assertTrue(manager.hasLiveInstance("env-1", "myenv/pg:1"), "live predicate matches resolver");
        assertEquals("container-abc123def456",
                manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().containerId());
        assertTrue(manager.resolveRunning("env-1", "other:ref").isEmpty(), "routing keyed by (env, ref)");
        assertFalse(manager.hasLiveInstance("env-1", "other:ref"), "different image is not live");
    }

    @Test
    void stop_marks_stopped_and_drops_routing() {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true, null);

        StatefulInstanceManager.StopResult stop = manager.stop("env-1", started.instanceId());

        assertEquals(StatefulStatus.STOPPED, stop.status());
        assertTrue(cli.sawCallContaining("stop"), "podman stop issued");
        assertTrue(cli.sawCallContaining("rm", "-f"), "podman rm issued");
        assertTrue(manager.resolveRunning("env-1", "myenv/pg:1").isEmpty(), "stopped instance is not routable");
        assertFalse(manager.hasLiveInstance("env-1", "myenv/pg:1"), "stopped instance is not live");
    }

    @Test
    void second_start_while_live_joins_the_instance() {
        // One live instance per (env, imageRef). A start that meets a genuinely-live container
        // joins it (same ids, no new container, no bounce): the dispatch cold-start race loser and
        // a control-plane start under a diverged backend row both land on the one live instance.
        StatefulInstanceManager.StartResult first = manager.start("env-1", "myenv/pg:1", true, null);
        assertEquals(StatefulStatus.RUNNING, first.status());

        StatefulInstanceManager.StartResult second = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, second.status());
        assertEquals(first.instanceId(), second.instanceId(), "join returns the live identity");
        assertEquals(first.containerId(), second.containerId(), "join returns the live container");
        assertEquals(1, runCallCount(), "join never starts a second container");
        assertFalse(cli.sawCallContaining("stop"), "join never bounces the live container");
        assertTrue(manager.hasLiveInstance("env-1", "myenv/pg:1"), "instance stays live");
    }

    @Test
    void start_with_a_stale_live_record_drops_it_and_starts() {
        // Divergence the other way: the registry thinks RUNNING but the host has nothing — the
        // stale record must not block the start (the old already-live refuse stranded this case).
        StatefulInstanceManager.StartResult first = manager.start("env-1", "myenv/pg:1", true, null);
        assertEquals(StatefulStatus.RUNNING, first.status());
        inspectState = "exited"; // host truth: the tracked container is gone

        StatefulInstanceManager.StartResult second = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, second.status());
        assertEquals(first.instanceId(), second.instanceId(), "identity reused for the stale record");
        assertTrue(manager.hasLiveInstance("env-1", "myenv/pg:1"));
    }

    @Test
    void stop_then_start_reuses_and_runs_again() {
        // Editor Stop → Build & Run cycle depends on restart after STOPPED.
        StatefulInstanceManager.StartResult first = manager.start("env-1", "myenv/pg:1", true, null);
        manager.stop("env-1", first.instanceId());

        StatefulInstanceManager.StartResult again = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, again.status());
        assertEquals(first.instanceId(), again.instanceId(), "identity + volume reused after stop");
        assertTrue(manager.hasLiveInstance("env-1", "myenv/pg:1"));
    }

    @Test
    void daemon_managed_hint_may_reissue_while_live() {
        // Docling path: instanceIdHint keeps idempotent restart even when already RUNNING.
        StatefulInstanceManager.StartResult first = manager.start("local", "docling/serve:1", true,
                StatefulCaps.defaults(), "docling", null, null);
        assertEquals(StatefulStatus.RUNNING, first.status());

        StatefulInstanceManager.StartResult reissue = manager.start("local", "docling/serve:1", true,
                StatefulCaps.defaults(), "docling", null, null);

        assertEquals(StatefulStatus.RUNNING, reissue.status());
        assertEquals("docling", reissue.instanceId());
        assertNull(reissue.message(), "daemon-managed re-issue is not an already-live refuse");
    }

    @Test
    void stop_untracked_instance_reaps_by_deterministic_name() {
        // Daemon restarted while the container kept running: nothing is tracked, but the container
        // name is deterministic (prefix + instanceId), so the orphan is reaped by name instead of
        // being reported STOPPED while still running.
        StatefulInstanceManager.StopResult stop = manager.stop("env-1", "inst_never_seen");

        assertEquals(StatefulStatus.STOPPED, stop.status(), "stopping an unknown instance still reports STOPPED");
        String expectedName = props.getContainerNamePrefix() + "inst_never_seen";
        assertTrue(cli.sawCallContaining("stop", expectedName), "orphan stop issued by name");
        assertTrue(cli.sawCallContaining("rm", "-f", expectedName), "orphan rm issued by name");
    }

    @Test
    void removesNamedContainerWhenPodmanRunFailsAfterCreation() {
        runFails = true;

        StatefulInstanceManager.StartResult result = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.FAILED, result.status());
        assertTrue(cli.sawCallContaining("rm", "-f"),
                "a failed detached start must not leave a created named container behind");
    }

    @Test
    void status_maps_podman_state() {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true, null);

        StatefulInstanceManager.StatusResult running = manager.status("env-1", started.instanceId());
        assertEquals(StatefulStatus.RUNNING, running.status());
        assertEquals("container-abc123def456", running.containerId());
        assertNotNull(running.health());

        // Simulate the container exiting → status reconciles to STOPPED + drops routing.
        inspectState = "exited";
        StatefulInstanceManager.StatusResult exited = manager.status("env-1", started.instanceId());
        assertEquals(StatefulStatus.STOPPED, exited.status());
        assertTrue(manager.resolveRunning("env-1", "myenv/pg:1").isEmpty());
    }

    @Test
    void status_unknown_instance_is_unknown() {
        StatefulInstanceManager.StatusResult r = manager.status("env-1", "inst_missing");
        assertEquals(StatefulStatus.UNKNOWN, r.status());
    }

    @Test
    void load_image_parses_digest_and_reports_loaded() throws Exception {
        // Plant the uploaded blob the manager reads for `podman load`.
        Files.writeString(new OciImageBlobStore(props).pathFor("myenv/pg:1"), "dummy-tar-bytes");

        StatefulInstanceManager.LoadResult r = manager.loadImage("env-1", "myenv/pg:1", "Postgres");

        assertEquals(StatefulStatus.LOADED, r.status());
        assertEquals("myenv/pg:1", r.ref());
        assertEquals("sha256:deadbeefcafe0011", r.digest(), "digest parsed from podman load output");
        assertTrue(cli.sawCallContaining("load", "-i"), "podman load -i issued");
    }

    @Test
    void load_image_without_a_blob_is_failed() {
        StatefulInstanceManager.LoadResult r = manager.loadImage("env-1", "no/blob:here", "x");
        assertEquals(StatefulStatus.FAILED, r.status());
        assertNull(r.digest());
    }

    @Test
    void start_without_runtime_is_failed_but_records_instance() {
        cli.available = false;
        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);
        assertEquals(StatefulStatus.FAILED, r.status());
        assertNotNull(r.instanceId(), "a FAILED start still records an instanceId for later reconcile");
    }

    /**
     * A daemon-managed instance (e.g. Docling) asks for a recognizable, deterministic identity
     * instead of a random {@code inst_<uuid>} — this is what makes "docling" findable by name in
     * {@code podman ps} / the stateful-instance inventory rather than an opaque generated id.
     */
    @Test
    void start_with_instanceIdHint_uses_the_hint_as_the_instance_and_container_name() {
        StatefulInstanceManager.StartResult r = manager.start("local", "docling/serve:1", true,
                StatefulCaps.defaults(), "docling", null, null);

        assertEquals(StatefulStatus.RUNNING, r.status());
        assertEquals("docling", r.instanceId(), "the hint is used verbatim, not a random uuid");
        assertEquals("sq-stateful-docling", manager.containerNameFor("docling"));
        List<String> run = cli.firstCallWith("run");
        assertNotNull(run);
        assertTrue(run.contains("--name") && run.contains("sq-stateful-docling"),
                "the container is named deterministically from the hint");
    }

    /** A daemon-managed instance the DAEMON PROCESS itself calls into needs its port published. */
    @Test
    void start_with_hostPort_publishes_a_loopback_port_mapping() {
        manager.start("local", "docling/serve:1", true, StatefulCaps.defaults(), "docling", 5001, 5001);

        List<String> run = cli.firstCallWith("run");
        assertNotNull(run);
        int idx = run.indexOf("-p");
        assertTrue(idx >= 0 && idx + 1 < run.size(), "-p flag with a value was emitted");
        assertEquals("127.0.0.1:5001:5001", run.get(idx + 1), "loopback-only port publish");
    }

    /**
     * BYO images may ship curl but no python3 — plant fails, the instance stays RUNNING, and the
     * health Object names the cause so Engine Settings cannot miss it.
     */
    @Test
    void start_with_plant_failure_stays_running_and_health_names_missing_python3() {
        plantFails = true;
        HelperRequestRouter helper = new HelperRequestRouter(mapper,
                new com.studioquack.sqs.aimodel.AiModelProviderProxy(
                        new com.studioquack.sqs.aimodel.AiModelProviderHolder(), mapper), null);
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                helper, true, 10);

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, r.status(), "plant failure must not fail the start");
        assertNotNull(r.health());
        assertInstanceOf(Map.class, r.health());
        @SuppressWarnings("unchecked")
        Map<String, Object> health = (Map<String, Object>) r.health();
        assertEquals("unavailable", health.get("sqHelperBroker"));
        Object note = health.get("sqHelperBrokerNote");
        assertNotNull(note, "unavailable broker must carry a cause-naming note");
        assertTrue(String.valueOf(note).contains("python3"),
                "note must name python3: " + note);
        assertEquals("healthy", health.get("Status"), "podman health content must remain");
    }

    /**
     * The belt-and-suspenders removal path a daemon-managed caller uses when its own tracked
     * instanceId may be stale (e.g. right after a daemon restart, before anything is tracked again).
     */
    @Test
    void stopByContainerName_removes_the_container_even_when_nothing_is_tracked() {
        manager.stopByContainerName("sq-stateful-docling");

        assertTrue(cli.sawCallContaining("stop", "sq-stateful-docling"), "podman stop issued by name");
        assertTrue(cli.sawCallContaining("rm", "-f", "sq-stateful-docling"), "podman rm issued by name");
    }

    @Test
    void recycleForImage_bounces_live_instance_with_same_id_and_caps() {
        StatefulCaps caps = new StatefulCaps(2, 1024, 4096, null);
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true, caps);
        assertEquals(StatefulStatus.RUNNING, started.status());
        String instanceId = started.instanceId();
        assertEquals(1, runCallCount());

        StatefulInstanceManager.RecycleResult recycled = manager.recycleForImage("env-1", "myenv/pg:1");

        assertTrue(recycled.recycled());
        assertEquals(instanceId, recycled.instanceId(), "identity reused via reuseOrCreate");
        assertEquals(StatefulStatus.RUNNING, recycled.status());
        assertTrue(manager.hasLiveInstance("env-1", "myenv/pg:1"));
        StatefulInstance live = manager.byInstanceId(instanceId).orElseThrow();
        assertEquals(StatefulStatus.RUNNING, live.status());
        assertEquals(caps, live.caps(), "caps captured before stop and reapplied");
        assertEquals(2, runCallCount(), "a second podman run was issued");
    }

    @Test
    void recycleForImage_absent_is_noop() {
        StatefulInstanceManager.RecycleResult recycled = manager.recycleForImage("env-unknown", "no/such:ref");

        assertFalse(recycled.recycled());
        assertEquals(0, runCallCount(), "no podman run issued");
    }

    @Test
    void recycleForImage_skips_platform_managed_docling() {
        StatefulInstanceManager.StartResult started = manager.start("local", "docling/serve:1", true,
                StatefulCaps.defaults(), "docling", null, null);
        assertEquals(StatefulStatus.RUNNING, started.status());
        long runs = runCallCount();

        StatefulInstanceManager.RecycleResult recycled = manager.recycleForImage("local", "docling/serve:1");

        assertFalse(recycled.recycled(), "platform-managed instances are not bounced");
        assertTrue(manager.hasLiveInstance("local", "docling/serve:1"), "instance stays live");
        assertEquals(runs, runCallCount(), "no extra podman run");
    }

    @Test
    void start_reads_dispatch_label_from_image_and_stamps_container() {
        imageLabelsJson = "{\"" + StatefulRunArgs.DISPATCH_LABEL + "\":\"1\"}";

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, r.status());
        StatefulInstance live = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
        assertTrue(live.dispatchCapable(), "image label 1 must mark the instance dispatch-capable");
        List<String> run = cli.firstCallWith("run");
        assertNotNull(run);
        assertTrue(run.contains(StatefulRunArgs.DISPATCH_LABEL + "=1"),
                "container must carry the dispatch provenance label: " + run);
        assertTrue(cli.sawCallContaining("image", "inspect"), "image inspect issued before run");
        // Inspect precedes run in the recorded call list.
        int inspectAt = -1;
        int runAt = -1;
        for (int i = 0; i < cli.calls.size(); i++) {
            List<String> argv = cli.calls.get(i);
            if (inspectAt < 0 && argv.contains("image") && argv.contains("inspect")) {
                inspectAt = i;
            }
            if (runAt < 0 && argv.contains("run")) {
                runAt = i;
            }
        }
        assertTrue(inspectAt >= 0 && runAt >= 0 && inspectAt < runAt,
                "image inspect must happen BEFORE podman run");
    }

    @Test
    void start_without_dispatch_label_stays_fifo_and_omits_container_label() {
        imageLabelsJson = "";

        manager.start("env-1", "myenv/pg:1", true, null);

        StatefulInstance live = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
        assertFalse(live.dispatchCapable(), "absent image label → not capable");
        List<String> run = cli.firstCallWith("run");
        assertNotNull(run);
        assertFalse(run.stream().anyMatch(a -> a.startsWith(StatefulRunArgs.DISPATCH_LABEL + "=")),
                "unlabeled image must not stamp the dispatch label on the container");
    }

    @Test
    void start_survives_image_inspect_failure_as_not_capable() {
        imageInspectFails = true;

        StatefulInstanceManager.StartResult r = manager.start("env-1", "myenv/pg:1", true, null);

        assertEquals(StatefulStatus.RUNNING, r.status(), "inspect failure must not fail the start");
        StatefulInstance live = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
        assertFalse(live.dispatchCapable(), "inspect failure → not capable (default FIFO)");
        List<String> run = cli.firstCallWith("run");
        assertNotNull(run);
        assertFalse(run.stream().anyMatch(a -> a.startsWith(StatefulRunArgs.DISPATCH_LABEL + "=")));
    }

    @Test
    void start_treats_true_image_label_as_dispatch_capable() {
        imageLabelsJson = "{\"" + StatefulRunArgs.DISPATCH_LABEL + "\":\"true\"}";

        manager.start("env-1", "myenv/pg:1", true, null);

        assertTrue(manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().dispatchCapable());
        assertTrue(cli.firstCallWith("run").contains(StatefulRunArgs.DISPATCH_LABEL + "=1"),
                "container label is always =1 even when the image said true");
    }

    /**
     * Healthy successor takes routing immediately; the predecessor stays DRAINING (and is not
     * stopped) until in-flight work on the old container id ends.
     */
    @Test
    void replaceWithCurrentImage_healthy_successor_drains_then_stops_old() throws Exception {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true,
                StatefulCaps.defaults(), null, null, null, null);
        assertEquals(StatefulStatus.RUNNING, started.status());
        String instanceId = started.instanceId();
        String oldCid = started.containerId();
        // Hold the predecessor object — after the switch {@code byInstanceId} is the successor.
        StatefulInstance predecessor = manager.byInstanceId(instanceId).orElseThrow();
        String oldName = predecessor.containerName();

        manager.beginDispatch(oldCid);

        AtomicReference<StatefulInstanceManager.ImageReplaceResult> result = new AtomicReference<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        CountDownLatch draining = new CountDownLatch(1);
        Thread replace = new Thread(() -> {
            try {
                result.set(manager.replaceWithCurrentImage(instanceId, Duration.ofSeconds(30),
                        phase -> {
                            if (StatefulStatus.DRAINING.equals(phase)) {
                                draining.countDown();
                            }
                        }));
            } catch (Throwable t) {
                failed.set(t);
            }
        }, "image-replace-drain");
        replace.start();

        boolean sawDrain = draining.await(10, TimeUnit.SECONDS);
        boolean routedToSuccessor = manager.resolveRunning("env-1", "myenv/pg:1")
                .map(live -> !oldCid.equals(live.containerId()))
                .orElse(false);
        assertTrue(sawDrain || routedToSuccessor,
                "onPhase DRAINING or routing flip must happen within 10s");

        try {
            StatefulInstance live = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
            assertNotEquals(oldCid, live.containerId(), "routing must point at the successor");
            assertEquals(StatefulStatus.RUNNING, live.status());
            assertEquals(StatefulStatus.DRAINING, predecessor.status());
            assertEquals(oldCid, predecessor.containerId());
            assertTrue(manager.inventory().stream().anyMatch(i ->
                            StatefulStatus.DRAINING.equals(i.status()) && oldCid.equals(i.containerId())),
                    "inventory must list the draining predecessor");
            assertFalse(cli.sawCallContaining("stop", oldName),
                    "old container must stay up while in-flight work drains");
        } finally {
            manager.endDispatch(oldCid);
            replace.join(15_000);
        }

        assertNull(failed.get(), () -> "replace thread failed: " + failed.get());
        assertFalse(replace.isAlive(), "replace must finish after in-flight drain");
        StatefulInstanceManager.ImageReplaceResult r = result.get();
        assertNotNull(r);
        assertTrue(r.replaced());
        assertFalse(r.oldForceStopped());
        assertEquals(StatefulStatus.STOPPED, predecessor.status());
        assertNull(predecessor.containerId());
        StatefulInstance after = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
        assertNotEquals(oldCid, after.containerId());
        assertTrue(cli.sawCallContaining("stop", oldName));
        assertTrue(cli.sawCallContaining("rm", "-f", oldName));
    }

    /**
     * An unhealthy successor is reaped; the old container keeps serving and is never stopped.
     * Flip the probe only AFTER the original start so the first instance still comes up.
     */
    @Test
    void replaceWithCurrentImage_unhealthy_successor_leaves_old_untouched() {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true,
                StatefulCaps.defaults(), null, null, null, null);
        assertEquals(StatefulStatus.RUNNING, started.status());
        String instanceId = started.instanceId();
        String oldCid = started.containerId();
        String oldName = manager.byInstanceId(instanceId).orElseThrow().containerName();
        String successorName = manager.containerNameFor(instanceId) + "-b";

        appProbeFails = true;
        appProbeExit = 1;

        StatefulInstanceManager.ImageReplaceResult r =
                manager.replaceWithCurrentImage(instanceId, Duration.ofSeconds(5));

        assertFalse(r.replaced());
        assertNotNull(r.reason());
        assertFalse(r.reason().isBlank());
        assertTrue(r.reason().contains("app listener") || r.reason().contains("not ready"),
                "failure must name the readiness gate, was: " + r.reason());
        StatefulInstance live = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
        assertEquals(oldCid, live.containerId(), "routing must stay on the original container");
        assertEquals(StatefulStatus.RUNNING, live.status());
        assertEquals(oldCid, live.containerId());
        assertFalse(cli.sawCallContaining("stop", oldName),
                "unhealthy successor must not stop the old container");
        assertTrue(cli.sawCallContaining("rm", "-f", successorName),
                "failed successor name must be reaped");
    }

    /** Drain deadline expires while work is still in-flight → old is force-stopped. */
    @Test
    void replaceWithCurrentImage_drain_timeout_force_stops_old() {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true,
                StatefulCaps.defaults(), null, null, null, null);
        assertEquals(StatefulStatus.RUNNING, started.status());
        String instanceId = started.instanceId();
        String oldCid = started.containerId();
        StatefulInstance predecessor = manager.byInstanceId(instanceId).orElseThrow();
        String oldName = predecessor.containerName();

        manager.beginDispatch(oldCid);
        StatefulInstanceManager.ImageReplaceResult r =
                manager.replaceWithCurrentImage(instanceId, Duration.ofSeconds(2));
        manager.endDispatch(oldCid);

        assertTrue(r.replaced());
        assertTrue(r.oldForceStopped());
        assertEquals(StatefulStatus.STOPPED, predecessor.status());
        assertNull(predecessor.containerId());
        assertTrue(cli.sawCallContaining("stop", oldName));
        assertTrue(manager.resolveRunning("env-1", "myenv/pg:1").isPresent());
    }

    /**
     * Alternate names flip base ↔ {@code base-b} so a second replace does not accumulate
     * {@code -b-b}.
     */
    @Test
    void replaceWithCurrentImage_names_alternate_base_and_base_b() {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true,
                StatefulCaps.defaults(), null, null, null, null);
        assertEquals(StatefulStatus.RUNNING, started.status());
        String instanceId = started.instanceId();
        String base = manager.containerNameFor(instanceId);
        assertEquals(base, manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().containerName());

        StatefulInstanceManager.ImageReplaceResult first =
                manager.replaceWithCurrentImage(instanceId, Duration.ofMillis(50));
        assertTrue(first.replaced());
        assertEquals(base + "-b",
                manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().containerName());
        assertEquals(base + "-b", lastRunContainerName());

        StatefulInstanceManager.ImageReplaceResult second =
                manager.replaceWithCurrentImage(instanceId, Duration.ofMillis(50));
        assertTrue(second.replaced());
        assertEquals(base,
                manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().containerName());
        assertEquals(base, lastRunContainerName());
    }

    @Test
    void replaceWithCurrentImage_not_running_is_refused() {
        StatefulInstanceManager.ImageReplaceResult r =
                manager.replaceWithCurrentImage("no-such", Duration.ZERO);

        assertFalse(r.replaced());
        assertEquals("not running", r.reason());
        assertFalse(r.oldForceStopped());
    }

    /** Starts a replace that stays DRAINING on one held dispatch; returns the running replace thread. */
    private Thread drainingReplace(String instanceId, String oldCid,
                                   AtomicReference<StatefulInstanceManager.ImageReplaceResult> result) throws Exception {
        manager.beginDispatch(oldCid);
        CountDownLatch draining = new CountDownLatch(1);
        Thread replace = new Thread(() -> result.set(manager.replaceWithCurrentImage(instanceId,
                Duration.ofSeconds(30), phase -> draining.countDown())), "image-replace-drain");
        replace.start();
        assertTrue(draining.await(10, TimeUnit.SECONDS), "replace must reach DRAINING");
        return replace;
    }

    /** The drain can last drain-max-wait; anything taking the start lock must not stall behind it. */
    @Test
    void replaceWithCurrentImage_drain_does_not_hold_the_start_lock() throws Exception {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true,
                StatefulCaps.defaults(), null, null, null, null);
        AtomicReference<StatefulInstanceManager.ImageReplaceResult> result = new AtomicReference<>();
        Thread replace = drainingReplace(started.instanceId(), started.containerId(), result);
        try {
            long t0 = System.nanoTime();
            manager.recycleForImage("env-1", "myenv/pg:1", "some-other-container");
            long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(tookMs < 5_000, "start-lock holder blocked for " + tookMs + " ms during the drain");
            assertTrue(replace.isAlive(), "replace is still draining");
        } finally {
            manager.endDispatch(started.containerId());
            replace.join(15_000);
        }
        assertTrue(result.get().replaced());
    }

    /** A dispatch that resolved the old container before the switch can never be admitted to it after. */
    @Test
    void replaceWithCurrentImage_drained_container_never_admits_a_late_dispatch() {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true,
                StatefulCaps.defaults(), null, null, null, null);
        String oldCid = started.containerId();

        assertTrue(manager.replaceWithCurrentImage(started.instanceId(), Duration.ofMillis(50)).replaced());

        String newCid = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().containerId();
        assertFalse(manager.beginDispatch(oldCid), "drained container must refuse admission");
        assertEquals(0, manager.inFlight(oldCid));
        assertTrue(manager.beginDispatch(newCid), "successor admits");
        manager.endDispatch(newCid);
        assertEquals(0, manager.inFlight(newCid));
    }

    /** A second replace while one still drains would `rm -f` the name the predecessor drains under. */
    @Test
    void replaceWithCurrentImage_refused_while_a_previous_replacement_drains() throws Exception {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true,
                StatefulCaps.defaults(), null, null, null, null);
        String oldName = manager.byInstanceId(started.instanceId()).orElseThrow().containerName();
        AtomicReference<StatefulInstanceManager.ImageReplaceResult> first = new AtomicReference<>();
        Thread replace = drainingReplace(started.instanceId(), started.containerId(), first);
        try {
            int callsBefore = cli.calls.size();
            StatefulInstanceManager.ImageReplaceResult second =
                    manager.replaceWithCurrentImage(started.instanceId(), Duration.ofMillis(50));

            assertFalse(second.replaced());
            assertEquals("previous replacement still draining", second.reason());
            assertTrue(cli.calls.subList(callsBefore, cli.calls.size()).stream()
                            .noneMatch(argv -> argv.contains(oldName)),
                    "the refused replace must not touch the draining predecessor");
        } finally {
            manager.endDispatch(started.containerId());
            replace.join(15_000);
        }
        assertTrue(first.get().replaced());
    }

    /** A dead-broker recovery naming a replaced container must leave the routed successor alone. */
    @Test
    void recycleForImage_for_a_replaced_container_does_not_bounce_the_routed_successor() {
        StatefulInstanceManager.StartResult started = manager.start("env-1", "myenv/pg:1", true,
                StatefulCaps.defaults(), null, null, null, null);
        String oldCid = started.containerId();
        assertTrue(manager.replaceWithCurrentImage(started.instanceId(), Duration.ofMillis(50)).replaced());
        StatefulInstance successor = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();

        StatefulInstanceManager.RecycleResult r = manager.recycleForImage("env-1", "myenv/pg:1", oldCid);

        assertFalse(r.recycled());
        assertEquals("already replaced", r.message());
        StatefulInstance after = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
        assertEquals(successor.containerId(), after.containerId(), "successor keeps serving");
        assertFalse(cli.sawCallContaining("stop", successor.containerName()));
    }
}
