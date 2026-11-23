package com.studioquack.sqs.docling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.OciImageBlobStore;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulRuntimeProperties;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link DoclingContainer} unit test — proves it drives Docling through the real {@link
 * StatefulInstanceManager} control plane (against a deterministic {@link FakeOciCli}, no real
 * container runtime), not a second ad-hoc {@code podman run} lifecycle
 */
class DoclingContainerTest {

    private HttpServer fakeDoclingServer;
    /**
     * The manager's reverse relay — a mock whose overlay starts DOWN (the daemon-boot race
     * posture): the reverse publish defers instead of reaping the healthy container. The relay's
     * real bind/ACL behaviour is covered by {@code OverlayReversePortRelayTest}.
     */
    private OverlayReversePortRelay relay;

    @AfterEach
    void tearDown() {
        if (fakeDoclingServer != null) {
            fakeDoclingServer.stop(0);
        }
    }

    /** {@code -e NAME} in argv, value only in the recorded process environment. */
    private static void assertEnv(FakeOciCli cli, java.util.List<String> argv, String name, String value) {
        int flag = -1;
        for (int i = 0; i + 1 < argv.size(); i++) {
            if ("-e".equals(argv.get(i)) && name.equals(argv.get(i + 1))) {
                flag = i;
                break;
            }
        }
        assertTrue(flag >= 0, "missing -e " + name + " in " + argv);
        assertFalse(argv.stream().anyMatch(a -> a.startsWith(name + "=") || a.equals(value) && value.length() > 3),
                "env value must not be an argv element: " + name);
        int idx = cli.calls.indexOf(argv);
        assertTrue(idx >= 0 && idx < cli.envs.size(), "no recorded env for the run");
        assertEquals(value, cli.envs.get(idx).get(name));
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private StatefulInstanceManager managerOver(FakeOciCli cli) throws IOException {
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        props.setBlobStore(Files.createTempDirectory("docling-container-test").toString());
        relay = org.mockito.Mockito.mock(OverlayReversePortRelay.class);
        when(relay.anyOverlayUp()).thenReturn(false);
        return new StatefulInstanceManager(cli, new ObjectMapper(), null, props,
                new OciImageBlobStore(props), null, false, 10, null, relay);
    }

    /** No probe → the resolver always lands on the multi-arch CPU image (the safe default). */
    private static DoclingImageResolver cpuOnlyResolver() {
        return new DoclingImageResolver(null, DoclingImageResolver.DEFAULT_GPU_IMAGE);
    }

    @Test
    void ensureRunning_starts_docling_as_a_named_persistent_stateful_instance_with_its_port_published()
            throws Exception {
        int port = freePort();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                // The real container becomes healthy the moment `podman run` succeeds — stand up a
                // fake /health endpoint on the published port so awaitReady() observes it, exactly
                // as the real Docling image's healthcheck would.
                case "run" -> {
                    startFakeDoclingHealthServer(port);
                    yield new OciCli.Result(0, "container-docling-1\n", "", false);
                }
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        DoclingContainer container = new DoclingContainer(managerOver(cli), "docling/serve:1",
                port, port, /* readyTimeoutSeconds */ 5L, /* peerStartGraceSeconds */ 5L,
                /* recreateAfterSeconds */ 30L, /* autoStart */ true,
                /* ramMb */ 0, /* cpus */ 0, /* diskMb */ 0,
                /* numThreads */ 4, /* workers */ 4, /* device */ "cpu", /* gpus */ 0,
                /* pullTimeoutSeconds */ 300,
                "http://127.0.0.1:" + port, cpuOnlyResolver(), null);

        assertDoesNotThrow(container::ensureRunning, "the stateful instance comes up and becomes healthy");

        List<String> run = cli.firstCallWith("run");
        assertNotNull(run, "podman run was issued through the manager, not skipped");
        assertTrue(run.contains("--name") && run.contains("sq-stateful-docling"),
                "recognizable, deterministic container name — not a random inst_<uuid>");
        assertFalse(run.contains("--rm"), "docling is a persistent stateful instance, not ephemeral");
        int publishIdx = run.indexOf("-p");
        assertTrue(publishIdx >= 0, "the daemon-reachable port is published");
        assertEquals("127.0.0.1:" + port + ":" + port, run.get(publishIdx + 1),
                "loopback-only — the converter has no business being reachable from elsewhere");
        // Unset RAM uses the host ceiling (not unbounded). CPU stays unset.
        int ceiling = com.studioquack.sqs.sandbox.oci.SandboxHostBudget.maxRamMb(0);
        assertEquals(ceiling + "m", run.get(run.indexOf("--memory") + 1));
        assertEquals(ceiling + "m", run.get(run.indexOf("--memory-swap") + 1), "swap disabled");
        assertEquals("1024", run.get(run.indexOf("--pids-limit") + 1));
        assertFalse(run.contains("--cpus"), "no CPU cap is imposed by default");
        // The unused sync endpoint's wait ceiling is not stamped — the async path's budget is
        // DoclingClient's poll loop (sqs.docling.convert-timeout-seconds), not a side-car env.
        assertTrue(run.stream().noneMatch(a -> a.contains("DOCLING_SERVE_MAX_SYNC_WAIT")),
                "DOCLING_SERVE_MAX_SYNC_WAIT is the unused sync-endpoint ceiling: " + run);
        assertEnv(cli, run, DoclingContainer.NUM_THREADS_ENV, "4");
        assertEnv(cli, run, DoclingContainer.WORKERS_ENV, "4");
        assertEnv(cli, run, DoclingContainer.DEVICE_ENV, "cpu");
        // Single-tenant side-car: failed tasks must return the RAW error, not docling-serve's
        // sanitised "Internal processing error." placeholder — the async client surfaces it verbatim.
        assertEnv(cli, run, DoclingContainer.DEBUG_ERROR_DETAILS_ENV, "true");
        // The readiness gate probes 127.0.0.1:$SQ_APP_PORT (default 8080); docling-serve binds its
        // own port, so the container env must point the gate at it or a healthy start is reaped.
        assertEnv(cli, run, "SQ_APP_PORT", Integer.toString(port));
        // The default responder answers `image exists` with ok, so the image is already local and
        // NO pull is issued — the long-budget pull is reserved for a genuinely absent image.
        assertFalse(cli.sawCallContaining("pull"),
                "an image already in the local store must not be re-pulled on every start");
        assertFalse(run.contains("--gpus"),
                "cpu resolution never emits a GPU flag: " + run);
    }

    @Test
    void ensureRunning_recreates_a_healthy_side_car_left_on_a_stale_image_by_a_previous_build()
            throws Exception {
        int port = freePort();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                case "run" -> {
                    startFakeDoclingHealthServer(port);
                    yield new OciCli.Result(0, "container-docling-1\n", "", false);
                }
                // The container dying releases the published port — mirror that or the recreate's
                // awaitPortReleased() outlives the test.
                case "stop", "rm" -> {
                    if (fakeDoclingServer != null) {
                        fakeDoclingServer.stop(0);
                        fakeDoclingServer = null;
                    }
                    yield new OciCli.Result(0, "", "", false);
                }
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        StatefulInstanceManager manager = managerOver(cli);
        // The PREVIOUS build (no ASR layer) starts and registers the side-car on the base image.
        DoclingContainer previousBuild = new DoclingContainer(manager, "docling/serve:1",
                port, port, 5L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:" + port, cpuOnlyResolver(), null);
        previousBuild.ensureRunning();
        assertTrue(cli.firstCallWith("run").contains("docling/serve:1"),
                "setup: the previous build ran the base image");

        // The UPGRADED build over the same host state: the ASR-derived image is already in the
        // local store (the fake answers `image exists` ok), so it is the desired runtime image —
        // and the live base-image container answers healthy on the port.
        DoclingContainer upgradedBuild = new DoclingContainer(manager, "docling/serve:1",
                port, port, 5L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:" + port, cpuOnlyResolver(),
                new DoclingAsrImageBuilder(manager, true, 120));

        upgradedBuild.ensureRunning();

        assertTrue(cli.sawCallContaining("stop", "sq-stateful-docling"),
                "the drifted base-image container is torn down, not adopted: " + cli.calls);
        List<List<String>> runs = cli.calls.stream()
                .filter(c -> c.size() > 1 && "run".equals(c.get(1))).toList();
        assertEquals(2, runs.size(), "one run per build — the upgrade recreates: " + cli.calls);
        assertTrue(runs.get(1).contains("localhost/sq-docling-serve-asr:1-asr1"),
                "the recreate starts the ASR-derived image: " + runs.get(1));
    }

    /**
     * The Windows slow-link defect: the inline {@code podman run} pull of the ~4.4 GB image was
     * killed at the 120s lifecycle ceiling (exit=-1) and the side-car NEVER came up. The pull now
     * runs BEFORE the start, on its own budget, and only when the image is actually absent.
     */
    @Test
    void ensureRunning_pulls_the_image_first_when_it_is_not_in_the_local_store() throws Exception {
        int port = freePort();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                // `podman image exists <ref>` → exit 1: genuinely absent.
                case "image" -> new OciCli.Result(1, "", "no such image", false);
                case "pull" -> new OciCli.Result(0, "sha256:deadbeef\n", "", false);
                case "run" -> {
                    startFakeDoclingHealthServer(port);
                    yield new OciCli.Result(0, "container-docling-1\n", "", false);
                }
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        DoclingContainer container = new DoclingContainer(managerOver(cli), "docling/serve:1",
                port, port, 5L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:" + port, cpuOnlyResolver(), null);

        assertDoesNotThrow(container::ensureRunning);

        assertTrue(cli.sawCallContaining("image", "exists", "docling/serve:1"),
                "the local store is checked before pulling");
        assertTrue(cli.sawCallContaining("pull", "docling/serve:1"),
                "an absent image is pulled BEFORE podman run — never inline under the 120s cap");
        assertNotNull(cli.firstCallWith("run"), "the start proceeds once the pull landed");
    }

    /** A failed pull is a NAMED error naming the image — and no `podman run` is even attempted. */
    @Test
    void ensureRunning_fails_loudly_naming_the_image_when_the_pull_fails() throws Exception {
        int port = freePort();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                case "image" -> new OciCli.Result(1, "", "no such image", false);
                case "pull" -> new OciCli.Result(125, "",
                        "Error: initializing source docker://bad/image:1: reading manifest", false);
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        DoclingContainer container = new DoclingContainer(managerOver(cli), "bad/image:1",
                port, port, 5L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:" + port, cpuOnlyResolver(), null);

        IllegalStateException ex = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, container::ensureRunning);
        assertTrue(ex.getMessage().contains("could not pull the Docling image bad/image:1"),
                "the pull failure names the image and the budget, not a generic start failure: "
                        + ex.getMessage());
        assertFalse(cli.sawCallContaining("run", "bad/image:1"),
                "a failed pull never proceeds to podman run");
    }

    /** An operator who wants a cap still gets one — "uncapped" is the default, not the only option. */
    @Test
    void ensureRunning_applies_an_explicit_cap_when_one_is_configured() throws Exception {
        int port = freePort();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                case "run" -> {
                    startFakeDoclingHealthServer(port);
                    yield new OciCli.Result(0, "container-docling-1\n", "", false);
                }
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        DoclingContainer container = new DoclingContainer(managerOver(cli), "docling/serve:1",
                port, port, 5L, 5L, 30L, true,
                /* ramMb */ 4096, /* cpus */ 3, /* diskMb */ 0, /* numThreads */ 4, /* workers */ 4,
                "cpu", /* gpus */ 0, /* pullTimeoutSeconds */ 300,
                "http://127.0.0.1:" + port, cpuOnlyResolver(), null);

        assertDoesNotThrow(container::ensureRunning);

        List<String> run = cli.firstCallWith("run");
        int memIdx = run.indexOf("--memory");
        assertTrue(memIdx >= 0);
        assertEquals("4096m", run.get(memIdx + 1));
        assertEquals("3", run.get(run.indexOf("--cpus") + 1));
    }

    @Test
    void ensureRunning_surfaces_a_named_failure_when_the_image_cannot_be_started() throws Exception {
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return "run".equals(sub)
                    ? new OciCli.Result(125, "", "Error: initializing source docker://bad/image:1: reading manifest", false)
                    : new OciCli.Result(0, "", "", false);
        };
        DoclingContainer container = new DoclingContainer(managerOver(cli), "bad/image:1",
                5001, 5001, 1L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:59999", cpuOnlyResolver(), null);

        IllegalStateException ex = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, container::ensureRunning);
        assertTrue(ex.getMessage().contains("bad/image:1"),
                "a missing/unpullable image is a NAMED failure, never a silent no-op: " + ex.getMessage());
    }

    @Test
    void reconcile_disable_removes_the_deterministic_container_even_when_nothing_is_tracked()
            throws Exception {
        FakeOciCli cli = new FakeOciCli();
        DoclingContainer container = new DoclingContainer(managerOver(cli), "docling/serve:1",
                5001, 5001, 5L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:5001", cpuOnlyResolver(), null);

        // No prior ensureRunning() call in THIS process (e.g. right after a daemon restart) — the
        // in-memory registry has nothing tracked, yet disable must still remove the real container.
        container.reconcile(false);
        awaitCallContaining(cli, "sq-stateful-docling", 2_000);

        assertTrue(cli.sawCallContaining("stop", "sq-stateful-docling"));
        assertTrue(cli.sawCallContaining("rm", "-f", "sq-stateful-docling"));
    }

    /**
     * The reinstall defect: an engine deleted while its daemon was offline never ran the
     * uninstaller, so its Docling container (restart=always) revived with the Podman machine on
     * the next install — and predating deterministic naming, it showed next to ours as a SECOND
     * open "Docling" in the stateful inventory. The enable path must reap such strays (any
     * stateful-labeled container in the docling-serve image family without the canonical name)
     * before starting, while leaving unrelated stateful containers alone.
     */
    @Test
    void ensureRunning_reaps_a_previous_installs_stray_docling_container_before_starting()
            throws Exception {
        int port = freePort();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                // Host truth: a stray from the previous install (random legacy name, docling-serve
                // image, running) plus an unrelated user stateful instance that must survive.
                case "ps" -> new OciCli.Result(0,
                        "aaa111\tsq-stateful-inst_ab12cd34ef56\tquay.io/docling-project/docling-serve-cpu:latest\trunning\n"
                                + "bbb222\tsq-stateful-mine\tcustom/app:1\trunning\n", "", false);
                case "run" -> {
                    startFakeDoclingHealthServer(port);
                    yield new OciCli.Result(0, "container-docling-1\n", "", false);
                }
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        DoclingContainer container = new DoclingContainer(managerOver(cli), "docling/serve:1",
                port, port, 5L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:" + port, cpuOnlyResolver(), null);

        assertDoesNotThrow(container::ensureRunning);

        assertTrue(cli.sawCallContaining("stop", "sq-stateful-inst_ab12cd34ef56"),
                "the stray Docling family container is stopped");
        assertTrue(cli.sawCallContaining("rm", "-f", "sq-stateful-inst_ab12cd34ef56"),
                "the stray Docling family container is removed");
        assertFalse(cli.sawCallContaining("sq-stateful-mine"),
                "an unrelated stateful instance is never touched by the sweep");
        assertNotNull(cli.firstCallWith("run"), "our canonical container still starts");
        assertTrue(cli.firstCallWith("run").contains("sq-stateful-docling"));
    }

    /**
     * Disable severs the relationship with the WHOLE Docling family on the host, not just the
     * canonical name: a stray from a previous install must stop serving (and stop showing) the
     * moment the toggle goes off.
     */
    @Test
    void reconcile_disable_also_reaps_stray_docling_family_containers() throws Exception {
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            if ("ps".equals(sub)) {
                return new OciCli.Result(0,
                        "aaa111\tsq-stateful-inst_ab12cd34ef56\tlocalhost/sq-docling-serve-asr:latest-asr1\trunning\n",
                        "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        DoclingContainer container = new DoclingContainer(managerOver(cli), "docling/serve:1",
                5001, 5001, 5L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:5001", cpuOnlyResolver(), null);

        container.reconcile(false);
        awaitCallContaining(cli, "sq-stateful-inst_ab12cd34ef56", 2_000);

        assertTrue(cli.sawCallContaining("rm", "-f", "sq-stateful-docling"),
                "the canonical container is removed as before");
        assertTrue(cli.sawCallContaining("rm", "-f", "sq-stateful-inst_ab12cd34ef56"),
                "the ASR-image stray from a previous install is removed too");
    }

    /**
     * Cross-env latch: with the overlay down at start, the reverse publish DEFERS (the container
     * still starts and serves locally — no relay publish is even attempted once the deferral is
     * known); once the overlay reports up, the next {@code ensureRunning} asserts the publish
     * itself through the manager's ensure path, targeting the container's bridge address.
     */
    @Test
    void ensureRunning_publishes_the_reverse_port_once_the_overlay_is_up() throws Exception {
        int healthPort = freePort();   // hostPort — the fake /health server binds here
        int overlayPort = freePort();  // containerPort — published on the overlay under this port
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                case "run" -> {
                    startFakeDoclingHealthServer(healthPort);
                    yield new OciCli.Result(0, "container-docling-1\n", "", false);
                }
                // Every inspect answers "running, with a bridge address" — the ensure path's
                // publish needs the container's dial target.
                case "inspect" -> new OciCli.Result(0,
                        "[{\"Id\":\"container-docling-1\",\"Name\":\"/sq-stateful-docling\","
                                + "\"State\":{\"Status\":\"running\",\"Running\":true},"
                                + "\"Config\":{\"Labels\":{}},"
                                + "\"NetworkSettings\":{\"Networks\":{\"net\":{\"IPAddress\":\"100.64.0.2\"}}}}]",
                        "", false);
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        DoclingContainer container = new DoclingContainer(managerOver(cli), "docling/serve:1",
                healthPort, overlayPort, 5L, 5L, 30L, true, 0, 0, 0, 4, 4, "cpu", 0, 300,
                "http://127.0.0.1:" + healthPort, cpuOnlyResolver(), null);

        // Overlay down: the publish attempt throws exactly as the real bind does, and the manager
        // must DEFER (container kept, serving locally) instead of reaping it.
        org.mockito.Mockito.doThrow(new IllegalStateException(
                        "reverse port " + overlayPort + " could not be published on network default: "
                                + "overlay network default is down"))
                .when(relay).publish(anyInt(), anyString(), anyInt(), anyString(),
                        org.mockito.ArgumentMatchers.any());

        container.ensureRunning();
        assertNotNull(cli.firstCallWith("run"), "the container starts despite the deferred publish");
        verify(relay).publish(overlayPort, "100.64.0.2", overlayPort, "docling", null);

        // Overlay up: the publish now succeeds; the healthy fast path asserts it via the manager's
        // ensure path (the start-time attempt above was the deferred one).
        when(relay.anyOverlayUp()).thenReturn(true);
        org.mockito.Mockito.doNothing().when(relay).publish(anyInt(), anyString(), anyInt(),
                anyString(), org.mockito.ArgumentMatchers.any());
        container.ensureRunning();

        verify(relay, org.mockito.Mockito.times(2))
                .publish(overlayPort, "100.64.0.2", overlayPort, "docling", null);
    }

    private void startFakeDoclingHealthServer(int port) {
        try {
            fakeDoclingServer = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
            fakeDoclingServer.createContext("/health", exchange -> {
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
            });
            fakeDoclingServer.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@code reconcile} dispatches to its own background thread — poll briefly for the call to land. */
    private void awaitCallContaining(FakeOciCli cli, String token, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cli.sawCallContaining(token)) {
                return;
            }
            Thread.sleep(20);
        }
    }
}
