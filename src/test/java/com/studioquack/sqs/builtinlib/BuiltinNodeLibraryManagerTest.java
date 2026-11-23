package com.studioquack.sqs.builtinlib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.OciImageBlobStore;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulRuntimeProperties;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link BuiltinNodeLibraryManager} unit test — drives the whole pipeline against a {@link
 * FakeOciCli} (no real container runtime) and a loopback {@link HttpServer} standing in for the
 * release tree
 */
class BuiltinNodeLibraryManagerTest {

    @TempDir
    Path dataDir;

    private HttpServer fakeBackend;
    /** Optional: tarball GET records whether Podman was still down (overlap proof). */
    private volatile BooleanSupplier podmanUpDuringDownload;
    private final AtomicInteger tarballHitsWhilePodmanDown = new AtomicInteger();

    /**
     * When true, an {@code exec} whose stdin contains {@code SQ_END} returns exit 1 (same
     * readiness-gate failure as {@code StatefulInstanceManagerTest}). Default false so start/ensure
     * tests keep working.
     */
    private volatile boolean failAppProbe;

    @AfterEach
    void tearDown() {
        if (fakeBackend != null) {
            fakeBackend.stop(0);
        }
    }

    /* ------------------------------------------------------------------ */
    /* fakes                                                               */
    /* ------------------------------------------------------------------ */

    /** Records buildSync invocations; never runs a real podman build. */
    private static class RecordingBuildService extends OciImageBuildService {
        private final AtomicReference<String> builtRef = new AtomicReference<>();
        private final AtomicInteger builds = new AtomicInteger();
        private boolean buildSucceeds = true;
        /** When non-null, buildSync blocks on this latch so concurrency tests can overlap runs. */
        private volatile CountDownLatch buildGate;

        RecordingBuildService(OciBuildContextStore contextStore, ExecutorService executor) {
            super(null, contextStore, null, null, executor, null);
        }

        @Override
        public SyncBuildResult buildSync(String envId, String buildId, String ref, int wallClockSeconds) {
            builds.incrementAndGet();
            builtRef.set(ref);
            CountDownLatch gate = buildGate;
            if (gate != null) {
                try {
                    assertTrue(gate.await(30, TimeUnit.SECONDS), "test build gate must open");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return buildSucceeds
                    ? new SyncBuildResult(true, "built", "")
                    : new SyncBuildResult(false, "", "boom");
        }
    }

    private StatefulInstanceManager managerOver(FakeOciCli cli) throws IOException {
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        props.setBlobStore(Files.createTempDirectory("builtin-lib-blobs").toString());
        return new StatefulInstanceManager(cli, new ObjectMapper(), null, props,
                new OciImageBlobStore(props), null, false, 10);
    }

    /**
     * Host responder: unique {@code run} ids, optional app-probe failure, volumes absent,
     * image-exists / default-ok otherwise so {@code freshInstall} and {@code alreadyCurrent} stay valid.
     */
    private FakeOciCli hostCli(boolean imagePresent) {
        FakeOciCli cli = new FakeOciCli();
        AtomicInteger runIds = new AtomicInteger();
        cli.responder = argv -> {
            if (argv.contains("image") && argv.contains("exists")) {
                return imagePresent
                        ? new OciCli.Result(0, "", "", false)
                        : new OciCli.Result(1, "", "no such image", false);
            }
            String sub = argv.size() > 1 ? argv.get(1) : "";
            if ("run".equals(sub)) {
                // First id stays historical; later runs (image-replace successors) get a new id
                // so in-flight accounting cannot collapse old and successor onto one key.
                int n = runIds.incrementAndGet();
                String id = "container-builtin-lib-" + n;
                return new OciCli.Result(0, id + "\n", "", false);
            }
            if ("volume".equals(sub)) {
                return argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
            }
            if ("exec".equals(sub) && failAppProbe) {
                String stdin = new String(cli.lastStdin, StandardCharsets.UTF_8);
                if (stdin.contains("SQ_END")) {
                    return new OciCli.Result(1, "", "listener never came up", false);
                }
            }
            return new OciCli.Result(0, "", "", false);
        };
        return cli;
    }

    /** Builds a real (tiny) tar.gz with the Containerfile contract roots; returns its sha256. */
    private static byte[] makeContextTarball(Path work, StringBuilder shaOut) throws Exception {
        Path root = Files.createTempDirectory(work, "ctx");
        // Unique content: two tarballs built in the same second would otherwise share a sha256.
        Files.writeString(root.resolve("Containerfile"), "FROM scratch\n# " + System.nanoTime() + "\n");
        Files.writeString(root.resolve("pom.xml"), "<project/>\n");
        Files.createDirectories(root.resolve("src"));
        Files.createDirectories(root.resolve("helper-src"));
        Files.createDirectories(root.resolve("nodes"));
        Path tar = work.resolve("lib-" + Long.toHexString(System.nanoTime()) + ".tar.gz");
        Process p = new ProcessBuilder("tar", "-czf", tar.toAbsolutePath().toString(),
                "-C", root.toAbsolutePath().toString(),
                "Containerfile", "pom.xml", "src", "helper-src", "nodes").start();
        assertEquals(0, p.waitFor(), "test tarball must build");
        byte[] bytes = Files.readAllBytes(tar);
        shaOut.append(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        return bytes;
    }

    /** Fake release tree; counts tarball GETs so skip-paths can prove no download happened. */
    private String serveReleases(byte[] tarball, String sha) throws IOException {
        return serveReleases(tarball, sha, "lib.tar.gz");
    }

    private String serveReleases(byte[] tarball, String sha, String fileName) throws IOException {
        AtomicInteger tarballHits = new AtomicInteger();
        fakeBackend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeBackend.createContext("/releases/client/node-library/release-manifest.json", ex -> {
            byte[] body = ("{\"version\":\"1.0.0\",\"file\":\"" + fileName + "\",\"sha256\":\"" + sha + "\"}")
                    .getBytes();
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        });
        fakeBackend.createContext("/releases/client/node-library/" + fileName, ex -> {
            tarballHits.incrementAndGet();
            BooleanSupplier podman = podmanUpDuringDownload;
            if (podman != null && !podman.getAsBoolean()) {
                tarballHitsWhilePodmanDown.incrementAndGet();
            }
            ex.sendResponseHeaders(200, tarball.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(tarball);
            }
        });
        fakeBackend.start();
        this.tarballHits = tarballHits;
        return "http://127.0.0.1:" + fakeBackend.getAddress().getPort();
    }

    private AtomicInteger tarballHits = new AtomicInteger();

    private BuiltinNodeLibraryManager manager(FakeOciCli cli, RecordingBuildService builds,
                                              OciBuildContextStore contexts, String baseUrl,
                                              boolean enabled, String envId) throws IOException {
        return manager(managerOver(cli), cli, builds, contexts, baseUrl, enabled, envId);
    }

    /**
     * Same as {@link #manager(FakeOciCli, RecordingBuildService, OciBuildContextStore, String, boolean, String)}
     * but reuses a pre-built {@link StatefulInstanceManager} so a live start on {@code sim}
     * is the instance {@code replaceWithCurrentImage} will drain-replace.
     */
    private BuiltinNodeLibraryManager manager(StatefulInstanceManager sim, FakeOciCli cli,
                                              RecordingBuildService builds,
                                              OciBuildContextStore contexts, String baseUrl,
                                              boolean enabled, String envId) {
        // Single attempt, no backoff — failure-path tests must not pay the real retry policy. Drain
        // defaults to 30m via the existing test ctor; recycle tests hold in-flight work and
        // endDispatch instead of waiting that bound.
        return new BuiltinNodeLibraryManager(sim, builds, contexts, cli,
                com.studioquack.sqs.release.ReleaseHttpClient.forTests(java.net.http.HttpClient.newHttpClient()),
                enabled, baseUrl, envId, dataDir.toString(),
                BuiltinNodeLibraryManager.IMAGE_REF, 1, java.time.Duration.ZERO);
    }

    /** Shared SIM + CLI + manager with a LIVE system instance and a changed manifest sha. */
    private record Wired(BuiltinNodeLibraryManager lib, StatefulInstanceManager sim, FakeOciCli cli,
                         RecordingBuildService builds, ExecutorService executor,
                         String newSha, String oldCid) {}

    /**
     * Old build marker + new release tarball, then {@link StatefulInstanceManager#start} of the
     * platform instance on the same manager {@link BuiltinNodeLibraryManager} will hold.
     */
    private Wired wiredLiveShaChange() throws Exception {
        StringBuilder oldSha = new StringBuilder();
        makeContextTarball(dataDir, oldSha);
        Path markerDir = Files.createDirectories(dataDir.resolve("node-library"));
        Files.writeString(markerDir.resolve("built-sha256.txt"), oldSha + "\n");

        StringBuilder newSha = new StringBuilder();
        byte[] newTarball = makeContextTarball(dataDir, newSha);
        String baseUrl = serveReleases(newTarball, newSha.toString(), "lib-new.tar.gz");

        FakeOciCli cli = hostCli(/* imagePresent= */ true);
        StatefulInstanceManager sim = managerOver(cli);
        var started = sim.start("env-1", BuiltinNodeLibraryManager.IMAGE_REF, true,
                StatefulCaps.uncapped(), BuiltinNodeLibraryManager.INSTANCE_ID, null, null, null);
        assertEquals(StatefulStatus.RUNNING, started.status());
        assertNotNull(started.containerId());

        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        BuiltinNodeLibraryManager lib = manager(sim, cli, builds, contexts, baseUrl, true, "env-1");
        return new Wired(lib, sim, cli, builds, executor, newSha.toString(), started.containerId());
    }

    /**
     * High-priority sampler started before {@code updateNow} so the short RECYCLING window (a few
     * FakeOciCli calls) is not missed.
     */
    private static final class StageWatch implements AutoCloseable {
        private final List<String> stages = new ArrayList<>();
        private volatile boolean sawRecycling;
        private volatile boolean sawDraining;
        private final Thread sampler;

        StageWatch(BuiltinNodeLibraryManager mgr) {
            sampler = new Thread(() -> {
                String last = null;
                while (!sawDraining && !Thread.currentThread().isInterrupted()) {
                    BuiltinNodeLibraryManager.NodeLibrarySnapshot snap = mgr.snapshot();
                    if (snap != null && snap.updateStage() != null) {
                        String stage = snap.updateStage();
                        synchronized (stages) {
                            if (!stage.equals(last)) {
                                stages.add(stage);
                                last = stage;
                            }
                        }
                        if (BuiltinNodeLibraryManager.STAGE_RECYCLING.equals(stage)) {
                            sawRecycling = true;
                        }
                        if (BuiltinNodeLibraryManager.STAGE_DRAINING.equals(stage)) {
                            sawDraining = true;
                            break;
                        }
                    }
                    Thread.onSpinWait();
                }
            }, "nl-stage-sampler");
            sampler.setDaemon(true);
            sampler.setPriority(Thread.MAX_PRIORITY);
            sampler.start();
        }

        List<String> awaitRecyclingThenDraining() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!sawDraining && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            List<String> copy;
            synchronized (stages) {
                copy = new ArrayList<>(stages);
            }
            assertTrue(sawRecycling, "must observe RECYCLING, stages=" + copy);
            assertTrue(sawDraining, "must observe DRAINING after RECYCLING, stages=" + copy);
            int rec = copy.indexOf(BuiltinNodeLibraryManager.STAGE_RECYCLING);
            int drn = copy.indexOf(BuiltinNodeLibraryManager.STAGE_DRAINING);
            assertTrue(rec >= 0 && drn > rec, "DRAINING must follow RECYCLING, stages=" + copy);
            return copy;
        }

        @Override
        public void close() {
            sampler.interrupt();
        }
    }

    private static boolean sawStopOf(FakeOciCli cli, String containerName) {
        return cli.calls.stream().anyMatch(argv -> argv.contains("stop") && argv.contains(containerName));
    }

    /** Waits for the async update pipeline to leave the UPDATING state. */
    private static void awaitPipelineSettled(BuiltinNodeLibraryManager mgr) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        BuiltinNodeLibraryManager.NodeLibrarySnapshot snap = mgr.snapshot();
        while (snap != null && BuiltinNodeLibraryManager.STATE_UPDATING.equals(snap.updateState())) {
            assertTrue(System.nanoTime() < deadline, "update pipeline must settle within 30s");
            Thread.sleep(25);
            snap = mgr.snapshot();
        }
    }

    /* ------------------------------------------------------------------ */
    /* boot ensure                                                         */
    /* ------------------------------------------------------------------ */

    @Test
    void freshInstall_downloadsBuildsAndStartsUncappedSystemInstance() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(/* imagePresent= */ false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);

        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");
        assertFalse(mgr.isUsable(), "first install has no marker — start-run must wait");
        assertNotNull(mgr.startRunBlockReason());
        mgr.runSafely();

        assertTrue(mgr.isUsable(), "successful first build unblocks start-run");
        assertNull(mgr.startRunBlockReason());
        assertTrue(mgr.snapshot().usable());
        assertEquals(1, builds.builds.get(), "image must be built once");
        assertEquals(BuiltinNodeLibraryManager.IMAGE_REF, builds.builtRef.get());
        assertEquals(1, tarballHits.get(), "tarball downloaded exactly once");
        assertEquals(sha.toString(),
                Files.readString(dataDir.resolve("node-library/built-sha256.txt")).trim(),
                "build marker records the manifest sha256");

        List<List<String>> runs = cli.calls.stream()
                .filter(argv -> argv.size() > 1 && "run".equals(argv.get(1))).toList();
        assertEquals(1, runs.size(), "exactly one container run");
        List<String> run = runs.get(0);
        assertTrue(run.contains("sq-stateful-" + BuiltinNodeLibraryManager.INSTANCE_ID),
                "deterministic platform container name");
        assertTrue(run.contains(BuiltinNodeLibraryManager.IMAGE_REF), "runs the built image");
        // The engine's own runtime: no resource limit a host may be unable to apply.
        assertFalse(run.contains("--memory"), "no RAM limit");
        assertFalse(run.contains("--memory-swap"));
        assertFalse(run.contains("--pids-limit"), "no pid limit");
        assertFalse(run.contains("--cpus"), "no CPU limit");
        assertFalse(run.contains("--storage-opt"), "no disk limit");
        executor.shutdownNow();
    }

    @Test
    void upToDateImage_skipsDownloadAndBuildAndOnlyEnsuresInstance() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());
        // Marker from a previous boot's successful build of THIS manifest content.
        Path markerDir = Files.createDirectories(dataDir.resolve("node-library"));
        Files.writeString(markerDir.resolve("built-sha256.txt"), sha + "\n");

        FakeOciCli cli = hostCli(/* imagePresent= */ true);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);

        manager(cli, builds, contexts, baseUrl, true, "env-1").runSafely();

        assertEquals(0, builds.builds.get(), "no rebuild when the marker matches");
        assertEquals(0, tarballHits.get(), "no download when the marker matches");
        assertTrue(cli.calls.stream().anyMatch(argv -> argv.size() > 1 && "run".equals(argv.get(1))),
                "instance start still ensured");
        executor.shutdownNow();
    }

    @Test
    void startRefusedAtBoot_isHealedBySupervision_withoutAnOperator() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());
        Path markerDir = Files.createDirectories(dataDir.resolve("node-library"));
        Files.writeString(markerDir.resolve("built-sha256.txt"), sha + "\n");

        FakeOciCli cli = hostCli(/* imagePresent= */ true);
        var host = cli.responder;
        java.util.concurrent.atomic.AtomicBoolean refuseRun = new java.util.concurrent.atomic.AtomicBoolean(true);
        cli.responder = argv -> refuseRun.get() && argv.size() > 1 && "run".equals(argv.get(1))
                ? new OciCli.Result(125, "", "overlay egress not up yet", false)
                : host.apply(argv);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        StatefulInstanceManager sim = managerOver(cli);
        BuiltinNodeLibraryManager mgr = manager(sim, cli, builds, contexts, baseUrl, true, "env-1");

        mgr.runSafely();
        assertTrue(mgr.isUsable(), "the image is built — start-run is not blocked");
        assertFalse(sim.hasLiveInstance("env-1", BuiltinNodeLibraryManager.IMAGE_REF), "boot start was refused");

        refuseRun.set(false);
        assertFalse(mgr.supervise(), "a pass that had to start the library keeps the short cadence");

        assertTrue(sim.hasLiveInstance("env-1", BuiltinNodeLibraryManager.IMAGE_REF),
                "the engine restarts its own library on the next supervision pass");
        executor.shutdownNow();
    }

    @Test
    void shaMismatch_refusesBuildAndDeletesCachedTarball() throws Exception {
        StringBuilder ignored = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, ignored);
        String baseUrl = serveReleases(tarball, "0000000000000000000000000000000000000000000000000000000000000000");

        FakeOciCli cli = hostCli(false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);

        manager(cli, builds, contexts, baseUrl, true, "env-1").runSafely();

        assertEquals(0, builds.builds.get(), "mismatched bytes are never built");
        assertFalse(Files.exists(dataDir.resolve("node-library/lib.tar.gz")),
                "the tampered cache copy is deleted");
        assertFalse(cli.calls.stream().anyMatch(argv -> argv.size() > 1 && "run".equals(argv.get(1))),
                "no instance start on a failed pipeline");
        executor.shutdownNow();
    }

    /** A compromised origin serves a consistent tarball + sha256, but cannot sign with the pinned key. */
    @Test
    void manifestSignedByUnpinnedKey_refusesDownloadAndBuild() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());
        java.security.KeyPairGenerator gen = java.security.KeyPairGenerator.getInstance("Ed25519");
        java.security.KeyPair pinned = gen.generateKeyPair();
        java.security.KeyPair attacker = gen.generateKeyPair();
        fakeBackend.createContext("/releases/client/node-library/release-manifest.json.sig.v2", ex -> {
            byte[] body;
            try {
                java.security.Signature s = java.security.Signature.getInstance("Ed25519");
                s.initSign(attacker.getPrivate());
                s.update(("{\"version\":\"1.0.0\",\"file\":\"lib.tar.gz\",\"sha256\":\"" + sha + "\"}").getBytes());
                body = java.util.Base64.getEncoder().encode(s.sign());
            } catch (java.security.GeneralSecurityException e) {
                throw new IOException(e);
            }
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        });

        FakeOciCli cli = hostCli(false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        BuiltinNodeLibraryManager mgr = new BuiltinNodeLibraryManager(
                managerOver(cli), builds, contexts, cli,
                com.studioquack.sqs.release.ReleaseHttpClient.forTests(java.net.http.HttpClient.newHttpClient()),
                true, baseUrl, "env-1", dataDir.toString(), BuiltinNodeLibraryManager.IMAGE_REF,
                1, java.time.Duration.ZERO, java.time.Duration.ofMinutes(30), false,
                new com.studioquack.sqs.release.ReleaseSignatureVerifier(List.of(
                        java.util.Base64.getEncoder().encodeToString(pinned.getPublic().getEncoded())), false),
                java.time.Duration.ZERO);

        mgr.runSafely();

        assertEquals(0, tarballHits.get(), "an unverified manifest never triggers a download");
        assertEquals(0, builds.builds.get(), "an unverified manifest is never built");
        executor.shutdownNow();
    }

    @Test
    void disabledOrUnconfigured_idlesWithoutTouchingNetworkOrRuntime() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);

        manager(cli, builds, contexts, baseUrl, false, "env-1").runSafely();
        manager(cli, builds, contexts, baseUrl, true, "").runSafely();
        manager(cli, builds, contexts, "", true, "env-1").runSafely();

        assertEquals(0, builds.builds.get());
        assertEquals(0, tarballHits.get());
        assertTrue(cli.calls.isEmpty(), "not even an image-exists probe");
        executor.shutdownNow();
    }

    /* ------------------------------------------------------------------ */
    /* on-demand update (update-node-library command)                      */
    /* ------------------------------------------------------------------ */

    @Test
    void updateNow_changedManifestSha_usesDrainedReplacement() throws Exception {
        Wired wired = wiredLiveShaChange();
        String oldName = "sq-stateful-" + BuiltinNodeLibraryManager.INSTANCE_ID;
        String oldCid = wired.oldCid();
        int callsAfterStart = wired.cli().calls.size();

        // Hold in-flight work so drain is observable (RECYCLING → DRAINING, no stop-first).
        wired.sim().beginDispatch(oldCid);
        // Pause briefly on the successor `run` so the StageWatch sampler deterministically
        // observes the transient RECYCLING stage before it flips to DRAINING.
        var baseResponder = wired.cli().responder;
        wired.cli().responder = argv -> {
            if (argv.size() > 1 && "run".equals(argv.get(1))) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            return baseResponder.apply(argv);
        };
        try (StageWatch watch = new StageWatch(wired.lib())) {
            BuiltinNodeLibraryManager.UpdateAck ack = wired.lib().updateNow();
            assertTrue(ack.accepted(), "first update request is accepted: " + ack.reason());
            watch.awaitRecyclingThenDraining();
            assertFalse(sawStopOf(wired.cli(), oldName),
                    "old container must stay up while DRAINING — not stop-first");
        } finally {
            wired.sim().endDispatch(oldCid);
        }
        awaitPipelineSettled(wired.lib());

        assertEquals(1, wired.builds().builds.get(), "changed sha256 forces exactly one rebuild");
        assertEquals(wired.newSha(),
                Files.readString(dataDir.resolve("node-library/built-sha256.txt")).trim(),
                "marker advances to the new manifest sha256");

        int successorRun = -1;
        int stopOld = -1;
        List<List<String>> calls = wired.cli().calls;
        for (int i = 0; i < calls.size(); i++) {
            List<String> argv = calls.get(i);
            if (i >= callsAfterStart && argv.size() > 1 && "run".equals(argv.get(1)) && successorRun < 0) {
                successorRun = i;
            }
            if (argv.contains("stop") && argv.contains(oldName)) {
                stopOld = i;
            }
        }
        assertTrue(successorRun >= 0, "successor container must be started (a run after the live start)");
        assertTrue(stopOld > successorRun, "stop of the old name must follow the successor run");

        StatefulInstance live = wired.sim().resolveRunning("env-1", BuiltinNodeLibraryManager.IMAGE_REF)
                .orElseThrow();
        assertNotEquals(oldCid, live.containerId(), "routing must point at the successor");

        BuiltinNodeLibraryManager.NodeLibrarySnapshot snap = wired.lib().snapshot();
        assertNotNull(snap);
        assertEquals(BuiltinNodeLibraryManager.STATE_IDLE, snap.updateState());
        assertNull(snap.updateError());
        assertEquals(wired.newSha(), snap.builtSha256());
        assertNotNull(snap.updatedAt(), "a successful update reports when it happened");
        wired.executor().shutdownNow();
    }

    @Test
    void updateNow_successorUnhealthy_failsAndKeepsPreviousInstance() throws Exception {
        Wired wired = wiredLiveShaChange();
        String oldName = "sq-stateful-" + BuiltinNodeLibraryManager.INSTANCE_ID;
        String oldCid = wired.oldCid();
        // Flip only after the predecessor is already RUNNING.
        failAppProbe = true;

        assertTrue(wired.lib().updateNow().accepted());
        awaitPipelineSettled(wired.lib());

        BuiltinNodeLibraryManager.NodeLibrarySnapshot snap = wired.lib().snapshot();
        assertNotNull(snap);
        assertEquals(BuiltinNodeLibraryManager.STATE_FAILED, snap.updateState());
        assertNotNull(snap.updateError());
        assertTrue(snap.updateError().contains("previous instance keeps serving"),
                "failure must say the old instance keeps serving, was: " + snap.updateError());
        assertTrue(snap.updateError().contains("did not become healthy"),
                "failure must name the health gate, was: " + snap.updateError());

        StatefulInstance live = wired.sim().resolveRunning("env-1", BuiltinNodeLibraryManager.IMAGE_REF)
                .orElseThrow();
        assertEquals(oldCid, live.containerId(), "routing must stay on the original container");
        assertEquals(StatefulStatus.RUNNING, live.status());
        assertFalse(sawStopOf(wired.cli(), oldName),
                "unhealthy successor must not stop the old container");
        wired.executor().shutdownNow();
    }

    @Test
    void updateNow_alreadyCurrent_isANoOpBeyondEnsureStarted() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());
        Path markerDir = Files.createDirectories(dataDir.resolve("node-library"));
        Files.writeString(markerDir.resolve("built-sha256.txt"), sha + "\n");

        FakeOciCli cli = hostCli(/* imagePresent= */ true);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");

        assertTrue(mgr.updateNow().accepted());
        awaitPipelineSettled(mgr);

        assertEquals(0, builds.builds.get(), "already-current manifest never rebuilds");
        assertEquals(0, tarballHits.get(), "already-current manifest never downloads");
        // start()'s own pre-clean is an `rm -f`; a `stop` only ever comes from the recycle path.
        assertFalse(cli.calls.stream().anyMatch(argv -> argv.contains("stop")),
                "no recycle when nothing changed");
        assertTrue(cli.calls.stream().anyMatch(argv -> argv.size() > 1 && "run".equals(argv.get(1))),
                "instance start is still ensured");
        executor.shutdownNow();
    }

    @Test
    void checkForRelease_matchingSha_doesNotEnterThePipeline() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());
        Path markerDir = Files.createDirectories(dataDir.resolve("node-library"));
        Files.writeString(markerDir.resolve("built-sha256.txt"), sha + "\n");

        FakeOciCli cli = hostCli(true);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");
        int calls = cli.calls.size();

        mgr.checkForRelease();

        assertEquals(0, builds.builds.get(), "a matching sha256 must not rebuild");
        assertEquals(0, tarballHits.get(), "a matching sha256 must not download");
        assertEquals(calls, cli.calls.size(), "a matching sha256 must not touch the runtime");
        assertEquals(BuiltinNodeLibraryManager.STATE_IDLE, mgr.snapshot().updateState());
        assertNull(mgr.snapshot().updateError());
        executor.shutdownNow();
    }

    @Test
    void checkForRelease_changedSha_startsTheDrainedUpdate() throws Exception {
        Wired wired = wiredLiveShaChange();
        wired.lib().checkForRelease();
        awaitPipelineSettled(wired.lib());

        assertEquals(1, wired.builds().builds.get(), "a changed sha256 starts exactly one rebuild");
        assertEquals(wired.newSha(),
                Files.readString(dataDir.resolve("node-library/built-sha256.txt")).trim());
        assertEquals(BuiltinNodeLibraryManager.STATE_IDLE, wired.lib().snapshot().updateState());
        assertNull(wired.lib().snapshot().updateError());
        wired.executor().shutdownNow();
    }

    @Test
    void checkForRelease_manifestUnreadable_leavesTheLibraryIdle() throws Exception {
        HttpServer empty = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        empty.start();
        try {
            Files.createDirectories(dataDir.resolve("node-library"));
            Files.writeString(dataDir.resolve("node-library/built-sha256.txt"), "abc\n");
            String baseUrl = "http://127.0.0.1:" + empty.getAddress().getPort();
            FakeOciCli cli = hostCli(true);
            OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
            ExecutorService executor = Executors.newSingleThreadExecutor();
            RecordingBuildService builds = new RecordingBuildService(contexts, executor);
            BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");

            mgr.checkForRelease();

            assertEquals(0, builds.builds.get());
            assertEquals(BuiltinNodeLibraryManager.STATE_IDLE, mgr.snapshot().updateState());
            assertNull(mgr.snapshot().updateError(), "a missed check must not look like a failed update");
            executor.shutdownNow();
        } finally {
            empty.stop(0);
        }
    }

    @Test
    void updateNow_whilePipelineRuns_secondRequestIsRefusedNotQueued() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(/* imagePresent= */ false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        builds.buildGate = new CountDownLatch(1);
        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");

        assertTrue(mgr.updateNow().accepted());
        // Wait until the first pipeline is actually parked inside the build leg.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (builds.builds.get() == 0) {
            assertTrue(System.nanoTime() < deadline, "first update must reach the build leg");
            Thread.sleep(10);
        }
        BuiltinNodeLibraryManager.NodeLibrarySnapshot inFlight = mgr.snapshot();
        assertNotNull(inFlight);
        assertEquals(BuiltinNodeLibraryManager.STATE_UPDATING, inFlight.updateState());
        assertEquals(BuiltinNodeLibraryManager.STAGE_BUILDING, inFlight.updateStage());

        BuiltinNodeLibraryManager.UpdateAck second = mgr.updateNow();
        assertFalse(second.accepted(), "a concurrent update is refused");
        assertNotNull(second.reason());

        builds.buildGate.countDown();
        awaitPipelineSettled(mgr);
        assertEquals(1, builds.builds.get(), "the refused second request never ran a build");
        executor.shutdownNow();
    }

    @Test
    void updateNow_failedBuild_surfacesFailedStateWithReason() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(/* imagePresent= */ false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        builds.buildSucceeds = false;
        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");

        assertTrue(mgr.updateNow().accepted());
        awaitPipelineSettled(mgr);

        BuiltinNodeLibraryManager.NodeLibrarySnapshot snap = mgr.snapshot();
        assertNotNull(snap);
        assertEquals(BuiltinNodeLibraryManager.STATE_FAILED, snap.updateState());
        assertNotNull(snap.updateError(), "the failure reason rides the status block");
        assertTrue(snap.updateError().contains("boom"), "the build's own error is what surfaces");
        executor.shutdownNow();
    }

    @Test
    void updateNow_disabledOrUnenrolled_isRejectedSynchronously() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);

        assertFalse(manager(cli, builds, contexts, baseUrl, false, "env-1").updateNow().accepted(),
                "disabled library refuses the update");
        assertFalse(manager(cli, builds, contexts, baseUrl, true, "").updateNow().accepted(),
                "unenrolled daemon refuses the update");
        assertNull(manager(cli, builds, contexts, baseUrl, false, "env-1").snapshot(),
                "disabled library omits the status block");
        assertEquals(0, builds.builds.get());
        assertEquals(0, tarballHits.get());
        executor.shutdownNow();
    }

    /* ------------------------------------------------------------------ */
    /* readiness + speedups                                                */
    /* ------------------------------------------------------------------ */

    @Test
    void disabledOrUnconfigured_isUsableImmediatelyAndDoesNotBlockStartRun() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());
        FakeOciCli cli = hostCli(false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);

        BuiltinNodeLibraryManager disabled = manager(cli, builds, contexts, baseUrl, false, "env-1");
        assertTrue(disabled.isUsable());
        assertNull(disabled.startRunBlockReason());

        BuiltinNodeLibraryManager unenrolled = manager(cli, builds, contexts, baseUrl, true, "");
        assertTrue(unenrolled.isUsable());
        assertNull(unenrolled.startRunBlockReason());
        executor.shutdownNow();
    }

    @Test
    void leftoverBuildMarker_isUsableBeforePipelineRuns() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());
        Files.createDirectories(dataDir.resolve("node-library"));
        Files.writeString(dataDir.resolve("node-library/built-sha256.txt"), sha + "\n");

        FakeOciCli cli = hostCli(true);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");

        assertTrue(mgr.isUsable(), "a previous successful build must not refuse start-run on restart");
        assertNull(mgr.startRunBlockReason());
        executor.shutdownNow();
    }

    @Test
    void downloadOverlapsPodmanWait() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(false);
        cli.available = false;
        podmanUpDuringDownload = () -> cli.available;
        Thread flip = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            cli.available = true;
        }, "flip-podman");
        flip.setDaemon(true);

        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        // Build the TLS client before the 200ms window starts. Cold trust-store init must not
        // consume the overlap the assertion is measuring.
        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");
        flip.start();
        mgr.runSafely();

        assertTrue(tarballHitsWhilePodmanDown.get() > 0,
                "tarball GET must start while awaitPodman is still waiting");
        assertEquals(1, builds.builds.get());
        executor.shutdownNow();
    }

    @Test
    void failedBuild_reusesExtractedContextOnRetry() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        builds.buildSucceeds = false;
        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");

        mgr.runSafely();
        assertEquals(1, builds.builds.get());
        Path canary = contexts.contextDir(BuiltinNodeLibraryManager.BUILD_ID).resolve(".canary-reuse");
        Files.writeString(canary, "keep");

        mgr.runSafely();
        assertEquals(2, builds.builds.get(), "retry must build again");
        assertTrue(Files.exists(canary),
                "a complete extract from the failed attempt must be reused (not deleted+untarred)");
        executor.shutdownNow();
    }

    @Test
    void bootPipeline_neverGivesUp_andRetryNowWakesTheSlowPhase() throws Exception {
        // The build fails while the overlay is down (egress gate). Now: slow phase forever, woken
        // early when egress comes up, and the state reads UPDATING/WAITING (not a dead FAILED).
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        builds.buildSucceeds = false;
        BuiltinNodeLibraryManager mgr = new BuiltinNodeLibraryManager(
                managerOver(cli), builds, contexts, cli,
                com.studioquack.sqs.release.ReleaseHttpClient.forTests(java.net.http.HttpClient.newHttpClient()),
                true, baseUrl, "env-1", dataDir.toString(), BuiltinNodeLibraryManager.IMAGE_REF,
                /* fast attempts */ 1, java.time.Duration.ZERO, java.time.Duration.ofMinutes(30),
                /* retryForever */ true);

        Thread boot = new Thread(mgr::runSafely, "boot-ensure-under-test");
        boot.setDaemon(true);
        boot.start();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (builds.builds.get() < 1 || !BuiltinNodeLibraryManager.STAGE_WAITING.equals(
                mgr.snapshot().updateStage())) {
            assertTrue(System.nanoTime() < deadline, "first attempt must fail and park in WAITING");
            Thread.sleep(20);
        }
        var parked = mgr.snapshot();
        assertEquals(BuiltinNodeLibraryManager.STATE_UPDATING, parked.updateState(),
                "between attempts the library is still coming — never a terminal FAILED");
        assertNotNull(parked.updateError(), "…but the last failure stays visible");
        assertFalse(mgr.isUsable());
        assertTrue(mgr.startRunBlockReason().contains("retrying automatically"), mgr.startRunBlockReason());
        assertFalse(mgr.awaitUsable(java.time.Duration.ofMillis(300)), "not usable while parked");

        // Overlay egress comes up → the bootstrap kicks the pipeline; the build now succeeds.
        builds.buildSucceeds = true;
        mgr.retryNow();
        assertTrue(mgr.awaitUsable(java.time.Duration.ofSeconds(30)),
                "the retried attempt must build the image and make the library usable");
        awaitPipelineSettled(mgr);
        assertEquals(2, builds.builds.get(), "exactly one more build after the wake-up");
        assertEquals(BuiltinNodeLibraryManager.STATE_IDLE, mgr.snapshot().updateState());
        assertNull(mgr.startRunBlockReason());
        boot.join(TimeUnit.SECONDS.toMillis(5));
        executor.shutdownNow();
    }

    @Test
    void startBootEnsure_submitsPipelineAndBecomesUsable() throws Exception {
        StringBuilder sha = new StringBuilder();
        byte[] tarball = makeContextTarball(dataDir, sha);
        String baseUrl = serveReleases(tarball, sha.toString());

        FakeOciCli cli = hostCli(false);
        OciBuildContextStore contexts = new OciBuildContextStore(dataDir.toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordingBuildService builds = new RecordingBuildService(contexts, executor);
        BuiltinNodeLibraryManager mgr = manager(cli, builds, contexts, baseUrl, true, "env-1");

        mgr.startBootEnsure();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!mgr.isUsable()) {
            assertTrue(System.nanoTime() < deadline, "boot ensure must become usable");
            Thread.sleep(25);
        }
        awaitPipelineSettled(mgr);
        assertEquals(1, builds.builds.get());
        executor.shutdownNow();
    }

    @Test
    void tailLines_keepsTheEndOfTheBuildOutput_whereMavensErrorIs() {
        StringBuilder log = new StringBuilder();
        for (int i = 1; i <= 100; i++) {
            log.append("STEP ").append(i).append('\n');
        }
        log.append("[ERROR] Could not transfer artifact … from central\r\n");
        String tail = BuiltinNodeLibraryManager.tailLines(log.toString(), 3, 8_192);
        assertEquals("STEP 99\nSTEP 100\n[ERROR] Could not transfer artifact … from central", tail);
        assertTrue(BuiltinNodeLibraryManager.tailLines(log.toString(), 40, 20).endsWith("from central"),
                "the char cap must keep the END, not the start");
        assertEquals("(no build output captured)", BuiltinNodeLibraryManager.tailLines(null, 40, 100));
    }
}
