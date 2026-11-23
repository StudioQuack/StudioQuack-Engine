package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.egress.SandboxNetworkGuard;
import com.studioquack.sqs.sandbox.egress.StubSandboxEgressManager;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.OciImageBuildProgressReporter;
import com.studioquack.sqs.sandbox.stateful.OciImageDigestReporter;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Argv shape (caps), context cleanup, and dangling-only image retention. */
class OciImageBuildServiceTest {

    @TempDir
    Path tempDir;

    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void buildArgvMatchesD67CapsExactly() {
        Path ctx = Path.of("/tmp/oci-build/abc").toAbsolutePath().normalize();
        List<String> argv = OciImageBuildService.buildArgv("/usr/bin/podman", "sq-local/app:1", ctx);
        assertEquals(List.of(
                "/usr/bin/podman",
                "build",
                "--memory", OciImageBuildService.BUILD_MEMORY,
                // podman build has no --cpus; CFS period/quota = 2 vCPU.
                "--cpu-period", OciImageBuildService.BUILD_CPU_PERIOD,
                "--cpu-quota", OciImageBuildService.BUILD_CPU_QUOTA,
                "--ulimit", OciImageBuildService.BUILD_FSIZE_ULIMIT,
                // Its own network: the build reaches the internet but is not adjacent to a running
                // job or stateful instance, which the default bridge would allow.
                "--network", SandboxNetworks.BUILD,
                // Pinned public resolvers: dependency fetch still resolves PUBLIC hosts, but the
                // bridge gateway's forwarder into the host's internal DNS is unused.
                "--dns", "9.9.9.9",
                "--dns", "1.1.1.1",
                "-t", "sq-local/app:1",
                ctx.toString()
        ), argv);
    }

    /**
     * Windows / WSL podman machine: rootless crun cannot open memory.max, so a capped build always
     * fails. The build must retry once without the cgroup caps and keep building that way.
     */
    @Test
    void retriesUncappedWhenTheHostCannotApplyCgroupCaps() throws Exception {
        String buildId = "build-wsl-1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));
        executor = Executors.newSingleThreadExecutor();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> new OciCli.Result(0, "sha256:abc", "", false);
        List<List<String>> argvs = new java.util.concurrent.CopyOnWriteArrayList<>();
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), mock(OciImageDigestReporter.class), executor,
                (argv, wall, sink) -> {
                    argvs.add(argv);
                    if (argv.contains("--memory")) {
                        emit(sink, "[1/3] STEP 7/7: RUN mvn -q -DskipTests package\n"
                                + "error running container: from /usr/bin/crun creating container for "
                                + "[/bin/sh -c mvn -q -DskipTests package]: open `memory.max` for writing: "
                                + "No such file or directory\n: exit status 1\n");
                        return new OciBuildLogStreamer.Outcome(1, false, "exit status 1");
                    }
                    return new OciBuildLogStreamer.Outcome(0, false, null);
                });

        OciImageBuildService.SyncBuildResult first = service.buildSync("env-1", buildId, "sq-local/lib:1", 60);
        assertTrue(first.success(), () -> "uncapped retry should succeed: " + first.error());
        assertEquals(2, argvs.size());
        assertTrue(argvs.get(0).contains("--memory"), "first attempt carries the caps");
        assertFalse(argvs.get(1).contains("--memory"), "retry drops --memory");
        assertFalse(argvs.get(1).contains("--cpu-quota"), "retry drops --cpu-quota");
        assertTrue(argvs.get(1).contains("--ulimit"), "the fsize ulimit (rlimit, not cgroup) stays");

        // The host is remembered: the next build goes straight to the uncapped argv.
        OciImageBuildService.SyncBuildResult second = service.buildSync("env-1", buildId, "sq-local/lib:1", 60);
        assertTrue(second.success());
        assertEquals(3, argvs.size());
        assertFalse(argvs.get(2).contains("--memory"));
    }

    /**
     * Egress can be ENFORCED inside a rootless machine's network namespace, so a non-DEGRADED
     * posture does not mean a rootful builder. Rootless buildah fails with "cannot use networks
     * as rootless" on --network sqs-build: a rootless builder must get the no-network argv.
     */
    @Test
    void rootlessBuilderBuildsWithoutTheNamedNetwork() throws Exception {
        List<List<String>> argvs = buildOnceWithRootlessAnswer("build-rootless-1", "true\n");
        assertEquals(1, argvs.size());
        assertFalse(argvs.get(0).contains("--network"), "rootless buildah cannot join sqs-build");
        assertFalse(argvs.get(0).contains(SandboxNetworks.BUILD));
    }

    @Test
    void rootfulBuilderKeepsTheSqsBuildNetwork() throws Exception {
        List<List<String>> argvs = buildOnceWithRootlessAnswer("build-rootful-1", "false\n");
        assertEquals(1, argvs.size());
        assertTrue(argvs.get(0).contains("--network"), "a rootful build stays on its own network");
        assertTrue(argvs.get(0).contains(SandboxNetworks.BUILD));
    }

    private List<List<String>> buildOnceWithRootlessAnswer(String buildId, String rootlessStdout) throws Exception {
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));
        executor = Executors.newSingleThreadExecutor();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> argv.contains("{{.Host.Security.Rootless}}")
                ? new OciCli.Result(0, rootlessStdout, "", false)
                : new OciCli.Result(0, "sha256:abc", "", false);
        List<List<String>> argvs = new java.util.concurrent.CopyOnWriteArrayList<>();
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), mock(OciImageDigestReporter.class), executor,
                (argv, wall, sink) -> {
                    argvs.add(argv);
                    return new OciBuildLogStreamer.Outcome(0, false, null);
                });
        OciImageBuildService.SyncBuildResult result = service.buildSync("env-1", buildId, "sq-local/lib:1", 60);
        assertTrue(result.success(), () -> "build should run: " + result.error());
        return argvs;
    }

    private static void emit(OciBuildLogStreamer.ChunkSink sink, String chunk) {
        try {
            sink.onChunk(chunk);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void ordinaryBuildFailureDoesNotDropTheCaps() throws Exception {
        String buildId = "build-fail-1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));
        executor = Executors.newSingleThreadExecutor();
        FakeOciCli cli = new FakeOciCli();
        AtomicInteger runs = new AtomicInteger();
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), mock(OciImageDigestReporter.class), executor,
                (argv, wall, sink) -> {
                    runs.incrementAndGet();
                    emit(sink, "[ERROR] COMPILATION ERROR : cannot find symbol\n");
                    return new OciBuildLogStreamer.Outcome(1, false, "exit status 1");
                });

        OciImageBuildService.SyncBuildResult result = service.buildSync("env-1", buildId, "sq-local/app:1", 60);
        assertFalse(result.success());
        assertEquals(1, runs.get(), "a real build error is not retried uncapped");
    }

    @Test
    void deletesContextOnSuccess() throws Exception {
        String buildId = "build-ok-1";
        String ref = "sq-local/ok:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        Path written = store.writeFile(buildId, "Dockerfile",
                "FROM scratch\n".getBytes(StandardCharsets.UTF_8));
        assertTrue(Files.isDirectory(written.getParent()));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-ok");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (argv.contains("inspect")) {
                return new OciCli.Result(0, "sha256:deadbeef", "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, "demo", OciImageBuildService.DEFAULT_WALL_CLOCK_SECONDS));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        // finally-block delete runs after streamer returns — brief wait for async cleanup.
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq("sha256:deadbeef"),
                eq("ACTIVE"), isNull(), eq(buildId));
        awaitContextGone(store, buildId, written.getParent());
    }

    @Test
    void refusesBuildWhenEgressUnavailable() throws Exception {
        String buildId = "build-egress-1";
        String ref = "sq-local/egress:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-egress-refuse");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        AtomicInteger streamCalls = new AtomicInteger();
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> {
                    streamCalls.incrementAndGet();
                    done.countDown();
                    return new OciBuildLogStreamer.Outcome(0, false, null);
                },
                StubSandboxEgressManager.unavailable("overlay-down"));

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, "demo", 60));

        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), isNull(),
                eq("FAILED"), eq("sandbox egress unavailable: overlay-down"), eq(buildId));
        assertEquals(0, streamCalls.get(), "UNAVAILABLE must not start podman build");
        assertFalse(done.await(200, TimeUnit.MILLISECONDS), "streamer must not run");
    }

    @Test
    void proceedsBuildWhenEgressEnforced() throws Exception {
        String buildId = "build-egress-ok";
        String ref = "sq-local/egress-ok:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-egress-ok");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (argv.contains("inspect")) {
                return new OciCli.Result(0, "sha256:deadbeef", "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                },
                StubSandboxEgressManager.enforced());

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, "demo", 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq("sha256:deadbeef"),
                eq("ACTIVE"), isNull(), eq(buildId));
    }

    @Test
    void deletesContextOnFailure() throws Exception {
        String buildId = "build-fail-1";
        String ref = "sq-local/fail:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        Path written = store.writeFile(buildId, "Dockerfile",
                "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-fail");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(1, false, "STEP failed");
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, null, 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), isNull(),
                eq("FAILED"), eq("STEP failed"), eq(buildId));
        // Failed rebuild must not rmi (tag never left the last working image).
        assertFalse(cli.sawCallContaining("rmi"), "failure must not delete any image");
        awaitContextGone(store, buildId, written.getParent());
    }

    @Test
    void successfulRebuildDeletesSupersededImageId() throws Exception {
        String buildId = "build-rebuild-1";
        String ref = "sq-local/rebuild:1";
        String oldId = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        String newId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-rebuild");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger inspectSeq = new AtomicInteger();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (isRepoTagsInspect(argv)) {
                // dangling previous id — no remaining tags after this ref moved
                return new OciCli.Result(0, "[]", "", false);
            }
            if (argv.contains("inspect")) {
                // pre-build previous id, then post-build new id
                return inspectSeq.getAndIncrement() == 0
                        ? new OciCli.Result(0, oldId, "", false)
                        : new OciCli.Result(0, newId, "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, "demo", 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq(newId),
                eq("ACTIVE"), isNull(), eq(buildId));
        assertTrue(cli.sawCallContaining("image", "inspect", "{{json .RepoTags}}", oldId),
                "dangling check inspects previous id RepoTags");
        List<String> rmi = awaitRmiCall(cli);
        assertNotNull(rmi, "dangling previous id should be collected");
        assertFalse(rmi.contains("-f"), "D78 cleanup must not force-remove");
        assertTrue(rmi.contains(oldId));
        assertEquals(List.of(cli.cliPath, "rmi", oldId), rmi);
    }

    @Test
    void successfulRebuildKeepsPreviousIdWhenOtherVersionStillTagged() throws Exception {
        String buildId = "build-shared-1";
        String ref = "sq-local/shared:2";
        String otherRef = "sq-local/shared:1";
        String oldId = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        String newId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-shared-tag");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger inspectSeq = new AtomicInteger();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (isRepoTagsInspect(argv)) {
                return new OciCli.Result(0, "[\"" + otherRef + "\"]", "", false);
            }
            if (argv.contains("inspect")) {
                return inspectSeq.getAndIncrement() == 0
                        ? new OciCli.Result(0, oldId, "", false)
                        : new OciCli.Result(0, newId, "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, "demo", 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq(newId),
                eq("ACTIVE"), isNull(), eq(buildId));
        assertFalse(cli.sawCallContaining("rmi"),
                "previous id still tagged by another version must be retained");
    }

    @Test
    void successfulRebuildSkipsRmiWhenPreviousIdInspectFails() throws Exception {
        String buildId = "build-inspect-fail-1";
        String ref = "sq-local/inspect-fail:2";
        String oldId = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        String newId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-inspect-fail");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger inspectSeq = new AtomicInteger();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (isRepoTagsInspect(argv)) {
                return new OciCli.Result(1, "", "no such image", false);
            }
            if (argv.contains("inspect")) {
                return inspectSeq.getAndIncrement() == 0
                        ? new OciCli.Result(0, oldId, "", false)
                        : new OciCli.Result(0, newId, "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, "demo", 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq(newId),
                eq("ACTIVE"), isNull(), eq(buildId));
        assertFalse(cli.sawCallContaining("rmi"),
                "inspect failure must skip rmi (fail-safe toward retention)");
    }

    @Test
    void firstBuildDeletesNothing() throws Exception {
        String buildId = "build-first-1";
        String ref = "sq-local/first:1";
        String newId = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-first");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger inspectSeq = new AtomicInteger();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (argv.contains("inspect")) {
                // no previous image → inspect fails pre-build; succeeds post-build
                return inspectSeq.getAndIncrement() == 0
                        ? new OciCli.Result(1, "", "no such image", false)
                        : new OciCli.Result(0, newId, "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, null, 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq(newId),
                eq("ACTIVE"), isNull(), eq(buildId));
        assertFalse(cli.sawCallContaining("rmi"), "first build has no previous image to remove");
    }

    @Test
    void failedBuildNeverCallsRmi() throws Exception {
        String buildId = "build-fail-rmi-1";
        String ref = "sq-local/fail-rmi:1";
        String oldId = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-fail-rmi");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (argv.contains("inspect")) {
                return new OciCli.Result(0, oldId, "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(1, false, "STEP failed");
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, null, 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), isNull(),
                eq("FAILED"), eq("STEP failed"), eq(buildId));
        assertFalse(cli.sawCallContaining("rmi"), "failed build must never call rmi");
    }

    @Test
    void rmiInUseStillReportsSuccess() throws Exception {
        String buildId = "build-rmi-busy-1";
        String ref = "sq-local/busy:1";
        String oldId = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        String newId = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-rmi-busy");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger inspectSeq = new AtomicInteger();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (isRepoTagsInspect(argv)) {
                return new OciCli.Result(0, "[]", "", false);
            }
            if (argv.contains("inspect")) {
                return inspectSeq.getAndIncrement() == 0
                        ? new OciCli.Result(0, oldId, "", false)
                        : new OciCli.Result(0, newId, "", false);
            }
            if (argv.contains("rmi")) {
                // running stateful instance still holds the prior image
                return new OciCli.Result(125, "", "image is in use by a container", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, null, 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        // cleanup refusal must not flip the build to FAILED
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq(newId),
                eq("ACTIVE"), isNull(), eq(buildId));
        assertNotNull(awaitRmiCall(cli));
    }

    @Test
    void identicalImageIdDeletesNothing() throws Exception {
        String buildId = "build-noop-1";
        String ref = "sq-local/noop:1";
        String sameId = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-noop");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (argv.contains("inspect")) {
                return new OciCli.Result(0, sameId, "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                });

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, null, 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq(sameId),
                eq("ACTIVE"), isNull(), eq(buildId));
        assertFalse(cli.sawCallContaining("rmi"), "identical id is a no-op for cleanup");
    }

    @Test
    void buildSyncArgvStillPassesNetworkGuard() throws Exception {
        String buildId = "sync-argv-1";
        String ref = "sq-local/sync-argv:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-sync-argv");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        AtomicReference<List<String>> captured = new AtomicReference<>();
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> {
                    captured.set(argv);
                    return new OciBuildLogStreamer.Outcome(0, false, null);
                });

        OciImageBuildService.SyncBuildResult result = service.buildSync("env-1", buildId, ref, 60);

        assertTrue(result.success());
        List<String> argv = captured.get();
        assertNotNull(argv, "buildSync must pass argv to the streamer");
        assertTrue(argv.contains("--network"));
        assertTrue(argv.contains(SandboxNetworks.BUILD));
        assertTrue(argv.contains("--memory"));
        assertTrue(argv.contains(OciImageBuildService.BUILD_MEMORY));
        assertTrue(argv.contains("--cpu-period"));
        assertTrue(argv.contains(OciImageBuildService.BUILD_CPU_PERIOD));
        assertTrue(argv.contains("--cpu-quota"));
        assertTrue(argv.contains(OciImageBuildService.BUILD_CPU_QUOTA));
        assertTrue(argv.contains("--ulimit"));
        assertTrue(argv.contains("--dns"));
        SandboxNetworkGuard.validateBuildNetwork(argv);
    }

    @Test
    void buildSyncOnDegradedHostWithoutOptOutIsRefused() throws Exception {
        // A build pulls from the network; on a host that cannot enforce egress and has not opted
        // out, it is refused before any CLI call.
        String buildId = "sync-degraded-refused-1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));
        executor = Executors.newSingleThreadExecutor();
        FakeOciCli cli = new FakeOciCli();
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class),
                mock(OciImageDigestReporter.class), executor,
                (argv, wall, sink) -> new OciBuildLogStreamer.Outcome(0, false, null),
                StubSandboxEgressManager.degraded());

        OciImageBuildService.SyncBuildResult result =
                service.buildSync("env-1", buildId, "sq-local/sync-degraded-refused:1", 60);

        assertFalse(result.success());
        assertTrue(result.error().contains("not enforced on this host"),
                "expected D-06 refusal; got: " + result.error());
        assertFalse(cli.sawCallContaining("build"), "no build CLI call on a refused host");
    }

    @Test
    void buildSyncOnDegradedOptOutHostOmitsNetworkAndSkipsEnsure() throws Exception {
        // DEGRADED with operator opt-out (rootless podman machine on a dev laptop): buildah cannot
        // join a named network, so the build must ride the implicit default bridge — no network
        // token, no sqs-build ensure calls — while keeping the caps and DNS pins.
        String buildId = "sync-degraded-1";
        String ref = "sq-local/sync-degraded:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-sync-degraded");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        AtomicReference<List<String>> captured = new AtomicReference<>();
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class),
                mock(OciImageDigestReporter.class), executor,
                (argv, wall, sink) -> {
                    captured.set(argv);
                    return new OciBuildLogStreamer.Outcome(0, false, null);
                },
                StubSandboxEgressManager.degradedOptOut());

        OciImageBuildService.SyncBuildResult result = service.buildSync("env-1", buildId, ref, 60);

        assertTrue(result.success());
        List<String> argv = captured.get();
        assertNotNull(argv, "buildSync must pass argv to the streamer");
        assertFalse(argv.contains("--network"), "degraded build must not name a network");
        assertFalse(argv.contains("--net"), "degraded build must not use the --net alias");
        assertTrue(argv.stream().noneMatch(t -> t.startsWith("--network=") || t.startsWith("--net=")),
                "degraded build must not carry =-form network flags");
        assertTrue(argv.contains("--memory"));
        assertTrue(argv.contains("--dns"), "DNS pins stay on the default bridge");
        SandboxNetworkGuard.validateBuildNetwork(argv, true);
        assertFalse(cli.sawCallContaining(SandboxNetworks.BUILD),
                "degraded build must not ensure/inspect sqs-build");
    }

    @Test
    void buildSyncSuccessDoesNotCallReportersAndLeavesContext() throws Exception {
        String buildId = "sync-ok-1";
        String ref = "sq-local/sync-ok:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-sync-ok");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> {
                    try {
                        sink.onChunk("STEP ok");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return new OciBuildLogStreamer.Outcome(0, false, null);
                });

        OciImageBuildService.SyncBuildResult result = service.buildSync("env-1", buildId, ref, 60);

        assertTrue(result.success());
        assertTrue(result.log().contains("STEP ok"));
        assertEquals("", result.error());
        verifyNoInteractions(progress, digest);
        assertTrue(store.contextExists(buildId), "sync path must leave context for the caller");
        assertFalse(cli.sawCallContaining("rmi"), "sync path must not rmi superseded images");
    }

    @Test
    void buildSyncFailureDoesNotCallReporters() throws Exception {
        String buildId = "sync-fail-1";
        String ref = "sq-local/sync-fail:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-sync-fail");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> new OciBuildLogStreamer.Outcome(1, false, "STEP failed"));

        OciImageBuildService.SyncBuildResult result = service.buildSync("env-1", buildId, ref, 60);

        assertFalse(result.success());
        assertTrue(result.error().contains("STEP failed"), result.error());
        verifyNoInteractions(progress, digest);
        assertTrue(store.contextExists(buildId), "failure must not delete context");
    }

    @Test
    void buildSyncTimeout() throws Exception {
        String buildId = "sync-timeout-1";
        String ref = "sq-local/sync-timeout:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-sync-timeout");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> new OciBuildLogStreamer.Outcome(-1, true, null));

        OciImageBuildService.SyncBuildResult result = service.buildSync("env-1", buildId, ref, 60);

        assertFalse(result.success());
        assertTrue(result.error().toLowerCase().contains("timed out"), result.error());
        verifyNoInteractions(progress, digest);
        assertTrue(store.contextExists(buildId), "timeout must not delete context");
    }

    @Test
    void successfulRebuildRecyclesThenDeletesSupersededImage() throws Exception {
        String buildId = "build-recycle-1";
        String ref = "sq-local/recycle:1";
        String oldId = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        String newId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-recycle");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger inspectSeq = new AtomicInteger();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (isRepoTagsInspect(argv)) {
                return new OciCli.Result(0, "[]", "", false);
            }
            if (argv.contains("inspect")) {
                return inspectSeq.getAndIncrement() == 0
                        ? new OciCli.Result(0, oldId, "", false)
                        : new OciCli.Result(0, newId, "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        StatefulInstanceManager sim = mock(StatefulInstanceManager.class);
        WarmPool pool = mock(WarmPool.class);
        when(sim.recycleForImage(any(), any())).thenReturn(
                new StatefulInstanceManager.RecycleResult(true, "inst_x", "RUNNING", null));
        when(pool.invalidateImage(any())).thenReturn(2);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                },
                null, null, sim, pool);

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, "demo", 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq(newId),
                eq("ACTIVE"), isNull(), eq(buildId));
        verify(sim).recycleForImage("env-1", ref);
        verify(pool).invalidateImage(ref);
        List<String> rmi = awaitRmiCall(cli);
        assertNotNull(rmi, "successful rebuild should rmi superseded id after recycle");
        assertTrue(rmi.contains(oldId));
    }

    @Test
    void recycleThrowDoesNotFailSuccessfulBuild() throws Exception {
        String buildId = "build-recycle-boom-1";
        String ref = "sq-local/recycle-boom:1";
        String oldId = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        String newId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-recycle-boom");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger inspectSeq = new AtomicInteger();
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            if (isRepoTagsInspect(argv)) {
                return new OciCli.Result(0, "[]", "", false);
            }
            if (argv.contains("inspect")) {
                return inspectSeq.getAndIncrement() == 0
                        ? new OciCli.Result(0, oldId, "", false)
                        : new OciCli.Result(0, newId, "", false);
            }
            return new OciCli.Result(0, "", "", false);
        };
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        StatefulInstanceManager sim = mock(StatefulInstanceManager.class);
        WarmPool pool = mock(WarmPool.class);
        when(sim.recycleForImage(any(), any())).thenThrow(new RuntimeException("boom"));
        when(pool.invalidateImage(any())).thenThrow(new RuntimeException("boom"));
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(0, false, null);
                    } finally {
                        done.countDown();
                    }
                },
                null, null, sim, pool);

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, "demo", 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), eq(newId),
                eq("ACTIVE"), isNull(), eq(buildId));
        verify(digest, never()).report(eq("env-1"), eq(ref), isNull(),
                eq("FAILED"), any(), eq(buildId));
    }

    @Test
    void failedAsyncBuildDoesNotRecycle() throws Exception {
        String buildId = "build-fail-recycle-1";
        String ref = "sq-local/fail-recycle:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch done = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-fail-recycle");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        StatefulInstanceManager sim = mock(StatefulInstanceManager.class);
        WarmPool pool = mock(WarmPool.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, mock(OciImageBuildProgressReporter.class), digest, executor,
                (argv, wall, sink) -> {
                    try {
                        return new OciBuildLogStreamer.Outcome(1, false, "STEP failed");
                    } finally {
                        done.countDown();
                    }
                },
                null, null, sim, pool);

        service.submit(new OciImageBuildService.BuildRequest(
                "env-1", buildId, ref, null, 60));

        assertTrue(done.await(3, TimeUnit.SECONDS));
        verify(digest, timeout(2000)).report(eq("env-1"), eq(ref), isNull(),
                eq("FAILED"), eq("STEP failed"), eq(buildId));
        verify(sim, never()).recycleForImage(any(), any());
        verify(pool, never()).invalidateImage(any());
    }

    @Test
    void buildSyncSuccessRecyclesAndInvalidates() throws Exception {
        String buildId = "sync-recycle-1";
        String ref = "sq-local/sync-recycle:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir);
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-sync-recycle");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        StatefulInstanceManager sim = mock(StatefulInstanceManager.class);
        WarmPool pool = mock(WarmPool.class);
        when(sim.recycleForImage(any(), any())).thenReturn(
                new StatefulInstanceManager.RecycleResult(true, "inst_x", "RUNNING", null));
        when(pool.invalidateImage(any())).thenReturn(2);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> new OciBuildLogStreamer.Outcome(0, false, null),
                null, null, sim, pool);

        OciImageBuildService.SyncBuildResult result = service.buildSync("env-1", buildId, ref, 60);

        assertTrue(result.success());
        assertEquals("", result.error());
        verify(sim).recycleForImage("env-1", ref);
        verify(pool).invalidateImage(ref);
        verifyNoInteractions(progress, digest);
    }

    private static void awaitContextGone(OciBuildContextStore store, String buildId, Path dir)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (!store.contextExists(buildId) && !Files.exists(dir)) {
                return;
            }
            Thread.sleep(20);
        }
        assertFalse(store.contextExists(buildId), "contextExists should be false after cleanup");
        assertFalse(Files.exists(dir), "context directory should be deleted");
    }

    private static boolean isRepoTagsInspect(List<String> argv) {
        return argv.contains("inspect")
                && argv.stream().anyMatch(a -> a != null && a.contains("RepoTags"));
    }

    /** Wait briefly for async cleanup to record an {@code rmi} call. */
    private static List<String> awaitRmiCall(FakeOciCli cli) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            List<String> rmi = cli.calls.stream()
                    .filter(a -> a.contains("rmi"))
                    .findFirst()
                    .orElse(null);
            if (rmi != null) {
                return rmi;
            }
            Thread.sleep(20);
        }
        return cli.calls.stream().filter(a -> a.contains("rmi")).findFirst().orElse(null);
    }
}
