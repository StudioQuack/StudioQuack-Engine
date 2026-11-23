package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.egress.EgressReadiness;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import com.studioquack.sqs.sandbox.egress.SandboxNetworkGuard;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.OciImageBuildProgressReporter;
import com.studioquack.sqs.sandbox.stateful.OciImageDigestReporter;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Async {@code podman build} for staged OCI contexts. Acknowledge is owned by {@code
 * BuildOciImageCommandHandler}; this service only runs the build on a bounded daemon executor,
 * streams log chunks, reports terminal digest status, and always deletes the staged context.
 */
@Slf4j
@Component
public class OciImageBuildService {

    /** Build-container memory cap (explicit in argv). */
    public static final String BUILD_MEMORY = "2048m";
    /**
     * Build-container CPU cap as CFS period/quota. {@code podman build} does not accept {@code --cpus}
     * (that flag is {@code podman run} only); 2 vCPU ≡ period 100000µs + quota 200000µs.
     */
    public static final String BUILD_CPU_PERIOD = "100000";
    public static final String BUILD_CPU_QUOTA = "200000";
    /** 4 GiB file-size ulimit — disk ceiling for build containers. */
    public static final String BUILD_FSIZE_ULIMIT = "fsize=4294967296:4294967296";
    /** Default wall-clock for a local image build (20 min). */
    public static final int DEFAULT_WALL_CLOCK_SECONDS = 1200;
    /** Bound for FAILED {@code error} strings posted to the digest callback. */
    public static final int MAX_ERROR_CHARS = 2048;
    /** Cap on {@link SyncBuildResult#log} — further streamer chunks are dropped. */
    public static final int MAX_SYNC_LOG_CHARS = 65_536;
    /** Trailing build output kept to classify a failure. */
    private static final int BUILD_FAILURE_TAIL_CHARS = 8_192;

    private static final Duration INSPECT_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration RMI_TIMEOUT = Duration.ofSeconds(30);
    private static final AtomicInteger THREAD_SEQ = new AtomicInteger();
    /** Same Jackson seam {@link ImageInspection} uses for podman {@code {{json …}}} arrays. */
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() { };

    private final OciCli cli;
    private final OciBuildContextStore contextStore;
    private final OciImageBuildProgressReporter progress;
    private final OciImageDigestReporter digestReporter;
    private final ExecutorService executor;
    private final BuildStreamer streamer;
    private final SandboxEgressManager egressManager;
    /** Daemon-owned DNS pins for the build argv (validated once at wiring time). */
    private final List<String> dnsServers;
    /** Bounce a live user instance onto a rebuilt image. Null in unit tests. */
    private final StatefulInstanceManager statefulInstances;
    /** Cool idle warm-pool slots for a rebuilt image. Null in unit tests. */
    private final WarmPool warmPool;
    /**
     * Set once a build failed only because the host cannot apply cgroup caps (see
     * {@link #streamBuild}). Builds only: build output includes RUN-step text the Containerfile
     * controls, so it must never mark {@link CgroupCaps} for every other container.
     */
    private volatile boolean buildCapsUnsupported;

    @Autowired
    public OciImageBuildService(OciCli cli,
                                 OciBuildContextStore contextStore,
                                 OciImageBuildProgressReporter progress,
                                 OciImageDigestReporter digestReporter,
                                 SandboxEgressManager egressManager,
                                 org.springframework.core.env.Environment environment,
                                 @Autowired(required = false) @Lazy StatefulInstanceManager statefulInstances,
                                 @Autowired(required = false) WarmPool warmPool) {
        this(cli, contextStore, progress, digestReporter, defaultExecutor(),
                OciBuildLogStreamer::runStreaming, egressManager,
                // Builds legitimately resolve PUBLIC hosts (dependency fetch); pinning --dns keeps that working
                // while cutting the bridge-gateway path into the host's internal DNS (blank → the public default
                // set).
                com.studioquack.sqs.sandbox.egress.SandboxDns.parse(
                        environment == null ? "" : environment.getProperty(
                                com.studioquack.sqs.sandbox.egress.SandboxDns.CONFIG_KEY,
                                com.studioquack.sqs.sandbox.egress.SandboxDns.DEFAULT_SERVERS)),
                statefulInstances, warmPool);
    }

    /**
     * Test constructor: inject a custom executor and streamer for deterministic unit tests (no real
     * {@code podman build}). Null {@code egressManager} skips the gate.
     */
    public OciImageBuildService(OciCli cli,
                                 OciBuildContextStore contextStore,
                                 OciImageBuildProgressReporter progress,
                                 OciImageDigestReporter digestReporter,
                                 ExecutorService executor,
                                 BuildStreamer streamer) {
        this(cli, contextStore, progress, digestReporter, executor, streamer, null);
    }

    public OciImageBuildService(OciCli cli,
                                 OciBuildContextStore contextStore,
                                 OciImageBuildProgressReporter progress,
                                 OciImageDigestReporter digestReporter,
                                 ExecutorService executor,
                                 BuildStreamer streamer,
                                 SandboxEgressManager egressManager) {
        this(cli, contextStore, progress, digestReporter, executor, streamer, egressManager, null);
    }

    public OciImageBuildService(OciCli cli,
                                 OciBuildContextStore contextStore,
                                 OciImageBuildProgressReporter progress,
                                 OciImageDigestReporter digestReporter,
                                 ExecutorService executor,
                                 BuildStreamer streamer,
                                 SandboxEgressManager egressManager,
                                 List<String> dnsServers) {
        this(cli, contextStore, progress, digestReporter, executor, streamer, egressManager,
                dnsServers, null, null);
    }

    public OciImageBuildService(OciCli cli,
                                 OciBuildContextStore contextStore,
                                 OciImageBuildProgressReporter progress,
                                 OciImageDigestReporter digestReporter,
                                 ExecutorService executor,
                                 BuildStreamer streamer,
                                 SandboxEgressManager egressManager,
                                 List<String> dnsServers,
                                 StatefulInstanceManager statefulInstances,
                                 WarmPool warmPool) {
        this.cli = cli;
        this.contextStore = contextStore;
        this.progress = progress;
        this.digestReporter = digestReporter;
        this.executor = executor;
        this.streamer = streamer;
        this.egressManager = egressManager;
        this.dnsServers = com.studioquack.sqs.sandbox.egress.SandboxDns.validate(dnsServers);
        this.statefulInstances = statefulInstances;
        this.warmPool = warmPool;
    }

    /** Submit async build. Returns immediately. */
    public void submit(BuildRequest req) {
        if (req == null) {
            return;
        }
        try {
            executor.execute(() -> runBuild(req));
        } catch (RejectedExecutionException reject) {
            log.warn("[T-31] oci-build rejected (executor full) buildId={} ref={}",
                    req.buildId(), req.ref());
            reportFailed(req.envId(), req.buildId(), req.ref(), "build executor saturated");
            contextStore.deleteContext(req.buildId());
        }
    }

    public record BuildRequest(String envId, String buildId, String ref, String displayName,
                               int wallClockSeconds) {
    }

    /**
     * Terminal result of {@link #buildSync}. {@code error} is empty on success; {@code log} is
     * the concatenated streamer chunks (capped at {@link #MAX_SYNC_LOG_CHARS}).
     */
    public record SyncBuildResult(boolean success, String log, String error) {
    }

    /**
     * Synchronous test-image build. Reuses {@link #buildArgv}, the egress gate and {@code
     * SandboxNetworkEnsure} for {@code sqs-build}, and the bounded daemon executor.
     */
    public SyncBuildResult buildSync(String envId, String buildId, String ref, int wallClockSeconds) {
        log.debug("[T-31] oci-build-sync envId={} buildId={} ref={}", envId, buildId, ref);
        if (!contextStore.contextExists(buildId)) {
            return new SyncBuildResult(false, "", "build context missing");
        }
        if (!cli.available() || cli.cliPath() == null || cli.cliPath().isBlank()) {
            return new SyncBuildResult(false, "", "OCI runtime unavailable");
        }
        // Same refuse-while-UNAVAILABLE gate as runBuild; do not start the streamer.
        if (egressManager != null) {
            EgressReadiness readiness = egressManager.readiness();
            if (!readiness.internetAllowed()) {
                return new SyncBuildResult(false, "",
                        readiness.refusalMessage());
            }
        }
        int wallSecs = wallClockSeconds > 0 ? wallClockSeconds : DEFAULT_WALL_CLOCK_SECONDS;
        StringBuilder logBuf = new StringBuilder();
        AtomicReference<OciBuildLogStreamer.Outcome> outcomeRef = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        try {
            executor.execute(() -> {
                try {
                    outcomeRef.set(streamBuild(buildId, ref, wallSecs, chunk -> appendSyncLog(logBuf, chunk)));
                } catch (Exception ex) {
                    errorRef.set(message(ex));
                    log.warn("[T-31] oci-build-sync exception envId={} buildId={} ref={}: {}",
                            envId, buildId, ref, ex.toString());
                } finally {
                    latch.countDown();
                }
            });
        } catch (RejectedExecutionException reject) {
            log.warn("[T-31] oci-build-sync rejected (executor full) buildId={} ref={}",
                    buildId, ref);
            return new SyncBuildResult(false, "", "build executor saturated");
        }
        boolean finished;
        try {
            finished = latch.await(wallSecs, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return new SyncBuildResult(false, snapshotSyncLog(logBuf), "build timed out");
        }
        if (!finished) {
            return new SyncBuildResult(false, snapshotSyncLog(logBuf), "build timed out");
        }
        if (errorRef.get() != null) {
            return new SyncBuildResult(false, snapshotSyncLog(logBuf), errorRef.get());
        }
        OciBuildLogStreamer.Outcome outcome = outcomeRef.get();
        if (outcome != null && outcome.exitCode() == 0 && !outcome.timedOut()) {
            log.info("[T-31] oci-build-sync ok envId={} buildId={} ref={}", envId, buildId, ref);
            recycleAfterSuccessfulBuild(envId, ref);
            return new SyncBuildResult(true, snapshotSyncLog(logBuf), "");
        }
        String err = failureMessage(outcome);
        log.warn("[T-31] oci-build-sync failed envId={} buildId={} ref={} err={}",
                envId, buildId, ref, err);
        return new SyncBuildResult(false, snapshotSyncLog(logBuf), err);
    }

    /** Package-visible for tests: build argv without running. Uses the default DNS pins. */
    static List<String> buildArgv(String cliPath, String ref, Path contextDir) {
        return buildArgv(cliPath, ref, contextDir,
                com.studioquack.sqs.sandbox.egress.SandboxDns.defaults());
    }

    /** Package-visible for tests: build argv without running, with explicit DNS pins. */
    static List<String> buildArgv(String cliPath, String ref, Path contextDir, List<String> dnsServers) {
        return buildArgv(cliPath, ref, contextDir, dnsServers, false);
    }

    /**
     * Package-visible for tests: build argv without running, with explicit DNS pins and
     * degraded-posture flag. {@code degraded} is the no-named-network shape — a DEGRADED egress
     * posture or a rootless builder: the build rides the host's implicit default bridge — a rootless
     * buildah RUN step cannot join a named netavark network — so the argv carries NO {@code --network}
     * token at all.
     */
    static List<String> buildArgv(String cliPath, String ref, Path contextDir, List<String> dnsServers,
                                  boolean degraded) {
        return buildArgv(cliPath, ref, contextDir, dnsServers, degraded, true);
    }

    /**
     * Package-visible for tests. {@code resourceCaps=false} drops the cgroup-backed
     * {@code --memory}/{@code --cpu-*} caps for a host that cannot apply them (see
     * {@link CgroupCaps}); the rlimit-backed {@code fsize} ulimit stays.
     */
    static List<String> buildArgv(String cliPath, String ref, Path contextDir, List<String> dnsServers,
                                  boolean degraded, boolean resourceCaps) {
        List<String> argv = new ArrayList<>(14);
        argv.add(cliPath);
        argv.add("build");
        if (resourceCaps) {
            argv.add("--memory");
            argv.add(BUILD_MEMORY);
            argv.add("--cpu-period");
            argv.add(BUILD_CPU_PERIOD);
            argv.add("--cpu-quota");
            argv.add(BUILD_CPU_QUOTA);
        }
        argv.add("--ulimit");
        argv.add(BUILD_FSIZE_ULIMIT);
        // The build reaches the internet (it resolves its own dependencies) but runs on its own network,
        // so untrusted source being compiled is not adjacent to a running job or stateful instance.
        // Podman's default bridge would put it next to both.
        if (!degraded) {
            argv.add("--network");
            argv.add(SandboxNetworks.BUILD);
        }
        // Pinned resolvers: dependency fetch legitimately resolves PUBLIC hosts, and the pins keep that
        // working while the bridge gateway's forwarder (→ the host's internal DNS) is never consulted.
        // This argv is fully daemon-built (no attacker tail), and SandboxNetworkGuard.validateBuildNetwork
        // accepts daemon-owned --dns before the context.
        for (String dns : com.studioquack.sqs.sandbox.egress.SandboxDns.validate(dnsServers)) {
            argv.add("--dns");
            argv.add(dns);
        }
        argv.add("-t");
        argv.add(ref);
        argv.add(contextDir.toAbsolutePath().normalize().toString());
        // Build-time --network is a different surface than run; only sqs-build|none, or no network token
        // at all on a DEGRADED host.
        SandboxNetworkGuard.validateBuildNetwork(argv, degraded);
        return List.copyOf(argv);
    }

    /**
     * Ensure + argv. Shared by {@link #runBuild} and {@link #buildSync} so both paths pin {@code
     * sqs-build}, DNS, and caps through {@link #buildArgv}.
     */
    private List<String> prepareBuildArgv(String buildId, String ref, boolean resourceCaps) {
        // DEGRADED hosts (rootless podman machine on macOS/Windows) cannot attach a build step to a named
        // netavark network — buildah fails with "cannot use networks as rootless". Like every other
        // container on such a host, the build degrades to the implicit default bridge: no network to
        // ensure, no --network token in the argv.
        boolean degraded = egressManager != null
                && egressManager.readiness().state() == EgressReadiness.State.DEGRADED;
        // An ENFORCED posture no longer implies a rootful builder: egress rules can live in a
        // rootless machine's network namespace, yet rootless buildah still cannot join a named
        // network. A rootless builder therefore takes the same no-network shape.
        boolean rootless = !degraded && rootlessBuilder();
        if (degraded || rootless) {
            log.warn("[T-31] oci-build {} buildId={} ref={} — building on the host default bridge; "
                    + "sqs-build isolation is not enforceable for this build",
                    degraded ? "DEGRADED network posture" : "rootless builder", buildId, ref);
        } else {
            // Drive the ensure through the injected OciCli seam, not the static CliRunner, so this
            // stays unit-testable against FakeOciCli with no runtime on the box.
            SandboxNetworkEnsure.ensure(cli.cliPath(), SandboxNetworks.BUILD, (argv, timeout) -> {
                OciCli.Result r = cli.run(argv, timeout);
                // Stdout required: ensure inspects isolate/subnet before caching or migrating.
                return new SandboxNetworkEnsure.ExecResult(r.ok(), r.exitCode(), r.stdout(), r.stderr());
            });
        }
        Path contextDir = contextStore.contextDir(buildId);
        return buildArgv(cli.cliPath(), ref, contextDir, dnsServers, degraded || rootless, resourceCaps);
    }

    /** Cached answer of {@link #rootlessBuilder()}; null until one probe has succeeded. */
    private volatile Boolean rootlessBuilderCache;

    /**
     * True only when {@code podman info} positively reports a rootless engine — the one
     * builder that cannot honour {@code --network sqs-build} ("cannot use networks as
     * rootless"). A failed probe answers false without caching, so an engine that is still
     * starting is asked again on the next build instead of being pinned to a guess.
     */
    private boolean rootlessBuilder() {
        Boolean known = rootlessBuilderCache;
        if (known != null) {
            return known;
        }
        OciCli.Result r = cli.run(List.of(cli.cliPath(), "info", "--format", "{{.Host.Security.Rootless}}"),
                Duration.ofSeconds(15));
        if (!r.ok()) {
            return false;
        }
        boolean rootless = r.stdout() != null && "true".equalsIgnoreCase(r.stdout().trim());
        rootlessBuilderCache = rootless;
        return rootless;
    }

    /**
     * Runs the build, and when it fails only because the host cannot apply cgroup resource limits to
     * the build container, retries once without them and remembers that for later builds. Rootless
     * Podman on a WSL podman machine (Windows) has no delegated memory/cpu controllers, and crun dies
     * opening {@code memory.max}.
     */
    private OciBuildLogStreamer.Outcome streamBuild(String buildId, String ref, int wallSecs,
                                                    OciBuildLogStreamer.ChunkSink sink) {
        // One choke point, before podman build, on the file Buildah will compile. ONBUILD triggers inside
        // a third-party base image are not visible here.
        validateContainerfile(buildId);
        boolean caps = !buildCapsUnsupported
                && CgroupCaps.applies(CgroupCaps.Controller.MEMORY)
                && CgroupCaps.applies(CgroupCaps.Controller.CPU);
        // Tail only: crun's refusal is the last thing the build prints.
        StringBuilder tail = new StringBuilder();
        OciBuildLogStreamer.Outcome outcome = streamer.run(prepareBuildArgv(buildId, ref, caps),
                Duration.ofSeconds(wallSecs), chunk -> {
                    if (chunk != null) {
                        tail.append(chunk);
                        if (tail.length() > BUILD_FAILURE_TAIL_CHARS) {
                            tail.delete(0, tail.length() - BUILD_FAILURE_TAIL_CHARS);
                        }
                    }
                    sink.onChunk(chunk);
                });
        boolean failed = outcome == null || outcome.exitCode() != 0 || outcome.timedOut();
        if (outcome != null && outcome.lastErrorLine() != null) {
            tail.append('\n').append(outcome.lastErrorLine());
        }
        if (caps && failed && CgroupCaps.missingController(tail) != null) {
            buildCapsUnsupported = true;
            log.warn("[T-31] oci-build buildId={} ref={} — host cannot apply cgroup resource limits to "
                    + "build containers; retrying without --memory/--cpu caps (degraded posture)", buildId, ref);
            outcome = streamer.run(prepareBuildArgv(buildId, ref, false), Duration.ofSeconds(wallSecs), sink);
        }
        return outcome;
    }

    /**
     * Reject a Containerfile whose RUN flags can escape the build sandbox. Missing file fails closed:
     * podman would fail too, but only after the network is up.
     */
    private void validateContainerfile(String buildId) {
        Path contextDir = contextStore.contextDir(buildId);
        Path file = ContainerfilePolicy.resolveBuildFile(contextDir);
        if (file == null) {
            throw new IllegalStateException("Containerfile: build context has no Containerfile or Dockerfile");
        }
        try {
            ContainerfilePolicy.validate(java.nio.file.Files.readString(file));
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Containerfile: could not read " + file.getFileName());
        }
    }

    private void runBuild(BuildRequest req) {
        String envId = req.envId();
        String buildId = req.buildId();
        String ref = req.ref();
        try {
            if (!contextStore.contextExists(buildId)) {
                progress.report(envId, buildId, ref, "building", null, "build context missing");
                reportFailed(envId, buildId, ref, "build context missing");
                return;
            }
            progress.report(envId, buildId, ref, "building", null, null);
            if (!cli.available() || cli.cliPath() == null || cli.cliPath().isBlank()) {
                reportFailed(envId, buildId, ref, "OCI runtime unavailable");
                return;
            }
            // Builds attach sqs-build; refuse while overlay egress is UNAVAILABLE.
            if (egressManager != null) {
                EgressReadiness readiness = egressManager.readiness();
                if (!readiness.internetAllowed()) {
                    reportFailed(envId, buildId, ref,
                            readiness.refusalMessage());
                    return;
                }
            }
            int wallSecs = req.wallClockSeconds() > 0
                    ? req.wallClockSeconds() : DEFAULT_WALL_CLOCK_SECONDS;
            // Snapshot the current image id before build; tag reuse leaves it dangling only when the rebuild
            // succeeds with a different id.
            String previousImageId = resolveImageId(ref);
            OciBuildLogStreamer.Outcome outcome = streamBuild(buildId, ref, wallSecs,
                    chunk -> progress.report(envId, buildId, ref, "streaming", chunk, null));

            if (outcome != null && outcome.exitCode() == 0 && !outcome.timedOut()) {
                String newImageId = resolveImageId(ref);
                // Recycle before rmi so live containers release the superseded id.
                // Identical old/new ids means the image did not change — no bounce.
                boolean imageUnchanged = previousImageId != null && !previousImageId.isBlank()
                        && newImageId != null && !newImageId.isBlank()
                        && previousImageId.equals(newImageId);
                if (!imageUnchanged) {
                    recycleAfterSuccessfulBuild(envId, ref);
                }
                deleteSupersededImage(ref, previousImageId, newImageId);
                String digest = newImageId != null ? newImageId : inspectDigest(ref);
                digestReporter.report(envId, ref, digest, "ACTIVE", null, buildId);
                progress.report(envId, buildId, ref, "building", null, "build complete");
                log.info("[T-31] oci-build ok buildId={} ref={} digest={}", buildId, ref, digest);
                return;
            }

            // Failure must not delete: the tag never moved off the last working image.
            String err = failureMessage(outcome);
            reportFailed(envId, buildId, ref, err);
            log.warn("[T-31] oci-build failed buildId={} ref={} err={}", buildId, ref, err);
        } catch (Exception ex) {
            // Same as failure path: leave any previously tagged working image alone.
            reportFailed(envId, buildId, ref, message(ex));
            log.warn("[T-31] oci-build exception buildId={} ref={}: {}", buildId, ref, ex.toString());
        } finally {
            // ALWAYS delete staged context (success, failure, missing, reject path above).
            contextStore.deleteContext(buildId);
        }
    }

    /**
     * Current image id for {@code ref}, or {@code null} when absent / unparseable. Used for
     * previous/new comparison — shapes and ids only, never build context.
     */
    private String resolveImageId(String ref) {
        if (ref == null || ref.isBlank() || !cli.available() || cli.cliPath() == null) {
            return null;
        }
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "image", "inspect",
                    "--format", "{{.Id}}", ref), INSPECT_TIMEOUT);
            if (r.ok() && !r.out().isEmpty()) {
                return r.out();
            }
            return null;
        } catch (Exception e) {
            log.debug("[T-31] oci-build resolveImageId failed ref={}: {}", ref, e.toString());
            return null;
        }
    }

    private String inspectDigest(String ref) {
        try {
            String id = resolveImageId(ref);
            if (id != null) {
                return id;
            }
            // Fallback: first RepoDigests entry when Id format is empty.
            OciCli.Result digests = cli.run(List.of(cli.cliPath(), "image", "inspect",
                    "--format", "{{range .RepoDigests}}{{.}}{{end}}", ref), INSPECT_TIMEOUT);
            return digests.ok() && !digests.out().isEmpty() ? digests.out() : null;
        } catch (Exception e) {
            log.warn("[T-31] oci-build inspect failed ref={}: {}", ref, e.toString());
            return null;
        }
    }

    /**
     * Best-effort bounce of a live stateful instance and cooldown of idle warm-pool slots after a
     * successful rebuild. Never throws; never reports FAILED.
     */
    private void recycleAfterSuccessfulBuild(String envId, String ref) {
        if (ref == null || ref.isBlank()) {
            return;
        }
        if (statefulInstances != null) {
            try {
                statefulInstances.recycleForImage(envId, ref);
            } catch (Exception e) {
                log.warn("[T-31] oci-build recycle skipped envId={} ref={}: {}",
                        envId, ref, e.toString());
            }
        }
        if (warmPool != null) {
            try {
                warmPool.invalidateImage(ref);
            } catch (Exception e) {
                log.warn("[T-31] oci-build warm-pool invalidate skipped envId={} ref={}: {}",
                        envId, ref, e.toString());
            }
        }
    }

    /**
     * After a successful rebuild, drop the previous image id only when it is dangling (no RepoTags
     * other than the rebuilt {@code ref}). Inspect/parse failure skips {@code rmi} (fail-safe toward
     * retention).
     */
    private void deleteSupersededImage(String ref, String previousImageId, String newImageId) {
        if (previousImageId == null || previousImageId.isBlank()
                || newImageId == null || newImageId.isBlank()
                || previousImageId.equals(newImageId)
                || !cli.available() || cli.cliPath() == null) {
            return;
        }
        if (previousImageHasOtherTags(previousImageId, ref)) {
            return;
        }
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "rmi", previousImageId), RMI_TIMEOUT);
            if (r.ok()) {
                log.info("[T-31] oci-build removed superseded image ref={} oldId={}",
                        ref, shortId(previousImageId));
                return;
            }
            log.warn("[T-31] oci-build superseded-image cleanup skipped ref={} oldId={} (in use?): {}",
                    ref, shortId(previousImageId), r.firstErrLine());
        } catch (Exception e) {
            log.warn("[T-31] oci-build superseded-image cleanup skipped ref={} oldId={}: {}",
                    ref, shortId(previousImageId), e.toString());
        }
    }

    /**
     * {@code true} when the previous id still carries a tag other than {@code rebuiltRef},
     * or when inspect/parse fails (retain). {@code false} only for a dangling id — empty
     * RepoTags, or only the rebuilt ref which has already moved to the new id.
     */
    private boolean previousImageHasOtherTags(String previousImageId, String rebuiltRef) {
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "image", "inspect",
                    "--format", "{{json .RepoTags}}", previousImageId), INSPECT_TIMEOUT);
            if (!r.ok()) {
                log.warn("[T-31] oci-build superseded-image inspect skipped rmi ref={} oldId={}: {}",
                        rebuiltRef, shortId(previousImageId), r.firstErrLine());
                return true;
            }
            List<String> tags = parseRepoTags(r.out());
            if (tags == null) {
                log.warn("[T-31] oci-build superseded-image RepoTags unparseable; skipping rmi ref={} oldId={}",
                        rebuiltRef, shortId(previousImageId));
                return true;
            }
            for (String tag : tags) {
                if (tag != null && !tag.isBlank() && !tag.equals(rebuiltRef)) {
                    log.info("[T-31] oci-build keeping image still tagged ref={} oldId={} remainingTag={}",
                            rebuiltRef, shortId(previousImageId), tag);
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            log.warn("[T-31] oci-build superseded-image inspect skipped rmi ref={} oldId={}: {}",
                    rebuiltRef, shortId(previousImageId), e.toString());
            return true;
        }
    }

    /** JSON array of RepoTags. {@code null} means unparseable (caller skips rmi). */
    private static List<String> parseRepoTags(String raw) {
        if (raw == null) {
            return null;
        }
        String t = raw.trim();
        if (t.isEmpty()) {
            return null;
        }
        if ("null".equals(t)) {
            return List.of();
        }
        try {
            List<String> parsed = JSON.readValue(t, STRING_LIST);
            return parsed == null ? List.of() : parsed;
        } catch (Exception e) {
            return null;
        }
    }

    private static String shortId(String id) {
        return id == null ? "" : id.substring(0, Math.min(12, id.length()));
    }

    private void reportFailed(String envId, String buildId, String ref, String error) {
        digestReporter.report(envId, ref, null, "FAILED", boundError(error), buildId);
    }

    private static String failureMessage(OciBuildLogStreamer.Outcome outcome) {
        if (outcome == null) {
            return "build failed";
        }
        if (outcome.timedOut()) {
            return "build timed out";
        }
        if (outcome.lastErrorLine() != null && !outcome.lastErrorLine().isBlank()) {
            return outcome.lastErrorLine();
        }
        return "build failed (exit " + outcome.exitCode() + ")";
    }

    private static void appendSyncLog(StringBuilder buf, String chunk) {
        if (chunk == null || chunk.isEmpty()) {
            return;
        }
        synchronized (buf) {
            int room = MAX_SYNC_LOG_CHARS - buf.length();
            if (room <= 0) {
                return;
            }
            if (chunk.length() <= room) {
                buf.append(chunk);
            } else {
                buf.append(chunk, 0, room);
            }
        }
    }

    private static String snapshotSyncLog(StringBuilder buf) {
        synchronized (buf) {
            return buf.toString();
        }
    }

    private static String boundError(String error) {
        if (error == null || error.isBlank()) {
            return "build failed";
        }
        return error.length() <= MAX_ERROR_CHARS ? error : error.substring(0, MAX_ERROR_CHARS);
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    private static ExecutorService defaultExecutor() {
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "oci-build-" + THREAD_SEQ.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        // core=1, max=2, queue=8 — reject when saturated (submit() reports FAILED).
        return new ThreadPoolExecutor(
                1, 2,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(8),
                factory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }

    /** Process-exec seam so tests drive success/failure without a real podman build. */
    @FunctionalInterface
    public interface BuildStreamer {
        OciBuildLogStreamer.Outcome run(List<String> argv, Duration wallClock,
                                        OciBuildLogStreamer.ChunkSink sink);
    }
}
