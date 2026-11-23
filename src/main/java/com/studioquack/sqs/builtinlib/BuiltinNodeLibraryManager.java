package com.studioquack.sqs.builtinlib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService;
import com.studioquack.sqs.release.ReleaseHttpClient;
import com.studioquack.sqs.release.ReleaseSignatureException;
import com.studioquack.sqs.release.ReleaseSignatureVerifier;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Platform-managed like the Docling side-car: the operator cannot stop it from the control plane.
 * The container runs in its own systemd scope ({@code ContainerScope}) so engine restarts never
 * touch it, and {@code — restart unless-stopped} brings it back after a host reboot without any
 * re-provisioning work here.
 */
@Slf4j
@Component
public class BuiltinNodeLibraryManager implements SmartInitializingSingleton {

    /** Image every built-in node definition references; built locally, never pulled. */
    public static final String IMAGE_REF = "sq-local/sq-builtin-node-library:1.0.0";
    /** Deterministic instance id — the BE platform guard recognises this exact id. */
    public static final String INSTANCE_ID = StatefulInstance.BUILTIN_NODE_LIBRARY_INSTANCE_ID;
    /** Release-tree path the BE serves the build context from (anonymous GET). */
    static final String RELEASE_PATH = "/releases/client/node-library/";
    /** Build-context buildId; passes {@code OciBuildPathValidator} ([A-Za-z0-9._-]+). */
    static final String BUILD_ID = "builtin-node-library-1.0.0";
    /** The maven-inside-image build routinely runs 5–15 min on a fresh host. */
    private static final int BUILD_WALL_CLOCK_SECONDS = 3600;
    static final int BUILD_LOG_TAIL_LINES = 40;
    static final int BUILD_LOG_TAIL_CHARS = 8_192;

    private static final Duration PODMAN_WAIT = Duration.ofMinutes(10);
    private static final Duration PODMAN_POLL = Duration.ofSeconds(5);
    private static final Duration HTTP_READ = Duration.ofSeconds(60);
    private static final Duration DOWNLOAD_READ = Duration.ofMinutes(10);
    /** Short bounded retry for the download phase — the install just talked to the BE. */
    private static final int FETCH_ATTEMPTS = 3;
    private static final Duration FETCH_BACKOFF = Duration.ofSeconds(20);
    /** Untar is a process exec like every container op; bounded so a wedged tar dies. */
    private static final Duration UNTAR_TIMEOUT = Duration.ofMinutes(5);

    /** {@code updateState} values in {@link NodeLibrarySnapshot} (wire contract with the BE/FE). */
    public static final String STATE_IDLE = "IDLE";
    public static final String STATE_UPDATING = "UPDATING";
    public static final String STATE_FAILED = "FAILED";
    /** {@code updateStage} values while {@link #STATE_UPDATING}; null otherwise. */
    public static final String STAGE_CHECKING = "CHECKING";
    public static final String STAGE_DOWNLOADING = "DOWNLOADING";
    public static final String STAGE_BUILDING = "BUILDING";
    public static final String STAGE_RECYCLING = "RECYCLING";
    public static final String STAGE_DRAINING = "DRAINING";
    /** Between boot attempts: the last attempt failed ({@code updateError}), the next is scheduled. */
    public static final String STAGE_WAITING = "WAITING";

    /** Slow phase after the fast boot attempts: one attempt every 5 min, forever. */
    static final Duration SLOW_RETRY_BACKOFF = Duration.ofMinutes(5);
    /** Bound for {@link #awaitUsable}: fits inside the BE's 30 s overlay command budget. */
    public static final Duration START_RUN_WAIT = Duration.ofSeconds(20);

    private final StatefulInstanceManager statefulInstanceManager;
    private final OciImageBuildService buildService;
    private final OciBuildContextStore contextStore;
    private final OciCli cli;
    private final ObjectMapper json = new ObjectMapper();
    /** Channel TLS client (JDK/OS trust store, hostname verification on) for {@code /releases}. */
    private final ReleaseHttpClient releaseHttp;
    /** Pinned-key check on the manifest: the sha256 it carries is only as good as its origin. */
    private final ReleaseSignatureVerifier releaseSignatures;

    private final boolean enabled;
    private final String backendBaseUrl;
    private final String envId;
    private final Path dataDir;
    private final String imageRef;

    /** Whole-pipeline retry: transient boot-time failures (egress not probed yet, BE still
     *  deploying the release tree) must not strand the executor until the next daemon restart. */
    private final int runAttempts;
    private final Duration runBackoff;
    /** Bound for in-flight drain on the old node-library container during image replace. */
    private final Duration drainMaxWait;
    /**
     * Gap between automatic manifest checks once the library is usable.
     * {@link Duration#ZERO} disables the scheduler (tests, and an operator opt-out).
     */
    private final Duration updateCheckInterval;
    /**
     * Shortest gap between supervision passes (one container, and it is running) — used after a pass
     * that had to heal something.
     */
    private final Duration superviseInterval;
    /** Idle ceiling for the supervision gap; egress-up still triggers an immediate pass. */
    static final Duration SUPERVISE_MAX_INTERVAL = Duration.ofMinutes(5);
    /** Current supervision gap; guarded by {@code this}. */
    private Duration superviseGap;
    /** The next scheduled pass; guarded by {@code this}. */
    private java.util.concurrent.ScheduledFuture<?> nextPass;

    /** Exactly-one-pipeline guard shared by the boot ensure, the on-demand update, and the auto check. */
    private final AtomicBoolean pipelineRunning = new AtomicBoolean(false);
    /** Wakes the boot loop's between-attempt wait early ({@link #retryNow()}). */
    private final Object retryWake = new Object();
    private boolean retryRequested;
    /** Slow phase enabled (production); tests pin it off to keep the give-up path observable. */
    private final boolean retryForever;
    /** Stage while UPDATING (CHECKING/DOWNLOADING/BUILDING/RECYCLING/DRAINING); null otherwise. */
    private volatile String updateStage;
    /** Last pipeline failure reason; cleared when a run starts and when one succeeds. */
    private volatile String updateError;
    /** Set when a build actually completes (boot or update); null on the already-current path. */
    private volatile Instant lastBuildAt;
    /** Marker-file cache — only this class writes the marker, so a lazy single load is safe. */
    private volatile String builtSha;
    private volatile boolean builtShaLoaded;
    /**
     * Start-run readiness. True when a run may proceed: opted-out / unenrolled, a previous successful
     * build marker is already on disk, or this pipeline just built the image.
     */
    private volatile boolean usable;
    /**
     * Bounded pool: one slot for the pipeline, one for the download that overlaps
     * {@link #awaitPodman()}. Size 2 is load-bearing — submitting the download from
     * inside {@code run()} on a single-thread pool would deadlock on {@code Future.get()}.
     */
    private final ExecutorService executor;
    /** Manifest poll. Null until {@link #afterSingletonsInstantiated()} and when the interval is zero. */
    private ScheduledExecutorService updateChecks;
    /**
     * Supervision passes, on their own thread: a pass can wait on the start lock behind a drain,
     * and must not hold up the manifest poll. Null until {@link #afterSingletonsInstantiated()}.
     */
    private ScheduledExecutorService supervisor;

    @org.springframework.beans.factory.annotation.Autowired
    public BuiltinNodeLibraryManager(
            StatefulInstanceManager statefulInstanceManager,
            OciImageBuildService buildService,
            OciBuildContextStore contextStore,
            OciCli cli,
            // Nullable: slice tests boot without a release client — the manager then
            // skips the download leg instead of failing the whole context.
            @org.springframework.lang.Nullable ReleaseHttpClient releaseHttp,
            @Value("${sqs.builtin-library.enabled:true}") boolean enabled,
            @Value("${sqs.backend.base-url:}") String backendBaseUrl,
            @Value("${sqs.channel.env-id:}") String envId,
            @Value("${SQS_DATA_DIR:./data}") String dataDir,
            @Value("${sqs.builtin-library.image:" + IMAGE_REF + "}") String imageRef,
            @Value("${sqs.builtin-library.drain-max-wait:PT30M}") Duration drainMaxWait,
            @Value("${sqs.builtin-library.update-check-interval:PT5M}") Duration updateCheckInterval,
            @Value("${sqs.builtin-library.supervise-interval:PT30S}") Duration superviseInterval,
            ReleaseSignatureVerifier releaseSignatures) {
        this(statefulInstanceManager, buildService, contextStore, cli, releaseHttp, enabled,
                backendBaseUrl, envId, dataDir, imageRef, 3, Duration.ofSeconds(60), drainMaxWait, true,
                releaseSignatures, updateCheckInterval, superviseInterval);
    }

    /** Test ctor: pins the retry policy so a failure-path test does not pay the real backoff. */
    BuiltinNodeLibraryManager(
            StatefulInstanceManager statefulInstanceManager,
            OciImageBuildService buildService,
            OciBuildContextStore contextStore,
            OciCli cli,
            ReleaseHttpClient releaseHttp,
            boolean enabled, String backendBaseUrl, String envId, String dataDir, String imageRef,
            int runAttempts, Duration runBackoff) {
        this(statefulInstanceManager, buildService, contextStore, cli, releaseHttp, enabled,
                backendBaseUrl, envId, dataDir, imageRef, runAttempts, runBackoff, Duration.ofMinutes(30), false);
    }

    /** Test ctor: also pins drain wait so a recycle-path test does not wait the real 30 minutes. */
    BuiltinNodeLibraryManager(
            StatefulInstanceManager statefulInstanceManager,
            OciImageBuildService buildService,
            OciBuildContextStore contextStore,
            OciCli cli,
            ReleaseHttpClient releaseHttp,
            boolean enabled, String backendBaseUrl, String envId, String dataDir, String imageRef,
            int runAttempts, Duration runBackoff, Duration drainMaxWait) {
        this(statefulInstanceManager, buildService, contextStore, cli, releaseHttp, enabled,
                backendBaseUrl, envId, dataDir, imageRef, runAttempts, runBackoff, drainMaxWait, false);
    }

    /** Full ctor: {@code retryForever} enables the production slow phase after the fast attempts. */
    BuiltinNodeLibraryManager(
            StatefulInstanceManager statefulInstanceManager,
            OciImageBuildService buildService,
            OciBuildContextStore contextStore,
            OciCli cli,
            ReleaseHttpClient releaseHttp,
            boolean enabled, String backendBaseUrl, String envId, String dataDir, String imageRef,
            int runAttempts, Duration runBackoff, Duration drainMaxWait, boolean retryForever) {
        this(statefulInstanceManager, buildService, contextStore, cli, releaseHttp, enabled,
                backendBaseUrl, envId, dataDir, imageRef, runAttempts, runBackoff, drainMaxWait, retryForever,
                ReleaseSignatureVerifier.transitional(), Duration.ZERO);
    }

    BuiltinNodeLibraryManager(
            StatefulInstanceManager statefulInstanceManager,
            OciImageBuildService buildService,
            OciBuildContextStore contextStore,
            OciCli cli,
            ReleaseHttpClient releaseHttp,
            boolean enabled, String backendBaseUrl, String envId, String dataDir, String imageRef,
            int runAttempts, Duration runBackoff, Duration drainMaxWait, boolean retryForever,
            ReleaseSignatureVerifier releaseSignatures, Duration updateCheckInterval) {
        this(statefulInstanceManager, buildService, contextStore, cli, releaseHttp, enabled,
                backendBaseUrl, envId, dataDir, imageRef, runAttempts, runBackoff, drainMaxWait, retryForever,
                releaseSignatures, updateCheckInterval, Duration.ZERO);
    }

    BuiltinNodeLibraryManager(
            StatefulInstanceManager statefulInstanceManager,
            OciImageBuildService buildService,
            OciBuildContextStore contextStore,
            OciCli cli,
            ReleaseHttpClient releaseHttp,
            boolean enabled, String backendBaseUrl, String envId, String dataDir, String imageRef,
            int runAttempts, Duration runBackoff, Duration drainMaxWait, boolean retryForever,
            ReleaseSignatureVerifier releaseSignatures, Duration updateCheckInterval,
            Duration superviseInterval) {
        this.superviseInterval = superviseInterval == null || superviseInterval.isNegative()
                ? Duration.ZERO : superviseInterval;
        this.releaseSignatures = releaseSignatures;
        this.retryForever = retryForever;
        this.updateCheckInterval = updateCheckInterval == null || updateCheckInterval.isNegative()
                ? Duration.ZERO : updateCheckInterval;
        this.statefulInstanceManager = statefulInstanceManager;
        this.buildService = buildService;
        this.contextStore = contextStore;
        this.cli = cli;
        this.releaseHttp = releaseHttp;
        this.enabled = enabled;
        this.backendBaseUrl = backendBaseUrl == null ? "" : backendBaseUrl.replaceAll("/+$", "");
        this.envId = envId == null ? "" : envId.trim();
        this.dataDir = Path.of(dataDir == null || dataDir.isBlank() ? "./data" : dataDir)
                .toAbsolutePath().normalize();
        this.imageRef = imageRef == null || imageRef.isBlank() ? IMAGE_REF : imageRef.trim();
        this.runAttempts = Math.max(1, runAttempts);
        this.runBackoff = runBackoff == null || runBackoff.isNegative() ? Duration.ZERO : runBackoff;
        this.drainMaxWait = drainMaxWait == null || drainMaxWait.isNegative()
                ? Duration.ofMinutes(30) : drainMaxWait;
        this.executor = newPipelineExecutor();
        // Opt-out / unenrolled hosts are usable immediately. A leftover build marker means a previous boot
        // already finished the image — restart must not refuse start-run while we re-verify.
        this.usable = !pipelineRequired() || readMarker(markerFile()) != null;
    }

    /**
     * Starts the boot ensure the moment every singleton exists (engine beans wired) and
     * BEFORE {@code ApplicationReadyEvent}. That overlaps download/build with Tomcat bind,
     * channel enrol, WireGuard and the ready-event listeners — the previous hook waited
     * for all of those to finish first.
     */
    @Override
    public void afterSingletonsInstantiated() {
        startBootEnsure();
        startUpdateChecks();
        startSupervisor();
    }

    private void startSupervisor() {
        if (!pipelineRequired() || superviseInterval.isZero()) {
            return;
        }
        supervisor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "builtin-lib-supervisor");
            t.setDaemon(true);
            return t;
        });
        superviseGap = superviseInterval;
        schedulePass(superviseInterval);
    }

    /** Replaces the pending pass with one {@code delay} from now. */
    private synchronized void schedulePass(Duration delay) {
        if (supervisor == null) {
            return;
        }
        if (nextPass != null) {
            nextPass.cancel(false);
        }
        try {
            nextPass = supervisor.schedule(this::supervisePass, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException shuttingDown) {
            nextPass = null;
        }
    }

    /** One pass, then the next one: twice the gap after a healthy pass, the shortest gap after a heal. */
    private void supervisePass() {
        boolean healthy = supervise();
        synchronized (this) {
            superviseGap = healthy
                    ? (superviseGap.multipliedBy(2).compareTo(SUPERVISE_MAX_INTERVAL) > 0
                            ? SUPERVISE_MAX_INTERVAL : superviseGap.multipliedBy(2))
                    : superviseInterval;
            schedulePass(superviseGap);
        }
    }

    /**
     * One supervision pass — the library is the engine's own executor. Skipped while a pipeline run
     * owns the instance.
     */
    boolean supervise() {
        if (!pipelineRequired() || !usable || pipelineRunning.get()) {
            return true;
        }
        try {
            if (!cli.available() || cli.cliPath() == null || cli.cliPath().isBlank()) {
                return false;
            }
            if (!imageExists()) {
                log.warn("builtin-lib: image {} is gone from the host — rebuilding it", imageRef);
                startBootEnsure();
                return false;
            }
            return ensureStarted() && !replaceIfNetworkLost();
        } catch (Exception e) {
            log.warn("builtin-lib: supervision pass failed: {}", e.toString());
            return false;
        }
    }

    /**
     * Polls the staged manifest once the library is usable. A matching sha256 returns without touching
     * {@link #pipelineRunning}.
     */
    private void startUpdateChecks() {
        if (!pipelineRequired() || updateCheckInterval.isZero()) {
            return;
        }
        updateChecks = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "builtin-lib-update-check");
            t.setDaemon(true);
            return t;
        });
        long delayMs = updateCheckInterval.toMillis();
        updateChecks.scheduleWithFixedDelay(this::checkForRelease, delayMs, delayMs, TimeUnit.MILLISECONDS);
        log.info("builtin-lib: automatic update check every {}s once the library is usable",
                updateCheckInterval.toSeconds());
    }

    /**
     * Package-visible so a test can run it without waiting out the interval. A manifest that cannot be
     * read is logged and left for the next interval — it does not mark the library
     * {@link #STATE_FAILED} or hide the Update button's retry.
     */
    void checkForRelease() {
        if (!pipelineRequired() || !usable || pipelineRunning.get()) {
            return;
        }
        Manifest manifest = fetchManifestQuiet();
        if (manifest == null) {
            return;
        }
        String built = readMarker(markerFile());
        if (built == null || built.isBlank() || manifest.sha256().equals(built)) {
            return;
        }
        log.info("builtin-lib: staged release sha256 changed ({} → {}) — automatic update",
                abbrev(built), abbrev(manifest.sha256()));
        UpdateAck ack = updateNow();
        if (!ack.accepted()) {
            log.info("builtin-lib: automatic update not started: {}", ack.reason());
        }
    }

    /** Package-visible so tests can drive the same submit path the boot hook uses. */
    void startBootEnsure() {
        if (!pipelineRequired()) {
            usable = true;
            return;
        }
        try {
            executor.execute(this::runSafely);
        } catch (RejectedExecutionException rejected) {
            log.warn("builtin-lib: boot ensure rejected — executor shutting down");
            updateError = "bootstrap executor rejected the boot ensure";
        }
    }

    /**
     * Operator-triggered update (BE {@code update-node-library} command): runs the pipeline ONCE on a
     * dedicated thread and answers immediately — the build leg routinely takes 5–15 min, far beyond
     * any command-lane budget. Progress and failure are observable through {@link #snapshot} on the
     * next status poll.
     */
    public UpdateAck updateNow() {
        if (!enabled) {
            return UpdateAck.reject("builtin node library is disabled (sqs.builtin-library.enabled=false)");
        }
        if (envId.isBlank() || backendBaseUrl.isBlank()) {
            return UpdateAck.reject("daemon is not enrolled yet (envId / backend base-url missing)");
        }
        if (releaseHttp == null) {
            return UpdateAck.reject("secure channel HTTP clients unavailable");
        }
        if (!pipelineRunning.compareAndSet(false, true)) {
            return UpdateAck.reject("an update is already in progress");
        }
        updateError = null;
        try {
            executor.execute(() -> {
                try {
                    if (!run()) {
                        // run() already logged + recorded the specific reason.
                        updateError = lastFailureOr("update failed");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    log.warn("builtin-lib: on-demand update interrupted");
                } catch (Exception e) {
                    log.warn("builtin-lib: on-demand update failed: {}", e.toString());
                    updateError = e.getMessage() == null ? e.toString() : e.getMessage();
                } finally {
                    updateStage = null;
                    pipelineRunning.set(false);
                }
            });
        } catch (RejectedExecutionException rejected) {
            pipelineRunning.set(false);
            return UpdateAck.reject("update executor is shutting down");
        }
        return UpdateAck.accept();
    }

    /**
     * The {@code nodeLibrary} block of the {@code status} command result, or {@code null} when
     * the library is disabled (the block is then omitted rather than reporting a phantom).
     * Reads are volatile/one-file lazy — safe on the every-few-seconds status poll.
     */
    @org.springframework.lang.Nullable
    public NodeLibrarySnapshot snapshot() {
        if (!enabled) {
            return null;
        }
        boolean running = pipelineRunning.get();
        String state = running ? STATE_UPDATING : (updateError != null ? STATE_FAILED : STATE_IDLE);
        return new NodeLibrarySnapshot(imageRef, builtSha(), state,
                running ? updateStage : null, updateError, updatedAt(), usable);
    }

    /**
     * True when {@code start-run} may proceed. Cheap volatile read — safe on the command thread.
     */
    public boolean isUsable() {
        return usable;
    }

    /**
     * Operator-facing start-run refusal, or {@code null} when the run may start.
     * Never blocks: a minutes-long build must not hold the command lane.
     */
    @org.springframework.lang.Nullable
    public String startRunBlockReason() {
        if (!enabled || usable) {
            return null;
        }
        String stage = updateStage;
        if (updateError != null && !pipelineRunning.get()) {
            return "built-in node library failed to become ready: " + updateError;
        }
        String detail = updateError != null && STAGE_WAITING.equals(stage)
                ? " (last attempt: " + updateError + "; retrying automatically)"
                : (stage != null ? " (" + stage.toLowerCase(Locale.ROOT) + ")" : "");
        return "built-in node library is not ready yet" + detail
                + " — the engine keeps building it; retry the run in a few minutes";
    }

    /**
     * Visible for testing — the boot pipeline with retries, never throws and (in production) never
     * gives up. The system executor is what every built-in node dispatches into; a boot that gave up
     * after three minutes because the overlay handshake was still failing left the engine without it
     * until a manual restart.
     */
    void runSafely() {
        if (!pipelineRunning.compareAndSet(false, true)) {
            // Practically impossible at boot (the channel comes up after ready), but the guard
            // makes boot vs on-demand ordering irrelevant: the in-flight run IS the ensure.
            log.info("builtin-lib: boot ensure skipped — a pipeline run is already in progress");
            return;
        }
        try {
            for (int attempt = 1; ; attempt++) {
                try {
                    if (run()) {
                        return;
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    log.warn("builtin-lib: bootstrap interrupted; the next daemon boot retries");
                    return;
                } catch (Exception e) {
                    fail(e.getMessage() == null ? e.toString() : e.getMessage());
                    log.warn("builtin-lib: bootstrap attempt {} failed: {}", attempt, e.toString());
                }
                boolean fastPhase = attempt < runAttempts;
                if (!fastPhase && !retryForever) {
                    if (runAttempts > 1) {
                        log.warn("builtin-lib: bootstrap gave up after {} attempts; the next daemon boot "
                                + "retries (or Engine Settings → Stateful Instances → Update)", runAttempts);
                    }
                    // Surface the boot failure on the status channel: the Update button is the retry path.
                    updateError = lastFailureOr("bootstrap failed");
                    return;
                }
                Duration backoff = fastPhase ? runBackoff : SLOW_RETRY_BACKOFF;
                if (!fastPhase && attempt == runAttempts) {
                    log.warn("builtin-lib: {} fast attempts failed ({}) — switching to the slow phase: one "
                            + "attempt every {} min for as long as the daemon runs, sooner when the overlay "
                            + "comes up. No restart needed.", runAttempts, lastFailureOr("unknown"),
                            SLOW_RETRY_BACKOFF.toMinutes());
                }
                updateStage = STAGE_WAITING;
                if (!awaitRetry(backoff)) {
                    return; // interrupted (shutdown)
                }
            }
        } finally {
            updateStage = null;
            pipelineRunning.set(false);
        }
    }

    /** Sleeps {@code backoff} or until {@link #retryNow()} fires; {@code false} when interrupted. */
    private boolean awaitRetry(Duration backoff) {
        long deadline = System.nanoTime() + backoff.toNanos();
        synchronized (retryWake) {
            try {
                while (!retryRequested) {
                    long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                    if (remainingMs <= 0) {
                        break;
                    }
                    retryWake.wait(remainingMs);
                }
                retryRequested = false;
                return true;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * Asks for an immediate attempt: wakes a boot loop waiting between attempts, or — when no pipeline
     * is running and the library is not usable (e.g. an on-demand update failed) — starts a fresh boot
     * ensure. Called when overlay egress comes up, since the egress gate is the usual reason an early
     * attempt failed.
     */
    public void retryNow() {
        if (!pipelineRequired()) {
            return;
        }
        if (usable) {
            schedulePass(Duration.ZERO);
            return;
        }
        if (pipelineRunning.get()) {
            synchronized (retryWake) {
                retryRequested = true;
                retryWake.notifyAll();
            }
            log.info("builtin-lib: retry requested (overlay egress up) — attempting now");
            return;
        }
        log.info("builtin-lib: library not usable and no pipeline running — starting the boot ensure");
        startBootEnsure();
    }

    /**
     * Used by {@code start-run} so a run that arrives seconds before the first build finishes is
     * started, not refused. Bounded by the caller (the serial command lane must not stall deploys for
     * a multi-minute build).
     */
    public boolean awaitUsable(Duration max) {
        long deadline = System.nanoTime() + max.toNanos();
        while (!usable && System.nanoTime() < deadline) {
            try {
                Thread.sleep(250L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return usable;
            }
        }
        return usable;
    }

    /**
     * @return {@code true} when the instance is ensured or the pipeline is.
     */
    private boolean run() throws Exception {
        long startedAt = System.nanoTime();
        if (!enabled) {
            markUsable();
            log.info("builtin-lib: disabled (sqs.builtin-library.enabled=false) — no system executor");
            return true;
        }
        if (envId.isBlank() || backendBaseUrl.isBlank()) {
            // Same precondition the channel has: without enrolment config there is no env to
            // register the instance under and nowhere to download from. Idle, not an error.
            markUsable();
            log.warn("builtin-lib: envId/backend base-url not configured — system executor idle this boot");
            return true;
        }
        String httpsRefusal = com.studioquack.sqs.channel.config.HttpsEndpointPolicy.refusal(backendBaseUrl);
        if (httpsRefusal != null) {
            // Config error: do not download and do not retry. Startup also refuses this URL.
            fail(httpsRefusal);
            log.error("builtin-lib: NOT downloading releases — {}", httpsRefusal);
            return true;
        }
        if (releaseHttp == null) {
            // Slice tests / channel-less boots: no TLS posture to fetch releases with.
            markUsable();
            log.warn("builtin-lib: secure channel HTTP clients unavailable — system executor idle this boot");
            return true;
        }
        updateStage = STAGE_CHECKING;
        Manifest manifest = fetchManifestWithRetry();
        if (manifest == null) {
            return false; // reason recorded inside
        }

        Path markerFile = markerFile();
        String builtShaAtStart = readMarker(markerFile);
        boolean markerMatches = manifest.sha256().equals(builtShaAtStart);

        // Fresh install / changed sha: start the tarball GET on the second pool slot so it
        // overlaps awaitPodman() (late-starting machine after install, up to 10 min).
        Future<Path> download = null;
        if (!markerMatches) {
            updateStage = STAGE_DOWNLOADING;
            download = submitDownload(manifest);
        }

        if (!awaitPodman()) {
            cancelQuietly(download);
            fail("container runtime not available after " + PODMAN_WAIT);
            return false;
        }

        if (imageExists() && markerMatches) {
            markUsable();
            log.info("builtin-lib: image {} already built for manifest sha256={} — ensuring instance",
                    imageRef, abbrev(manifest.sha256()));
            ensureStarted();
            updateError = null;
            log.info("builtin-lib: already-current path finished in {}ms", elapsedMs(startedAt));
            return true;
        }

        updateStage = STAGE_DOWNLOADING;
        Path tarball = download != null ? takeDownload(download) : downloadWithRetry(manifest);
        if (tarball == null) {
            return false; // reason recorded inside
        }
        verifySha256(tarball, manifest.sha256());

        // Reuse a complete extract from a previous attempt that failed at BUILDING —
        // delete+untar is wasted work when the sha256-verified tree is already on disk.
        if (contextReusable(manifest.sha256())) {
            log.info("builtin-lib: reusing extracted build context (sha256={})",
                    abbrev(manifest.sha256()));
        } else {
            contextStore.deleteContext(BUILD_ID);
            Path contextDir = contextStore.contextDir(BUILD_ID);
            Files.createDirectories(contextDir);
            untar(tarball, contextDir);
            sanityCheckContext(contextDir);
            writeExtractedSha(manifest.sha256());
        }

        updateStage = STAGE_BUILDING;
        log.info("builtin-lib: building {} from {} (this runs maven inside the image — several "
                + "minutes on a fresh host)", imageRef, tarball.getFileName());
        OciImageBuildService.SyncBuildResult build =
                buildService.buildSync(envId, BUILD_ID, imageRef, BUILD_WALL_CLOCK_SECONDS);
        if (!build.success()) {
            // podman reports only "RUN mvn … exit status 1"; the reason (dependency
            // download, compile error, OOM) is in the build output and was dropped.
            log.warn("builtin-lib: image build output (last {} lines):\n{}",
                    BUILD_LOG_TAIL_LINES, tailLines(build.log(), BUILD_LOG_TAIL_LINES, BUILD_LOG_TAIL_CHARS));
            fail("image build failed: " + build.error());
            return false;
        }
        writeMarker(markerFile, manifest.sha256());
        lastBuildAt = Instant.now();
        markUsable();
        // The verified tarball stays cached for rebuilds; the extracted tree is dead weight.
        contextStore.deleteContext(BUILD_ID);
        log.info("builtin-lib: image build ready in {}ms", elapsedMs(startedAt));

        // Upgrade path: start the successor beside the old instance; routing switches only
        // after the new container is healthy, then in-flight work drains on the old one.
        if (builtShaAtStart != null && !builtShaAtStart.isBlank()) {
            updateStage = STAGE_RECYCLING;
            log.info("builtin-lib: manifest sha256 changed ({} → {}) — drained replacement, not stop-first",
                    abbrev(builtShaAtStart), abbrev(manifest.sha256()));
            var result = statefulInstanceManager.replaceWithCurrentImage(
                    INSTANCE_ID, drainMaxWait,
                    phase -> {
                        if ("DRAINING".equals(phase)) {
                            updateStage = STAGE_DRAINING;
                        }
                    });
            if (!result.replaced()) {
                if ("not running".equals(result.reason())) {
                    ensureStarted();
                } else {
                    fail("new node-library instance did not become healthy: " + result.reason()
                            + " — the previous instance keeps serving");
                    return false; // old keeps serving; FE shows FAILED + Update as retry
                }
            }
            // replaced=true: successor is already routed; do not call ensureStarted()
        } else {
            ensureStarted(); // first build, no previous instance
        }
        updateError = null;
        return true;
    }

    /* ------------------------------------------------------------------ */
    /* stages                                                              */
    /* ------------------------------------------------------------------ */

    /** Polls for the runtime; a late-starting Podman machine is normal right after install. */
    private boolean awaitPodman() throws InterruptedException {
        long deadline = System.nanoTime() + PODMAN_WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            if (cli.available() && cli.cliPath() != null && !cli.cliPath().isBlank()) {
                return true;
            }
            Thread.sleep(PODMAN_POLL.toMillis());
        }
        return false;
    }

    private boolean imageExists() {
        OciCli.Result r = cli.run(List.of(cli.cliPath(), "image", "exists", imageRef),
                Duration.ofSeconds(30));
        return r.ok();
    }

    /**
     * Starts (or joins, when the container outlived a daemon restart) the system instance. The
     * singleton reconcile runs first: at boot the host reconcile has not adopted the survivor yet.
     *
     * @return {@code true} when the routed container was already running and nothing.
     */
    private boolean ensureStarted() {
        int removed = statefulInstanceManager.reconcilePlatformSingleton(envId, imageRef, INSTANCE_ID);
        String routedBefore = statefulInstanceManager.resolveRunning(envId, imageRef)
                .map(StatefulInstance::containerId).orElse(null);
        // No resource limits: the library is the engine's own runtime (StatefulInstance#isBuiltinNodeLibrary).
        StatefulInstanceManager.StartResult result = statefulInstanceManager.start(
                envId, imageRef, /* stateful= */ true, StatefulCaps.uncapped(),
                INSTANCE_ID, null, null, (java.util.Map<String, String>) null);
        if (!StatefulStatus.RUNNING.equals(result.status())) {
            log.warn("builtin-lib: system instance start returned status={} ({}) — the engine retries in "
                    + "{}s and as soon as overlay egress comes up", result.status(),
                    result.message() == null ? "no detail" : result.message(), superviseInterval.toSeconds());
            return false;
        }
        boolean joined = routedBefore != null && routedBefore.equals(result.containerId());
        if (!joined) {
            log.info("builtin-lib: system executor RUNNING instance={} container={} image={} (no resource "
                    + "limits, platform-managed)", result.instanceId(), result.containerId(), imageRef);
        }
        return joined && removed == 0;
    }

    /**
     * A library restarted in place under gVisor keeps serving dispatches (they arrive over
     * {@code podman exec}) while every outbound call fails. Replace it with the same drained hand-over
     * an update uses; holding {@link #pipelineRunning} keeps an update from racing it.
     *
     * @return {@code true} when a replacement was attempted
     */
    private boolean replaceIfNetworkLost() {
        StatefulInstance inst = statefulInstanceManager.resolveRunning(envId, imageRef).orElse(null);
        if (inst == null || !statefulInstanceManager.sandboxNetworkLost(inst)
                || !pipelineRunning.compareAndSet(false, true)) {
            return false;
        }
        try {
            updateStage = STAGE_RECYCLING;
            log.warn("builtin-lib: container {} has no network besides loopback (restarted in place under "
                    + "gVisor) — drained replacement", inst.containerId());
            var result = statefulInstanceManager.replaceWithCurrentImage(INSTANCE_ID, drainMaxWait, phase -> {
                if ("DRAINING".equals(phase)) {
                    updateStage = STAGE_DRAINING;
                }
            });
            if (!result.replaced()) {
                log.warn("builtin-lib: network recovery replacement did not complete: {} — retrying on the "
                        + "next supervision pass", result.reason());
            }
        } finally {
            pipelineRunning.set(false);
        }
        return true;
    }

    /**
     * One manifest read for the automatic check. Same release client and signature
     * verifier as {@link #fetchManifestWithRetry()}, but a miss does not call {@link #fail}
     * — the running executor stays {@link #STATE_IDLE} and the next interval tries again.
     */
    private Manifest fetchManifestQuiet() {
        String url = backendBaseUrl + RELEASE_PATH + "release-manifest.json";
        try {
            URI manifestUri = URI.create(url);
            HttpResponse<byte[]> resp = releaseHttp.client().send(
                    releaseHttp.get(manifestUri, HTTP_READ).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() != 200) {
                log.info("builtin-lib: automatic update check: manifest GET {} → HTTP {}",
                        url, resp.statusCode());
                return null;
            }
            HttpResponse<byte[]> sigResp = releaseHttp.client().send(
                    releaseHttp.get(URI.create(url + ".sig.v2"), HTTP_READ).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (sigResp.statusCode() != 200 && sigResp.statusCode() != 404) {
                log.info("builtin-lib: automatic update check: manifest signature GET {}.sig.v2 → HTTP {}",
                        url, sigResp.statusCode());
                return null;
            }
            byte[] signature = sigResp.statusCode() == 200 ? sigResp.body() : null;
            try {
                releaseSignatures.verify("node-library manifest", resp.body(), signature);
            } catch (ReleaseSignatureException e) {
                log.error("builtin-lib: automatic update check refused: {}", e.getMessage());
                return null;
            }
            Manifest parsed = manifestFields(resp.body());
            if (parsed == null) {
                log.info("builtin-lib: automatic update check: manifest at {} missing/invalid file+sha256", url);
            }
            return parsed;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e) {
            log.info("builtin-lib: automatic update check skipped: {}", e.toString());
            return null;
        }
    }

    private Manifest fetchManifestWithRetry() throws InterruptedException {
        String url = backendBaseUrl + RELEASE_PATH + "release-manifest.json";
        for (int attempt = 1; attempt <= FETCH_ATTEMPTS; attempt++) {
            try {
                URI manifestUri = URI.create(url);
                HttpResponse<byte[]> resp = releaseHttp.client().send(
                        releaseHttp.get(manifestUri, HTTP_READ).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                HttpResponse<byte[]> sigResp = resp.statusCode() != 200 ? null : releaseHttp.client().send(
                        releaseHttp.get(URI.create(url + ".sig.v2"), HTTP_READ).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                if (resp.statusCode() != 200) {
                    fail("manifest GET " + url + " → HTTP " + resp.statusCode());
                    log.warn("builtin-lib: manifest GET {} → HTTP {} (attempt {}/{})", url,
                            resp.statusCode(), attempt, FETCH_ATTEMPTS);
                } else if (sigResp.statusCode() != 200 && sigResp.statusCode() != 404) {
                    fail("manifest signature GET " + url + ".sig.v2 → HTTP " + sigResp.statusCode());
                    log.warn("builtin-lib: manifest signature GET {}.sig.v2 → HTTP {} (attempt {}/{})", url,
                            sigResp.statusCode(), attempt, FETCH_ATTEMPTS);
                } else {
                    try {
                        releaseSignatures.verify("node-library manifest", resp.body(),
                                sigResp.statusCode() == 200 ? sigResp.body() : null);
                    } catch (ReleaseSignatureException e) {
                        fail(e.getMessage());
                        log.error("builtin-lib: {} — refusing to download or build", e.getMessage());
                        return null;
                    }
                    Manifest manifest = manifestFields(resp.body());
                    if (manifest == null) {
                        fail("manifest at " + url + " missing/invalid file+sha256");
                        log.warn("builtin-lib: manifest at {} missing/invalid file+sha256", url);
                        return null;
                    }
                    return manifest;
                }
            } catch (IOException e) {
                fail("manifest GET failed: " + e);
                log.warn("builtin-lib: manifest GET {} failed: {} (attempt {}/{})", url,
                        e.toString(), attempt, FETCH_ATTEMPTS);
            }
            if (attempt < FETCH_ATTEMPTS) {
                Thread.sleep(FETCH_BACKOFF.toMillis());
            }
        }
        return null;
    }

    /** Manifest fields after the signature check. Null when {@code file} or {@code sha256} is unusable. */
    private Manifest manifestFields(byte[] body) throws IOException {
        JsonNode node = json.readTree(body);
        String file = text(node, "file");
        String sha = text(node, "sha256");
        if (file == null || sha == null || file.contains("/") || file.contains("\\")) {
            return null;
        }
        return new Manifest(file, sha.toLowerCase(Locale.ROOT));
    }

    /**
     * Downloads the tarball to the cache dir, REUSING a previously fetched copy whose sha256
     * already matches the manifest (a run that failed at the build stage must not re-download).
     */
    private Path downloadWithRetry(Manifest manifest) throws Exception {
        Path dir = dataDir.resolve("node-library");
        Files.createDirectories(dir);
        Path target = dir.resolve(manifest.file());
        if (Files.isRegularFile(target) && manifest.sha256().equals(sha256Hex(target))) {
            log.info("builtin-lib: reusing cached {} (sha256 matches manifest)", target.getFileName());
            return target;
        }
        String url = backendBaseUrl + RELEASE_PATH + manifest.file();
        for (int attempt = 1; attempt <= FETCH_ATTEMPTS; attempt++) {
            Path tmp = target.resolveSibling(target.getFileName() + ".part");
            try {
                HttpResponse<InputStream> resp = releaseHttp.client().send(
                        releaseHttp.get(URI.create(url), DOWNLOAD_READ).build(),
                        HttpResponse.BodyHandlers.ofInputStream());
                if (resp.statusCode() != 200) {
                    fail("tarball GET " + url + " → HTTP " + resp.statusCode());
                    log.warn("builtin-lib: tarball GET {} → HTTP {} (attempt {}/{})", url,
                            resp.statusCode(), attempt, FETCH_ATTEMPTS);
                } else {
                    try (InputStream in = resp.body()) {
                        Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                    }
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                    return target;
                }
            } catch (IOException e) {
                fail("tarball GET failed: " + e);
                log.warn("builtin-lib: tarball GET {} failed: {} (attempt {}/{})", url,
                        e.toString(), attempt, FETCH_ATTEMPTS);
            } finally {
                Files.deleteIfExists(tmp);
            }
            if (attempt < FETCH_ATTEMPTS) {
                Thread.sleep(FETCH_BACKOFF.toMillis());
            }
        }
        return null;
    }

    /* ------------------------------------------------------------------ */
    /* helpers                                                             */
    /* ------------------------------------------------------------------ */

    /** Integrity gate: the bytes we build from are exactly what the release pipeline published. */
    private void verifySha256(Path file, String expectedSha256) throws Exception {
        String actual = sha256Hex(file);
        if (!expectedSha256.equals(actual)) {
            Files.deleteIfExists(file);
            throw new IllegalStateException("builtin-lib: sha256 mismatch for " + file.getFileName()
                    + " (expected " + abbrev(expectedSha256) + ", got " + abbrev(actual)
                    + ") — refusing to build; the cached copy was deleted");
        }
    }

    /**
     * No {@code -P}: absolute paths and {@code.} members stay refused by the default secure mode. The
     * input is sha256-verified against our own release manifest, so this is defence-in-depth, not the
     * integrity boundary.
     */
    private void untar(Path tarball, Path into) throws IOException, InterruptedException {
        Process p = new ProcessBuilder("tar", "-xzf",
                tarball.toAbsolutePath().toString(), "-C", into.toAbsolutePath().toString())
                .redirectErrorStream(true)
                .start();
        boolean done = p.waitFor(UNTAR_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        if (!done) {
            p.destroyForcibly();
            throw new IOException("tar extract timed out for " + tarball.getFileName());
        }
        if (p.exitValue() != 0) {
            String out = new String(p.getInputStream().readAllBytes()).trim();
            throw new IOException("tar extract failed (exit " + p.exitValue() + "): "
                    + (out.length() > 200 ? out.substring(0, 200) : out));
        }
    }

    /** The Containerfile build needs exactly these roots; fail loudly on a truncated archive. */
    private void sanityCheckContext(Path contextDir) throws IOException {
        for (String required : List.of("Containerfile", "pom.xml", "src", "helper-src", "nodes")) {
            if (!Files.exists(contextDir.resolve(required))) {
                throw new IOException("builtin-lib: extracted context missing " + required
                        + " — the release tarball does not match the Containerfile contract");
            }
        }
    }

    private Path markerFile() {
        return dataDir.resolve("node-library").resolve("built-sha256.txt");
    }

    /** Sidecar next to the extracted tree so a BUILDING-failed retry can skip re-untar. */
    private Path extractedShaFile() {
        return contextStore.contextDir(BUILD_ID).resolve(".sq-extracted-sha256");
    }

    private void writeExtractedSha(String sha256) throws IOException {
        Path file = extractedShaFile();
        Files.createDirectories(file.getParent());
        Files.writeString(file, sha256 + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }

    /** True when the staged context has every required root AND was extracted for this sha256. */
    private boolean contextReusable(String sha256) {
        if (sha256 == null || sha256.isBlank() || !contextStore.contextExists(BUILD_ID)) {
            return false;
        }
        Path dir = contextStore.contextDir(BUILD_ID);
        for (String required : List.of("Containerfile", "pom.xml", "src", "helper-src", "nodes")) {
            if (!Files.exists(dir.resolve(required))) {
                return false;
            }
        }
        return sha256.equals(readMarker(extractedShaFile()));
    }

    /** Enabled + enrolled + TLS clients present — the only case that actually builds. */
    private boolean pipelineRequired() {
        return enabled && !envId.isBlank() && !backendBaseUrl.isBlank() && releaseHttp != null;
    }

    private void markUsable() {
        usable = true;
    }

    private Future<Path> submitDownload(Manifest manifest) {
        return executor.submit(() -> downloadWithRetry(manifest));
    }

    private Path takeDownload(Future<Path> download) throws Exception {
        try {
            return download.get();
        } catch (InterruptedException interrupted) {
            download.cancel(true);
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new IllegalStateException(cause);
        }
    }

    private static void cancelQuietly(Future<?> future) {
        if (future != null) {
            future.cancel(true);
        }
    }

    private static long elapsedMs(long startedAtNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
    }

    private static ExecutorService newPipelineExecutor() {
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "builtin-node-library");
            t.setDaemon(true);
            return t;
        };
        return Executors.newFixedThreadPool(2, factory);
    }

    @PreDestroy
    void shutdown() {
        if (updateChecks != null) {
            updateChecks.shutdownNow();
        }
        if (supervisor != null) {
            supervisor.shutdownNow();
        }
        executor.shutdownNow();
    }

    private static String readMarker(Path markerFile) {
        try {
            return Files.isRegularFile(markerFile)
                    ? Files.readString(markerFile).trim().toLowerCase(java.util.Locale.ROOT) : null;
        } catch (IOException e) {
            return null;
        }
    }

    private void writeMarker(Path markerFile, String sha256) throws IOException {
        Files.createDirectories(markerFile.getParent());
        Files.writeString(markerFile, sha256 + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
        this.builtSha = sha256;
        this.builtShaLoaded = true;
    }

    /** Marker sha, lazily loaded once — only this class writes the marker file. */
    private String builtSha() {
        if (!builtShaLoaded) {
            builtSha = readMarker(markerFile());
            builtShaLoaded = true;
        }
        return builtSha;
    }

    /** Last successful build time; falls back to the marker file's mtime after a restart. */
    private String updatedAt() {
        if (lastBuildAt != null) {
            return lastBuildAt.toString();
        }
        try {
            Path marker = markerFile();
            if (Files.isRegularFile(marker)) {
                return Files.getLastModifiedTime(marker).toInstant().toString();
            }
        } catch (IOException ignored) {
            // cosmetic field — absence is honest
        }
        return null;
    }

    /** Records the pipeline failure reason for {@link #snapshot()} and returns it. */
    /** Last {@code maxLines} lines of {@code log}, capped at {@code maxChars} from the end. */
    static String tailLines(String log, int maxLines, int maxChars) {
        if (log == null || log.isBlank()) {
            return "(no build output captured)";
        }
        String s = log.length() > maxChars ? log.substring(log.length() - maxChars) : log;
        String[] lines = s.split("\\R");
        int from = Math.max(0, lines.length - maxLines);
        return String.join("\n", java.util.Arrays.copyOfRange(lines, from, lines.length));
    }

    private String fail(String reason) {
        updateError = reason;
        return reason;
    }

    /** The last recorded failure, or {@code fallback} when none was recorded. */
    private String lastFailureOr(String fallback) {
        return updateError != null ? updateError : fallback;
    }

    private static String sha256Hex(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buf = new byte[64 * 1024];
            for (int n; (n = in.read(buf)) > 0; ) {
                digest.update(buf, 0, n);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return v != null && v.isTextual() && !v.asText().isBlank() ? v.asText().trim() : null;
    }

    private static String abbrev(String sha) {
        return sha == null ? "none" : sha.substring(0, Math.min(12, sha.length()));
    }

    /** The manifest fields the pipeline consumes (version is informational today). */
    record Manifest(String file, String sha256) {
    }

    /**
     * Synchronous answer to an {@code update-node-library} command: {@code accepted} means the
     * pipeline is now running in the background; a rejection carries the operator-facing
     * {@code reason} (disabled / unenrolled / already in progress).
     */
    public record UpdateAck(boolean accepted, String reason) {
        public static UpdateAck accept() {
            return new UpdateAck(true, null);
        }

        public static UpdateAck reject(String reason) {
            return new UpdateAck(false, reason);
        }
    }

    /**
     * Wire shape of the {@code nodeLibrary} block in the {@code status} command result — consumed by
     * the control plane stateful-instance list merge and rendered on Engine Settings → Stateful
     * Instances. {@code updateStage} is non-null only while {@code updateState} is {@code UPDATING};
     * {@code updateError} is non-null only after a failed run.
     */
    public record NodeLibrarySnapshot(String imageRef, String builtSha256, String updateState,
                                      String updateStage, String updateError, String updatedAt,
                                      boolean usable) {
    }
}
