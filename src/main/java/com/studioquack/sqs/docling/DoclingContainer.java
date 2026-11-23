package com.studioquack.sqs.docling;

import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager.StartResult;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs the Docling document-conversion side-car as a first-class STATEFUL Podman instance, through
 * the same {@link StatefulInstanceManager} control plane a user-started stateful instance uses —
 * not a second, ad-hoc container lifecycle. This gives Docling a deterministic, recognizable name
 * ({@link #INSTANCE_ID}, so the container is {@code sq-stateful-docling}), the standard
 * {@code studioquack.sqs.stateful=1} provenance labels the daemon's container telemetry.
 */
@Slf4j
@Component
public class DoclingContainer {

    /**
     * Deterministic instance id AND container-name suffix — the manager names the container
     * {@code sq-stateful-docling}, not a random {@code sq-stateful-inst_<uuid>}, so it is
     * recognizable in {@code podman ps} and the stateful-instance inventory.
     */
    static final String INSTANCE_ID = "docling";

    /**
     * Image-family marker for the stray sweep: every Docling side-car image we ever ran — the registry
     * {@code docling-serve-*} variants AND the ASR-derived {@code localhost/sq-docling-serve-asr:*} —
     * contains this substring. Matched as a substring (the image is registry-qualified), mirroring the
     * backend's platform-instance classifier.
     */
    static final String IMAGE_FAMILY_MARKER = "docling-serve";

    /**
     * Side-car env for OCR/pipeline worker threads ({@code DOCLING_NUM_THREADS}).
     * WHY set explicitly: matches docling-serve's default and the installer Podman machine 4 CPU,
     * and keeps the knob visible next to the worker count rather than relying on an implicit default.
     */
    static final String NUM_THREADS_ENV = "DOCLING_NUM_THREADS";

    /**
     * Side-car env for docling-serve's LOCAL-engine conversion workers (
     * {@code DOCLING_SERVE_ENG_LOC_NUM_WORKERS}, upstream default 2). This is the CONCURRENT
     * conversion count — distinct from {@link #NUM_THREADS_ENV}, which is OCR threads inside ONE
     * conversion.
     */
    static final String WORKERS_ENV = "DOCLING_SERVE_ENG_LOC_NUM_WORKERS";

    /**
     * WHY cpu: the Linux Podman container cannot use {@code mps} (Metal/MPS is a macOS-host process
     * only); {@code cuda} needs NVIDIA hardware plus a {@code docling-serve-cu*} image. Explicit
     * {@code cpu} avoids auto-probing devices that are useless inside the AppleHV Linux VM.
     */
    static final String DEVICE_ENV = "DOCLING_DEVICE";

    /**
     * Side-car env that makes docling-serve return the RAW task error instead of its sanitised
     * "Internal processing error." placeholder ({@code DOCLING_SERVE_DEBUG_ERROR_DETAILS}). WHY on:
     * the side-car is single-tenant — loopback-only locally, and cross-env callers pass the same-owner
     * mesh grant plus the inbound NAC rule.
     */
    static final String DEBUG_ERROR_DETAILS_ENV = "DOCLING_SERVE_DEBUG_ERROR_DETAILS";

    /**
     * Fixed, single-tenant label. Each daemon serves exactly one environment, so there is
     * exactly one Docling instance per daemon; the value only matters as inventory provenance
     * ({@code studioquack.sqs.env=} label), never as an isolation boundary.
     */
    private static final String ENV_ID = "local";

    /** Serialises reconcile/ensureRunning work off whichever thread called us (see class javadoc). */
    private final ExecutorService reconcileExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "docling-reconcile");
        t.setDaemon(true);
        return t;
    });

    /** Serialises ensureRunning so two concurrent Docling dispatches don't both try to start it. */
    private final ReentrantLock lock = new ReentrantLock();

    /** How often a long readiness wait reports progress, so it is never silent for minutes. */
    private static final Duration PROGRESS_LOG_INTERVAL = Duration.ofSeconds(30);

    /** How long to wait for the published port to stop accepting before starting a new instance. */
    private static final Duration PORT_RELEASE_TIMEOUT = Duration.ofSeconds(15);

    private final StatefulInstanceManager statefulInstanceManager;
    private final String image;
    private final int hostPort;
    private final int containerPort;
    private final Duration readyTimeout;
    /** How long a second caller waits for an in-flight start before failing fast (never queues behind it). */
    private final Duration peerStartGrace;
    /** Unreachable for this long with the container up ⇒ recreate it once rather than wait out the budget. */
    private final Duration recreateAfter;
    private final boolean autoStart;
    /** Resource caps for the Docling side-car (OCR model load needs GiB, not the 512 MiB sandbox default). */
    private final StatefulCaps caps;
    /**
     * {@code sqs.sandbox.max-ram-mb} used when {@code sqs.docling.ram-mb} is unset. Field-injected so
     * slice tests keep the existing constructor; 0 derives the host ceiling.
     */
    @org.springframework.beans.factory.annotation.Value("${sqs.sandbox.max-ram-mb:0}")
    private int configuredMaxRamMb;
    /**
     * Worker threads stamped into {@link #NUM_THREADS_ENV} on start
     * ({@code sqs.docling.num-threads}, default 4).
     */
    private final int numThreads;
    /**
     * Concurrent conversions stamped into {@link #WORKERS_ENV} on start
     * ({@code sqs.docling.workers}, default 4) — docling-serve's upstream default of 2 is what
     * serialized the third parallel Docling node behind the first two.
     */
    private final int workers;
    /**
     * Accelerator device stamped into {@link #DEVICE_ENV} on start
     * ({@code sqs.docling.device}, default auto: {@code cuda} with the GPU image, else {@code cpu}).
     */
    private final String device;
    /**
     * GPU count requested on the {@code podman run} (rides {@link StatefulCaps#gpu} → the shared
     * {@code — gpus} flag). Auto-resolved to 1 when the platform resolver picked the CUDA image;
     * explicit {@code sqs.docling.gpus} always wins.
     */
    private final int gpus;
    /**
     * Dedicated wall-clock budget for the one-time image pull (
     * {@code sqs.docling.pull-timeout-seconds}, default 1800). The image is ~4.4 GB; the generic 120s
     * lifecycle timeout killed the inline {@code podman run} pull at ~121s on every slow-link host
     * (exit=-1, looping per reconcile) — the "Docling never comes up" defect.
     */
    private final Duration pullTimeout;
    private final String healthUrl;
    /**
     * ASR layer builder — derives the whisper/ffmpeg image the side-car actually starts from.
     * Null in tests / minimal assemblies: the base image is then started as-is and audio inputs
     * fail with Docling's own "whisper is not installed" message.
     */
    private final DoclingAsrImageBuilder asrImageBuilder;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3)).build();

    private volatile boolean ready;
    /** The manager-assigned id from our last successful start, so {@link #stop()} targets it exactly. */
    private volatile String trackedInstanceId;
    /** Boot-stable desired image cache — see {@link #desiredRuntimeImage()}. */
    private volatile String desiredRuntimeImage;

    public DoclingContainer(
            StatefulInstanceManager statefulInstanceManager,
            // Blank default = AUTO platform resolution (DoclingImageResolver): CPU multi-arch image
            // everywhere, CUDA image only when the runtime probe confirms GPU passthrough. An
            // explicit value is operator intent and is used verbatim.
            @Value("${sqs.docling.image:}") String image,
            @Value("${sqs.docling.port:5001}") int hostPort,
            @Value("${sqs.docling.container-port:5001}") int containerPort,
            @Value("${sqs.docling.start-timeout-seconds:600}") long readyTimeoutSeconds,
            @Value("${sqs.docling.peer-start-grace-seconds:90}") long peerStartGraceSeconds,
            @Value("${sqs.docling.recreate-after-seconds:60}") long recreateAfterSeconds,
            @Value("${sqs.docling.auto-start:true}") boolean autoStart,
            // RAM 0 means "not explicitly configured": uses the host ceiling (same cap node-declared RAM is
            // clamped to) so the side-car is not memory-unbounded. A positive value is the operator cap.
            @Value("${sqs.docling.ram-mb:0}") int ramMb,
            @Value("${sqs.docling.cpus:0}") int cpus,
            @Value("${sqs.docling.disk-mb:0}") int diskMb,
            // WHY 4: matches docling-serve DOCLING_NUM_THREADS default and the installer Podman
            // machine 4 CPU; set explicitly so the knob is visible next to the worker count.
            // Constructor order: numThreads, workers, device, gpus, then pullTimeoutSeconds.
            @Value("${sqs.docling.num-threads:4}") int numThreads,
            // WHY 4: upstream docling-serve defaults the local engine to 2 concurrent conversions,
            // which parked every third parallel Docling node in the task queue.
            @Value("${sqs.docling.workers:4}") int workers,
            // Blank = auto (cuda with the resolved GPU image, cpu otherwise). WHY not hardcoded cpu:
            // the GPU variant is useless without DOCLING_DEVICE=cuda, and cuda on the CPU image is
            // equally wrong — the device must follow the image, which is the resolver's job.
            @Value("${sqs.docling.device:}") String device,
            // -1 = auto (1 with the GPU image, else 0). >=0 is explicit operator intent.
            @Value("${sqs.docling.gpus:-1}") int gpus,
            // WHY 1800: the image is ~4.4 GB; a 10 Mbps link needs ~60 min in the worst case, but
            // 30 min covers the realistic slow-link envelope without parking a reconcile forever.
            @Value("${sqs.docling.pull-timeout-seconds:1800}") long pullTimeoutSeconds,
            @Value("${sqs.docling.url:http://127.0.0.1:5001}") String baseUrl,
            DoclingImageResolver platformResolver,
            // Trailing + nullable: slice tests construct the container without the ASR layer.
            @org.springframework.lang.Nullable DoclingAsrImageBuilder asrImageBuilder) {
        this.statefulInstanceManager = statefulInstanceManager;
        this.hostPort = hostPort;
        this.containerPort = containerPort;
        // Generous: the first call may include an image pull over a slow link, and failing that at two
        // minutes would make the feature look broken on exactly the run that was meant to enable it.
        this.readyTimeout = Duration.ofSeconds(readyTimeoutSeconds);
        this.peerStartGrace = Duration.ofSeconds(peerStartGraceSeconds);
        this.recreateAfter = Duration.ofSeconds(recreateAfterSeconds);
        this.autoStart = autoStart;
        // Negative RAM/CPU/disk normalise to 0. RAM 0 is resolved at start to the host ceiling.
        this.caps = new StatefulCaps(Math.max(0, cpus), Math.max(0, ramMb), Math.max(0, diskMb), null);
        this.numThreads = Math.max(1, numThreads);
        this.workers = Math.max(1, workers);
        this.pullTimeout = Duration.ofSeconds(Math.max(60, pullTimeoutSeconds));
        DoclingImageResolver.DoclingPlatform platform = platformResolver.resolve(image, device, gpus);
        this.image = platform.image();
        this.device = platform.device();
        this.gpus = platform.gpus();
        log.info("docling: platform resolved image={} device={} gpus={} ({})",
                this.image, this.device, this.gpus, platform.reason());
        this.healthUrl = baseUrl.replaceAll("/+$", "") + "/health";
        this.asrImageBuilder = asrImageBuilder;
    }

    /** Applied {@code DOCLING_DEVICE} (tests / diagnostics). */
    String device() {
        return device;
    }

    /** Resolved image ref actually started (tests / diagnostics). */
    String image() {
        return image;
    }

    /** Resolved GPU count requested on the run (tests / diagnostics). */
    int gpus() {
        return gpus;
    }

    /**
     * Ensures the side-car is reachable, starting it (as a stateful instance) if necessary. Blocking —
     * callers on the engine thread must not call this directly; {@link LocalDoclingBackend} only calls
     * it from the delegate's own off-engine dispatch pool.
     *
     * @throws IllegalStateException When it cannot be brought up — the caller turns this into a named
     * node failure rather than a silent pass-through
     */
    public void ensureRunning() {
        if (ready && healthyCurrentInstance()) {
            publishOverlayAccess();
            return;
        }
        if (!autoStart) {
            // An operator running Docling themselves (compose, systemd, a shared host). Do not fight
            // them by starting a second one; just require that theirs answers.
            if (!isHealthy()) {
                throw new IllegalStateException("Docling is not reachable at " + healthUrl
                        + " and sqs.docling.auto-start is false");
            }
            ready = true;
            return;
        }
        // A cold start can take minutes (image pull). Waiting for a peer's start to finish is fine;
        // QUEUEING BEHIND IT IS NOT.
        boolean acquired;
        try {
            acquired = lock.tryLock(peerStartGrace.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for Docling to start");
        }
        if (!acquired) {
            // Someone else owns the start and is still working. Say exactly that, so the node failure
            // reads as "not warm yet, retry" rather than a bogus timeout.
            if (isHealthy()) {
                ready = true;
                publishOverlayAccess();
                return;
            }
            throw new IllegalStateException("Docling is still starting (another conversion is waiting on the "
                    + "same cold start); it did not become reachable at " + healthUrl + " within " + peerStartGrace);
        }
        try {
            // One Docling per host is the invariant — reap previous installs' survivors before we
            // touch ours (a stray holding the port would otherwise fail or shadow the start).
            sweepStrayInstances();
            if (ready && isHealthy()) {
                publishOverlayAccess();
                return;
            }
            if (healthyCurrentInstance()) {
                ready = true;
                publishOverlayAccess();
                return;
            }
            start();
            awaitReady();
            ready = true;
            publishOverlayAccess();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Healthy AND running the image this build would start. A bare health check adopts whatever
     * answers on the port — including a previous install's side-car on a stale spec (e.g. the base
     * image without the ASR layer), which then survives every daemon upgrade forever.
     */
    private boolean healthyCurrentInstance() {
        if (!isHealthy()) {
            return false;
        }
        String running = statefulInstanceManager.byInstanceId(INSTANCE_ID)
                .map(StatefulInstance::imageRef).orElse(null);
        String desired = desiredRuntimeImage();
        if (running == null || running.equals(desired)) {
            return true;
        }
        log.info("docling: live side-car runs image {} but this build wants {} — recreating "
                + "(a daemon upgrade does not retrofit a running container)", running, desired);
        statefulInstanceManager.stopByContainerName(statefulInstanceManager.containerNameFor(INSTANCE_ID));
        trackedInstanceId = null;
        return false;
    }

    /**
     * The image this build would start, resolved once per process: the ASR-derived ref when the
     * layer image is in the local store (building it when absent — the same work {@link #start()}
     * would do), the base image otherwise. Boot-stable, so the per-dispatch drift check above
     * costs no subprocess after the first call.
     */
    private String desiredRuntimeImage() {
        String d = desiredRuntimeImage;
        if (d == null) {
            d = asrImageBuilder != null ? asrImageBuilder.resolveRuntimeImage(image) : image;
            desiredRuntimeImage = d;
        }
        return d;
    }

    /**
     * Applies the per-environment Docling entitlement: brings the stateful instance up when enabled,
     * tears it down when disabled. Runs on a dedicated background thread — the caller (
     * {@code ReloadConfigCommandHandler}) processes commands off the shared heartbeat channel, and a
     * cold image pull can take minutes, so reconciling synchronously there would stall.
     */
    public void reconcile(boolean enabled) {
        reconcileExecutor.submit(() -> {
            try {
                if (enabled) {
                    ensureRunning();
                } else {
                    stop();
                }
            } catch (Exception e) {
                log.warn("docling: reconcile(enabled={}) failed: {}", enabled, e.toString());
            }
        });
    }

    private void start() {
        // Never publish onto a port that is still accepting. The instance is always recreated on the SAME
        // host port, and on a stop/start cycle the host-side forward for the old container can outlive it.
        awaitPortReleased();
        ensureImagePresent();
        // ASR layer: start the derived whisper/ffmpeg image when available so voice/audio inputs
        // (DuckBot .webm notes) transcribe instead of failing with "whisper is not installed".
        // A skipped/failed build returns the base image unchanged.
        String runtimeImage = asrImageBuilder != null ? asrImageBuilder.resolveRuntimeImage(image) : image;
        int ram = caps.ramMb() > 0
                ? caps.ramMb()
                : com.studioquack.sqs.sandbox.oci.SandboxHostBudget.maxRamMb(configuredMaxRamMb);
        log.info("docling: starting stateful instance image={} hostPort={} caps=cpu:{} ramMb:{} gpus:{} "
                        + "(Apple Silicon Podman VMs need ≥8 GiB or the OCR preload thrash-locks)",
                runtimeImage, hostPort, caps.cpu(), ram, gpus);
        // Align the side-car's thread pool / worker count / device with daemon knobs.
        // The unused sync endpoint's DOCLING_SERVE_MAX_SYNC_WAIT is not set: conversion uses the
        // async task API, whose wall-clock budget lives on DoclingClient's poll loop.
        Map<String, String> env = new HashMap<>();
        env.put(NUM_THREADS_ENV, Integer.toString(numThreads));
        env.put(WORKERS_ENV, Integer.toString(workers));
        env.put(DEVICE_ENV, device);
        env.put(DEBUG_ERROR_DETAILS_ENV, "true");
        // The manager's cold-start readiness gate probes 127.0.0.1:$SQ_APP_PORT (default 8080, the BYO-app
        // contract). docling-serve is a fixed third-party image that ignores SQ_APP_PORT and always binds
        // its own port.
        env.put("SQ_APP_PORT", Integer.toString(containerPort));
        // GPU rides the SAME caps channel as every other stateful instance (StatefulCaps.gpu → the
        // shared --gpus flag builder); the resolver only ever set it when the runtime probe
        // confirmed passthrough, so emitting it here cannot fail the run on a GPU-less host.
        StatefulCaps effectiveCaps = new StatefulCaps(caps.cpu(), ram, caps.diskMb(),
                gpus > 0 ? gpus : null);
        // ReversePort = containerPort: publish Docling's listener on this engine's overlay address so a
        // SAME-OWNER sibling engine's Docling node (target_env dropdown) can convert here daemon→daemon.
        // The reverse relay enforces the inbound Network Access Control rule (source engine, port) at
        // accept time — default deny — and the WG gateway only programs the port chain once such a rule
        // exists, so publishing grants nobody unprompted access.
        StartResult result = statefulInstanceManager.start(ENV_ID, runtimeImage, /* stateful= */ true,
                effectiveCaps, INSTANCE_ID, hostPort, containerPort, env, containerPort);
        trackedInstanceId = result.instanceId();
        if (!StatefulStatus.RUNNING.equals(result.status())) {
            // Clear failure message — a missing/unpullable image lands here, never a silent no-op.
            throw new IllegalStateException("could not start the Docling stateful instance (status="
                    + result.status() + "); check that the image " + runtimeImage + " is reachable/pullable");
        }
    }

    /**
     * Covers the paths that never pass through the manager's publish step — a start that joined a live
     * container, a publish deferred while the overlay was down, a container started by an older daemon
     * without the reverse label. Idempotent and best-effort; the handshake-time republish in the
     * manager remains the fallback.
     */
    private void publishOverlayAccess() {
        try {
            statefulInstanceManager.ensureReversePortPublished(ENV_ID, INSTANCE_ID, containerPort);
        } catch (Exception e) {
            log.debug("docling: reverse-port ensure failed (handshake republish remains the "
                    + "fallback): {}", e.toString());
        }
    }

    /**
     * Pulls the side-car image BEFORE the start when it is not already in the local store, on the
     * dedicated {@link #pullTimeout} budget. The generic 120s lifecycle timeout is correct for a warm
     * {@code podman run} but was fatal for the inline pull of a ~4.4 GB image.
     */
    private void ensureImagePresent() {
        if (statefulInstanceManager.imageExists(image)) {
            return;
        }
        log.info("docling: image {} not in the local store — pulling (budget {}s; the CPU variant is "
                + "~4.4 GB, so a slow link needs the headroom)", image, pullTimeout.getSeconds());
        StatefulInstanceManager.LoadResult pull =
                statefulInstanceManager.pullImage(ENV_ID, image, "docling", pullTimeout);
        if (!StatefulStatus.ACTIVE.equals(pull.status())) {
            throw new IllegalStateException("could not pull the Docling image " + image + " within "
                    + pullTimeout.getSeconds() + "s — check that quay.io is reachable from this host "
                    + "(or pre-pull it: podman pull " + image + ")");
        }
        log.info("docling: image {} pulled (digest={})", image, pull.digest());
    }

    private void stop() {
        ready = false;
        // Drop the tracked registry entry vithe normal path when we have one (keeps status consistent for
        // diagnostics), then belt-and-suspenders remove the real container by its deterministic name. That
        // second call is what makes disable correct even right after a daemon restart, when the manager's
        // in-memory registry is empty but the container from a previous boot is still running.
        if (trackedInstanceId != null) {
            statefulInstanceManager.stop(ENV_ID, trackedInstanceId);
        }
        statefulInstanceManager.stopByContainerName(statefulInstanceManager.containerNameFor(INSTANCE_ID));
        trackedInstanceId = null;
        // Disable severs the relationship with EVERY Docling family container on the host, not just
        // ours: a previous install's survivor (offline uninstall, pre-deterministic naming) must not
        // keep serving — and showing — next to the next enable's instance.
        sweepStrayInstances();
        log.info("docling: stateful instance stopped (Engine Settings → General toggle disabled)");
    }

    /**
     * Reaps every stateful-labeled container in the Docling image family whose name is not the
     * canonical {@code sq-stateful-docling}. Without the sweep both the stray and our instance show in
     * the stateful inventory as two open "Docling" instances, and a stray still holding the published
     * port fails or shadows our start.
     */
    private void sweepStrayInstances() {
        try {
            String canonicalName = statefulInstanceManager.containerNameFor(INSTANCE_ID);
            for (StatefulInstanceManager.StatefulContainerView c
                    : statefulInstanceManager.listStatefulContainers()) {
                if (c.name() == null || canonicalName.equals(c.name())) {
                    continue;
                }
                boolean family = c.image() != null && c.image().contains(IMAGE_FAMILY_MARKER);
                if (!family) {
                    continue;
                }
                log.warn("docling: reaping stray Docling container {} (image={}, running={}) — one "
                        + "Docling instance per host is the invariant; the survivor of a previous "
                        + "install has no owner", c.name(), c.image(), c.running());
                statefulInstanceManager.stopByContainerName(c.name());
            }
        } catch (Exception e) {
            log.warn("docling: stray-instance sweep failed (best-effort): {}", e.toString());
        }
    }

    private void awaitReady() {
        long start = System.nanoTime();
        long deadline = start + readyTimeout.toNanos();
        long nextProgressLog = start + PROGRESS_LOG_INTERVAL.toNanos();
        long recreateAt = start + recreateAfter.toNanos();
        boolean recreated = false;
        String lastReason = "not probed yet";

        while (System.nanoTime() < deadline) {
            String reason = probe();
            if (reason == null) {
                log.info("docling: side-car ready at {} after {}s", healthUrl, elapsedSeconds(start));
                return;
            }
            lastReason = reason;

            // A warm image answers in seconds, so a minute of refusals means the container is not serving on
            // the published port at all — a crash loop, or a host-side port publish left stale by an
            // immediately preceding stop/start on the same port. Both are fixed by recreating the instance,
            // and neither improves by waiting out the rest of the budget.
            if (!recreated && System.nanoTime() >= recreateAt) {
                recreated = true;
                log.warn("docling: still unreachable at {} after {}s ({}); recreating the instance once "
                                + "before waiting out the remaining budget",
                        healthUrl, elapsedSeconds(start), reason);
                try {
                    stop();
                    start();
                } catch (RuntimeException recreateFailed) {
                    log.warn("docling: recreate attempt failed: {}", recreateFailed.toString());
                }
                // Fail fast after the recreate: the fresh instance gets ONE more recreate-after window, not the
                // rest of the (10-minute) budget. The long budget only ever exists for the first-ever image pull,
                // which the pre-recreate window already covered.
                deadline = System.nanoTime() + recreateAfter.toNanos();
            } else if (System.nanoTime() >= nextProgressLog) {
                // Otherwise the wait is completely silent for up to ten minutes.
                nextProgressLog = System.nanoTime() + PROGRESS_LOG_INTERVAL.toNanos();
                log.info("docling: waiting for {} ({}s elapsed, last probe: {})",
                        healthUrl, elapsedSeconds(start), reason);
            }
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for Docling to start");
            }
        }
        // Remove it before giving up: leaving it behind means the next attempt fights a dead container
        // for the same host port and fails for a second, unrelated reason.
        log.warn("docling: instance did not answer {} within {} (last probe: {}); removing it so the "
                + "next attempt starts clean", healthUrl, readyTimeout, lastReason);
        try {
            stop();
        } catch (RuntimeException cleanupFailed) {
            log.warn("docling: could not remove the unhealthy instance: {}", cleanupFailed.toString());
        }
        throw new IllegalStateException("Docling did not become ready within " + readyTimeout
                + " (last probe: " + lastReason + ")");
    }

    private static long elapsedSeconds(long startNanos) {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
    }

    /**
     * Something still listening here is either the previous instance shutting down — normal, clears in
     * a moment — or a forward/process that will keep accepting without ever serving Docling. The
     * second case cannot be fixed by starting another container on the same port, so it is called out
     * by name instead of being spent as ten minutes of readiness polling.
     */
    private void awaitPortReleased() {
        long deadline = System.nanoTime() + PORT_RELEASE_TIMEOUT.toNanos();
        boolean everAccepted = false;
        while (System.nanoTime() < deadline) {
            if (!portAccepts()) {
                if (everAccepted) {
                    log.info("docling: host port {} released; starting a fresh instance", hostPort);
                }
                return;
            }
            everAccepted = true;
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("docling: host port {} is still accepting connections after {} but is not serving Docling. "
                        + "Something else holds it (a stale container port-forward, or an unrelated process). "
                        + "A new instance published on the same port cannot be reached — free port {} or set "
                        + "sqs.docling.port to a different one.",
                hostPort, PORT_RELEASE_TIMEOUT, hostPort);
    }

    /** @return {@code true} when a TCP connect to the published port succeeds. */
    private boolean portAccepts() {
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("127.0.0.1", hostPort), 1000);
            return true;
        } catch (Exception refused) {
            return false;
        }
    }

    private boolean isHealthy() {
        return probe() == null;
    }

    /**
     * @return The reason is the whole point: "connection refused" (nothing listening on the published
     * port), a socket timeout.
     */
    private String probe() {
        try {
            HttpResponse<Void> response = http.send(
                    HttpRequest.newBuilder(URI.create(healthUrl))
                            .timeout(Duration.ofSeconds(3)).GET().build(),
                    HttpResponse.BodyHandlers.discarding());
            int status = response.statusCode();
            if (status >= 200 && status < 400) {
                return null;
            }
            return "HTTP " + status + " from " + healthUrl;
        } catch (Exception notUpYet) {
            String message = notUpYet.getMessage();
            return notUpYet.getClass().getSimpleName()
                    + (message == null || message.isBlank() ? "" : ": " + message);
        }
    }

    /**
     * Left RUNNING on daemon shutdown. Restarting the daemon should not cost the next conversion
     * another cold start; only our own bookkeeping thread is stopped here.
     */
    @PreDestroy
    void onShutdown() {
        reconcileExecutor.shutdownNow();
        log.debug("docling: leaving the stateful instance running across daemon restart");
    }
}
