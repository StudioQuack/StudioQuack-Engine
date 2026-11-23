package com.studioquack.sqs.sandbox.warmpool;

import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.egress.EgressReadiness;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperPipeChannel;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.oci.HardeningArgs;
import com.studioquack.sqs.sandbox.oci.HelperBroker;
import com.studioquack.sqs.sandbox.oci.ImageInspection;
import com.studioquack.sqs.sandbox.oci.NodeRunner;
import com.studioquack.sqs.sandbox.oci.PreloadModules;
import com.studioquack.sqs.sandbox.oci.PythonRunner;
import com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import com.studioquack.sqs.sandbox.oci.SeccompProfile;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * OCI-backed {@link Warmer}: keeps one hardened, long-lived {@code sleep} container alive per warm
 * slot (with the dep-cache mounted when a script-language hint is present + the full hardening
 * posture applied at create time) so a dispatch {@code exec}s into it instead of paying container
 * cold-start.
 */
@Slf4j
public final class OciWarmer implements Warmer {

    private static final Duration SPAWN_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration OP_TIMEOUT = Duration.ofSeconds(15);
    /** The parked container idles on a long sleep; the pool reaps + re-warms well before it expires. */
    private static final String SLEEP_SECONDS = "86400";
    /**
     * One line per owned container: {@code <id>|<role>|<bootId>}. {@code index} is required because
     * the label keys contain dots, which the {@code .Labels.key} form cannot express.
     */
    private static final String OWNED_FORMAT = "{{.ID}}|{{index .Labels \"" + HardeningArgs.LABEL_ROLE
            + "\"}}|{{index .Labels \"" + HardeningArgs.LABEL_BOOT + "\"}}";

    private final ContainerRuntime runtime;
    private final String depCacheRoot;
    /** Persistent-runner wiring: preload source + on/off + readiness budget. */
    private final DependencyManifestHolder depsHolder;
    private final boolean pythonRunnerEnabled;
    private final int runnerReadyTimeoutSec;
    /** JavaScript/TypeScript runner: off unless the pool wiring enables it ({@link #nodeRunner}). */
    private volatile boolean nodeRunnerEnabled;
    private volatile int nodeRunnerReadyTimeoutSec = 120;
    /**
     * The shared job-registry router every {@link HelperPipeChannel} routes frames through (the Spring
     * {@link HelperRequestRouter} bean). {@code null} disables the broker entirely (e.g. tests that
     * don't wire the helper channel) — the slot just never gets one.
     */
    private final HelperRequestRouter helperRouter;
    private final boolean helperBrokerEnabled;
    private final int helperBrokerReadyTimeoutSec;
    /** Stamped on every warm container so {@link #ownedContainers()} can find this daemon's leftovers. */
    private final SandboxOwnership ownership;
    /**
     * Resolves a warmed image to its script-mode language wire name when the image is one of the
     * language defaults (or a property override). Non-null → bootstrap/runner/broker path; null →
     * entrypoint (custom) image path.
     */
    private final Function<String, String> scriptLanguageForImage;
    /** Operator default CPU cap when the WarmSpec / per-image caps leave cpus unset (0 = uncapped). */
    private final double defaultWarmCpus;
    /** Operator default RAM cap when the WarmSpec / per-image caps leave ram unset (0 = uncapped). */
    private final int defaultWarmMemoryMb;
    /** Daemon-owned DNS pins applied to every warm slot's argv (validated at wiring time). */
    private final java.util.List<String> dnsServers;
    /** Refuse warm when overlay egress is UNAVAILABLE (warm always attaches {@code sqs-jobs}). */
    private final SandboxEgressManager egressManager;
    /**
     * Host GPU capability probe, consulted only when a warm spec carries a gpu cap (null probe =
     * capability unknown = refuse the cap; fail-closed). Never consulted otherwise, so a GPU-less
     * configuration adds nothing to the warm hot path.
     */
    private final com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe gpuProbe;
    /** One open pipe channel per LIVE broker container, so {@link #cool} can release it deterministically. */
    private final Map<String, HelperPipeChannel> channels = new ConcurrentHashMap<>();

    public OciWarmer(ContainerRuntime runtime, String depCacheRoot) {
        this(runtime, depCacheRoot, null, false, 120, null, false, 10, SandboxOwnership.unowned());
    }

    /** Back-compat: older signature (no helper broker wiring) — the broker stays disabled. */
    public OciWarmer(ContainerRuntime runtime, String depCacheRoot,
                     DependencyManifestHolder depsHolder,
                     boolean pythonRunnerEnabled, int runnerReadyTimeoutSec) {
        this(runtime, depCacheRoot, depsHolder, pythonRunnerEnabled, runnerReadyTimeoutSec,
                null, false, 10, SandboxOwnership.unowned());
    }

    /** Back-compat: older signature — default image→language resolver, uncapped operator defaults. */
    public OciWarmer(ContainerRuntime runtime, String depCacheRoot,
                     DependencyManifestHolder depsHolder,
                     boolean pythonRunnerEnabled, int runnerReadyTimeoutSec,
                     HelperRequestRouter helperRouter,
                     boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec,
                     SandboxOwnership ownership) {
        this(runtime, depCacheRoot, depsHolder, pythonRunnerEnabled, runnerReadyTimeoutSec,
                helperRouter, helperBrokerEnabled, helperBrokerReadyTimeoutSec,
                ownership, defaultScriptLanguageResolver(), 0.0, 0, null);
    }

    /** Back-compat: older signature — no egress readiness manager (gate skipped when null). */
    public OciWarmer(ContainerRuntime runtime, String depCacheRoot,
                     DependencyManifestHolder depsHolder,
                     boolean pythonRunnerEnabled, int runnerReadyTimeoutSec,
                     HelperRequestRouter helperRouter,
                     boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec,
                     SandboxOwnership ownership,
                     Function<String, String> scriptLanguageForImage,
                     double defaultWarmCpus, int defaultWarmMemoryMb) {
        this(runtime, depCacheRoot, depsHolder, pythonRunnerEnabled, runnerReadyTimeoutSec,
                helperRouter, helperBrokerEnabled, helperBrokerReadyTimeoutSec, ownership,
                scriptLanguageForImage, defaultWarmCpus, defaultWarmMemoryMb, null);
    }

    public OciWarmer(ContainerRuntime runtime, String depCacheRoot,
                     DependencyManifestHolder depsHolder,
                     boolean pythonRunnerEnabled, int runnerReadyTimeoutSec,
                     HelperRequestRouter helperRouter,
                     boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec,
                     SandboxOwnership ownership,
                     Function<String, String> scriptLanguageForImage,
                     double defaultWarmCpus, int defaultWarmMemoryMb,
                     SandboxEgressManager egressManager) {
        this(runtime, depCacheRoot, depsHolder, pythonRunnerEnabled, runnerReadyTimeoutSec,
                helperRouter, helperBrokerEnabled, helperBrokerReadyTimeoutSec, ownership,
                scriptLanguageForImage, defaultWarmCpus, defaultWarmMemoryMb, egressManager, null);
    }

    /** Full signature: the GPU probe gates any gpu cap on the WarmSpec (null = refuse gpu). */
    public OciWarmer(ContainerRuntime runtime, String depCacheRoot,
                     DependencyManifestHolder depsHolder,
                     boolean pythonRunnerEnabled, int runnerReadyTimeoutSec,
                     HelperRequestRouter helperRouter,
                     boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec,
                     SandboxOwnership ownership,
                     Function<String, String> scriptLanguageForImage,
                     double defaultWarmCpus, int defaultWarmMemoryMb,
                     SandboxEgressManager egressManager,
                     com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe gpuProbe) {
        this(runtime, depCacheRoot, depsHolder, pythonRunnerEnabled, runnerReadyTimeoutSec,
                helperRouter, helperBrokerEnabled, helperBrokerReadyTimeoutSec, ownership,
                scriptLanguageForImage, defaultWarmCpus, defaultWarmMemoryMb, egressManager,
                gpuProbe, null);
    }

    /**
     * Full signature + the daemon-owned DNS pins (null → the public default set). A warm slot is
     * created BEFORE its job is known, so the pins must come from operator config, never from anything
     * a dispatch could influence.
     */
    public OciWarmer(ContainerRuntime runtime, String depCacheRoot,
                     DependencyManifestHolder depsHolder,
                     boolean pythonRunnerEnabled, int runnerReadyTimeoutSec,
                     HelperRequestRouter helperRouter,
                     boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec,
                     SandboxOwnership ownership,
                     Function<String, String> scriptLanguageForImage,
                     double defaultWarmCpus, int defaultWarmMemoryMb,
                     SandboxEgressManager egressManager,
                     com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe gpuProbe,
                     java.util.List<String> dnsServers) {
        this.runtime = runtime;
        this.ownership = ownership == null ? SandboxOwnership.unowned() : ownership;
        this.depCacheRoot = depCacheRoot;
        this.depsHolder = depsHolder;
        this.pythonRunnerEnabled = pythonRunnerEnabled;
        this.runnerReadyTimeoutSec = runnerReadyTimeoutSec > 0 ? runnerReadyTimeoutSec : 120;
        this.helperRouter = helperRouter;
        this.helperBrokerEnabled = helperBrokerEnabled;
        this.helperBrokerReadyTimeoutSec = helperBrokerReadyTimeoutSec > 0 ? helperBrokerReadyTimeoutSec : 10;
        this.scriptLanguageForImage = scriptLanguageForImage == null
                ? defaultScriptLanguageResolver() : scriptLanguageForImage;
        this.defaultWarmCpus = defaultWarmCpus > 0 ? defaultWarmCpus : 0.0;
        this.defaultWarmMemoryMb = defaultWarmMemoryMb > 0 ? defaultWarmMemoryMb : 0;
        this.egressManager = egressManager;
        this.gpuProbe = gpuProbe;
        this.dnsServers = com.studioquack.sqs.sandbox.egress.SandboxDns.validate(dnsServers);
    }

    /** Enables the JavaScript/TypeScript persistent runner ({@link NodeRunner}) for new warm slots. */
    public OciWarmer nodeRunner(boolean enabled, int readyTimeoutSec) {
        this.nodeRunnerEnabled = enabled;
        this.nodeRunnerReadyTimeoutSec = readyTimeoutSec > 0 ? readyTimeoutSec : 120;
        return this;
    }

    /**
     * Maps an image to the language whose {@link SandboxLanguage#defaultImage()} equals it
     * (first match in enum order); null when no language claims the image.
     */
    private static Function<String, String> defaultScriptLanguageResolver() {
        return imageRef -> {
            if (imageRef == null || imageRef.isBlank()) {
                return null;
            }
            for (SandboxLanguage l : SandboxLanguage.values()) {
                if (l.defaultImage() != null && l.defaultImage().equals(imageRef)) {
                    return l.wire();
                }
            }
            return null;
        };
    }

    @Override
    public String kind() {
        return "oci";
    }

    @Override
    public WarmSlot warm(WarmSpec spec) throws WarmException {
        if (spec == null || spec.imageRef() == null || spec.imageRef().isBlank()) {
            throw new WarmException("no image ref for warm slot");
        }
        if (!runtime.available()) {
            throw new WarmException("no container runtime available");
        }
        // Warm always attaches sqs-jobs; refuse while overlay egress is UNAVAILABLE. WarmPool already logs
        // WarmException at debug (acquire/refill) / batched INFO (reconcile).
        if (egressManager != null) {
            EgressReadiness readiness = egressManager.readiness();
            if (!readiness.internetAllowed()) {
                throw new WarmException(readiness.refusalMessage());
            }
        }
        // Non-null → this image is a known script-mode default (bootstrap/runner/broker path).
        String scriptLang = scriptLanguageForImage.apply(spec.imageRef());
        SandboxLanguage lang = SandboxLanguage.fromWire(
                scriptLang != null ? scriptLang : spec.language()); // May be null for custom images.
        // Per-image caps on the WarmSpec win over operator defaults.
        int ramMb = spec.ramMb() > 0 ? spec.ramMb() : defaultWarmMemoryMb;
        double cpus = spec.cpus() > 0 ? spec.cpus() : defaultWarmCpus;
        // Gpu has NO operator default and no silent attempt: a positive cap is honoured only when the
        // runtime probe confirms passthrough; otherwise the warm is refused outright (a slot that fails
        // every create is worse than a clear error at cap-apply time).
        if (spec.gpu() > 0 && (gpuProbe == null || !gpuProbe.gpuPassthroughAvailable())) {
            throw new WarmException("gpu cap (" + spec.gpu() + ") requested for image "
                    + spec.imageRef() + " but this host reports no GPU passthrough — the cap is "
                    + "refused rather than attempted");
        }
        String cli = runtime.cli();
        HardeningArgs.Builder b = HardeningArgs.builder(cli, lang, spec.imageRef())
                .detached(true)
                .ownership(ownership)
                .role(HardeningArgs.ROLE_WARM)
                .runtimeFlag(runtime.runtimeFlagFor(null))
                .seccompProfilePath(SeccompProfile.profilePath(runtime.isolationEnforced()))
                .seccompRequired(runtime.isolationEnforced())
                // Daemon-owned DNS pins: warm slots get the identical resolver posture a cold job would (podman:
                // last --dns wins; the guard refuses tail overrides).
                .dnsServers(dnsServers)
                // A custom (entrypoint) image's ENTRYPOINT is the per-job exec argv, not the parked container's
                // main process: without clearing it here the entrypoint runs at create with "sleep" as its args, a
                // one-shot app exits immediately, and --rm deletes the slot's container before any dispatch can
                // exec into it.
                .clearEntrypoint(scriptLang == null)
                .containerCommand(List.of("sleep", SLEEP_SECONDS));
        if (ramMb > 0) {
            b.limits(SandboxLimits.of(ramMb, 0L));
        }
        if (cpus > 0) {
            b.cpus(cpus);
        }
        // Gpu rides the SAME builder + the one shared flag shape (HardeningArgs.appendGpuCap).
        if (spec.gpu() > 0) {
            b.gpus(spec.gpu());
        }
        // disc → /sandbox tmpfs size: rootfs is --read-only, so this is the job's only writable path
        // (Podman has no portable rootfs quota under podman-machine).
        if (spec.diskMb() > 0) {
            b.sandboxTmpfsMb(spec.diskMb());
        }
        // Dep-cache only when we have a resolved script language and the bucket dir exists.
        if (lang != null) {
            String depDir = depCacheHostDir(lang);
            if (depDir != null) {
                b.depCacheHostDir(depDir);
            }
        }
        // Jobs network must exist before attach; fail loud (no silent bridge fallback).
        try {
            SandboxNetworkEnsure.ensureJobs(cli);
        } catch (IllegalStateException e) {
            throw new WarmException(e.getMessage() == null ? "jobs network ensure failed" : e.getMessage());
        }
        List<String> argv = b.build().toArgv();
        CliRunner.Result r = CliRunner.run(argv, SPAWN_TIMEOUT);
        if (!r.ok()) {
            String stderr = firstLine(r.stderr());
            // Post-sleep WSL/Podman recovery (Windows only): if the spawn failure looks like a broken
            // SSH/socket connection, trigger the automatic WSL + machine restart and retry the spawn once.
            // This handles the case where the cached CapabilityReport still says "available" but the
            // underlying WSL distro became unreachable after Windows sleep.
            if (runtime.recoveryService().checkAndRecover(r, runtime)) {
                // Recovery succeeded — retry the spawn once.
                r = CliRunner.run(argv, SPAWN_TIMEOUT);
                if (!r.ok()) {
                    stderr = firstLine(r.stderr());
                    throw new WarmException("warm spawn failed after recovery (exit=" + r.exitCode() + "): " + stderr);
                }
            } else {
                // A WG drop mid-pull (or a podman-machine restart) can leave the overlay store with an orphaned
                // reference: the layer diff dir is gone but the image still lists it. Every spawn then fails with
                // the same "can't stat lower layer" until the corrupt image is dropped and re-pulled.
                if (com.studioquack.sqs.sandbox.oci.OciStorageRepair.isOverlayStorageCorruption(stderr)) {
                    com.studioquack.sqs.sandbox.oci.OciStorageRepair.repair(cli, spec.imageRef());
                }
                // Warm spawns run detached, so stderr is the runtime's own; the next reconcile
                // pass then spawns without the cgroup cap crun could not open.
                com.studioquack.sqs.sandbox.oci.CgroupCaps.noteDetachedStartFailure(r.stderr());
                throw new WarmException("warm spawn failed (exit=" + r.exitCode() + "): " + stderr);
            }
        }
        String containerId = r.stdout().trim();
        if (containerId.isEmpty()) {
            throw new WarmException("warm spawn returned no container id");
        }
        String slotLanguage = scriptLang != null ? scriptLang
                : (spec.language() == null ? "" : spec.language());
        log.debug("warmpool.oci.spawned lang={} image={} container={}",
                slotLanguage, spec.imageRef(), shortId(containerId));
        WarmSlot slot = new WarmSlot(slotLanguage, "w_" + UUID.randomUUID(), containerId,
                kind(), spec.imageRef(), Instant.now());

        if (scriptLang == null) {
            // ENTRYPOINT (custom) image — capture Entrypoint+Cmd; image owns its start. No python runner, no
            // helper broker.
            try {
                List<String> ep = ImageInspection.entrypointArgv(cli, spec.imageRef());
                if (ep.isEmpty()) {
                    CliRunner.run(List.of(cli, "rm", "-f", containerId), OP_TIMEOUT);
                    throw new WarmException("custom image '" + spec.imageRef()
                            + "' declares no Entrypoint/Cmd — a custom execution image must define its own start command");
                }
                return slot.withEntrypoint(ep);
            } catch (IllegalStateException e) {
                CliRunner.run(List.of(cli, "rm", "-f", containerId), OP_TIMEOUT);
                throw new WarmException(e.getMessage() == null ? "image inspect failed" : e.getMessage());
            }
        }

        // Script mode: start the long-lived, pre-importing runner inside the fresh container so a dispatch
        // execs a tiny client into an already-warm interpreter (no per-job import). Best-effort: on any
        // failure the slot is still usable via the cold bootstrap (fresh interpreter) — just not
        // preloaded.
        if (pythonRunnerEnabled && lang == SandboxLanguage.PYTHON) {
            slot = slot.withRunnerReady(startRunner(cli, containerId, lang, runnerReadyTimeoutSec));
        } else if (nodeRunnerEnabled && NodeRunner.supports(lang)) {
            slot = slot.withRunnerReady(startRunner(cli, containerId, lang, nodeRunnerReadyTimeoutSec));
        }

        // Start the in-container helper broker alongside the runner, off the SAME warm-time budget (never
        // per-dispatch). Currently python-only: the broker script is a python3 process and
        // studioquack.execute()/llm.* exist only in the python SDK today ("no HTTP fallback" decision).
        if (helperBrokerEnabled && helperRouter != null && lang == SandboxLanguage.PYTHON) {
            try {
                HelperPipeChannel channel = startHelperBroker(cli, containerId);
                channels.put(containerId, channel);
                slot = slot.withBroker(true, HelperBroker.SOCKET_PATH);
            } catch (WarmException e) {
                CliRunner.run(List.of(cli, "rm", "-f", containerId), OP_TIMEOUT); // don't leak the container
                throw e;
            }
        }
        return slot;
    }

    /**
     * Plants {@code sq_helper_broker.py} (short-lived exec, base64 blob — same technique as
     * {@link PythonRunner#startScript}), launches it long-lived on the file (never on stdin — see
     * {@link HelperBroker} javadoc for why), confirms its UDS is bound, then wraps the process in a
     * {@link HelperPipeChannel}. Throws (never returns a half-started channel) on any failure so the
     * caller can fail the whole warm attempt.
     */
    private HelperPipeChannel startHelperBroker(String cli, String containerId) throws WarmException {
        CliRunner.Result plant = CliRunner.run(
                List.of(cli, "exec", containerId, "sh", "-c", HelperBroker.plantScript()), OP_TIMEOUT);
        if (!plant.ok()) {
            throw new WarmException("helper-broker plant failed (exit=" + plant.exitCode() + "): "
                    + firstLine(plant.stderr()));
        }
        Process process;
        try {
            process = HelperBroker.launch(cli, containerId).start();
        } catch (IOException e) {
            throw new WarmException("helper-broker launch failed: " + e.getMessage());
        }
        CliRunner.Result ready = CliRunner.run(
                List.of(cli, "exec", containerId, "sh", "-c",
                        HelperBroker.readinessScript(helperBrokerReadyTimeoutSec)),
                Duration.ofSeconds(helperBrokerReadyTimeoutSec + 5L));
        if (!ready.ok()) {
            process.destroyForcibly();
            throw new WarmException("helper-broker did not bind its socket within "
                    + helperBrokerReadyTimeoutSec + "s (container=" + shortId(containerId) + ")");
        }
        log.debug("warmpool.broker container={} ready=true", shortId(containerId));
        return new HelperPipeChannel(process, helperRouter);
    }

    /**
     * Plants + starts the language's persistent runner ({@code sq_python_runner.py} or
     * {@code sq_node_runner.js}) in the warm container and blocks until it has preloaded + bound its
     * socket (ready marker). Returns whether the runner is up; never throws.
     */
    private boolean startRunner(String cli, String containerId, SandboxLanguage lang, int timeoutSec) {
        try {
            String preloads = PreloadModules.csvFor(depsHolder, lang);
            String depContainerDir = depCacheHostDir(lang) != null
                    ? HardeningArgs.depContainerDir(lang) : null;
            boolean python = lang == SandboxLanguage.PYTHON;
            String startScript = python
                    ? PythonRunner.startScript(preloads, depContainerDir)
                    : NodeRunner.startScript(lang, preloads, depContainerDir);
            CliRunner.Result start = CliRunner.run(
                    List.of(cli, "exec", "-d", containerId, "sh", "-c", startScript), OP_TIMEOUT);
            if (!start.ok()) {
                log.debug("warmpool.runner.start-failed lang={} container={} exit={}",
                        lang.wire(), shortId(containerId), start.exitCode());
                return false;
            }
            CliRunner.Result ready = CliRunner.run(
                    List.of(cli, "exec", containerId, "sh", "-c", python
                            ? PythonRunner.readinessScript(timeoutSec)
                            : NodeRunner.readinessScript(timeoutSec)),
                    Duration.ofSeconds(timeoutSec + 5L));
            int preloadCount = preloads.isBlank() ? 0 : preloads.split(",").length;
            // Warm slots are single-use, so the pool spawns a replacement after every job and the
            // reconciler tops it up on a timer — at INFO this was the single noisiest daemon line.
            // A runner that FAILS to come ready is the case worth seeing, so that stays at INFO.
            if (ready.ok()) {
                log.debug("warmpool.runner lang={} container={} ready=true preloads={}",
                        lang.wire(), shortId(containerId), preloadCount);
            } else {
                log.info("warmpool.runner lang={} container={} ready=false preloads={} — slot falls back "
                        + "to the legacy bootstrap (fresh interpreter per job)",
                        lang.wire(), shortId(containerId), preloadCount);
            }
            return ready.ok();
        } catch (Exception e) {
            log.debug("warmpool.runner.error container={}: {}", shortId(containerId), e.toString());
            return false;
        }
    }

    @Override
    public void cool(WarmSlot slot) {
        if (slot == null || !slot.hasContainer()) {
            return;
        }
        remove(slot.containerId());
    }

    @Override
    public void remove(String containerId) {
        if (containerId == null || containerId.isBlank() || runtime.cli() == null) {
            return;
        }
        HelperPipeChannel channel = channels.remove(containerId);
        if (channel != null) {
            channel.close();
        }
        CliRunner.run(List.of(runtime.cli(), "rm", "-f", containerId), OP_TIMEOUT);
        log.debug("warmpool.oci.cooled container={}", shortId(containerId));
    }

    /**
     * Every container on the host stamped with this daemon's owner id, in any state — an exited one
     * is still a leftover the operator sees in {@code podman ps -a}.
     *
     * <p>Returns nothing when the identity is unknown: an owner-blind sweep on a host running two
     * engines would remove the other one's working containers.
     */
    @Override
    public List<OwnedContainer> ownedContainers() {
        if (!ownership.identified() || runtime.cli() == null || !runtime.available()) {
            return List.of();
        }
        // --no-trunc: `podman run -d` prints the full id, `podman ps` truncates to 12 chars by
        // default, and the caller compares these against slot container ids for equality.
        CliRunner.Result r = CliRunner.run(List.of(runtime.cli(), "ps", "-a", "--no-trunc",
                "--filter", "label=" + HardeningArgs.LABEL_OWNER + "=" + ownership.ownerId(),
                "--format", OWNED_FORMAT), OP_TIMEOUT);
        if (!r.ok() || r.stdout().isBlank()) {
            return List.of();
        }
        List<OwnedContainer> owned = new ArrayList<>();
        for (String line : r.stdout().split("\\r?\\n")) {
            String[] parts = line.trim().split("\\|", -1);
            if (parts.length < 3 || parts[0].isBlank()) {
                continue;
            }
            owned.add(new OwnedContainer(parts[0], parts[1], parts[2]));
        }
        return owned;
    }

    @Override
    public boolean alive(WarmSlot slot) {
        if (slot == null || !slot.hasContainer() || runtime.cli() == null) {
            return false;
        }
        CliRunner.Result r = CliRunner.run(
                List.of(runtime.cli(), "inspect", "-f", "{{.State.Running}}", slot.containerId()), OP_TIMEOUT);
        return r.ok() && "true".equals(r.stdout().trim());
    }

    /**
     * Podman overlay corruption after an interrupted pull: the image index references a lower
     * layer whose diff dir was never fully written (or was vacuumed). Match the exact stderr
     * signature so we only reset storage for this failure, never for an ordinary spawn error.
     */
    /** The host dep-cache dir for a language, or null when unset / absent (mount skipped). */
    private String depCacheHostDir(SandboxLanguage lang) {
        if (depCacheRoot == null || depCacheRoot.isBlank() || lang == null) {
            return null;
        }
        // Resolve by dep BUCKET (js/ts share "node"), matching the installer + HardeningArgs mount.
        java.io.File dir = new java.io.File(depCacheRoot, HardeningArgs.depBucket(lang));
        return dir.isDirectory() ? dir.getAbsolutePath() : null;
    }

    private static String shortId(String id) {
        return id == null ? "" : id.substring(0, Math.min(12, id.length()));
    }

    private static String firstLine(String s) {
        if (s == null) {
            return "";
        }
        int nl = s.indexOf('\n');
        return nl < 0 ? s.trim() : s.substring(0, nl).trim();
    }
}
