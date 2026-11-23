package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.egress.EgressReadiness;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperPipeChannel;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.warmpool.WarmSlot;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Step 3 — gives a COLD dispatch (warm-pool miss) the SAME container shape a warm slot already
 * has: a detached, fully-hardened container the daemon then {@code exec}s the job into, with an
 * in-container helper broker planted + launched exactly like {@link
 * com.studioquack.sqs.sandbox.warmpool.OciWarmer} does for a warm slot.
 */
@Slf4j
public final class ColdContainerLauncher {

    /** Bounds the `podman run -d` create call itself (Go/OciWarmer parity: image pull + boot). */
    private static final Duration CREATE_TIMEOUT = Duration.ofSeconds(120);
    /** Bounds broker plant / launch-readiness-poll / container-remove calls. */
    private static final Duration OP_TIMEOUT = Duration.ofSeconds(15);

    /** Floor for the container's own idle bound, even for a very short job. */
    private static final long MIN_LIFETIME_SECONDS = 60L;
    /**
     * Ceiling for the container's own idle bound — the defence-in-depth backstop against a daemon
     * crash that never runs the {@code finally}-block removal. This must never be shorter than a
     * legitimately configured job wall-clock deadline (self-terminating the container mid-job would
     * {@code SIGKILL} the still-running {@code exec}), so it mirrors {@code OciWarmer.SLEEP_SECONDS} —
     * the same 24h backstop a warm slot already idles under before its own reaper would reap it.
     */
    private static final long MAX_LIFETIME_SECONDS = 86_400L;
    /** Headroom added over the job's own wall-clock deadline so create+broker+exec never races it. */
    private static final long LIFETIME_BUFFER_SECONDS = 60L;

    private final ContainerRuntime runtime;
    private final HelperRequestRouter helperRouter;
    private final boolean helperBrokerEnabled;
    private final int helperBrokerReadyTimeoutSec;
    private final SandboxOwnership ownership;
    private final SandboxEgressManager egressManager;
    /** Daemon-owned DNS pins for the job argv (null/empty → the public default set). */
    private final List<String> dnsServers;

    public ColdContainerLauncher(ContainerRuntime runtime, HelperRequestRouter helperRouter,
                                 boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec) {
        this(runtime, helperRouter, helperBrokerEnabled, helperBrokerReadyTimeoutSec,
                SandboxOwnership.unowned(), null);
    }

    public ColdContainerLauncher(ContainerRuntime runtime, HelperRequestRouter helperRouter,
                                 boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec,
                                 SandboxOwnership ownership) {
        this(runtime, helperRouter, helperBrokerEnabled, helperBrokerReadyTimeoutSec, ownership, null);
    }

    public ColdContainerLauncher(ContainerRuntime runtime, HelperRequestRouter helperRouter,
                                 boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec,
                                 SandboxOwnership ownership, SandboxEgressManager egressManager) {
        this(runtime, helperRouter, helperBrokerEnabled, helperBrokerReadyTimeoutSec, ownership,
                egressManager, null);
    }

    public ColdContainerLauncher(ContainerRuntime runtime, HelperRequestRouter helperRouter,
                                 boolean helperBrokerEnabled, int helperBrokerReadyTimeoutSec,
                                 SandboxOwnership ownership, SandboxEgressManager egressManager,
                                 List<String> dnsServers) {
        this.runtime = runtime;
        this.helperRouter = helperRouter;
        this.helperBrokerEnabled = helperBrokerEnabled;
        this.helperBrokerReadyTimeoutSec = helperBrokerReadyTimeoutSec > 0 ? helperBrokerReadyTimeoutSec : 10;
        this.ownership = ownership == null ? SandboxOwnership.unowned() : ownership;
        this.egressManager = egressManager;
        // Validate once at wiring time so a malformed sqs.sandbox.dns.servers fails the FIRST
        // container create loudly instead of being re-parsed per job.
        this.dnsServers = com.studioquack.sqs.sandbox.egress.SandboxDns.validate(dnsServers);
    }

    /** One cold, single-job container: created detached, optionally broker-backed, ready to exec into. */
    public record ColdContainer(String containerId, HelperPipeChannel brokerChannel, String helperSocketPath) {

        public boolean brokerReady() {
            return brokerChannel != null;
        }

        /**
         * Adapts this cold container into the same {@link WarmSlot} shape {@code buildArgv}'s exec
         * branch already knows how to consume — so the exec argv, the env wiring (incl. the helper
         * socket path) and the stdin/stdout/stderr contract are byte-identical to the warm path. The
         * persistent Python runner is never started for a cold container (that stays a warm-only,
         * amortised-at-warm-time optimisation), so {@code runnerReady} is always {@code false}.
         */
        public WarmSlot toWarmSlot(String language, String image) {
            return new WarmSlot(language, "", containerId, "oci-cold", image, Instant.now())
                    .withBroker(brokerReady(), helperSocketPath);
        }
    }

    /** Creates a detached, hardened, single-job container with the SAME per-job posture the old `run --rm` cold path applied (tier/egress/limits/cpus/seccomp/dep-cache/non-root/read-only + tmpfs), idling on a bounded {@code sleep} exactly like a warm slot's container — then, for Python only (mirrors {@code OciWarmer}; 's "no HTTP fallback" decision means {@code execute}/{@code llm.*} exist only in the Python SDK), plants + launches the in-container helper broker and waits for its readiness marker. */
    public ColdContainer create(SandboxLanguage lang, String image, IsolationTier tier, EgressPolicy egress,
                                SandboxLimits limits, double cpus, int diskMb, String seccompProfilePath,
                                String depCacheHostDir, long wallMs) {
        return create(lang, image, tier, egress, limits, cpus, diskMb, seccompProfilePath,
                depCacheHostDir, wallMs, false);
    }

    /**
     * Full form of {@link #create}, adding the entrypoint switch.
     *
     * @param clearEntrypoint {@code true} for a custom (entrypoint) image: the image's own ENTRYP.
     */
    public ColdContainer create(SandboxLanguage lang, String image, IsolationTier tier, EgressPolicy egress,
                                SandboxLimits limits, double cpus, int diskMb, String seccompProfilePath,
                                String depCacheHostDir, long wallMs, boolean clearEntrypoint) {
        String cli = runtime.cli();
        HardeningArgs.Builder b = HardeningArgs.builder(cli, lang, image)
                .detached(true)
                .ownership(ownership)
                .role(HardeningArgs.ROLE_JOB)
                .runtimeFlag(runtime.runtimeFlagFor(tier))
                .egress(egress)
                .limits(limits)
                .cpus(cpus)
                .seccompProfilePath(seccompProfilePath)
                .seccompRequired(runtime.isolationEnforced())
                // The daemon-owned DNS pins ride every cold job's argv too.
                .dnsServers(dnsServers)
                .clearEntrypoint(clearEntrypoint)
                .containerCommand(List.of("sleep", boundedLifetimeSeconds(wallMs)));
        // Rootfs is --read-only, so the /sandbox tmpfs size IS the enforceable disc cap.
        if (diskMb > 0) {
            b.sandboxTmpfsMb(diskMb);
        }
        if (depCacheHostDir != null && !depCacheHostDir.isBlank()) {
            b.depCacheHostDir(depCacheHostDir);
        }
        // Defence in depth: refuse OPEN attach when overlay egress is UNAVAILABLE (primary gate is
        // PodmanSandboxExecution; never silently use the host default route).
        if (egress != EgressPolicy.NONE && egressManager != null) {
            EgressReadiness readiness = egressManager.readiness();
            if (!readiness.internetAllowed()) {
                throw new IllegalStateException(
                        readiness.refusalMessage());
            }
        }
        // Jobs network must exist before OPEN attach; NONE stays --network none and does not need the
        // dedicated network. Fail loud (no silent bridge fallback).
        if (egress != EgressPolicy.NONE) {
            SandboxNetworkEnsure.ensureJobs(cli);
        }
        HardeningArgs hardened = b.build();
        List<String> argv = hardened.toArgv();

        CliRunner.Result r = CliRunner.run(argv, CREATE_TIMEOUT);
        if (!r.ok() && OciStorageRepair.isOverlayStorageCorruption(r.stderr())) {
            // Same orphaned-lower-layer corruption the warm pool heals: the warm self-heal runs on
            // a reconcile loop, so a host that only ever takes the cold path would spin exit=127
            // forever. Repair once here, then re-create against the freshly re-pulled image.
            OciStorageRepair.repair(cli, image);
            r = CliRunner.run(argv, CREATE_TIMEOUT);
        }
        // `run -d` stderr is the runtime's own: a cgroup controller crun cannot open is dropped
        // and the create retried, at most once per controller.
        while (!r.ok() && CgroupCaps.noteDetachedStartFailure(r.stderr())) {
            argv = hardened.toArgv();
            r = CliRunner.run(argv, CREATE_TIMEOUT);
        }
        if (!r.ok()) {
            throw new IllegalStateException("cold container create failed (exit=" + r.exitCode() + "): "
                    + firstLine(r.stderr()));
        }
        String containerId = r.stdout().trim();
        if (containerId.isEmpty()) {
            throw new IllegalStateException("cold container create returned no container id");
        }
        log.debug("sandbox.cold.created lang={} image={} container={}",
                lang == null ? "custom" : lang.wire(), image, shortId(containerId));

        if (!helperBrokerEnabled || helperRouter == null || lang != SandboxLanguage.PYTHON) {
            return new ColdContainer(containerId, null, "");
        }
        try {
            HelperPipeChannel channel = startHelperBroker(cli, containerId);
            return new ColdContainer(containerId, channel, HelperBroker.SOCKET_PATH);
        } catch (RuntimeException e) {
            // Same "not best-effort" posture as OciWarmer.warm(): a python job whose broker fails to bind
            // would silently be unable to call studioquack.execute()/llm.* — fail the whole cold attempt
            // rather than hand out a container that looks usable but cannot serve the callback, and never leak
            // the container we just created.
            removeContainer(cli, containerId);
            throw e;
        }
    }

    /**
     * Always removes the container, and closes its broker pipe first when one was started. Best-effort
     * and idempotent (mirrors {@code OciWarmer.cool}); safe to call exactly once per successful {@link
     * #create} result, from a {@code finally} block that also covers the job's own timeout/exception.
     */
    public void release(ColdContainer container) {
        if (container == null || container.containerId() == null || container.containerId().isBlank()) {
            return;
        }
        if (container.brokerChannel() != null) {
            container.brokerChannel().close();
        }
        removeContainer(runtime.cli(), container.containerId());
    }

    /**
     * Cancel-instance teardown: kills + removes a cold job container BY ID ALONE (no {@link
     * ColdContainer} handle available from outside {@link #create}/{@link #release}'s caller). Uses
     * the exact same {@code podman rm -f} this class already runs from {@link #release}; idempotent
     * and best-effort on an unknown/already-gone id.
     */
    public void forceKill(String containerId) {
        if (runtime == null || containerId == null || containerId.isBlank()) {
            return;
        }
        removeContainer(runtime.cli(), containerId);
    }

    /* ------------------------------------------------------------------ */
    /* broker plant + launch + readiness (mirrors OciWarmer exactly)      */
    /* ------------------------------------------------------------------ */

    private HelperPipeChannel startHelperBroker(String cli, String containerId) {
        CliRunner.Result plant = CliRunner.run(
                List.of(cli, "exec", containerId, "sh", "-c", HelperBroker.plantScript()), OP_TIMEOUT);
        if (!plant.ok()) {
            throw new IllegalStateException("helper-broker plant failed (exit=" + plant.exitCode() + "): "
                    + firstLine(plant.stderr()));
        }
        Process process;
        try {
            process = HelperBroker.launch(cli, containerId).start();
        } catch (IOException e) {
            throw new IllegalStateException("helper-broker launch failed: " + e.getMessage());
        }
        CliRunner.Result ready = CliRunner.run(
                List.of(cli, "exec", containerId, "sh", "-c",
                        HelperBroker.readinessScript(helperBrokerReadyTimeoutSec)),
                Duration.ofSeconds(helperBrokerReadyTimeoutSec + 5L));
        if (!ready.ok()) {
            process.destroyForcibly();
            throw new IllegalStateException("helper-broker did not bind its socket within "
                    + helperBrokerReadyTimeoutSec + "s (container=" + shortId(containerId) + ")");
        }
        log.debug("sandbox.cold.broker container={} ready=true", shortId(containerId));
        return new HelperPipeChannel(process, helperRouter);
    }

    private void removeContainer(String cli, String containerId) {
        if (cli == null || containerId == null || containerId.isBlank()) {
            return;
        }
        CliRunner.run(List.of(cli, "rm", "-f", containerId), OP_TIMEOUT);
        log.debug("sandbox.cold.removed container={}", shortId(containerId));
    }

    /**
     * The idle bound for the cold container's {@code sleep} command: long enough to cover create +
     * broker start + the job's own wall-clock deadline with headroom, but capped so a container
     * that outlives the daemon's own {@code finally}-block removal (e.g. a killed/crashed daemon)
     * still self-terminates rather than idling forever.
     */
    static String boundedLifetimeSeconds(long wallMs) {
        // WallMs <= 0 is the documented "unbounded" job posture (the daemon awaits the exec with no
        // deadline). The container's own idle bound must not be the one that ends such a job: with the 60
        // s floor here, PID 1 (`sleep 60`) exited while a cold bootstrap was still spooling a 21 MB
        // file-bearing payload, and the exec died as "exited non-zero (137)".
        long base = wallMs > 0 ? (wallMs / 1000L) + LIFETIME_BUFFER_SECONDS : MAX_LIFETIME_SECONDS;
        long clamped = Math.max(MIN_LIFETIME_SECONDS, Math.min(MAX_LIFETIME_SECONDS, base));
        return Long.toString(clamped);
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
