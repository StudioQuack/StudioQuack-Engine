package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.IsolationTier;
import com.studioquack.sqs.sandbox.SandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxImageResolution;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.egress.EgressProxyProbe;
import com.studioquack.sqs.sandbox.egress.EgressReadiness;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.helperchan.HelperFailure;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperResult;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.secrets.SecretResolution;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.sandbox.warmpool.WarmSlot;
import com.studioquack.sqs.sandbox.warmpool.WarmTargetEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The production {@link SandboxExecutionPort}: runs a prepared node job in a hardened container
 * via the detected Podman runtime, driven entirely by process exec ({@link CliRunner}) — never a
 * container Java client. It is the seam {@code ScriptDispatchDelegate} calls in place of the
 * backend's HTTP dispatcher (the control plane→daemon network hop collapses to a local call).
 */
@Slf4j
@Component
public class PodmanSandboxExecution implements SandboxExecutionPort {

    private final ContainerRuntime runtime;
    private final SecretResolver secretResolver;
    private final Redactor redactor;
    private final HelperRequestRouter helperRouter;
    private final WarmPool warmPool;
    private final Environment environment;
    private final DependencyManifestHolder depsHolder;
    private final ColdContainerLauncher coldContainerLauncher;
    private final SandboxEgressManager egressManager;
    private final com.studioquack.sqs.runtime.DispatchCancellationRegistry dispatchCancellationRegistry;
    private final long defaultWallMs;
    private final double maxCpus;
    private final String depCacheRoot;
    private final String callerEnvId;
    private final String egressProxyUrl;
    private final String egressNoProxy;
    /**
     * Advisory egress-proxy health. Null only in tests built without one: a null probe keeps the
     * legacy always-inject behaviour so pre-gate tests stay unchanged.
     */
    private final EgressProxyProbe egressProxyProbe;

    /** Back-compat: tests that do not pin egress readiness get a null manager (gate skipped). */
    public PodmanSandboxExecution(ContainerRuntime runtime,
                                  SecretResolver secretResolver,
                                  Redactor redactor,
                                  HelperRequestRouter helperRouter,
                                  WarmPool warmPool,
                                  Environment environment,
                                  DependencyManifestHolder depsHolder,
                                  SandboxOwnership ownership,
                                  com.studioquack.sqs.runtime.DispatchCancellationRegistry dispatchCancellationRegistry) {
        this(runtime, secretResolver, redactor, helperRouter, warmPool, environment, depsHolder,
                ownership, dispatchCancellationRegistry, null);
    }

    /** Back-compat: tests that pin egress readiness but not the proxy probe (gate skipped). */
    public PodmanSandboxExecution(ContainerRuntime runtime,
                                  SecretResolver secretResolver,
                                  Redactor redactor,
                                  HelperRequestRouter helperRouter,
                                  WarmPool warmPool,
                                  Environment environment,
                                  DependencyManifestHolder depsHolder,
                                  SandboxOwnership ownership,
                                  com.studioquack.sqs.runtime.DispatchCancellationRegistry dispatchCancellationRegistry,
                                  SandboxEgressManager egressManager) {
        this(runtime, secretResolver, redactor, helperRouter, warmPool, environment, depsHolder,
                ownership, dispatchCancellationRegistry, egressManager, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public PodmanSandboxExecution(ContainerRuntime runtime,
                                  SecretResolver secretResolver,
                                  Redactor redactor,
                                  HelperRequestRouter helperRouter,
                                  WarmPool warmPool,
                                  Environment environment,
                                  DependencyManifestHolder depsHolder,
                                  SandboxOwnership ownership,
                                  com.studioquack.sqs.runtime.DispatchCancellationRegistry dispatchCancellationRegistry,
                                  SandboxEgressManager egressManager,
                                  EgressProxyProbe egressProxyProbe) {
        this.runtime = runtime;
        this.secretResolver = secretResolver;
        this.redactor = redactor;
        this.helperRouter = helperRouter;
        this.warmPool = warmPool;
        this.environment = environment;
        this.depsHolder = depsHolder;
        this.egressManager = egressManager;
        this.egressProxyProbe = egressProxyProbe;
        this.dispatchCancellationRegistry = dispatchCancellationRegistry;
        // Default 24h wall (sqs.sandbox.default-wall-ms) so a silent node cannot run forever.
        // An agent parked on a long LLM wait can raise the env or set 0 to restore unbounded.
        this.defaultWallMs = environment.getProperty("sqs.sandbox.default-wall-ms", Long.class, 86_400_000L);
        this.maxCpus = environment.getProperty("sqs.sandbox.max-cpus", Double.class, 1.0);
        this.depCacheRoot = environment.getProperty("sqs.sandbox.dep-cache-root", "");
        this.callerEnvId = environment.getProperty("sqs.env-id", "");
        this.egressProxyUrl = environment.getProperty("sqs.sandbox.egress-proxy.url", "").trim();
        this.egressNoProxy = environment.getProperty("sqs.sandbox.egress-proxy.no-proxy", "").trim();
        // Step 3 — same helper-broker config keys OciWarmer reads, so a cold job gets the identical broker
        // posture (enabled/timeout) a warm slot would have. HelperRouter is null on a mock/test double
        // that never stubs it; ColdContainerLauncher treats that the same as "broker disabled".
        this.coldContainerLauncher = new ColdContainerLauncher(runtime, helperRouter,
                environment.getProperty("sqs.sandbox.helper-broker.enabled", Boolean.class, true),
                environment.getProperty("sqs.sandbox.helper-broker.ready-timeout-sec", Integer.class, 10),
                ownership, egressManager,
                // The daemon-owned DNS pins (sqs.sandbox.dns.servers; blank → the public default set).
                // Parsed+validated once here; a malformed value throws on the FIRST job's argv build, never
                // silently resolving via the bridge gateway.
                com.studioquack.sqs.sandbox.egress.SandboxDns.parse(
                        environment.getProperty(
                                com.studioquack.sqs.sandbox.egress.SandboxDns.CONFIG_KEY,
                                com.studioquack.sqs.sandbox.egress.SandboxDns.DEFAULT_SERVERS)));
    }

    @Override
    public String name() {
        return "oci (" + (runtime.kind() == null ? "none" : runtime.kind().cli()) + ", "
                + runtime.effectiveTier().wireId() + ")";
    }

    @Override
    public boolean runtimeAvailable() {
        return runtime.available();
    }

    @Override
    public SandboxJobResponse execute(SandboxJobRequest job) {
        long start = System.nanoTime();
        SandboxLanguage lang = job.language();
        // A custom image owns its start argv: entrypoint mode execs the image's recorded entrypoint with
        // SQ_CONTEXT and skips the script bake + stdin bootstrap entirely. It needs NO script language: a
        // build-mode node (Script toggle off) persists no templateScript at all, so the image ref alone
        // decides.
        boolean entrypointMode = SandboxImageResolution.isEntrypointImage(job.ociImageRef(), lang, environment::getProperty);
        if (lang == null && !entrypointMode) {
            return SandboxJobResponse.failure(-1, "", "unresolved language",
                    Map.of(), SandboxJobResponse.ERROR_UNSUPPORTED_LANGUAGE,
                    "no sandbox language resolved for the node", elapsedMs(start), null,
                    SandboxJobResponse.FailureKind.INFRA);
        }
        String image = resolveImage(job, lang);
        if (image == null) {
            return SandboxJobResponse.failure(-1, "", "no image for language " + lang.wire(),
                    Map.of(), SandboxJobResponse.ERROR_UNSUPPORTED_LANGUAGE,
                    "no OCI image configured for language " + lang.wire()
                            + " (set sqs.sandbox.image." + lang.wire() + ")", elapsedMs(start), null,
                    SandboxJobResponse.FailureKind.INFRA);
        }
        if (!runtime.available()) {
            return SandboxJobResponse.failure(-1, "", "no container runtime",
                    Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    "no container runtime available on this host (degraded to "
                            + runtime.effectiveTier().wireId() + ")", elapsedMs(start), null,
                    SandboxJobResponse.FailureKind.INFRA);
        }
        // Fail closed rather than silently running untrusted code under weaker isolation.
        Optional<String> isolationShortfall = runtime.isolationShortfall(job.isolationTier());
        if (isolationShortfall.isPresent()) {
            return SandboxJobResponse.failure(-1, "", "isolation floor not met",
                    Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    isolationShortfall.get(), elapsedMs(start), null,
                    SandboxJobResponse.FailureKind.INFRA);
        }
        // Fail closed when OPEN egress would attach to a networked sandbox but overlay enforcement is
        // UNAVAILABLE (never silently use the host default route). ONE volatile readiness() read;
        // NONE-egress jobs are air-gapped and always allowed.
        EgressPolicy resolvedEgress = job.egress() == null ? EgressPolicy.DEFAULT : job.egress();
        if (resolvedEgress != EgressPolicy.NONE && egressManager != null) {
            EgressReadiness readiness = egressManager.readiness();
            if (!readiness.internetAllowed()) {
                return SandboxJobResponse.failure(-1, "", "sandbox egress unavailable",
                        Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                        readiness.refusalMessage(), elapsedMs(start), null,
                        SandboxJobResponse.FailureKind.INFRA);
            }
            if (readiness.state() == EgressReadiness.State.DEGRADED) {
                // Startup already warned once; per-dispatch WARN would flood the warm-pool path.
                log.debug("sandbox.egress.degraded reason={}", readiness.reason());
            }
        }

        String helperToken = null;
        WarmSlot slot = null;
        ColdContainerLauncher.ColdContainer cold = null;
        // The bridge IP this dispatch programmed overlay grants for (null when the engine holds no
        // resolvable outbound grant). Cleared in finally before the slot is released.
        String grantedBridgeIp = null;
        try {
            // Secrets stay in memory: rendered dotenv text rides stdin into the container tmpfs.
            SecretResolution secrets = secretResolver.resolveToEnv(job.secretsTree());
            String envFileContent = secrets.envContent();

            helperToken = registerHelperJob(job);
            if (helperToken != null) {
                final String token = helperToken;
                dispatchCancellationRegistry.bindHelperRevoke(job.jobId(), () -> helperRouter.revoke(token));
            }
            long acquireStart = System.nanoTime();
            // Image-keyed acquire: entrypoint mode passes the JOB's caps (used only when the image has no
            // registered warm-target caps — the pool's registered caps win); language mode passes null (shared
            // language slots stay as today). A null language is legal in entrypoint mode (build-mode node, no
            // templateScript) — the pool key is the image.
            slot = warmPool.acquire(image, lang == null ? null : lang.wire(), entrypointMode ? jobCaps(job) : null).orElse(null);
            long warmAcquireMs = elapsedMs(acquireStart);
            boolean warm = slot != null && slot.hasContainer();

            // Step 3 — a warm-pool miss no longer runs the job as the container's own main process (which left
            // it with no second process to exec a broker into). Instead it gets the SAME shape a warm slot
            // has: create the container detached + hardened with this job's own posture, plant+launch the
            // broker in it, THEN exec the job in — see ColdContainerLauncher for the "no leaked container on
            // any exit path" guarantee.
            long coldCreateMs = 0L;
            if (!warm) {
                long createStart = System.nanoTime();
                cold = createColdContainer(job, lang, image, entrypointMode);
                coldCreateMs = elapsedMs(createStart);
                slot = cold.toWarmSlot(lang == null ? "custom" : lang.wire(), image);
                if (entrypointMode) {
                    // A cold container of a custom image still needs the image's own start argv —
                    // capture it the same way OciWarmer does at warm time; an image with no
                    // entrypoint cannot serve a dispatch.
                    List<String> ep = ImageInspection.entrypointArgv(runtime.cli(), image);
                    if (ep.isEmpty()) {
                        throw new IllegalStateException("custom image '" + image
                                + "' declares no Entrypoint/Cmd — a custom execution image must define its own start command");
                    }
                    slot = slot.withEntrypoint(ep);
                }
            }

            // Correctness assert — a mismatch is a BUG, never a silent fallback.
            if (slot != null && slot.hasContainer() && slot.image() != null && !slot.image().isBlank()
                    && !image.equals(slot.image())) {
                throw new IllegalStateException("warm-pool slot image mismatch: slot runs '" + slot.image()
                        + "' but the job resolved '" + image + "'");
            }
            if (entrypointMode && slot != null && slot.hasContainer() && !slot.isEntrypointImage()) {
                throw new IllegalStateException("warm-pool handed a script-mode slot (no entrypoint) to an "
                        + "entrypoint-image job for '" + image + "'");
            }

            // Bind the live container to this dispatch BEFORE the (possibly long) blocking exec below, so a
            // concurrent cancel-instance command can reap it (podman kill/removal through the SAME teardown
            // this method already uses in `finally`) even while CliRunner.run is still blocked. Never re-adds
            // the container to the warm pool: killContainer only removes.
            if (slot != null && slot.hasContainer()) {
                dispatchCancellationRegistry.bindContainer(job.jobId(), slot.containerId(), this::killContainer);
            }

            // NONE-egress jobs have no container network at all: nothing to grant.
            if (slot != null && slot.hasContainer() && resolvedEgress != EgressPolicy.NONE
                    && egressManager != null && egressManager.holdsOutboundGrants()) {
                // Network Access Control, caller side: the engine's outbound grants open their
                // (peer, port) pairs for this container; the rest of the overlay stays
                // default-deny. An engine without a resolvable grant pays nothing here.
                grantedBridgeIp = containerBridgeIp(slot.containerId());
                if (grantedBridgeIp == null) {
                    throw new IllegalStateException("could not resolve the container's bridge "
                            + "address to program its overlay grants");
                }
                if (!egressManager.applyContainerGrants(grantedBridgeIp)) {
                    throw new IllegalStateException("overlay grants could not be programmed "
                            + "(host rule programming failed)");
                }
            }

            Map<String, String> containerEnv = containerEnv(job, helperToken, lang, slot);
            List<String> argv = buildArgv(job, lang, helperToken, slot, containerEnv);
            // Files ride the same stdin channel as the script and the .env, so an attachment or a spilled
            // parameter reaches the container's tmpfs without ever touching the host disk. Entrypoint images
            // skip the script bake + stdin bootstrap; context rides SQ_CONTEXT.
            byte[] stdin = slot.isEntrypointImage()
                    ? new byte[0]
                    : ContainerBootstrap.stdinPayload(job.source(), envFileContent,
                            job.attachedFiles().stream()
                                    .map(f -> new ContainerBootstrap.SandboxFile(f.name(), f.content()))
                                    .toList());
            Duration deadline = deadlineFor(job.limits());

            long runStart = System.nanoTime();
            // Values ride the podman child environment, never -e NAME=value.
            CliRunner.Result result = CliRunner.run(argv, stdin, deadline,
                    stdoutCap(job.limits()), stderrCap(job.limits()), containerEnv);
            // Exec has returned: the pool thread is about to persist/resume locally. A racing cancel-instance
            // must not interrupt it (ClosedByInterruptException closes the store).
            dispatchCancellationRegistry.markEnginePhase(job.jobId());
            long containerMs = elapsedMs(runStart);
            long totalMs = elapsedMs(start);

            // Per-dispatch timing so a slow node is diagnosable: WARM (exec into an already-ready slot) vs
            // COLD (this dispatch paid create+broker itself — coldCreateMs is that cost, reported honestly
            // instead of folded into containerMs as the old single `run --rm` did), whether the shared dep
            // cache is mounted (depCache=false → the script pays a per-run dependency install, the usual
            // "python is slow" cause), whether the persistent runner or the helper broker is up, the warm-slot
            // acquire cost, the exec wall time, and the total.
            boolean runner = warm && usesRunner(job, lang, slot);
            log.info("sandbox.timing job={} node={} lang={} path={} runner={} broker={} entrypoint={} "
                            + "depCacheMounted={} warmAcquireMs={} coldCreateMs={} containerMs={} totalMs={}",
                    job.jobId(), job.currentNodeId(), lang == null ? "custom" : lang.wire(),
                    warm ? "warm-exec" : "cold-create-exec", runner, slot.brokerReady(),
                    slot.isEntrypointImage(),
                    depCacheHostDir(lang) != null, warmAcquireMs, coldCreateMs, containerMs, totalMs);

            return toResponse(job, result, helperToken, slot, totalMs);
        } catch (Exception e) {
            // Podman/sandbox SETUP failure (secrets, helper registration, warm/cold container create, broker
            // plant/launch, or the exec itself) — CONTAINER-kind by definition, never a user script problem.
            // The redacted exception detail IS the "operator container diagnostics" this failure class
            // promises: e.getMessage() only ever reaches an operator via redactor.redact(), and the classified
            // errorMessage never repeats the raw text.
            String diagnostics = redactor.redact(String.valueOf(e.getMessage()), job.secretLiterals());
            log.warn("sandbox.dispatch.error job={} node={}: {}", job.jobId(), job.currentNodeId(),
                    e.getClass().getSimpleName() + ": " + diagnostics);
            return SandboxJobResponse.failure(-1, "", diagnostics,
                    Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    "sandbox dispatch failed: " + e.getClass().getSimpleName() + ": " + diagnostics,
                    elapsedMs(start), null, SandboxJobResponse.FailureKind.CONTAINER);
        } finally {
            // Order matters: revoke the token (no more callbacks) BEFORE tearing down the container that could
            // still be relaying one. A cold container is torn down here unconditionally — on the happy path,
            // on a wall-clock timeout, AND on any exception above, including one thrown by
            // buildArgv/CliRunner.run itself — so the `--rm` guarantee the old single `run --rm` path had is
            // never weaker now that create and exec are two steps.
            if (helperToken != null) {
                helperRouter.revoke(helperToken);
            }
            // Unbind BEFORE tearing down: narrows the race window in which a concurrent cancel-instance
            // could try to kill a container this method is already removing (harmless either way —
            // both teardown paths are idempotent — but this keeps the registry's view fresh sooner).
            dispatchCancellationRegistry.unbindContainer(job.jobId());
            // Retract this dispatch's overlay grants before the slot returns to the pool: the next
            // job on this warm container gets the engine's grants as they stand then.
            if (grantedBridgeIp != null && egressManager != null) {
                egressManager.clearContainerGrants(grantedBridgeIp);
            }
            if (cold != null) {
                coldContainerLauncher.release(cold);
            } else if (slot != null) {
                warmPool.release(slot);
            }
        }
    }

    /**
     * Graceful then forced teardown of a container this dispatch created: {@code podman kill -s TERM},
     * wait up to 3 s, then the SAME {@link WarmPool#forceKill} + {@link
     * ColdContainerLauncher#forceKill} path {@link #execute}'s {@code finally} already uses — never by
     * re-adding it to the warm pool. Tries both backstops unconditionally (warm vs cold is unknown
     * from outside {@link #execute}); both are idempotent.
     */
    void killContainer(String containerId) {
        if (containerId == null || containerId.isBlank()) {
            return; // "missing container" is a successful no-op, not a failure
        }
        try {
            sendSigterm(containerId);
            if (waitUntilGone(containerId)) {
                return; // process already exited — skip SIGKILL/rm
            }
        } catch (RuntimeException e) {
            log.debug("sandbox.cancel.term skipped container={}: {}", containerId, e.toString());
        }
        try {
            warmPool.forceKill(containerId);
        } catch (RuntimeException e) {
            log.debug("sandbox.cancel.kill warm-pool teardown skipped container={}: {}",
                    containerId, e.toString());
        }
        try {
            coldContainerLauncher.forceKill(containerId);
        } catch (RuntimeException e) {
            log.debug("sandbox.cancel.kill cold-launcher teardown skipped container={}: {}",
                    containerId, e.toString());
        }
    }

    /** {@code podman kill -s TERM <id>} via the same CLI this class already uses. Never throws. */
    void sendSigterm(String containerId) {
        String cli = runtime == null ? null : runtime.cli();
        if (cli == null || cli.isBlank()) {
            return;
        }
        try {
            CliRunner.run(List.of(cli, "kill", "-s", "TERM", containerId), Duration.ofSeconds(2));
        } catch (RuntimeException e) {
            log.debug("sandbox.cancel.term failed container={}: {}", containerId, e.toString());
        }
    }

    /** Polls up to 3 s for the container to stop or disappear. {@code true} = gone (skip forceKill). */
    boolean waitUntilGone(String containerId) {
        String cli = runtime == null ? null : runtime.cli();
        if (cli == null || cli.isBlank()) {
            return false;
        }
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
        if (!containerRunning(cli, containerId)) {
            return true;
        }
        while (System.nanoTime() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (!containerRunning(cli, containerId)) {
                return true;
            }
        }
        return !containerRunning(cli, containerId);
    }

    /** {@code podman inspect .State.Running}; inspect failure means the container is gone. */
    boolean containerRunning(String cli, String containerId) {
        try {
            CliRunner.Result r = CliRunner.run(
                    List.of(cli, "inspect", "-f", "{{.State.Running}}", containerId),
                    Duration.ofSeconds(2));
            return r.ok() && "true".equals(r.stdout() == null ? "" : r.stdout().trim());
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** The container's address on its sandbox network, via {@code podman inspect} (null-safe). */
    private String containerBridgeIp(String containerId) {
        if (containerId == null || containerId.isBlank()) {
            return null;
        }
        try {
            CliRunner.Result r = CliRunner.run(
                    List.of(runtime.cli(), "inspect", "-f",
                            "{{range $k, $v := .NetworkSettings.Networks}}{{$v.IPAddress}}{{\"\\n\"}}{{end}}",
                            containerId),
                    Duration.ofSeconds(10));
            if (!r.ok() || r.stdout() == null) {
                return null;
            }
            for (String line : r.stdout().split("\\R")) {
                String ip = line.trim();
                if (!ip.isEmpty()) {
                    return ip;
                }
            }
            return null;
        } catch (Exception e) {
            log.debug("sandbox.egress: inspect for bridge IP failed container={}: {}", containerId, e.toString());
            return null;
        }
    }

    /**
     * Resolves this job's own hardening posture (isolation tier, cpu cap, dep-cache mount, egress,
     * limits) exactly as the old cold `run --rm` branch of {@code buildArgv} did, then hands it to
     * {@link ColdContainerLauncher#create} to materialise a detached, broker-backed container.
     */
    private ColdContainerLauncher.ColdContainer createColdContainer(SandboxJobRequest job, SandboxLanguage lang,
                                                                     String image, boolean entrypointMode) {
        IsolationTier tier = runtime.resolveEffective(job.isolationTier());
        // Prefer the per-node vCPU cap wired from the template's executionRequirements.cpu (a
        // previously-ignored gap) over the daemon-global sqs.sandbox.max-cpus fallback.
        double cpus = job.cpus() > 0 ? job.cpus() : maxCpus;
        return coldContainerLauncher.create(lang, image, tier,
                job.egress() == null ? EgressPolicy.DEFAULT : job.egress(),
                job.limits(), cpus, job.diskMb(),
                SeccompProfile.profilePath(runtime.isolationEnforced()), depCacheHostDir(lang),
                deadlineFor(job.limits()).toMillis(), entrypointMode);
    }

    /**
     * Job-supplied caps for an entrypoint-image warm acquire. Null when every dimension is unset
     * ({@code <= 0}) so the pool keeps its registered / uncapped posture; otherwise each positive
     * field is forwarded and zeros stay {@code null}.
     */
    private static WarmTargetEntry.Caps jobCaps(SandboxJobRequest job) {
        double cpu = job.cpus();
        int ramMb = job.limits().ramMb();
        int diskMb = job.diskMb();
        if (cpu <= 0 && ramMb <= 0 && diskMb <= 0) {
            return null;
        }
        return new WarmTargetEntry.Caps(
                cpu > 0 ? cpu : null,
                ramMb > 0 ? ramMb : null,
                diskMb > 0 ? diskMb : null);
    }

    /* ------------------------------------------------------------------ */
    /* argv + run wiring                                                  */
    /* ------------------------------------------------------------------ */

    /** Builds the {@code exec} argv into an already-running, hardened container. */
    /**
     * Test seam: argv is name-only {@code -e}; {@code processEnv} is what the podman child process
     * receives.
     */
    record ExecLaunch(List<String> argv, Map<String, String> processEnv) {
    }

    ExecLaunch execLaunch(SandboxJobRequest job, SandboxLanguage lang, String helperToken, WarmSlot slot) {
        Map<String, String> env = containerEnv(job, helperToken, lang, slot);
        return new ExecLaunch(buildArgv(job, lang, helperToken, slot, env), env);
    }

    private List<String> buildArgv(SandboxJobRequest job, SandboxLanguage lang,
                                   String helperToken, WarmSlot slot, Map<String, String> env) {
        // Entrypoint slots never touch the language bootstrap (no script file to build).
        List<String> command = slot.isEntrypointImage() ? List.of() : ContainerBootstrap.command(lang);

        List<String> exec = new ArrayList<>();
        exec.add(runtime.cli());
        exec.add("exec");
        exec.add("-i");
        exec.add("-w");
        exec.add(HardeningArgs.SANDBOX_DIR);
        // Bare -e NAME. Podman copies the value from its own environment.
        com.studioquack.sqs.sandbox.runtime.ContainerEnv.appendNameOnly(exec, env);
        exec.add(slot.containerId());
        // Entrypoint image: the slot's recorded Entrypoint+Cmd IS the start argv. Runner path: dispatch
        // into the pre-warmed persistent interpreter via the tiny client so the job reuses
        // already-imported modules (no per-run `import openai/httpx/.`).
        if (slot.isEntrypointImage()) {
            exec.addAll(slot.entrypoint());
        } else if (usesRunner(job, lang, slot)) {
            exec.addAll(lang == SandboxLanguage.PYTHON
                    ? PythonRunner.clientCommand(lang) : NodeRunner.clientCommand(lang));
        } else {
            exec.addAll(command);
        }
        return exec;
    }

    /**
     * Whether this job runs in the slot's pre-started runner. A Node job that sets a startup-only
     * variable gets a fresh process instead: the runner was started before the job's env existed.
     */
    private boolean usesRunner(SandboxJobRequest job, SandboxLanguage lang, WarmSlot slot) {
        if (slot == null || slot.isEntrypointImage() || !slot.runnerReady()) {
            return false;
        }
        // The runner was started with its slot's interpreter flags (TypeScript strips types), so a
        // slot warmed for another language (shared image override) takes the cold bootstrap.
        if (SandboxLanguage.fromWire(slot.language()) != lang) {
            return false;
        }
        if (lang == SandboxLanguage.PYTHON) {
            return true;
        }
        return NodeRunner.supports(lang) && !NodeRunner.needsFreshProcess(
                job.env(), !PreloadModules.csvFor(depsHolder, lang).isEmpty());
    }

    /** Env vars injected into the sandbox — UDS helper socket + per-job bearer (pipe path). */
    private Map<String, String> containerEnv(SandboxJobRequest job, String helperToken, SandboxLanguage lang,
                                             WarmSlot slot) {
        Map<String, String> env = new LinkedHashMap<>();
        if (helperToken != null) {
            // Router is in-JVM and always up — bearer injection is unconditional when a token exists.
            env.put("SQ_HELPER_BEARER", helperToken);
            env.put("SQVM_HELPER_TOKEN", helperToken);
            // The in-container UDS a helper broker (started at warm/cold create, see OciWarmer /
            // ColdContainerLauncher / HelperPipeChannel) binds. Prefer the slot's own recorded path when its
            // broker is confirmed up; otherwise fall back to the well-known constant.
            String socketPath = (slot != null && slot.brokerReady() && !slot.helperSocketPath().isBlank())
                    ? slot.helperSocketPath() : HelperBroker.SOCKET_PATH;
            env.put("SQ_HELPER_SOCKET", socketPath);
        }
        env.put("SQVM_LLM_JOB_MODE", job.jobMode());
        if (job.envId() != null && !job.envId().isBlank()) {
            env.put("SQ_HELPER_CALLER_ENV_ID", job.envId());
        } else if (!callerEnvId.isBlank()) {
            env.put("SQ_HELPER_CALLER_ENV_ID", callerEnvId);
        }
        // Make the RO-mounted shared dep cache importable (Go the control plane parity). Without
        // PYTHONPATH / NODE_PATH the interpreter can't see /opt/sq/deps/<bucket>, so scripts fail to
        // import pre-installed packages (and appear to need a re-install).
        applyDepImportEnv(env, lang);
        if (job.env() != null) {
            env.putAll(job.env());
        }
        // Context delivery for entrypoint images; the helper env (SQ_HELPER_SOCKET + bearers) is
        // unchanged.
        if (slot != null && slot.isEntrypointImage()
                && job.contextBase64() != null && !job.contextBase64().isBlank()) {
            env.put("SQ_CONTEXT", job.contextBase64());
        }
        applyEgressProxyEnv(env);
        return env;
    }

    /**
     * Points sandbox internet traffic at the platform egress proxy so a third-party service never sees
     * this host's real IP. Applied AFTER {@code job.env()} on purpose: the proxy is a platform
     * control, so a job-supplied variable must not be able to unset it.
     */
    private void applyEgressProxyEnv(Map<String, String> env) {
        if (egressProxyUrl.isBlank()) {
            return;
        }
        if (egressProxyProbe != null && !egressProxyProbe.shouldInject()) {
            egressProxyProbe.noteInjectionSkipped();
            log.debug("sandbox.egress.proxy: DOWN ({}) — dispatching with direct egress, "
                    + "no proxy env injected", egressProxyProbe.snapshot().reason());
            return;
        }
        // Both cases are required: curl/pip/requests read the lower-case names, while many SDKs and
        // JVM/Go toolchains only read the upper-case ones.
        env.put("HTTP_PROXY", egressProxyUrl);
        env.put("http_proxy", egressProxyUrl);
        env.put("HTTPS_PROXY", egressProxyUrl);
        env.put("https_proxy", egressProxyUrl);
        String noProxy = noProxyList();
        env.put("NO_PROXY", noProxy);
        env.put("no_proxy", noProxy);
    }

    /** Loopback always bypasses the proxy; operators may add more via config. */
    private String noProxyList() {
        LinkedHashSet<String> hosts = new LinkedHashSet<>();
        hosts.add("localhost");
        hosts.add("127.0.0.1");
        for (String extra : egressNoProxy.split(",")) {
            String host = extra.trim();
            if (!host.isEmpty()) {
                hosts.add(host);
            }
        }
        return String.join(",", hosts);
    }

    /**
     * Sets the interpreter import path for the shared dep cache and the preload-module hint, when a
     * dep bucket exists for this language. python → {@code PYTHONPATH=/opt/sq/deps/python}
     * (+{@code PYTHONNOUSERSITE}); js/ts → {@code NODE_PATH=/opt/sq/deps/node/node_modules}. Preload
     * modules (from the manifest, {@code preload=true} for this language) are passed as
     * {@code SQ_PRELOAD_MODULES} so the helper preamble imports them up-front.
     */
    private void applyDepImportEnv(Map<String, String> env, SandboxLanguage lang) {
        if (lang == null || depCacheHostDir(lang) == null) {
            return; // no bucket dir on disk → no mount → nothing to point at
        }
        String containerDepDir = HardeningArgs.depContainerDir(lang); // /opt/sq/deps/<bucket>
        String w = lang.wire();
        if ("python".equals(w)) {
            env.put("PYTHONPATH", containerDepDir);
            env.put("PYTHONNOUSERSITE", "1");
        } else if ("javascript".equals(w) || "typescript".equals(w) || "node".equals(w)) {
            // npm --prefix installs under <bucket>/node_modules; NODE_PATH must point there.
            env.put("NODE_PATH", containerDepDir + "/node_modules");
        }
        String preload = PreloadModules.csvFor(depsHolder, lang);
        if (!preload.isBlank()) {
            env.put("SQ_PRELOAD_MODULES", preload);
            // Surface exactly which dependencies the helper preamble imports up-front (preload=true in
            // the manifest) and the dep-cache mount they resolve from — so a slow first import or a
            // missing mount is visible in the log.
            log.info("sandbox.preload lang={} depMount={} modules=[{}]", w, containerDepDir, preload);
        }
    }

    private String registerHelperJob(SandboxJobRequest job) {
        Map<String, Object> ec = job.executionContext() == null
                ? new LinkedHashMap<>() : new LinkedHashMap<>(job.executionContext());
        // Ensure the helper channel can serve the routing ids even if the ctx blob omitted them.
        ec.putIfAbsent("workflowExecutionId", job.workflowExecutionId());
        ec.putIfAbsent("currentNodeId", job.currentNodeId());
        if (job.envId() != null) {
            ec.putIfAbsent("envId", job.envId());
        }
        bridgeFunctionPortTransforms(job, ec);
        HelperJob helperJob = HelperJob.builder(job.jobId(), job.language() == null ? "custom" : job.language().wire())
                .executionContext(ec)
                .jobMode(job.jobMode())
                .secretLiterals(job.secretLiterals())
                .build();
        return helperRouter.register(helperJob);
    }

    /**
     * Bridges the function-port transform directives onto the helper job's server-side execution
     * context so {@code studioquack.execute()} can apply the request/response JOLT + script I/O
     * mapping between the caller's FUNCTION port and the target sub-workflow.
     */
    private static void bridgeFunctionPortTransforms(SandboxJobRequest job, Map<String, Object> ec) {
        Map<String, Object> inputVariables = job.inputVariables();
        if (inputVariables == null) {
            return;
        }
        Object directives = inputVariables.get("__sq_function_port_transforms__");
        if (directives != null) {
            ec.putIfAbsent("__sq_function_port_transforms__", directives);
        }
    }

    /* ------------------------------------------------------------------ */
    /* output parsing + response                                          */
    /* ------------------------------------------------------------------ */

    private SandboxJobResponse toResponse(SandboxJobRequest job, CliRunner.Result result,
                                          String helperToken, WarmSlot slot, long wallMs) {
        String rawStdout = result.stdout();
        String rawStderr = result.stderr();

        // Per-port outputs: helper-channel result wins; stdout marker fills gaps.
        Map<String, Object> outputs = mergeOutputs(helperToken, rawStdout);
        drainStdoutEvents(helperToken, rawStdout);

        String containerId = slot != null && slot.hasContainer() ? slot.containerId() : null;
        // Redact streams BEFORE they can be surfaced/persisted (per-port outputs stay raw → sealed store).
        // Also scrub the just-in-time-resolved secret literals for THIS job so a script that echoes a
        // bound password/token to stdout/stderr can never land it in the persisted sandbox-log.
        java.util.Set<String> secretLiterals = job.secretLiterals();
        String stdout = redactor.redact(rawStdout, secretLiterals);
        String stderr = redactor.redact(rawStderr, secretLiterals);

        // Typed error: an explicit studioquack.fail(code,msg) POST to /fail wins over the exit code, so a
        // business error routes to its per-code boundary even if the script exited 0.
        HelperFailure typed = typedFailure(helperToken);
        if (typed != null) {
            // An explicit studioquack.fail(code,msg) is always the user's OWN script/business logic
            // raising a typed error — SCRIPT, never CONTAINER/INFRA.
            return SandboxJobResponse.failure(result.exitCode(), stdout, stderr, outputs,
                    typed.errorCode(), redactor.redact(typed.message(), secretLiterals), wallMs, containerId,
                    SandboxJobResponse.FailureKind.SCRIPT);
        }

        if (result.timedOut()) {
            // A deadline is a runtime/container-level cutoff, not something the script itself raised.
            return SandboxJobResponse.failure(-1, stdout, stderr, outputs,
                    SandboxJobResponse.ERROR_DEADLINE,
                    "sandbox wall-clock deadline exceeded", wallMs, containerId,
                    SandboxJobResponse.FailureKind.CONTAINER);
        }
        if (result.exitCode() != 0) {
            // Untyped crash → the catch-all SANDBOX_FAILED code (unchanged), classified SCRIPT: the
            // container itself ran fine, it is the process inside it that exited non-zero.
            String detail = failureDetail("sandbox exited non-zero (" + result.exitCode() + ")", stderr, stdout);
            log.warn("sandbox.job.failed node={} job={} exit={} container={} detail={}",
                    job.currentNodeId(), job.jobId(), result.exitCode(),
                    containerId == null ? "<cold>" : containerId, detail);
            return SandboxJobResponse.failure(result.exitCode(), stdout, stderr, outputs,
                    SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    detail, wallMs, containerId, SandboxJobResponse.FailureKind.SCRIPT);
        }
        return SandboxJobResponse.success(result.exitCode(), stdout, stderr, outputs, wallMs, containerId);
    }

    private static String failureDetail(String base, String stderr, String stdout) {
        String err = compact(stderr);
        if (!err.isBlank()) {
            return base + ": " + err;
        }
        String out = compact(stdout);
        if (!out.isBlank()) {
            return base + " (stdout): " + out;
        }
        return base;
    }

    private static String compact(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String oneLine = value.replace('\r', '\n')
                .lines()
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .reduce((a, b) -> a + " | " + b)
                .orElse("");
        int max = 1200;
        return oneLine.length() <= max ? oneLine : oneLine.substring(0, max) + "...";
    }

    /** @return the typed error the SDK posted to {@code /fail} for this job, or {@code null}. */
    private HelperFailure typedFailure(String helperToken) {
        if (helperToken == null) {
            return null;
        }
        return helperRouter.lookup(helperToken)
                .map(HelperJob::failure)
                .filter(HelperFailure::hasCode)
                .orElse(null);
    }

    /**
     * Folds {@code sqEvent:::} stdout markers into the same job log/metric collections the socket
     * transport writes to, so a caller cannot tell which transport an event arrived on. This is how
     * the emit-only SDKs reach the daemon.
     */
    // Package-private so the drain can be asserted directly: a silently-dropped event is the exact
    // failure this transport exists to eliminate, so the wiring itself has to be under test.
    void drainStdoutEvents(String helperToken, String rawStdout) {
        if (helperToken == null) {
            return;
        }
        List<Map<String, Object>> events = StdoutMarkerParser.events(rawStdout);
        if (events.isEmpty()) {
            return;
        }
        Optional<HelperJob> found = helperRouter.lookup(helperToken);
        if (found.isEmpty()) {
            return;
        }
        HelperJob job = found.get();
        for (Map<String, Object> event : events) {
            Map<String, Object> entry = new LinkedHashMap<>(event);
            Object kind = entry.remove("kind");
            if (kind instanceof String throwKind && helperRouter.foldStdoutEvent(job, throwKind, entry)) {
                continue;
            }
            entry.putIfAbsent("at", java.time.format.DateTimeFormatter.ISO_INSTANT
                    .format(java.time.Instant.now()));
            if ("metric".equals(kind)) {
                job.metrics().add(entry);
            } else if ("log".equals(kind)) {
                entry.putIfAbsent("level", "info");
                job.tryAppendLog(entry);
            }
            // Any other kind (e.g. "partial") is dropped: emit-partial has no daemon-side sink yet,
            // and inventing one here would diverge from the socket transport this must mirror.
        }
    }

    /** Merges the modern helper-channel result (wins) with the legacy stdout markers (fallback). */
    private Map<String, Object> mergeOutputs(String helperToken, String rawStdout) {
        Map<String, Object> out = new LinkedHashMap<>();
        // Legacy stdout markers first (so the helper result overwrites on conflict).
        StdoutMarkerParser.parse(rawStdout).forEach(out::put);
        if (helperToken != null) {
            Optional<HelperJob> job = helperRouter.lookup(helperToken);
            if (job.isPresent() && job.get().result() != null) {
                HelperResult r = job.get().result();
                if (r.items() != null) {
                    for (HelperResult.ResultItem item : r.items()) {
                        if (item != null && item.port() != null) {
                            out.put(item.port(), item.value());
                        }
                    }
                }
                if ((r.items() == null || r.items().isEmpty()) && r.payload() != null) {
                    out.putAll(r.payload());
                }
            }
        }
        return out;
    }

    /* ------------------------------------------------------------------ */
    /* image + config                                                     */
    /* ------------------------------------------------------------------ */

    /**
     * Node {@code ociImageRef} (non-blank) → {@code sqs.sandbox.image.<lang>} property → {@code
     * lang.defaultImage()}. Fixes the live defect where a node's custom image was silently ignored on
     * the ephemeral path.
     */
    private String resolveImage(SandboxJobRequest job, SandboxLanguage lang) {
        return SandboxImageResolution.resolve(
                job == null ? null : job.ociImageRef(), lang, environment::getProperty);
    }

    private String depCacheHostDir(SandboxLanguage lang) {
        // Null language is legal for entrypoint-mode jobs (custom image, no script language):
        // a language-less job has no dep bucket to mount.
        if (lang == null || depCacheRoot == null || depCacheRoot.isBlank()) {
            return null;
        }
        // Resolve by dep BUCKET (js/ts share the "node" install dir), matching the installer layout
        // and HardeningArgs.depContainerDir — this fixes the JS/TS mount being silently skipped.
        java.io.File dir = new java.io.File(depCacheRoot, HardeningArgs.depBucket(lang));
        return dir.isDirectory() ? dir.getAbsolutePath() : null;
    }

    private Duration deadlineFor(SandboxLimits limits) {
        long wall = limits != null && limits.wallMs() > 0 ? limits.wallMs() : defaultWallMs;
        // A non-positive wall is the documented "unbounded" posture — Duration.ZERO makes
        // CliRunner await the process with no deadline rather than imposing a default cutoff.
        return wall > 0 ? Duration.ofMillis(wall) : Duration.ZERO;
    }

    private int stdoutCap(SandboxLimits limits) {
        return limits != null && limits.stdoutBytes() > 0 ? limits.stdoutBytes() : 256 * 1024;
    }

    private int stderrCap(SandboxLimits limits) {
        return limits != null && limits.stderrBytes() > 0 ? limits.stderrBytes() : 256 * 1024;
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
