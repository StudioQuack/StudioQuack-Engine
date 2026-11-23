package com.studioquack.sqs.sandbox.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.IsolationTier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Resolves and caches the strongest available OCI container runtime on the host and maps it to one
 * of the three isolation tiers (best/intermediate/trusted). Podman-only, but honest about the
 * host.
 */
@Slf4j
@Component
public class ContainerRuntime {

    /** Throttle window for the lazy re-probe when the cached report says the runtime is absent. */
    private static final long REPROBE_INTERVAL_MS = 15_000L;

    private final ObjectMapper mapper;
    private final IsolationTier requestedTier;
    private final boolean isolationEnforced;
    private final AtomicReference<CapabilityReport> report = new AtomicReference<>();
    private final PodmanRecoveryService recoveryService;
    /** Reason of the last degraded-tier WARN; re-probes with the same reason log at DEBUG. */
    private volatile String lastDegradedReason;
    /** Wall-clock of the last (re-)detection, so the lazy re-probe stays throttled. */
    private volatile long lastProbeAtMs;

    @Autowired
    public ContainerRuntime(ObjectMapper mapper,
                            PodmanRecoveryService recoveryService,
                            @Value("${sqs.sandbox.isolation-tier:auto}") String requestedTierWire,
                            @Value("${sqs.sandbox.isolation-enforce:false}") boolean isolationEnforce) {
        this.mapper = mapper;
        this.recoveryService = recoveryService;
        this.requestedTier = IsolationTier.fromWire(requestedTierWire);
        this.isolationEnforced = isolationEnforce;
        this.report.set(detect());
        this.lastProbeAtMs = System.currentTimeMillis();
    }

    /** Test-convenience constructor — uses a default (no-op on non-Windows) recovery service. */
    public ContainerRuntime(ObjectMapper mapper, String requestedTierWire, boolean isolationEnforce) {
        this(mapper, new PodmanRecoveryService(), requestedTierWire, isolationEnforce);
    }

    /** Re-runs detection (the operator's "Re-detect" action) and returns the fresh report. */
    public CapabilityReport redetect() {
        CapabilityReport fresh = detect();
        report.set(fresh);
        lastProbeAtMs = System.currentTimeMillis();
        return fresh;
    }

    /**
     * Marks the cached report as stale so the next {@link #available()} call triggers an immediate
     * re-probe. Used when a CLI call fails with a connection error — the runtime WAS available but
     * became unreachable (e.g. after Windows sleep).
     */
    public void invalidate() {
        // Set lastProbeAtMs far enough back that the next available() check triggers reprobe.
        lastProbeAtMs = 0L;
        // Update the cached report to mark the runtime as unavailable so the re-probe path fires.
        CapabilityReport current = report.get();
        if (current.available()) {
            report.set(new CapabilityReport(false, current.kind(), current.cliPath(),
                    current.serverVersion(), current.runtimes(), current.gvisorAvailable(),
                    false, current.requestedTier(), current.recommendedTier(),
                    IsolationTier.TRUSTED, "", "Runtime invalidated — pending re-probe after connection failure"));
        }
    }

    /** The current cached capability report. */
    public CapabilityReport report() {
        return report.get();
    }

    /**
     * True when a live container runtime is available (CLI on PATH + daemon reachable). When the
     * cached report says no runtime, this lazily re-probes (throttled) so a runtime that started AFTER
     * the daemon booted — e.g. a lagging {@code podman machine start} on macOS — is picked up
     * automatically, without a manual "Re-detect".
     */
    public boolean available() {
        CapabilityReport r = report.get();
        if (!r.available()) {
            maybeReprobe();
            r = report.get();
        }
        return r.available();
    }

    /** Throttled lazy re-detection used by {@link #available()} while the runtime is still absent. */
    private void maybeReprobe() {
        if (System.currentTimeMillis() - lastProbeAtMs < REPROBE_INTERVAL_MS) {
            return;
        }
        synchronized (this) {
            if (System.currentTimeMillis() - lastProbeAtMs < REPROBE_INTERVAL_MS
                    || report.get().available()) {
                return;
            }
            lastProbeAtMs = System.currentTimeMillis();
            CapabilityReport fresh = detect();
            if (fresh.available()) {
                report.set(fresh);
                log.info("sandbox: container runtime became available on re-probe — kind={} cli={} tier={}",
                        fresh.kind(), fresh.cliPath(), fresh.effectiveTier().wireId());
            } else if (recoveryService.isActive() && fresh.cliPath() != null) {
                // CLI exists but daemon unreachable on Windows → likely post-sleep WSL breakdown.
                // Attempt automatic recovery (wsl --shutdown + podman machine start).
                if (recoveryService.attemptRecovery(this)) {
                    // Recovery succeeded — report already updated by redetect() inside recovery.
                    log.info("sandbox: runtime recovered via PodmanRecoveryService");
                }
            }
        }
    }

    /** The resolved Podman CLI binary path (e.g. {@code /opt/homebrew/bin/podman}), or null. */
    public String cli() {
        return report.get().cliPath();
    }

    /** The Podman recovery service for post-sleep WSL/machine recovery (Windows only). */
    public PodmanRecoveryService recoveryService() {
        return recoveryService;
    }

    /** The resolved runtime kind, or null. */
    public OciRuntimeKind kind() {
        return report.get().kind();
    }

    /** The effective isolation tier the daemon dispatches under. */
    public IsolationTier effectiveTier() {
        return report.get().effectiveTier();
    }

    /**
     * The {@code --runtime} flag value for a given (or the effective) tier: {@code "runsc"} for
     * {@link IsolationTier#BEST}, empty otherwise (CLI default, typically {@code runc}).
     */
    public String runtimeFlagFor(IsolationTier tier) {
        IsolationTier t = tier == null ? effectiveTier() : resolveEffective(tier);
        return t == IsolationTier.BEST ? "runsc" : "";
    }

    /** True when this daemon refuses to dispatch below its isolation floor. */
    public boolean isolationEnforced() {
        return isolationEnforced;
    }

    /**
     * The isolation floor when enforcement is on: the operator's pinned tier, or {@link
     * IsolationTier#BEST} when the pin is {@code auto}. Resolving {@code auto} to BEST is what makes
     * enforcement mean anything.
     *
     * @return The required floor, or null when enforcement is off.
     */
    public IsolationTier enforcedFloor() {
        return isolationEnforced ? (requestedTier == null ? IsolationTier.BEST : requestedTier) : null;
    }

    /**
     * Fail-closed isolation gate. When enforcement is on and the host cannot deliver the floor, the
     * job must be **rejected** instead of quietly running under a weaker sandbox — that silent degrade
     * is finding F-10.
     *
     * @return The reason dispatch must not proceed, or empty when it may.
     */
    public Optional<String> isolationShortfall(IsolationTier jobTier) {
        IsolationTier floor = enforcedFloor();
        if (floor == null) {
            return Optional.empty();
        }
        IsolationTier effective = resolveEffective(jobTier);
        if (effective.atLeastAsStrongAs(floor)) {
            return Optional.empty();
        }
        return Optional.of("isolation floor \"" + floor.wireId() + "\" is enforced but this host can "
                + "only provide \"" + effective.wireId() + "\""
                + (floor == IsolationTier.BEST
                        ? " — runsc (gVisor) is not registered with the active container CLI" : "")
                + "; refusing to run untrusted code under weaker isolation");
    }

    /**
     * Resolves a requested tier (or null=autodetect) to the effective tier, degrading when the
     * requested tier's dependency is missing — the exact semantics of Go
     * {@code selectAndRegisterBackend}.
     */
    public IsolationTier resolveEffective(IsolationTier requested) {
        CapabilityReport r = report.get();
        if (requested == null) {
            return r.recommendedTier();
        }
        return switch (requested) {
            case BEST -> r.gvisorAvailable() ? IsolationTier.BEST
                    : (r.containerCliAvailable() ? IsolationTier.INTERMEDIATE : IsolationTier.TRUSTED);
            case INTERMEDIATE -> r.containerCliAvailable() ? IsolationTier.INTERMEDIATE : IsolationTier.TRUSTED;
            case TRUSTED -> IsolationTier.TRUSTED;
        };
    }

    /** Full detection pass — resilient to a missing CLI / wedged daemon. */
    private CapabilityReport detect() {
        OciRuntimeDetector.ResolvedCli resolved = OciRuntimeDetector.resolveCli(null);
        if (resolved == null) {
            logDegraded("cli-missing", "sandbox: Podman CLI not found — degrading to TRUSTED (shared-kernel) tier; "
                    + "live-container dispatch unavailable. Install Podman to enable sandboxing.");
            return trustedOnly("Podman CLI not on PATH");
        }
        String version = OciRuntimeDetector.probeDaemon(resolved.cliPath());
        if (version == null) {
            logDegraded("daemon-down", "sandbox: {} CLI found at {} but the daemon did not answer the liveness probe — "
                            + "degrading to TRUSTED tier until it is reachable.",
                    resolved.kind(), resolved.cliPath());
            CapabilityReport partial = trustedOnly(resolved.kind() + " CLI present but daemon unreachable");
            return new CapabilityReport(false, resolved.kind(), resolved.cliPath(), null,
                    List.of(), false, false, requestedTier, IsolationTier.TRUSTED,
                    IsolationTier.TRUSTED, "", partial.advisory());
        }

        List<DetectedOciRuntime> runtimes = OciRuntimeDetector.detectRuntimes(
                resolved.cliPath(), resolved.kind(), mapper);
        boolean gvisor = runtimes.stream().anyMatch(rt -> rt.gvisor() && !"path".equals(rt.source()));
        boolean cliAvailable = true;

        IsolationTier recommended = gvisor ? IsolationTier.BEST : IsolationTier.INTERMEDIATE;
        IsolationTier effective = requestedTier == null ? recommended : resolveTier(requestedTier, gvisor, true);
        String advisory = buildAdvisory(requestedTier, effective, gvisor);
        String runtimeFlag = effective == IsolationTier.BEST ? "runsc" : "";

        log.info("sandbox: container runtime detected kind={} version={} runtimes={} gvisor={} "
                        + "requestedTier={} effectiveTier={} runtimeFlag={}",
                resolved.kind(), version,
                runtimes.stream().map(DetectedOciRuntime::name).toList(), gvisor,
                requestedTier == null ? "auto" : requestedTier.wireId(), effective.wireId(),
                runtimeFlag.isEmpty() ? "<default>" : runtimeFlag);

        lastDegradedReason = null;
        return new CapabilityReport(true, resolved.kind(), resolved.cliPath(), version, runtimes,
                gvisor, cliAvailable, requestedTier, recommended, effective, runtimeFlag, advisory);
    }

    /** Re-probes run every few seconds while Podman is down; only a change of reason is news. */
    private void logDegraded(String reason, String message, Object... args) {
        if (reason.equals(lastDegradedReason)) {
            log.debug(message, args);
            return;
        }
        lastDegradedReason = reason;
        log.warn(message, args);
    }

    private IsolationTier resolveTier(IsolationTier requested, boolean gvisor, boolean cli) {
        return switch (requested) {
            case BEST -> gvisor ? IsolationTier.BEST : (cli ? IsolationTier.INTERMEDIATE : IsolationTier.TRUSTED);
            case INTERMEDIATE -> cli ? IsolationTier.INTERMEDIATE : IsolationTier.TRUSTED;
            case TRUSTED -> IsolationTier.TRUSTED;
        };
    }

    private String buildAdvisory(IsolationTier requested, IsolationTier effective, boolean gvisor) {
        if (requested == IsolationTier.BEST && effective != IsolationTier.BEST) {
            return "Isolation tier \"best\" (gVisor) was requested but runsc is not registered with "
                    + "the active container CLI. Daemon degraded to \"" + effective.wireId()
                    + "\". Install gVisor and re-detect to clear this advisory.";
        }
        return "";
    }

    private CapabilityReport trustedOnly(String reason) {
        String advisory = "Sandbox degraded to TRUSTED (shared kernel): " + reason
                + ". Install Podman and re-detect for namespace isolation.";
        return new CapabilityReport(false, null, null, null, List.of(), false, false,
                requestedTier, IsolationTier.TRUSTED, IsolationTier.TRUSTED, "", advisory);
    }
}
