package com.studioquack.sqs.sandbox.warmpool;

import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Wires the {@link WarmPool} bean. The warm reservoir is backed by a REAL Podman/OCI warmer
 * whenever a container runtime is present, so a target set from the environment Runtime UI
 * actually spins warm containers on demand (the previous behaviour gated the warmer behind {@code
 * warmpool.enabled} and defaulted to a {@link NoopWarmer}, so "Apply" queued a target but never
 * opened a container).
 */
@Slf4j
@Configuration
public class WarmPoolConfig {

    private ScheduledExecutorService scheduler;
    private ExecutorService refillExecutor;
    private WarmPool pool;
    private Duration shutdownGrace = Duration.ZERO;
    private com.studioquack.sqs.sandbox.runtime.PodmanMachineKeepAlive keepAlive;

    @Bean
    public WarmPool warmPool(ContainerRuntime runtime,
                             WarmTargetStore warmTargetStore,
                             com.studioquack.sqs.deps.DependencyManifestHolder depsHolder,
                             com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter helperRouter,
                             com.studioquack.sqs.sandbox.oci.SandboxOwnership ownership,
                             com.studioquack.sqs.sandbox.egress.SandboxEgressManager egressManager,
                             com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe gpuProbe,
                             com.studioquack.sqs.sandbox.runtime.PodmanMachineKeepAlive podmanKeepAlive,
                             Environment environment,
                             @Value("${sqs.sandbox.warmpool.shutdown-grace-ms:15000}") long shutdownGraceMs,
                             @Value("${sqs.sandbox.dep-cache-root:}") String depCacheRoot,
                             @Value("${sqs.sandbox.python-runner.enabled:true}") boolean pythonRunnerEnabled,
                             @Value("${sqs.sandbox.python-runner.ready-timeout-sec:120}") int runnerReadyTimeoutSec,
                             @Value("${sqs.sandbox.node-runner.enabled:true}") boolean nodeRunnerEnabled,
                             @Value("${sqs.sandbox.node-runner.ready-timeout-sec:120}") int nodeRunnerReadyTimeoutSec,
                             @Value("${sqs.sandbox.helper-broker.enabled:true}") boolean helperBrokerEnabled,
                             @Value("${sqs.sandbox.helper-broker.ready-timeout-sec:10}") int helperBrokerReadyTimeoutSec) {
        // Script-mode image → language map: effective image = property override else enum default.
        // OciWarmer uses this to decide bootstrap (runner/broker/dep-cache) vs entrypoint mode.
        Map<String, String> scriptModeImages = new LinkedHashMap<>();
        for (SandboxLanguage l : SandboxLanguage.values()) {
            String prop = environment.getProperty("sqs.sandbox.image." + l.wire());
            String eff = (prop != null && !prop.isBlank()) ? prop : l.defaultImage();
            if (eff == null || eff.isBlank()) {
                continue;
            }
            scriptModeImages.putIfAbsent(eff.trim(), l.wire());
        }

        // Operator defaults for warm slots with no per-image caps (0 = uncapped — today's posture;
        // per-image caps from a warm-target entry always win —).
        double defaultWarmCpus = environment.getProperty(
                "sqs.sandbox.warmpool.default-cpus", Double.class, 0.0);
        int defaultWarmMemoryMb = environment.getProperty(
                "sqs.sandbox.warmpool.default-memory-mb", Integer.class, 0);

        // Always back the reservoir with the OCI warmer. OciWarmer.warm() re-checks runtime availability
        // on EVERY call and throws a (gracefully-handled) WarmException when Podman is absent, so the
        // reconcile loop below self-heals the moment a late-starting Podman comes up
        // (ContainerRuntime.available() re-probes lazily).
        Warmer warmer = new OciWarmer(runtime, depCacheRoot, depsHolder,
                pythonRunnerEnabled, runnerReadyTimeoutSec,
                helperRouter, helperBrokerEnabled, helperBrokerReadyTimeoutSec, ownership,
                (Function<String, String>) scriptModeImages::get, defaultWarmCpus, defaultWarmMemoryMb,
                egressManager, gpuProbe,
                // The daemon-owned DNS pins (blank → the public default set; see application.yml). Warm slots are
                // created before their job is known, so the resolver posture can only ever come from this operator
                // config.
                com.studioquack.sqs.sandbox.egress.SandboxDns.parse(
                        environment.getProperty(
                                com.studioquack.sqs.sandbox.egress.SandboxDns.CONFIG_KEY,
                                com.studioquack.sqs.sandbox.egress.SandboxDns.DEFAULT_SERVERS)))
                .nodeRunner(nodeRunnerEnabled, nodeRunnerReadyTimeoutSec);

        // No boot targets: warm slots exist only where the operator set an explicit target.
        Map<String, Integer> targets = new LinkedHashMap<>();

        // Parallel FIFO refill: a small daemon pool warms replacement slots off the dispatch thread so
        // "consume one → destroy it → warm a fresh one" overlaps (the single-use container model).
        this.refillExecutor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "warmpool-refill");
            t.setDaemon(true);
            return t;
        });
        this.pool = new WarmPool(warmer, targets, refillExecutor, ownership);
        this.shutdownGrace = Duration.ofMillis(Math.max(0L, shutdownGraceMs));

        // Seed status rows for the four warmable languages' effective images (factory defaults).
        for (String lang : WarmPool.warmableLanguages()) {
            String prop = environment.getProperty("sqs.sandbox.image." + lang);
            SandboxLanguage sl = SandboxLanguage.fromWire(lang);
            String image = (prop != null && !prop.isBlank())
                    ? prop.trim()
                    : (sl == null ? null : sl.defaultImage());
            if (image == null || image.isBlank()) {
                continue;
            }
            // Factory-default display is 0 for every image — matches the no-pre-warm posture.
            pool.registerImage(image.trim(), lang, 0);
        }

        // Persisted operator entries win over boot defaults (apply after seeds so language hints stick).
        for (WarmTargetEntry e : warmTargetStore.load()) {
            pool.registerImage(e.imageRef(), e.language(), 0);
            pool.setTarget(e.imageRef(), e.language(), e.target(), e.caps());
        }

        log.info("warmpool: warmer={} bootTargets={} owner={} boot={} (runtime={})",
                warmer.kind(), targets, ownership.ownerId(), ownership.bootId(),
                runtime.available() ? runtime.cli() : "pending-podman");

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warmpool-reconciler");
            t.setDaemon(true);
            return t;
        });
        // Reconcile immediately, then periodically (Go warmer cadence). This re-warms slots the operator
        // adds via set-warm-target without waiting for the first UI poll AND fills the reservoir once a
        // late Podman is detected — each pass is a cheap no-op while the runtime is still absent (or no
        // target is set).
        scheduler.scheduleWithFixedDelay(safe(() -> pool.reconcile()), 0, 30, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(safe(() -> pool.reapIdle(Duration.ofMinutes(5))),
                60, 60, TimeUnit.SECONDS);

        // Start the Podman keepalive heartbeat (Windows only — no-op on Linux/macOS).
        this.keepAlive = podmanKeepAlive;
        podmanKeepAlive.start(runtime);

        return pool;
    }

    /**
     * Ordinary JVM shutdown (SIGTERM, {@code Ctrl-C}, a supervisor restart): Spring closes the
     * context, so this runs before the process leaves. The schedulers are stopped FIRST — a reconcile
     * or refill firing mid-drain would open a container nothing is left to close — and the drain then
     * gives in-flight dispatches {@code sqs.sandbox.warmpool.shutdown-grace-ms} to hand their slots
     * back before forcing the rest down.
     */
    @PreDestroy
    public void shutdown() {
        if (keepAlive != null) {
            keepAlive.stop();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (refillExecutor != null) {
            refillExecutor.shutdownNow();
        }
        if (pool != null) {
            pool.drain(shutdownGrace);
        }
    }

    /** Wraps a task so a reconcile/reap exception never kills the scheduled thread. */
    private Runnable safe(Runnable r) {
        return () -> {
            try {
                r.run();
            } catch (RuntimeException e) {
                log.warn("warmpool: scheduled task failed: {}", e.getMessage());
            }
        };
    }
}
