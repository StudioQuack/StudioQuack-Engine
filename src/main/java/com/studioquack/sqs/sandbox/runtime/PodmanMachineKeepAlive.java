package com.studioquack.sqs.sandbox.runtime;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Keeps the WSL-backed Podman machine alive and responsive by. Windows-only. */
@Slf4j
@Component
public final class PodmanMachineKeepAlive {

    /** Heartbeat interval — how often we ping podman to check liveness. */
    private static final long HEARTBEAT_INTERVAL_SEC = 10;

    /** Timeout for the heartbeat probe command. */
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(5);

    /**
     * The WSL config key that prevents idle VM shutdown. Default WSL2 vmIdleTimeout is 60000ms (60s in
     * recent builds) or never in older ones.
     */
    private static final String WSL_CONFIG_KEY = "vmIdleTimeout";
    private static final String WSL_CONFIG_VALUE = "-1";

    private final boolean windows;
    private final PodmanRecoveryService recoveryService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> heartbeatFuture;
    private volatile ContainerRuntime runtime;

    public PodmanMachineKeepAlive(PodmanRecoveryService recoveryService) {
        this.recoveryService = recoveryService;
        this.windows = System.getProperty("os.name", "").toLowerCase().contains("win");
    }

    /** Initializes the keep-alive. Called once the {@link ContainerRuntime} is ready. */
    public void start(ContainerRuntime containerRuntime) {
        if (!windows || !started.compareAndSet(false, true)) {
            return;
        }
        this.runtime = containerRuntime;

        // 1) Ensure .wslconfig has vmIdleTimeout=-1
        ensureWslIdleTimeoutDisabled();

        // 2) Start the heartbeat scheduler
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "podman-keepalive");
            t.setDaemon(true);
            return t;
        });
        heartbeatFuture = scheduler.scheduleWithFixedDelay(
                this::heartbeat, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);

        log.info("sandbox: PodmanMachineKeepAlive started — heartbeat every {}s, WSL vmIdleTimeout=-1 ensured",
                HEARTBEAT_INTERVAL_SEC);
    }

    /** Stops the heartbeat scheduler. Called on daemon shutdown. */
    public void stop() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        started.set(false);
    }

    /**
     * Lightweight heartbeat: runs {@code podman version --format {{.Server.Version}}} (same as
     * {@link OciRuntimeDetector#probeDaemon}) and triggers recovery on connection failure.
     */
    private void heartbeat() {
        if (runtime == null || !runtime.available()) {
            // Runtime not yet available — the ContainerRuntime's own re-probe handles this case.
            return;
        }
        String cli = runtime.cli();
        if (cli == null) {
            return;
        }
        CliRunner.Result r = CliRunner.run(
                List.of(cli, "version", "--format", "{{.Server.Version}}"), HEARTBEAT_TIMEOUT);
        if (r.ok()) {
            return; // Podman is alive — nothing to do.
        }
        // Probe failed — check if it's a post-sleep connection failure.
        if (recoveryService.isConnectionFailure(r)) {
            log.warn("sandbox: keepalive heartbeat detected Podman unreachable — triggering recovery");
            // Invalidate the cached report so other code paths know runtime is down.
            runtime.invalidate();
            // Trigger recovery immediately.
            recoveryService.attemptRecovery(runtime);
        } else {
            // Non-connection failure (e.g. podman version format error) — don't recover, just log.
            log.debug("sandbox: keepalive heartbeat failed (non-connection): exit={} stderr={}",
                    r.exitCode(), r.stderr().trim());
        }
    }

    // ──────────────────────────────────────────────────────────────────────────────────────────────
    // WSL .wslconfig management
    // ──────────────────────────────────────────────────────────────────────────────────────────────

    /**
     * Ensures {@code %USERPROFILE%\.wslconfig} contains {@code [wsl2]\nvmIdleTimeout=-1} so the Podman
     * machine's WSL distro never auto-shuts down due to inactivity. If the file exists and already has
     * the setting, does nothing.
     */
    private void ensureWslIdleTimeoutDisabled() {
        try {
            String userProfile = System.getenv("USERPROFILE");
            if (userProfile == null || userProfile.isBlank()) {
                log.debug("sandbox: USERPROFILE not set — skipping .wslconfig check");
                return;
            }
            Path wslConfig = Path.of(userProfile, ".wslconfig");
            if (Files.exists(wslConfig)) {
                String content = Files.readString(wslConfig, StandardCharsets.UTF_8);
                if (content.contains(WSL_CONFIG_KEY + "=")) {
                    // Key exists — check if it's already -1.
                    if (content.contains(WSL_CONFIG_KEY + "=" + WSL_CONFIG_VALUE)) {
                        log.debug("sandbox: .wslconfig already has vmIdleTimeout=-1");
                    } else {
                        log.info("sandbox: .wslconfig has vmIdleTimeout set to a non-infinite value. "
                                + "Consider setting vmIdleTimeout=-1 under [wsl2] to prevent Podman "
                                + "machine idle shutdown. Current file left unchanged (user override respected).");
                    }
                    return;
                }
                // Key is absent — append under [wsl2] section.
                String toAppend;
                if (content.toLowerCase().contains("[wsl2]")) {
                    // Section exists, just append the key after it.
                    toAppend = WSL_CONFIG_KEY + "=" + WSL_CONFIG_VALUE + System.lineSeparator();
                    content = content.replaceFirst("(?i)(\\[wsl2\\][^\\[]*)", "$1" + toAppend);
                } else {
                    // No [wsl2] section — append one at the end.
                    toAppend = System.lineSeparator() + "[wsl2]" + System.lineSeparator()
                            + WSL_CONFIG_KEY + "=" + WSL_CONFIG_VALUE + System.lineSeparator();
                    content = content + toAppend;
                }
                Files.writeString(wslConfig, content, StandardCharsets.UTF_8);
                log.info("sandbox: added vmIdleTimeout=-1 to .wslconfig — WSL will no longer auto-shutdown Podman machine on idle");
            } else {
                // File does not exist — create with minimal config.
                String content = "[wsl2]" + System.lineSeparator()
                        + WSL_CONFIG_KEY + "=" + WSL_CONFIG_VALUE + System.lineSeparator();
                Files.writeString(wslConfig, content, StandardCharsets.UTF_8);
                log.info("sandbox: created .wslconfig with vmIdleTimeout=-1 — WSL will not auto-shutdown Podman machine on idle");
            }
        } catch (IOException e) {
            log.warn("sandbox: failed to ensure .wslconfig vmIdleTimeout setting: {}", e.getMessage());
        } catch (Exception e) {
            log.warn("sandbox: unexpected error checking .wslconfig: {}", e.getMessage());
        }
    }
}

