package com.studioquack.sqs.sandbox.runtime;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/** Brings the WSL-backed Podman machine back when the daemon cannot reach it. Windows-only. */
@Slf4j
@Component
public final class PodmanRecoveryService {

    /** Max restart attempts within the sliding window before backing off. */
    private static final int MAX_ATTEMPTS = 3;

    /** Sliding window duration — attempts counter resets after this period. */
    private static final Duration WINDOW_DURATION = Duration.ofMinutes(10);

    /** Delay between wsl --shutdown and podman machine start (let WSL fully tear down). */
    private static final Duration WSL_SHUTDOWN_SETTLE = Duration.ofSeconds(5);

    /** Timeout for each short recovery subprocess (wsl/podman commands). */
    private static final Duration CMD_TIMEOUT = Duration.ofSeconds(60);

    /** Same bounds as the Windows installer: init downloads the machine image. */
    private static final Duration MACHINE_INIT_TIMEOUT = Duration.ofMinutes(30);
    private static final Duration MACHINE_START_TIMEOUT = Duration.ofMinutes(10);

    /** Quiet period after a failed create or a restart-required verdict. */
    static final Duration CREATE_RETRY_BACKOFF = Duration.ofMinutes(30);

    /** Installer defaults (Docling OCR preload needs several GiB); same env overrides. */
    static final int DEFAULT_MACHINE_MEMORY_MIB = 8192;
    static final int DEFAULT_MACHINE_CPUS = 4;

    /**
     * WSL imported the machine image but could not start the distribution: what WSL2 looks like
     * before Windows has been restarted after enabling it. Same signature as the installer.
     */
    static final Pattern RESTART_REQUIRED = Pattern.compile(
            "(?i)CreateInstance|E_FAIL|0xffffffff|rpm --restore|\\brestart\\b|\\breboot\\b");

    /**
     * Patterns that indicate a Podman connection failure caused by a stale WSL/SSH session
     * (post-sleep). Case-insensitive matching against combined stdout+stderr of failed podman calls.
     */
    private static final List<Pattern> CONNECTION_FAILURE_PATTERNS = List.of(
            Pattern.compile("(?i)connection.*forcibly closed"),
            Pattern.compile("(?i)handshake failed"),
            Pattern.compile("(?i)unable to connect to podman"),
            Pattern.compile("(?i)failed to connect"),
            Pattern.compile("(?i)connection refused"),
            Pattern.compile("(?i)wsarecv"),
            Pattern.compile("(?i)ssh:.*error"),
            Pattern.compile("(?i)broken pipe"),
            Pattern.compile("(?i)connection reset")
    );

    /** Seam over {@link CliRunner#run(List, Duration)} so tests can script podman/wsl answers. */
    @FunctionalInterface
    interface Cli {
        CliRunner.Result run(List<String> argv, Duration timeout);
    }

    enum MachineState { EXISTS, MISSING, UNKNOWN }

    private final boolean windows;
    private final Cli cli;
    private final Executor createExecutor;
    private final Duration wslSettle;
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final AtomicInteger attempts = new AtomicInteger(0);
    private volatile Instant windowStart = Instant.now();
    /** No recovery attempt (and no log line) before this instant. */
    private volatile Instant backoffUntil = Instant.EPOCH;
    private volatile boolean restartRequired;

    public PodmanRecoveryService() {
        this(System.getProperty("os.name", "").toLowerCase().contains("win"), CliRunner::run,
                task -> Thread.ofPlatform().daemon().name("podman-machine-init").start(task),
                WSL_SHUTDOWN_SETTLE);
    }

    /** Visible for testing — allows forcing the Windows flag. */
    PodmanRecoveryService(boolean forceWindows) {
        this(forceWindows, CliRunner::run, Runnable::run, WSL_SHUTDOWN_SETTLE);
    }

    /** Visible for testing — scripted CLI, a synchronous create executor and no settle wait. */
    PodmanRecoveryService(boolean forceWindows, Cli cli, Executor createExecutor, Duration wslSettle) {
        this.windows = forceWindows;
        this.cli = cli;
        this.createExecutor = createExecutor;
        this.wslSettle = wslSettle;
    }

    /**
     * Returns true when this service is active (Windows host). On other OSes recovery is not needed.
     */
    public boolean isActive() {
        return windows;
    }

    /** True while WSL2 waits for a Windows restart before the Podman machine can be created. */
    public boolean restartRequired() {
        return restartRequired;
    }

    /**
     * Checks whether the given CLI failure output indicates a post-sleep Podman connection issue.
     *
     * @param cliResult the result of a failed podman CLI call (non-zero exit or timeout)
     * @return true if the error matches known post-sleep connection failure patterns
     */
    public boolean isConnectionFailure(CliRunner.Result cliResult) {
        if (!windows || cliResult == null || cliResult.ok()) {
            return false;
        }
        String combined = (cliResult.stdout() + " " + cliResult.stderr()).trim();
        for (Pattern p : CONNECTION_FAILURE_PATTERNS) {
            if (p.matcher(combined).find()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to recover the Podman machine after a detected connection failure. Thread-safe: only
     * one recovery runs at a time; concurrent callers return immediately.
     *
     * @param containerRuntime The runtime to re-probe after recovery.
     * @return True if recovery succeeded and the runtime is available again.
     */
    public boolean attemptRecovery(ContainerRuntime containerRuntime) {
        if (!windows) {
            return false;
        }
        Instant now = Instant.now();
        if (now.isBefore(backoffUntil)) {
            return false;
        }

        // Sliding window: reset attempt counter if the window has elapsed.
        if (now.isAfter(windowStart.plus(WINDOW_DURATION))) {
            attempts.set(0);
            windowStart = now;
        }

        // Bounded: one warning, then quiet until the window resets.
        if (attempts.get() >= MAX_ATTEMPTS) {
            backoffUntil = windowStart.plus(WINDOW_DURATION);
            log.warn("sandbox: podman recovery — {} restart attempts within {}min did not bring Podman "
                            + "back; pausing until {}. Manual fix: wsl --shutdown && podman machine start",
                    MAX_ATTEMPTS, WINDOW_DURATION.toMinutes(), backoffUntil);
            return false;
        }

        // Only one recovery at a time — concurrent callers bail immediately.
        if (!recovering.compareAndSet(false, true)) {
            log.debug("sandbox: podman recovery already in progress — skipping");
            return false;
        }

        boolean handedOff = false;
        try {
            String exe = cliOf(containerRuntime);
            if (machineState(exe) == MachineState.MISSING) {
                // `recovering` stays set until the create finishes: a `wsl --shutdown` from a
                // concurrent caller would kill the init half-way.
                createExecutor.execute(() -> {
                    try {
                        createMachine(containerRuntime, exe);
                    } finally {
                        recovering.set(false);
                    }
                });
                handedOff = true;
                return false;
            }
            return restartMachine(containerRuntime, exe, attempts.incrementAndGet());
        } finally {
            if (!handedOff) {
                recovering.set(false);
            }
        }
    }

    /**
     * Convenience: checks a CLI result and triggers recovery if it looks like a post-sleep failure.
     * Returns true if recovery was attempted and succeeded.
     *
     * @param cliResult The failed CLI result to inspect.
     * @param containerRuntime The runtime to re-probe after recovery.
     */
    public boolean checkAndRecover(CliRunner.Result cliResult, ContainerRuntime containerRuntime) {
        if (!isConnectionFailure(cliResult)) {
            return false;
        }
        return attemptRecovery(containerRuntime);
    }

    MachineState machineState(String exe) {
        CliRunner.Result list = cli.run(List.of(exe, "machine", "list", "--format", "{{.Name}}"), CMD_TIMEOUT);
        if (!list.ok()) {
            return MachineState.UNKNOWN;
        }
        return list.stdout().isBlank() ? MachineState.MISSING : MachineState.EXISTS;
    }

    /** Creates and starts the first Podman machine; judged by existence, never by exit code. */
    private void createMachine(ContainerRuntime containerRuntime, String exe) {
        int memMiB = envInt("SQ_PODMAN_MACHINE_MEMORY_MIB", DEFAULT_MACHINE_MEMORY_MIB, 2048);
        int cpus = envInt("SQ_PODMAN_MACHINE_CPUS", DEFAULT_MACHINE_CPUS, 1);
        log.info("sandbox: no Podman machine exists on this host yet — creating one ({} MiB / {} CPU). "
                + "The first run downloads a VM image and can take several minutes; sandboxed nodes "
                + "run in the TRUSTED tier until it is up.", memMiB, cpus);
        CliRunner.Result init = cli.run(List.of(exe, "machine", "init",
                "--memory", String.valueOf(memMiB), "--cpus", String.valueOf(cpus)), MACHINE_INIT_TIMEOUT);
        // wsl.exe reports failure on success on some hosts, so the exit code proves nothing.
        if (machineState(exe) != MachineState.EXISTS) {
            backoffUntil = Instant.now().plus(CREATE_RETRY_BACKOFF);
            String why = summarize(init);
            if (RESTART_REQUIRED.matcher(why).find()) {
                restartRequired = true;
                log.warn("sandbox: Windows must be RESTARTED before Podman can start its WSL2 machine ({}). "
                        + "Restart Windows — the engine creates the machine by itself after the restart.", why);
            } else {
                log.warn("sandbox: creating the Podman machine failed (exit={}: {}) — retrying in {} min. "
                                + "Manual fix: podman machine init && podman machine start",
                        init.exitCode(), why, CREATE_RETRY_BACKOFF.toMinutes());
            }
            return;
        }
        restartRequired = false;
        log.info("sandbox: Podman machine created — starting it");
        CliRunner.Result start = cli.run(List.of(exe, "machine", "start"), MACHINE_START_TIMEOUT);
        if (!start.ok() && !alreadyRunning(start)) {
            log.warn("sandbox: the new Podman machine did not start (exit={}: {}) — the next probe "
                    + "retries with a WSL restart", start.exitCode(), summarize(start));
            return;
        }
        reprobe(containerRuntime);
    }

    /** Post-sleep path: WSL shutdown, settle, machine start, re-probe. */
    private boolean restartMachine(ContainerRuntime containerRuntime, String exe, int attempt) {
        log.info("sandbox: podman unreachable — restarting WSL and the Podman machine (attempt {}/{})",
                attempt, MAX_ATTEMPTS);

        CliRunner.Result wslShutdown = cli.run(List.of("wsl", "--shutdown"), CMD_TIMEOUT);
        if (!wslShutdown.ok()) {
            log.warn("sandbox: wsl --shutdown returned exit={} stderr=[{}] — proceeding anyway",
                    wslShutdown.exitCode(), wslShutdown.stderr().trim());
        }
        try {
            Thread.sleep(wslSettle.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("sandbox: recovery interrupted during settle wait");
            return false;
        }

        CliRunner.Result machineStart = cli.run(List.of(exe, "machine", "start"), CMD_TIMEOUT);
        // "podman machine start" exits 125 with "already running" if the machine came back on
        // its own after WSL restart — that's fine.
        if (!machineStart.ok() && !alreadyRunning(machineStart)) {
            String why = summarize(machineStart);
            if (RESTART_REQUIRED.matcher(why).find()) {
                restartRequired = true;
                backoffUntil = Instant.now().plus(CREATE_RETRY_BACKOFF);
                log.warn("sandbox: Podman cannot start its WSL2 machine until Windows is RESTARTED ({}). "
                        + "Restart Windows — the engine starts the machine by itself afterwards.", why);
            } else {
                log.warn("sandbox: podman machine start failed exit={} ({})", machineStart.exitCode(), why);
            }
            return false;
        }
        restartRequired = false;
        return reprobe(containerRuntime);
    }

    private boolean reprobe(ContainerRuntime containerRuntime) {
        CapabilityReport fresh = containerRuntime.redetect();
        if (fresh.available()) {
            log.info("sandbox: podman recovery SUCCEEDED — runtime available again "
                    + "(kind={} version={})", fresh.kind(), fresh.serverVersion());
            return true;
        }
        log.warn("sandbox: podman recovery completed commands but runtime still "
                + "unreachable — daemon probe returned unavailable");
        return false;
    }

    private static String cliOf(ContainerRuntime containerRuntime) {
        String resolved = containerRuntime == null ? null : containerRuntime.cli();
        return resolved == null || resolved.isBlank() ? "podman" : resolved;
    }

    private static boolean alreadyRunning(CliRunner.Result result) {
        return (result.stdout() + " " + result.stderr()).toLowerCase().contains("already running");
    }

    /** The last {@code Error} line (podman wraps it in pages of blob digests), else everything. */
    static String summarize(CliRunner.Result result) {
        String all = (result.stderr() + "\n" + result.stdout()).trim();
        String line = all.lines().filter(l -> l.contains("Error")).reduce((a, b) -> b).orElse(all);
        String flat = line.replaceAll("\\s+", " ").trim();
        return flat.length() > 300 ? flat.substring(0, 300) + "..." : flat;
    }

    private static int envInt(String name, int fallback, int min) {
        try {
            String raw = System.getenv(name);
            return raw == null || raw.isBlank() ? fallback : Math.max(min, Integer.parseInt(raw.trim()));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
