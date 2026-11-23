package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.maintenance.DaemonLifecycleService.Exiter;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.ProcessRunner;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.ProcessResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Wires the maintenance/lifecycle subsystem: binds {@link MaintenanceProperties} and supplies the
 * PRODUCTION implementations of the {@link Exiter} / {@link ProcessRunner} seams that
 * {@link DaemonLifecycleService} drives. Both seams are overridable in tests, which inject fakes
 * so the decision logic runs without ending the JVM or shelling out.
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(MaintenanceProperties.class)
public class MaintenanceConfig {

    /**
     * How long a graceful close may take before the process halts anyway. Nothing else bounds it: one
     * wedged shutdown hook (a blocked channel stop, a hung container call).
     */
    static final long HARD_EXIT_AFTER_MS = 45_000L;

    /**
     * Production exit strategy: on a short-lived exit thread ({@link #exitThread}), wait out the grace
     * window (so the command result flushes on a heartbeat first), then gracefully close the Spring
     * context and hard-exit. The service manager decides whether the process comes back — see
     * {@link DaemonLifecycleService}.
     */
    @Bean
    public Exiter daemonExiter(ConfigurableApplicationContext context) {
        return (graceMillis, exitCode, reason) -> exitThread(() -> {
            try {
                if (graceMillis > 0) {
                    Thread.sleep(graceMillis);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            log.warn("maintenance: exiting daemon now (reason={}, code={})", reason, exitCode);
            startHardExitDeadline(exitCode, reason);
            int code = SpringApplication.exit(context, () -> exitCode);
            System.exit(code);
        }).start();
    }

    /**
     * As a daemon thread the JVM ended with status 0 the moment the context's last non-daemon thread
     * (Tomcat) stopped — reliably so during an update drain, which has already stopped the job
     * executor.
     */
    static Thread exitThread(Runnable body) {
        Thread t = new Thread(body, "sqs-maintenance-exit");
        t.setDaemon(false);
        return t;
    }

    /**
     * Daemon threads keep running while shutdown hooks execute, so this fires even when
     * {@code System.exit} itself is stuck in a hook. {@code halt} skips the remaining hooks and
     * keeps the exit code the supervisor keys off (75 relaunches under systemd/launchd).
     */
    private static void startHardExitDeadline(int exitCode, String reason) {
        Thread watchdog = new Thread(() -> {
            try {
                Thread.sleep(HARD_EXIT_AFTER_MS);
            } catch (InterruptedException ie) {
                return;
            }
            // The logging system may already be closed: write straight to stderr as well.
            String msg = "maintenance: graceful shutdown (reason=" + reason + ") did not finish within "
                    + HARD_EXIT_AFTER_MS + " ms — halting with code " + exitCode + " so the supervisor relaunches";
            System.err.println(msg);
            log.error(msg);
            Runtime.getRuntime().halt(exitCode);
        }, "sqs-exit-deadline");
        watchdog.setDaemon(true);
        watchdog.start();
    }

    /**
     * Production process runner for host lifecycle commands (e.g. systemd relaunch guard,
     * host unit unload): runs the argv, merges stderr into stdout, and enforces the configured
     * timeout (destroying the process on overrun).
     */
    @Bean
    public ProcessRunner daemonProcessRunner() {
        return (command, timeoutMillis) -> {
            ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true);
            Process process = pb.start();
            String output;
            try (var in = process.getInputStream()) {
                output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                output = "<failed reading process output: " + e.getMessage() + ">";
            }
            boolean finished = process.waitFor(Math.max(1L, timeoutMillis), TimeUnit.MILLISECONDS);
            if (!finished) {
                process.destroyForcibly();
                throw new IOException("update command timed out after " + timeoutMillis + "ms: " + command);
            }
            return new ProcessResult(process.exitValue(), output);
        };
    }
}
