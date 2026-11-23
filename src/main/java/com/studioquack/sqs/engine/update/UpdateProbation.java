package com.studioquack.sqs.engine.update;

import com.studioquack.sqs.maintenance.DaemonLifecycleService;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A freshly swapped-in build stays on probation until it proves it can talk to the control plane,
 * and gives the engine back to the previous build when it cannot.
 *
 * <p><b>Why "the process started" is not enough.</b> The launchers roll back a jar that fails to
 * BOOT ({@code run.sh} after three boots, the Windows helper on a dead health endpoint). A build
 * that boots but never reaches the control plane — a broken channel, a hang after start-up — used to
 * pass: the update marker was cleared at {@code ApplicationReadyEvent}, the engine stayed offline
 * and nothing could ever reach it again. Now {@code <jar>.update-pending} (written by the launcher
 * that swapped the jar in) is cleared only by the first 2xx heartbeat. With no such beat within
 * {@link #probationMillis}, this process restores {@code <jar>.previous} and relaunches onto it.
 *
 * <p><b>No loops.</b> The rollback leaves {@code <jar>.update-rollback} (the reason) and
 * re-writes {@code <jar>.update-attempt} with the failed build's sha. The restored build reports
 * the update as FAILED from those markers and never enters probation itself — even on Windows,
 * where the helper's swap writes a fresh {@code update-pending} for the restored jar.
 */
@Slf4j
@Component
public class UpdateProbation {

    static final String PENDING = ".update-pending";
    static final String ROLLBACK = ".update-rollback";
    static final String ATTEMPT = ".update-attempt";
    static final String FAILED_JAR = ".failed";

    private static final AtomicBoolean confirmed = new AtomicBoolean(false);
    private static volatile Runnable onConfirm;

    private final DaemonLifecycleService lifecycle;
    private final long probationMillis;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "sqs-update-probation");
        t.setDaemon(true);
        return t;
    });

    public UpdateProbation(DaemonLifecycleService lifecycle,
                           @Value("${sqs.update.probation-ms:600000}") long probationMillis) {
        this.lifecycle = lifecycle;
        this.probationMillis = probationMillis;
    }

    /** Called on every successful heartbeat; a no-op after the first. Never throws. */
    public static void confirmControlPlaneContact() {
        if (confirmed.compareAndSet(false, true)) {
            Runnable r = onConfirm;
            if (r != null) {
                try {
                    r.run();
                } catch (RuntimeException ex) {
                    log.warn("update-probation: confirm hook failed: {}", ex.toString());
                }
            }
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    void onReady() {
        try {
            start(DaemonUpdateService.runningJar());
        } catch (RuntimeException ex) {
            log.debug("update-probation: not started: {}", ex.toString());
        }
    }

    /** Package-visible for tests: begin probation for {@code jar} when it was just swapped in. */
    void start(Path jar) {
        Path pending = sibling(jar, PENDING);
        if (Files.exists(sibling(jar, ROLLBACK))) {
            // This process IS the restored build: report, never probation (no ping-pong).
            String reason = readQuietly(sibling(jar, ROLLBACK),
                    "the updated build was rolled back; the previous build was restored");
            DaemonBuild.updateFailed(reason);
            log.warn("update-probation: running the restored build after a failed update: {}", reason);
            deleteQuietly(sibling(jar, ROLLBACK));
            deleteQuietly(pending);
            return;
        }
        if (!Files.exists(pending)) {
            return;
        }
        onConfirm = () -> {
            deleteQuietly(pending);
            log.info("update-probation: the updated build reached the control plane — update confirmed");
        };
        if (confirmed.get()) {
            onConfirm.run();
            return;
        }
        log.info("update-probation: updated build on probation — it must reach the control plane within {} "
                + "or the previous build is restored", window());
        timer.schedule(() -> expire(jar), probationMillis, TimeUnit.MILLISECONDS);
    }

    void expire(Path jar) {
        if (confirmed.get() || !Files.exists(sibling(jar, PENDING))) {
            return;
        }
        String reason = "the updated build could not reach the control plane within " + window()
                + "; the previous build was restored";
        log.error("update-probation: {}", reason);
        try {
            rollback(jar, reason);
        } catch (Exception ex) {
            log.error("update-probation: rollback failed — the engine stays on the updated build: {}", ex.toString());
        }
    }

    /**
     * Restores {@code <jar>.previous} and relaunches. POSIX renames under the running JVM (its open
     * handle survives) and lets the supervisor or helper start the launcher on the old jar; Windows
     * locks a running jar, so the previous one is staged as {@code <jar>.new} for the helper to swap.
     */
    void rollback(Path jar, String reason) throws IOException {
        Path previous = sibling(jar, ".previous");
        if (!Files.isRegularFile(previous)) {
            throw new IOException("no previous build at " + previous);
        }
        Files.writeString(sibling(jar, ROLLBACK), reason);
        try {
            // The failed build's sha lets an older restored build (which predates ROLLBACK) still
            // report the update as failed through its .update-attempt reconcile.
            Files.writeString(sibling(jar, ATTEMPT), DaemonUpdateService.sha256(jar));
        } catch (Exception ex) {
            log.debug("update-probation: could not record the failed build's sha: {}", ex.toString());
        }
        deleteQuietly(sibling(jar, PENDING));
        Path staged = null;
        if (isWindows()) {
            staged = sibling(jar, ".new");
            Files.copy(previous, staged, StandardCopyOption.REPLACE_EXISTING);
        } else {
            Files.move(jar, sibling(jar, FAILED_JAR), StandardCopyOption.REPLACE_EXISTING);
            Files.move(previous, jar, StandardCopyOption.REPLACE_EXISTING);
        }
        if (lifecycle != null) {
            lifecycle.relaunch(jar, staged, 3_000L, "update-rollback");
        }
    }

    @PreDestroy
    void shutdown() {
        timer.shutdownNow();
    }

    /** "10 min" / "25 s" — whole minutes when the window is at least one. */
    private String window() {
        return probationMillis >= 60_000L ? (probationMillis / 60_000L) + " min" : (probationMillis / 1000L) + " s";
    }

    private static Path sibling(Path jar, String suffix) {
        return jar.resolveSibling(jar.getFileName() + suffix);
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    private static String readQuietly(Path file, String fallback) {
        try {
            String s = Files.readString(file).trim();
            return s.isEmpty() ? fallback : s;
        } catch (IOException ex) {
            return fallback;
        }
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException ex) {
            log.debug("update-probation: could not delete {}: {}", file, ex.toString());
        }
    }

    /** Test hook: forget process-wide state. */
    static void reset() {
        confirmed.set(false);
        onConfirm = null;
    }
}
