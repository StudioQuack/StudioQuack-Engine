package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.engine.update.DaemonUpdateService;
import com.studioquack.sqs.maintenance.ServiceManagerDetector.ServiceManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Daemon-executable lifecycle ops: {@code restart}, {@code uninstall}, and in-place update
 * relaunch. The daemon is a JVM process, so it can gracefully exit itself but cannot relaunch
 * itself — that is the supervisor's job, or the detached helper's: restart.
 */
@Slf4j
@Service
public class DaemonLifecycleService {

    /** Schedules a graceful daemon process exit (async, non-blocking). Seam for tests. */
    public interface Exiter {
        void scheduleExit(long graceMillis, int exitCode, String reason);
    }

    /** Runs a host lifecycle command (e.g. systemd relaunch guard, host unit unload). Seam for tests. */
    public interface ProcessRunner {
        ProcessResult run(List<String> command, long timeoutMillis) throws Exception;
    }

    /** Terminal result of a {@link ProcessRunner#run}. */
    public record ProcessResult(int exitCode, String output) {
    }

    /**
     * Outcome of a lifecycle request. {@code accepted} maps to an {@code ok}
     * command result; a non-accepted result maps to a {@code failed} outcome
     * carrying {@link #message()} (e.g. "not service-managed", "host-level").
     */
    public record LifecycleResult(boolean accepted, String action, String mechanism, String message) {
    }

    private final ServiceManagerDetector detector;
    private final MaintenanceProperties props;
    private final Exiter exiter;
    private final ProcessRunner processRunner;
    private final HostServiceUninstaller hostServiceUninstaller;
    private final SidecarUninstaller sidecarUninstaller;
    private final DaemonRelauncher relauncher;

    public DaemonLifecycleService(ServiceManagerDetector detector, MaintenanceProperties props,
                                  Exiter exiter, ProcessRunner processRunner,
                                  HostServiceUninstaller hostServiceUninstaller,
                                  SidecarUninstaller sidecarUninstaller,
                                  DaemonRelauncher relauncher) {
        this.detector = detector;
        this.props = props;
        this.exiter = exiter;
        this.processRunner = processRunner;
        this.hostServiceUninstaller = hostServiceUninstaller;
        this.sidecarUninstaller = sidecarUninstaller;
        this.relauncher = relauncher;
    }

    // ---------------------------------------------------------------------
    // restart
    // ---------------------------------------------------------------------

    /**
     * Schedules a graceful self-restart through {@link #relaunch}; refuses with an
     * operator-facing message only when nothing could bring the process back.
     */
    public LifecycleResult requestRestart() {
        long grace = Math.max(0, props.getRestartGraceMillis());
        try {
            ServiceManager mgr = relaunch(null, null, grace, "restart");
            String msg = "restart scheduled: graceful exit in " + grace + "ms; "
                    + (mgr.relaunchesOnExit() ? mgr + " relaunches" : "the relaunch helper starts")
                    + " the daemon.";
            log.info("maintenance.restart: {}", msg);
            return new LifecycleResult(true, "restart", mgr.name(), msg);
        } catch (Exception ex) {
            String msg = "restart refused: " + ex.getMessage() + ". Restart it from the host "
                    + "(systemctl restart / launchctl kickstart -k / Start-ScheduledTask).";
            log.warn("maintenance.restart: {}", msg);
            return new LifecycleResult(false, "restart", ServiceManager.NONE.name(), msg);
        }
    }

    /**
     * Ends this process so it comes back — on {@code staged} when given (an in-place update), else on
     * the same jar. systemd and launchd start {@code run.sh} again after exit 75, and the launcher
     * swaps {@code <jar>.new} in.
     *
     * @param jar The running boot jar, or {@code null} to resolve it when the helper needs it
     * @return The manager the decision was made for
     * @throws IllegalStateException When nothing could start this process again
     */
    public ServiceManager relaunch(Path jar, Path staged, long graceMillis, String reason) throws IOException {
        ServiceManager mgr = effectiveManager();
        if (mgr == ServiceManager.WINDOWS_SCHEDULED_TASK) {
            relauncher.spawn(detector.serviceLabel(), jar, staged);
            exiter.scheduleExit(graceMillis, 0, reason);
        } else if (needsLauncher(mgr)) {
            relauncher.spawn("", jar, staged);
            exiter.scheduleExit(graceMillis, 0, reason);
        } else {
            if (mgr == ServiceManager.SYSTEMD) {
                armSystemdRelaunchGuard(reason);
            }
            // Non-zero: systemd's Restart=on-failure does not relaunch a clean exit.
            exiter.scheduleExit(graceMillis, DaemonUpdateService.RELAUNCH_EXIT_CODE, reason);
        }
        return mgr;
    }

    /** Long after a healthy {@code Restart=on-failure} relaunch (exit, RestartSec, a slow stop). */
    static final long RELAUNCH_GUARD_DELAY_SECONDS = 60L;

    private static final Pattern SYSTEMD_UNIT_NAME = Pattern.compile("[A-Za-z0-9@._-]{1,200}");

    /**
     * Starts this unit again from a transient timer that lives outside the unit's cgroup. The engine
     * must come back whatever status this JVM ends with: {@code Restart=on-failure} leaves a clean
     * exit down, and then the staged jar is never swapped in and no rollback can run.
     */
    void armSystemdRelaunchGuard(String reason) {
        if (hostServiceUninstaller == null || processRunner == null) {
            return;
        }
        HostServiceUninstaller.ResolvedSystemdUnit unit = hostServiceUninstaller.resolveSystemdUnit().orElse(null);
        if (unit == null || !SYSTEMD_UNIT_NAME.matcher(unit.name()).matches()) {
            log.warn("maintenance.{}: no systemd unit name to guard the relaunch with", reason);
            return;
        }
        List<String> argv = systemdRelaunchGuardCommand(unit.name(), unit.userScope(), System.currentTimeMillis());
        try {
            ProcessResult r = processRunner.run(argv, 10_000L);
            if (r != null && r.exitCode() == 0) {
                log.info("maintenance.{}: relaunch guard armed — {} is started again in {}s if it is not back",
                        reason, unit.name(), RELAUNCH_GUARD_DELAY_SECONDS);
            } else {
                log.warn("maintenance.{}: relaunch guard not armed (exit {}): {}", reason,
                        r == null ? "none" : r.exitCode(), r == null ? "" : trimOutput(r.output()));
            }
        } catch (Exception ex) {
            log.warn("maintenance.{}: relaunch guard not armed: {}", reason, ex.toString());
        }
    }

    static List<String> systemdRelaunchGuardCommand(String unit, boolean userScope, long nowMillis) {
        List<String> argv = new ArrayList<>();
        argv.add("systemd-run");
        if (userScope) {
            argv.add("--user");
        }
        argv.add("--unit=" + unit + "-relaunch-" + nowMillis);
        argv.add("--on-active=" + RELAUNCH_GUARD_DELAY_SECONDS + "s");
        argv.add("--timer-property=AccuracySec=1s");
        argv.add("--description=StudioQuack engine relaunch guard");
        argv.add("systemctl");
        if (userScope) {
            argv.add("--user");
        }
        argv.add("start");
        argv.add(unit + ".service");
        return argv;
    }

    /** Fails before anything is stopped when {@link #relaunch} could not bring this process back. */
    public void requireRelaunchable(Path jar) {
        if (needsLauncher(effectiveManager())) {
            DaemonRelauncher.launcher(jar);
        }
    }

    /** No supervisor starts {@code run.sh} again — or Windows, where only the helper can swap the locked jar. */
    private static boolean needsLauncher(ServiceManager mgr) {
        return mgr != ServiceManager.WINDOWS_SCHEDULED_TASK && (!mgr.relaunchesOnExit() || isWindows());
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    // ---------------------------------------------------------------------
    // stop
    // ---------------------------------------------------------------------

    /** Back-compat overload — defaults to keeping the daemon's dependency cache and Podman. */
    public LifecycleResult requestUninstall() {
        return requestUninstall(false, false);
    }

    /** Back-compat overload — keeps Podman installed. */
    public LifecycleResult requestUninstall(boolean removeDependencies) {
        return requestUninstall(removeDependencies, false);
    }

    /**
     * Full engine teardown after an uninstall purge (ITEMS 2 + 3). When the side-car spawns we do NOT
     * self-exit: it stops us after the ACK grace, having already disabled the unit, so there is no
     * KeepAlive relaunch race.
     */
    public LifecycleResult requestUninstall(boolean removeDependencies, boolean removePodman) {
        ServiceManager mgr = effectiveManager();
        long grace = Math.max(0, props.getStopGraceMillis());

        SidecarUninstaller.SidecarResult sidecar = sidecarUninstaller == null
                ? SidecarUninstaller.SidecarResult.notSpawned("side-car uninstaller unavailable")
                : sidecarUninstaller.spawn(mgr, removeDependencies, removePodman, grace);
        if (sidecar.spawned()) {
            String msg = "uninstall: owned secrets purged; detached side-car teardown spawned ("
                    + sidecar.scriptPath() + "). It stops the daemon after ~" + grace + "ms, unloads the "
                    + "host unit (" + (sidecar.label().isBlank() ? mgr.name() : sidecar.label())
                    + "), removes the WireGuard adapter, stops podman, removePodman=" + removePodman
                    + ", and removeDependencies=" + removeDependencies + "."
                    + (sidecar.notes().isEmpty() ? "" : " sidecarNotes=[" + String.join("; ", sidecar.notes()) + "].");
            log.info("maintenance.uninstall: {}", msg);
            return new LifecycleResult(true, "uninstall", "side-car", msg);
        }


        long deferredDelay = grace + 2_000L;
        HostServiceUninstaller.UnloadResult unload = hostServiceUninstaller == null
                ? HostServiceUninstaller.UnloadResult.empty("host uninstaller unavailable")
                : hostServiceUninstaller.unload(mgr, deferredDelay);
        exiter.scheduleExit(grace, 0, "uninstall");
        String unitNote = " sidecar=[" + String.join("; ", sidecar.notes()) + "]."
                + " hostUnit=" + (unload.label().isBlank() ? mgr.name() : unload.label())
                + " disabled=" + unload.serviceDisabled()
                + " unitFileRemoved=" + unload.unitFileRemoved()
                + " deferredUnload=" + unload.deferredUnloadScheduled() + ".";
        if (!unload.notes().isEmpty()) {
            unitNote += " hostNotes=[" + String.join("; ", unload.notes()) + "].";
        }
        String relaunchNote = mgr.relaunchesOnExit() && !unload.hostSupervisionCleared()
                ? " NOTE: " + mgr + " may still relaunch — host disable/deferred unload failed;"
                + " run the host uninstall snippet (launchctl bootout / systemctl disable --now)."
                : "";
        String msg = "uninstall: owned secrets purged; graceful exit in " + grace + "ms."
                + unitNote + relaunchNote;
        log.info("maintenance.uninstall: {}", msg);
        if (!unload.hostSupervisionCleared() && mgr.relaunchesOnExit()) {
            log.warn("maintenance.uninstall: host supervision NOT cleared — KeepAlive/Restart may "
                    + "relaunch this daemon. notes={}", unload.notes());
        }
        return new LifecycleResult(true, "uninstall", mgr.name(), msg);
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    /** Detection, with the operator override ({@code sqs.maintenance.service-managed}) taking precedence. */
    private ServiceManager effectiveManager() {
        Boolean override = props.getServiceManaged();
        if (override != null) {
            if (override) {
                ServiceManager detected = detector.detect();
                // Honour the "yes it is managed" override even if auto-detect saw
                // nothing; keep a concrete manager name when one WAS detected.
                return detected.relaunchesOnExit() ? detected : ServiceManager.SYSTEMD;
            }
            return ServiceManager.NONE;
        }
        return detector.detect();
    }

    private static String trimOutput(String output) {
        if (output == null) {
            return "<no output>";
        }
        String t = output.strip();
        return t.length() > 500 ? t.substring(0, 500) + "…" : t;
    }
}
