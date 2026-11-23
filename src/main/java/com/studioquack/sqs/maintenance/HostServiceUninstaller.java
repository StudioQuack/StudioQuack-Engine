package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.maintenance.DaemonLifecycleService.ProcessResult;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.ProcessRunner;
import com.studioquack.sqs.maintenance.ServiceManagerDetector.ServiceManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Uninstall host step — disable/unload the launchd LaunchAgent or systemd unit that supervises
 * this daemon so {@code KeepAlive}/{@code Restart=} cannot relaunch after a graceful exit.
 * Environment Settings delete only reaches the daemon vithe pushed {@code uninstall} command.
 */
@Slf4j
@Component
public class HostServiceUninstaller {

    /** Explicit installer override (also written into future plists / unit files). */
    public static final String SERVICE_LABEL_ENV = "SQS_SERVICE_LABEL";

    private static final Pattern SYSTEMD_UNIT_IN_CGROUP =
            Pattern.compile("([\\w.@\\\\-]+\\.service)(?:/|$)");
    /** Safe launchd label / systemd unit token (reject shell metacharacters). */
    private static final Pattern SAFE_UNIT_TOKEN = Pattern.compile("^[A-Za-z0-9._@-]+$");

    private static final long CMD_TIMEOUT_MS = 8_000L;
    /** Default delay before detached bootout/stop when caller omits grace. */
    private static final long DEFAULT_DEFERRED_DELAY_MS = 5_000L;

    /** Outcome echoed into the uninstall ACK message / notes. */
    public record UnloadResult(boolean serviceDisabled, boolean unitFileRemoved,
                               boolean deferredUnloadScheduled, String label,
                               List<String> notes) {
        /** Shared empty/failure outcome (package + lifecycle callers). */
        public static UnloadResult empty(String note) {
            return new UnloadResult(false, false, false, "", List.of(note));
        }

        /** True when KeepAlive/Restart should. */
        public boolean hostSupervisionCleared() {
            return serviceDisabled || deferredUnloadScheduled;
        }
    }

    /** Process-environment + identity seam for unit tests. */
    public interface HostEnv {
        String get(String key);

        String userHome();

        long userId();

        /** Raw {@code /proc/self/cgroup} contents when present (Linux); empty otherwise. */
        String cgroup();

        /** {@code os.name} (or equivalent) for opportunistic unload when detection is NONE. */
        default String osName() {
            return System.getProperty("os.name", "");
        }
    }

    private final ProcessRunner processRunner;
    private final HostEnv hostEnv;

    /**
     * Production wiring. {@code @Autowired} is required because a package-private test
     * constructor also exists — without an explicit hint Spring falls back to the no-arg
     * {@code instantiateBean} path and fails with {@code NoSuchMethodException: <init>()}.
     */
    @Autowired
    public HostServiceUninstaller(ProcessRunner processRunner) {
        this(processRunner, new LiveHostEnv());
    }

    /** Test / alternate wiring (not a Spring bean constructor). */
    HostServiceUninstaller(ProcessRunner processRunner, HostEnv hostEnv) {
        this.processRunner = processRunner;
        this.hostEnv = hostEnv;
    }

    /** Uses {@link #DEFAULT_DEFERRED_DELAY_MS} before the detached bootout/stop. */
    public UnloadResult unload(ServiceManager mgr) {
        return unload(mgr, DEFAULT_DEFERRED_DELAY_MS);
    }

    /**
     * @param deferredDelayMs Delay before detached {@code bootout}/{@code stop}; should exceed the
     * uninstall ACK grace so the heartbeat can flush first
     */
    public UnloadResult unload(ServiceManager mgr, long deferredDelayMs) {
        long delay = Math.max(1_000L, deferredDelayMs);
        ServiceManager effective = mgr == null ? ServiceManager.NONE : mgr;
        try {
            if (effective == ServiceManager.LAUNCHD
                    || (effective == ServiceManager.NONE && isMac())) {
                return unloadLaunchd(delay);
            }
            if (effective == ServiceManager.SYSTEMD
                    || (effective == ServiceManager.NONE && isLinux())) {
                return unloadSystemd(delay);
            }
            if (effective == ServiceManager.WINDOWS_SERVICE
                    || (effective == ServiceManager.NONE && isWindows())) {
                return unloadWindowsTask();
            }
            return UnloadResult.empty("no supervising service manager detected — nothing to unload");
        } catch (Exception e) {
            String msg = "host service unload failed: " + safe(e);
            log.warn("uninstall: {}", msg);
            return UnloadResult.empty(msg);
        }
    }

    // ---------------------------------------------------------------------
    // launchd (macOS per-user LaunchAgent)
    // ---------------------------------------------------------------------

    private UnloadResult unloadLaunchd(long deferredDelayMs) {
        List<String> notes = new ArrayList<>();
        List<String> labels = resolveLaunchdLabels();
        if (labels.isEmpty()) {
            return UnloadResult.empty("launchd: could not resolve service label ("
                    + SERVICE_LABEL_ENV + " / XPC_SERVICE_NAME / LaunchAgents scan)");
        }

        boolean anyDisabled = false;
        boolean anyRemoved = false;
        boolean anyDeferred = false;
        String primary = labels.get(0);
        long uid = hostEnv.userId();

        for (String label : labels) {
            if (!isSafeUnitToken(label)) {
                notes.add("launchd: refusing unsafe label token: " + label);
                log.warn("uninstall: refusing unsafe launchd label={}", label);
                continue;
            }
            // Disable in both gui (Aqua login) and user domains — installs use gui/.
            boolean disabledGui = runOk(List.of("launchctl", "disable", "gui/" + uid + "/" + label),
                    notes, "launchctl disable gui/" + uid + "/" + label);
            boolean disabledUser = runOk(List.of("launchctl", "disable", "user/" + uid + "/" + label),
                    notes, "launchctl disable user/" + uid + "/" + label);
            anyDisabled = anyDisabled || disabledGui || disabledUser;

            Path plist = Path.of(hostEnv.userHome(), "Library", "LaunchAgents", label + ".plist");
            boolean removed = deleteQuietly(plist, notes);
            anyRemoved = anyRemoved || removed;

            // Deferred bootout: KeepAlive on an already-loaded job ignores disable-only.
            boolean deferred = scheduleDeferredLaunchdBootout(uid, label, plist, deferredDelayMs, notes);
            anyDeferred = anyDeferred || deferred;
            log.info("uninstall: launchd unload label={} disabledGui={} disabledUser={} "
                            + "plistRemoved={} deferredBootout={} path={}",
                    label, disabledGui, disabledUser, removed, deferred, plist);
        }

        if (!anyDisabled && !anyDeferred) {
            notes.add("launchd: disable and deferred bootout both failed — process may relaunch");
        }
        return new UnloadResult(anyDisabled, anyRemoved, anyDeferred, primary, notes);
    }

    /**
     * Labels for <em>this</em> daemon only (never every StudioQuack agent on the host).
     * Prefer a single installer/env marker; fall back to a LaunchAgents plist scan for stock names.
     */
    private List<String> resolveLaunchdLabels() {
        String explicit = trim(hostEnv.get(SERVICE_LABEL_ENV));
        if (isSafeUnitToken(explicit)) {
            return List.of(explicit);
        }
        String xpc = trim(hostEnv.get("XPC_SERVICE_NAME"));
        if (!xpc.isEmpty() && !"0".equals(xpc) && isSafeUnitToken(xpc)) {
            return List.of(xpc);
        }
        String prefix = trim(hostEnv.get("SQS_SECRETS_OS_SERVICE_PREFIX"));
        if (prefix.startsWith("com.studioquack.") && isSafeUnitToken(prefix)) {
            return List.of(prefix);
        }
        // No env markers (older install / detection NONE): pick stock plists that exist on disk.
        LinkedHashSet<String> labels = new LinkedHashSet<>();
        Path agents = Path.of(hostEnv.userHome(), "Library", "LaunchAgents");
        for (String stock : List.of("com.studioquack.sqs", "com.studioquack.sqs-pro")) {
            if (Files.isRegularFile(agents.resolve(stock + ".plist"))) {
                labels.add(stock);
            }
        }
        if (labels.size() > 1) {
            log.warn("uninstall: multiple LaunchAgents present {}; unloading all found stock labels",
                    labels);
        }
        return new ArrayList<>(labels);
    }

    private boolean scheduleDeferredLaunchdBootout(long uid, String label, Path plist,
                                                   long deferredDelayMs, List<String> notes) {
        long delaySec = Math.max(1L, (deferredDelayMs + 999L) / 1000L);
        String plistPath = plist.toAbsolutePath().normalize().toString().replace("'", "'\\''");
        // nohup + background so the helper outlives JVM exit (SIGHUP-safe).
        String inner = "sleep " + delaySec
                + "; launchctl bootout 'gui/" + uid + "/" + label + "' 2>/dev/null"
                + "; launchctl bootout 'user/" + uid + "/" + label + "' 2>/dev/null"
                + "; rm -f '" + plistPath + "'";
        String outer = "nohup /bin/sh -c " + shellSingleQuote(inner) + " >/dev/null 2>&1 &";
        boolean ok = runOk(List.of("/bin/sh", "-c", outer), notes, "launchctl deferred bootout");
        if (ok) {
            notes.add("deferred launchctl bootout scheduled in " + delaySec + "s for " + label);
        }
        return ok;
    }

    // ---------------------------------------------------------------------
    // systemd (Linux system or user unit)
    // ---------------------------------------------------------------------

    private UnloadResult unloadSystemd(long deferredDelayMs) {
        List<String> notes = new ArrayList<>();
        Optional<ResolvedSystemdUnit> unitOpt = resolveSystemdUnit();
        if (unitOpt.isEmpty()) {
            return UnloadResult.empty("systemd: could not resolve unit name ("
                    + SERVICE_LABEL_ENV + " / cgroup / stock label)");
        }
        ResolvedSystemdUnit unit = unitOpt.get();
        if (!isSafeUnitToken(unit.name())) {
            return UnloadResult.empty("systemd: refusing unsafe unit token: " + unit.name());
        }

        List<String> disableCmd = new ArrayList<>();
        if (unit.userScope()) {
            disableCmd.addAll(List.of("systemctl", "--user", "disable", unit.name()));
        } else {
            disableCmd.addAll(List.of("systemctl", "disable", unit.name()));
        }
        // No --now: stopping here would kill us before the uninstall ACK flushes.
        boolean disabled = runOk(disableCmd, notes, "systemctl disable");

        boolean removed = false;
        for (Path candidate : unit.unitFileCandidates(hostEnv.userHome())) {
            if (Files.isRegularFile(candidate)) {
                removed = deleteQuietly(candidate, notes) || removed;
            }
        }
        if (unit.userScope()) {
            runOk(List.of("systemctl", "--user", "daemon-reload"), notes, "systemctl --user daemon-reload");
        } else {
            runOk(List.of("systemctl", "daemon-reload"), notes, "systemctl daemon-reload");
        }

        boolean deferred = scheduleDeferredSystemdStop(unit, deferredDelayMs, notes);
        log.info("uninstall: systemd unload unit={} userScope={} disabled={} unitFileRemoved={} "
                        + "deferredStop={}",
                unit.name(), unit.userScope(), disabled, removed, deferred);
        if (!disabled && !deferred) {
            notes.add("systemd: disable and deferred stop both failed — process may relaunch");
        }
        return new UnloadResult(disabled, removed, deferred, unit.name(), notes);
    }

    private boolean scheduleDeferredSystemdStop(ResolvedSystemdUnit unit, long deferredDelayMs,
                                                List<String> notes) {
        long delaySec = Math.max(1L, (deferredDelayMs + 999L) / 1000L);
        String scope = unit.userScope() ? "systemctl --user" : "systemctl";
        String inner = "sleep " + delaySec
                + "; " + scope + " stop " + unit.name() + " 2>/dev/null"
                + "; " + scope + " reset-failed " + unit.name() + " 2>/dev/null"
                + "; " + scope + " disable " + unit.name() + " 2>/dev/null";
        String outer = "nohup /bin/sh -c " + shellSingleQuote(inner) + " >/dev/null 2>&1 &";
        boolean ok = runOk(List.of("/bin/sh", "-c", outer), notes, "systemctl deferred stop");
        if (ok) {
            notes.add("deferred " + scope + " stop scheduled in " + delaySec + "s for " + unit.name());
        }
        return ok;
    }

    /** The systemd unit this daemon runs under: installer label, else its own cgroup, else the tier default. */
    Optional<ResolvedSystemdUnit> resolveSystemdUnit() {
        String explicit = trim(hostEnv.get(SERVICE_LABEL_ENV));
        if (!explicit.isEmpty()) {
            return Optional.of(new ResolvedSystemdUnit(stripServiceSuffix(explicit), guessUserScope()));
        }
        Optional<String> fromCgroup = parseSystemdUnitFromCgroup(hostEnv.cgroup());
        if (fromCgroup.isPresent()) {
            String name = fromCgroup.get();
            boolean user = hostEnv.cgroup().contains("/user@") || hostEnv.cgroup().contains("user.slice");
            return Optional.of(new ResolvedSystemdUnit(stripServiceSuffix(name), user));
        }
        // Stock Linux label from the installer is studioquack-sqs.
        String prefix = trim(hostEnv.get("SQS_SECRETS_OS_SERVICE_PREFIX")).toLowerCase(Locale.ROOT);
        if (prefix.contains("sqs-pro")) {
            return Optional.of(new ResolvedSystemdUnit("studioquack-sqs-pro", guessUserScope()));
        }
        if (prefix.contains("studioquack") || prefix.contains("sqs")) {
            return Optional.of(new ResolvedSystemdUnit("studioquack-sqs", guessUserScope()));
        }
        // Prefer the stock Linux label when unknown; user-scope when not root.
        return Optional.of(new ResolvedSystemdUnit("studioquack-sqs-pro", guessUserScope()));
    }

    private boolean guessUserScope() {
        // Non-root installs are per-user; system units run as root.
        return hostEnv.userId() != 0L;
    }

    /** The daemon's own {@code .service} unit from {@code /proc/self/cgroup}, if it runs under one. */
    public static Optional<String> parseSystemdUnitFromCgroup(String cgroup) {
        if (cgroup == null || cgroup.isBlank()) {
            return Optional.empty();
        }
        Matcher m = SYSTEMD_UNIT_IN_CGROUP.matcher(cgroup);
        String last = null;
        while (m.find()) {
            String unit = m.group(1);
            // Skip the generic user@.service supervisor; want the app unit.
            if (unit.startsWith("user@") || unit.equals("init.scope")) {
                continue;
            }
            last = unit;
        }
        return Optional.ofNullable(last);
    }

    private static String stripServiceSuffix(String name) {
        String n = name.trim();
        return n.endsWith(".service") ? n.substring(0, n.length() - ".service".length()) : n;
    }

    record ResolvedSystemdUnit(String name, boolean userScope) {
        List<Path> unitFileCandidates(String home) {
            List<Path> out = new ArrayList<>();
            if (userScope) {
                out.add(Path.of(home, ".config", "systemd", "user", name + ".service"));
            }
            out.add(Path.of("/etc/systemd/system", name + ".service"));
            out.add(Path.of(home, ".config", "systemd", "user", name + ".service"));
            return out;
        }
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    // ---------------------------------------------------------------------
    // Windows (start-at-logon scheduled task)
    // ---------------------------------------------------------------------

    /**
     * The daemon supervises itself there through a task rather than an SCM service, because a plain
     * JVM never answers the Service Control Manager's start handshake. {@code /End} before
     * {@code /Delete}: ending first stops the task's own retry policy from treating our imminent exit
     * as a crash worth restarting.
     */
    private UnloadResult unloadWindowsTask() {
        List<String> notes = new ArrayList<>();
        String task = resolveWindowsTaskName();
        if (task.isEmpty()) {
            return UnloadResult.empty("windows: could not resolve the scheduled-task name ("
                    + SERVICE_LABEL_ENV + " unset and no stock name matched)");
        }
        boolean ended = runOk(List.of("schtasks", "/End", "/TN", task), notes, "schtasks /End " + task);
        boolean deleted = runOk(List.of("schtasks", "/Delete", "/TN", task, "/F"),
                notes, "schtasks /Delete " + task);
        removeWindowsNetAgent(notes);
        log.info("uninstall: windows scheduled task name={} ended={} deleted={}", task, ended, deleted);
        if (!deleted) {
            notes.add("windows: task still registered — it may relaunch the daemon at the next logon");
        }
        return new UnloadResult(deleted, deleted, false, task, notes);
    }

    /** The installer's label, else the stock task names, so we never delete another agent's task. */
    private String resolveWindowsTaskName() {
        String explicit = trim(hostEnv.get(SERVICE_LABEL_ENV));
        if (isSafeUnitToken(explicit)) {
            return explicit;
        }
        for (String stock : List.of("StudioQuackSQS", "StudioQuackSQSPro")) {
            ProcessResult probe = safeRun(List.of("schtasks", "/Query", "/TN", stock));
            if (probe != null && probe.exitCode() == 0) {
                return stock;
            }
        }
        return "";
    }

    /**
     * Best-effort mesh-agent removal on the Windows fallback path (no POSIX side-car).
     * The scheduled-task user may not be elevated — every step is independently logged.
     */
    private void removeWindowsNetAgent(List<String> notes) {
        String pf = trim(hostEnv.get("ProgramFiles"));
        if (pf.isEmpty()) {
            pf = "C:\\Program Files";
        }
        Path dest = Path.of(pf, "StudioQuack", "net-agent");
        Path cli = dest.resolve("sq-net.exe");
        Path exe = dest.resolve("sq-net-agent.exe");
        String pipe = "\\\\.\\pipe\\ProtectedPrefix\\Administrators\\StudioQuack\\sq-net-agent";
        if (Files.isRegularFile(cli)) {
            runOk(List.of(cli.toString(), "--socket=" + pipe, "down"), notes, "sq-net down");
            runOk(List.of(cli.toString(), "--socket=" + pipe, "logout"), notes, "sq-net logout");
        }
        if (Files.isRegularFile(exe)) {
            runOk(List.of(exe.toString(), "uninstall-system-daemon"), notes,
                    "sq-net-agent uninstall-system-daemon");
        }
        runOk(List.of("sc.exe", "stop", "sq-net-agent"), notes, "sc stop sq-net-agent");
        runOk(List.of("sc.exe", "delete", "sq-net-agent"), notes, "sc delete sq-net-agent");
        notes.add("windows mesh agent teardown attempted (service + ProgramFiles/ProgramData state)");
    }

    /** {@link ProcessRunner#run} that swallows failures — used for existence probes. */
    private ProcessResult safeRun(List<String> command) {
        try {
            return processRunner.run(command, CMD_TIMEOUT_MS);
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isWindows() {
        return hostEnv.osName().toLowerCase(Locale.ROOT).contains("win");
    }

    private boolean isMac() {
        return hostEnv.osName().toLowerCase(Locale.ROOT).contains("mac");
    }

    private boolean isLinux() {
        String os = hostEnv.osName().toLowerCase(Locale.ROOT);
        return os.contains("linux") || os.contains("unix");
    }

    private boolean runOk(List<String> command, List<String> notes, String label) {
        try {
            ProcessResult result = processRunner.run(command, CMD_TIMEOUT_MS);
            if (result.exitCode() == 0) {
                return true;
            }
            notes.add(label + " exited " + result.exitCode()
                    + (result.output() == null || result.output().isBlank()
                    ? "" : (": " + trimOutput(result.output()))));
            return false;
        } catch (Exception e) {
            notes.add(label + " failed: " + safe(e));
            return false;
        }
    }

    private static boolean deleteQuietly(Path path, List<String> notes) {
        try {
            if (!Files.exists(path)) {
                notes.add("unit file already absent: " + path);
                return true;
            }
            Files.deleteIfExists(path);
            boolean gone = !Files.exists(path);
            if (gone) {
                notes.add("deleted unit file: " + path);
            }
            return gone;
        } catch (IOException e) {
            notes.add("could not delete " + path + ": " + safe(e));
            return false;
        }
    }

    private static boolean isSafeUnitToken(String token) {
        return token != null && !token.isBlank() && SAFE_UNIT_TOKEN.matcher(token).matches();
    }

    /** POSIX single-quote wrap for embedding in {@code sh -c}. */
    static String shellSingleQuote(String s) {
        return "'" + s.replace("'", "'\\''") + "'";
    }

    private static String trim(String s) {
        return s == null ? "" : s.trim();
    }

    private static String trimOutput(String output) {
        String t = output.strip();
        return t.length() > 200 ? t.substring(0, 200) + "…" : t;
    }

    private static String safe(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /** Live process environment. */
    static final class LiveHostEnv implements HostEnv {
        private final Supplier<Map<String, String>> envSupplier;

        LiveHostEnv() {
            this(System::getenv);
        }

        LiveHostEnv(Supplier<Map<String, String>> envSupplier) {
            this.envSupplier = envSupplier;
        }

        @Override
        public String get(String key) {
            return envSupplier.get().get(key);
        }

        @Override
        public String userHome() {
            String home = System.getProperty("user.home");
            return home == null || home.isBlank() ? "." : home;
        }

        @Override
        public long userId() {
            // Prefer the JDK UnixSystem helper when present (macOS / Linux desktops).
            try {
                Object unix = Class.forName("com.sun.security.auth.module.UnixSystem")
                        .getConstructor().newInstance();
                Object uid = unix.getClass().getMethod("getUid").invoke(unix);
                if (uid instanceof Number n) {
                    return n.longValue();
                }
            } catch (Exception ignored) {
                // Fall through to `id -u`.
            }
            try {
                Process p = new ProcessBuilder("id", "-u").start();
                String out = new String(p.getInputStream().readAllBytes()).trim();
                p.waitFor();
                return Long.parseLong(out);
            } catch (Exception e) {
                // Non-zero default → prefer per-user unit paths over /etc/systemd.
                return 501L;
            }
        }

        @Override
        public String cgroup() {
            try {
                Path cg = Path.of("/proc/self/cgroup");
                if (Files.isRegularFile(cg)) {
                    return Files.readString(cg);
                }
            } catch (Exception ignored) {
                // non-Linux or restricted
            }
            return "";
        }

        @Override
        public String osName() {
            return System.getProperty("os.name", "");
        }
    }
}
