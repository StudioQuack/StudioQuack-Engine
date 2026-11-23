package com.studioquack.sqs.maintenance;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Best-effort detection of the OS service manager supervising this daemon. A JVM process cannot
 * always know for certain that it is service-managed, so this reads the signals each manager puts
 * into the supervised process itself.
 */
@Slf4j
@Component
public class ServiceManagerDetector {

    /** The detected (or overridden) supervising manager. */
    public enum ServiceManager {
        SYSTEMD, LAUNCHD, WINDOWS_SERVICE, WINDOWS_SCHEDULED_TASK, NONE;

        /**
         * Whether a supervisor watches this process. Under {@code NONE}
         * (background / foreground install) nothing does, so restart and update
         * hand over to {@link DaemonRelauncher} instead.
         */
        public boolean relaunchesOnExit() {
            return this != NONE;
        }
    }

    /** Installer-set explicit override env var (highest precedence). */
    public static final String OVERRIDE_ENV = "SQVM_SERVICE_MANAGER";

    /** Detect using the live process environment, cgroup and OS name. */
    public ServiceManager detect() {
        return detect(System.getenv(), System.getProperty("os.name", ""), ownCgroupLeaf(),
                ServiceManagerDetector::scheduledTaskExists);
    }

    /** {@link #detect(Map, String, String, Predicate)} with no known cgroup and no scheduled task. */
    public ServiceManager detect(Map<String, String> env, String osName) {
        return detect(env, osName, "", task -> false);
    }

    /**
     * @param env The process environment (never mutated)
     * @param osName
     * @param cgroupLeaf
     * @param scheduledTaskExists Whether a Windows scheduled task of that name is registered
     * @return The detected manager (never null; {@code NONE} when nothing matches)
     */
    public ServiceManager detect(Map<String, String> env, String osName, String cgroupLeaf,
                                 Predicate<String> scheduledTaskExists) {
        Map<String, String> e = env == null ? Map.of() : env;
        String os = osName == null ? "" : osName.toLowerCase(Locale.ROOT);

        // 1) Explicit installer override — always wins.
        String override = trim(e.get(OVERRIDE_ENV));
        if (!override.isEmpty()) {
            switch (override.toLowerCase(Locale.ROOT)) {
                case "systemd":
                    return ServiceManager.SYSTEMD;
                case "launchd":
                    return ServiceManager.LAUNCHD;
                case "windows-service":
                case "windows_service":
                case "sc":
                    return ServiceManager.WINDOWS_SERVICE;
                case "none":
                    return ServiceManager.NONE;
                default:
                    log.warn("maintenance: unknown {}='{}' — falling back to auto-detect",
                            OVERRIDE_ENV, override);
            }
        }

        // 2) systemd — INVOCATION_ID / JOURNAL_STREAM are set for every unit, and inherited by
        //    whatever a unit starts (a desktop terminal's shell), hence the cgroup check.
        String leaf = trim(cgroupLeaf);
        boolean unitEnv = !trim(e.get("INVOCATION_ID")).isEmpty() || !trim(e.get("JOURNAL_STREAM")).isEmpty();
        if (unitEnv && (leaf.isEmpty() || leaf.endsWith(".service"))) {
            return ServiceManager.SYSTEMD;
        }

        // 3) launchd — XPC_SERVICE_NAME is the job label for a managed process; an interactive
        //    shell reports "0", a terminal app's shell "application.<bundle id>…".
        String xpc = trim(e.get("XPC_SERVICE_NAME"));
        if (os.contains("mac") && !xpc.isEmpty() && !"0".equals(xpc) && !xpc.startsWith("application.")) {
            return ServiceManager.LAUNCHD;
        }

        // 4) Windows — the installer's scheduled task, named in SQS_SERVICE_LABEL and registered.
        String task = trim(e.get(HostServiceUninstaller.SERVICE_LABEL_ENV));
        if (os.contains("win") && !task.isEmpty()
                && scheduledTaskExists != null && scheduledTaskExists.test(task)) {
            return ServiceManager.WINDOWS_SCHEDULED_TASK;
        }
        return ServiceManager.NONE;
    }

    /** The supervisor label the installer put into this process's environment ({@code ""} when none). */
    public String serviceLabel() {
        return trim(System.getenv(HostServiceUninstaller.SERVICE_LABEL_ENV));
    }

    /** Last segment of this process's systemd cgroup (e.g. {@code studioquack-sqs.service}); {@code ""} off Linux. */
    static String ownCgroupLeaf() {
        Path cgroup = Path.of("/proc/self/cgroup");
        if (!Files.isReadable(cgroup)) {
            return "";
        }
        try {
            for (String line : Files.readAllLines(cgroup)) {
                // cgroup v2 "0::/path"; v1 "N:name=systemd:/path"
                String[] parts = line.split(":", 3);
                boolean unified = parts.length == 3 && "0".equals(parts[0]) && parts[1].isEmpty();
                if (parts.length == 3 && (unified || "name=systemd".equals(parts[1]))) {
                    return parts[2].substring(parts[2].lastIndexOf('/') + 1).trim();
                }
            }
        } catch (IOException | RuntimeException ex) {
            log.debug("maintenance: could not read /proc/self/cgroup: {}", ex.toString());
        }
        return "";
    }

    /** {@code schtasks /Query} exits 0 only for a registered task. */
    static boolean scheduledTaskExists(String task) {
        try {
            Process p = new ProcessBuilder("schtasks", "/Query", "/TN", task)
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start();
            if (!p.waitFor(10, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return false;
            }
            return p.exitValue() == 0;
        } catch (IOException ex) {
            log.debug("maintenance: schtasks /Query failed: {}", ex.toString());
            return false;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String trim(String s) {
        return s == null ? "" : s.trim();
    }
}
