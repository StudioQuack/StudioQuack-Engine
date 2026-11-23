package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.maintenance.HostServiceUninstaller.HostEnv;
import com.studioquack.sqs.maintenance.HostServiceUninstaller.LiveHostEnv;
import com.studioquack.sqs.maintenance.ServiceManagerDetector.ServiceManager;
import com.studioquack.sqs.sandbox.egress.PodmanNetnsTarget;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * The side-car uninstaller: a small, self-deleting shell script the daemon spawns detached so it
 * OUTLIVES the daemon and can tear the engine down cleanly. The previous in-process approach
 * scheduled a deferred {@code launchctl bootout} with {@code nohup ... &}, but that helper stayed
 * in the daemon's launchd job / process group.
 */
@Slf4j
@Component
public class SidecarUninstaller {

    /** Safe launchd label / systemd unit token (reject shell metacharacters). */
    private static final Pattern SAFE_UNIT_TOKEN = Pattern.compile("^[A-Za-z0-9._@-]+$");

    /** Launches the detached teardown command. Seam so tests capture the argv without spawning. */
    public interface Spawner {
        boolean spawnDetached(List<String> command) throws Exception;
    }

    /** Outcome echoed into the uninstall ACK / lifecycle message. */
    public record SidecarResult(boolean spawned, String scriptPath, String label, List<String> notes) {
        public static SidecarResult notSpawned(String note) {
            return new SidecarResult(false, "", "", List.of(note));
        }
    }

    private final Environment environment;
    private final HostEnv hostEnv;
    private final Spawner spawner;
    private final Path scriptDir;

    /** Production wiring — {@code @Autowired} so Spring never falls back to a no-arg constructor. */
    @Autowired
    public SidecarUninstaller(Environment environment) {
        this(environment, new LiveHostEnv(), defaultSpawner(), defaultScriptDir());
    }

    /** Test / alternate wiring (not a Spring bean constructor). */
    SidecarUninstaller(Environment environment, HostEnv hostEnv, Spawner spawner, Path scriptDir) {
        this.environment = environment;
        this.hostEnv = hostEnv;
        this.spawner = spawner;
        this.scriptDir = scriptDir;
    }

    /**
     * Writes and launches the detached teardown side-car.
     *
     * @param mgr The detected supervising manager (drives launchd vs systemd steps)
     * @param removeDeps
     * @param removePodman Also uninstall Podman itself when {@code true} (the mesh agent session is
     * always torn down — it belongs to this engine, whereas Podman may be shared)
     * @param ackGraceMillis Delay before the side-car stops the daemon, so the uninstall ACK flushes
     */
    public SidecarResult spawn(ServiceManager mgr, boolean removeDeps, boolean removePodman,
                               long ackGraceMillis) {
        try {
            if (isWindows() || mgr == ServiceManager.WINDOWS_SERVICE) {
                return SidecarResult.notSpawned(
                        "windows: daemon cannot self-uninstall a Windows service; use the host uninstall snippet");
            }
            if (!isMac() && !isLinux()) {
                return SidecarResult.notSpawned("unsupported OS for side-car uninstall: " + hostEnv.osName());
            }
            Plan plan = resolvePlan(removeDeps, removePodman, ackGraceMillis);
            String script = buildScript(plan);
            Path scriptPath = plan.scriptPath();
            Files.createDirectories(scriptPath.getParent());
            Files.writeString(scriptPath, script);
            trySetExecutable(scriptPath);

            String launch = "nohup /bin/sh " + shQuote(scriptPath.toString())
                    + " >/dev/null 2>&1 </dev/null &";
            boolean ok = spawner.spawnDetached(List.of("/bin/sh", "-c", launch));
            if (!ok) {
                return SidecarResult.notSpawned("side-car launcher exited non-zero (" + scriptPath + ")");
            }
            log.info("uninstall: detached side-car spawned script={} label={} removeDeps={} removePodman={} netCli={}",
                    scriptPath, plan.primaryLabel(), removeDeps, removePodman, plan.netCli());
            return new SidecarResult(true, scriptPath.toString(), plan.primaryLabel(), plan.notes());
        } catch (Exception e) {
            String msg = "side-car spawn failed: " + safe(e);
            log.warn("uninstall: {}", msg);
            return SidecarResult.notSpawned(msg);
        }
    }

    // ---------------------------------------------------------------------
    // plan resolution
    // ---------------------------------------------------------------------

    private Plan resolvePlan(boolean removeDeps, boolean removePodman, long ackGraceMillis) {
        List<String> notes = new ArrayList<>();
        boolean mac = isMac();
        long uid = hostEnv.userId();
        String home = hostEnv.userHome();

        List<String> launchdLabels = mac ? resolveLaunchdLabels() : List.of();
        Optional<ResolvedSystemdUnit> systemd = mac ? Optional.empty() : resolveSystemdUnit();

        boolean pro = looksPro(launchdLabels, systemd);
        String jarStem = pro ? "sqs-pro-client-daemon" : "sqs-client-daemon";
        long pid = ProcessHandle.current().pid();

        String depsDir = firstNonBlank(environment.getProperty("sqs.deps.root"),
                hostEnv.get("SQS_DEPS_DIR"), "./data/deps");
        Path deps = Path.of(depsDir).toAbsolutePath().normalize();

        // Buffer the ACK grace by 2s before we stop the daemon, then round UP to whole seconds for
        // `sleep`.
        long graceSec = Math.max(1L, (Math.max(0, ackGraceMillis) + 2_000L + 999L) / 1000L);

        String ts = Long.toString(System.currentTimeMillis());
        String leaf = "sq-uninstall-" + (pro ? "sqs-pro" : "sqs") + "-" + pid + "-" + ts + ".sh";
        Path scriptPath = scriptDir.resolve(leaf);

        if (mac && launchdLabels.isEmpty()) {
            notes.add("no launchd label resolved — teardown will stop the daemon by pid + pkill only");
        }

        // The mesh agent session outlives the JVM: `sq-net up` leaves a logged-in node, and the side-car
        // SIGKILLs the daemon, so @PreDestroy cannot be relied on to `down`/`logout`. The daemon is the
        // socket operator — no sudo.
        String netCli = firstNonBlank(environment.getProperty("sqs.channel.net.cli-path"),
                environment.getProperty("sqs.channel.net.cliPath"), "sq-net");
        String netIface = firstNonBlank(environment.getProperty("sqs.channel.net.interface-name"),
                environment.getProperty("sqs.channel.net.interfaceName"), "sqnet0");
        if (netCli.isBlank() || netCli.indexOf('\n') >= 0 || netCli.indexOf(';') >= 0) {
            notes.add("sq-net cli-path is not a safe token — skipping mesh teardown");
            netCli = "";
        }
        if (!netIface.isBlank() && !isSafeToken(netIface)) {
            notes.add("net interface name '" + netIface + "' is not a safe token — ignored (down/logout do not need it)");
        }

        // The embedded store holds Duckbot conversation history, the execution_context and the deployed
        // artifacts. Deleting the engine has to take the user's conversations with it — leaving them
        // behind would strand personal content on a machine whose owner believes they removed the product.
        String storeDir = firstNonBlank(hostEnv.get("SQS_DATA_DIR"), "./data");
        Path storeBase = Path.of(storeDir).toAbsolutePath().normalize().resolve("sqs-engine");

        return new Plan(mac, uid, home, launchdLabels, systemd.orElse(null),
                pid, jarStem, List.of("studioquack.sqs", "studioquack.sqs.stateful"),
                removeDeps, deps.toString(), removePodman, netCli,
                graceSec, scriptPath, storeBase.toString(), notes);
    }

    /** Labels for THIS daemon (never every StudioQuack agent): env markers, else stock plists on disk. */
    private List<String> resolveLaunchdLabels() {
        String explicit = trim(hostEnv.get(HostServiceUninstaller.SERVICE_LABEL_ENV));
        if (isSafeToken(explicit)) {
            return List.of(explicit);
        }
        String xpc = trim(hostEnv.get("XPC_SERVICE_NAME"));
        if (!xpc.isEmpty() && !"0".equals(xpc) && isSafeToken(xpc)) {
            return List.of(xpc);
        }
        String prefix = trim(hostEnv.get("SQS_SECRETS_OS_SERVICE_PREFIX"));
        if (prefix.startsWith("com.studioquack.") && isSafeToken(prefix)) {
            return List.of(prefix);
        }
        LinkedHashSet<String> labels = new LinkedHashSet<>();
        Path agents = Path.of(hostEnv.userHome(), "Library", "LaunchAgents");
        for (String stock : List.of("com.studioquack.sqs", "com.studioquack.sqs-pro")) {
            if (Files.isRegularFile(agents.resolve(stock + ".plist"))) {
                labels.add(stock);
            }
        }
        return new ArrayList<>(labels);
    }

    /** systemd unit resolution: env marker, cgroup, then the stock Linux label. */
    private Optional<ResolvedSystemdUnit> resolveSystemdUnit() {
        boolean userScope = hostEnv.userId() != 0L;
        String explicit = trim(hostEnv.get(HostServiceUninstaller.SERVICE_LABEL_ENV));
        if (!explicit.isEmpty()) {
            return Optional.of(new ResolvedSystemdUnit(stripServiceSuffix(explicit), userScope));
        }
        Optional<String> fromCgroup = HostServiceUninstaller.parseSystemdUnitFromCgroup(hostEnv.cgroup());
        if (fromCgroup.isPresent()) {
            boolean user = hostEnv.cgroup().contains("/user@") || hostEnv.cgroup().contains("user.slice");
            return Optional.of(new ResolvedSystemdUnit(stripServiceSuffix(fromCgroup.get()), user));
        }
        String prefix = trim(hostEnv.get("SQS_SECRETS_OS_SERVICE_PREFIX")).toLowerCase(Locale.ROOT);
        if (prefix.contains("sqs-pro")) {
            return Optional.of(new ResolvedSystemdUnit("studioquack-sqs-pro", userScope));
        }
        if (prefix.contains("studioquack") || prefix.contains("sqs")) {
            return Optional.of(new ResolvedSystemdUnit("studioquack-sqs", userScope));
        }
        // Prefer the stock Linux label when nothing else is known.
        return Optional.of(new ResolvedSystemdUnit("studioquack-sqs-pro", userScope));
    }

    private boolean looksPro(List<String> launchdLabels, Optional<ResolvedSystemdUnit> systemd) {
        for (String l : launchdLabels) {
            if (l.toLowerCase(Locale.ROOT).contains("sqs-pro")) {
                return true;
            }
        }
        if (systemd.isPresent() && systemd.get().name().toLowerCase(Locale.ROOT).contains("sqs-pro")) {
            return true;
        }
        return trim(hostEnv.get("SQS_SECRETS_OS_SERVICE_PREFIX")).toLowerCase(Locale.ROOT).contains("sqs-pro");
    }

    // ---------------------------------------------------------------------
    // script assembly (pure — unit-tested directly)
    // ---------------------------------------------------------------------

    /** Builds the POSIX-sh teardown script for {@code plan}. Package-private for unit tests. */
    String buildScript(Plan plan) {
        StringBuilder b = new StringBuilder(2048);
        b.append("#!/bin/sh\n");
        b.append("# StudioQuack side-car uninstaller — detached teardown that OUTLIVES the daemon.\n");
        b.append("# Generated at runtime by SidecarUninstaller; it removes itself when done.\n");
        b.append("set -u\n");
        // Common podman/homebrew locations so `command -v podman` works under the minimal
        // LaunchAgent/systemd PATH the daemon may have inherited.
        b.append("PATH=\"/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}\"; export PATH\n\n");

        b.append("# Re-exec into a NEW SESSION so a launchd bootout / systemd stop of the daemon's job\n");
        b.append("# cannot take this teardown process down with it (that is why KeepAlive kept winning).\n");
        b.append("if [ -z \"${SQ_SIDECAR_DETACHED:-}\" ]; then\n");
        b.append("  SQ_SIDECAR_DETACHED=1; export SQ_SIDECAR_DETACHED\n");
        b.append("  if command -v setsid >/dev/null 2>&1; then\n");
        b.append("    exec setsid /bin/sh \"$0\"\n");
        b.append("  elif command -v perl >/dev/null 2>&1; then\n");
        b.append("    exec perl -e 'use POSIX; POSIX::setsid(); exec {\"/bin/sh\"} \"/bin/sh\", @ARGV' \"$0\"\n");
        b.append("  fi\n");
        b.append("fi\n\n");

        b.append("# 0) Let the uninstall ACK flush on a heartbeat before we stop the daemon.\n");
        b.append("sleep ").append(plan.graceSec()).append("\n\n");

        b.append("# a) Graceful stop of the daemon process (hard kill follows after the unit is unloaded).\n");
        b.append("kill -TERM ").append(plan.pid()).append(" 2>/dev/null || true\n\n");

        b.append("# b) Unload + remove the host service unit so KeepAlive/Restart= cannot relaunch it.\n");
        if (plan.mac()) {
            appendLaunchdUnload(b, plan);
        } else {
            appendSystemdUnload(b, plan);
        }

        b.append("\n# a') Hard-kill any daemon JVM still alive (pid + jar-name needle safety net).\n");
        b.append("sleep 2\n");
        b.append("kill -KILL ").append(plan.pid()).append(" 2>/dev/null || true\n");
        b.append("pkill -f ").append(shQuote(plan.jarStem() + ".*\\.jar")).append(" 2>/dev/null || true\n\n");

        appendNetTeardown(b, plan);

        b.append("# c) Stop Podman: remove the engine's sandbox containers, then stop the machine (macOS).\n");
        b.append("if command -v podman >/dev/null 2>&1; then\n");
        // The egress anchor is a pod: `rm -f` refuses its infra container.
        b.append("  podman pod rm -f --ignore ").append(PodmanNetnsTarget.ANCHOR_POD).append(" >/dev/null 2>&1 || true\n");
        b.append("  for L in ").append(String.join(" ", plan.sandboxLabels())).append("; do\n");
        b.append("    IDS=\"$(podman ps -aq --filter label=$L 2>/dev/null || true)\"\n");
        b.append("    [ -n \"$IDS\" ] && podman rm -f $IDS >/dev/null 2>&1 || true\n");
        b.append("  done\n");
        if (plan.mac()) {
            // macOS runs a Podman VM; Linux is daemonless/rootless (no machine to stop).
            b.append("  podman machine stop >/dev/null 2>&1 || true\n");
        }
        b.append("fi\n\n");

        appendPodmanRemoval(b, plan);

        b.append("# d) Dependency cache — removed ONLY when the operator opted in.\n");
        if (plan.removeDeps()) {
            b.append("rm -rf ").append(shQuote(plan.depsDir())).append(" 2>/dev/null || true\n");
            b.append("echo \"side-car: dependency cache removed (").append(plan.depsDir()).append(")\"\n\n");
        } else {
            b.append("# kept: ").append(plan.depsDir()).append(" (reinstall reuses the preloaded packages)\n\n");
        }

        // Unconditional, unlike the dependency cache: the store holds the user's Duckbot conversations and
        // this environment's execution history, which must not outlive the engine they belong to. The glob
        // covers the local database's companions (.mv.db,.trace.db, and any lock file) without reaching
        // the sibling deps/ and channel/ directories.
        b.append("# e2) Embedded store — conversation history + execution state die with the engine.\n");
        // Quote each FULL filename: quoting only the base would leave the extension outside the
        // quotes, which a path containing a space would then split on.
        b.append("rm -f ").append(shQuote(plan.storeBase() + ".mv.db")).append(" ")
                .append(shQuote(plan.storeBase() + ".trace.db")).append(" ")
                .append(shQuote(plan.storeBase() + ".lock.db")).append(" 2>/dev/null || true\n");
        b.append("echo \"side-car: embedded store removed (").append(plan.storeBase()).append(".*)\"\n\n");

        b.append("# f) Remove our own script and exit.\n");
        b.append("rm -f ").append(shQuote(plan.scriptPath().toString())).append(" 2>/dev/null || true\n");
        b.append("exit 0\n");
        return b.toString();
    }

    /**
     * Runs on EVERY uninstall, never opt-in: {@code sq-net up} leaves a logged-in node, and KeepAlive/
     * {@code Restart=} would keep the root agent on the StudioQuack network after the engine JVM dies.
     * A detached side-car never prompts for a password.
     */
    private void appendNetTeardown(StringBuilder b, Plan plan) {
        b.append("# b') Tear down the mesh agent COMPLETELY (ALWAYS — a deleted engine must not leave\n");
        b.append("#     this host on StudioQuack's network). Operator CLI first, then root service.\n");
        if (plan.netCli() == null || plan.netCli().isBlank()) {
            b.append("# (no safe sq-net cli-path resolved — skipping CLI down/logout)\n");
        } else {
            String cli = shQuote(plan.netCli());
            b.append("NETCLI=").append(cli).append("\n");
            b.append("if command -v \"$NETCLI\" >/dev/null 2>&1 || [ -x \"$NETCLI\" ]; then\n");
            b.append("  \"$NETCLI\" down >/dev/null 2>&1 || true\n");
            b.append("  \"$NETCLI\" logout >/dev/null 2>&1 || true\n");
            b.append("  echo \"side-car: sq-net session torn down\"\n");
            b.append("else\n");
            b.append("  echo \"side-car: sq-net CLI not found — continuing privileged agent removal\"\n");
            b.append("fi\n");
        }
        b.append("if [ \"$(id -u)\" -eq 0 ]; then NETSUDO=\"\"\n");
        b.append("elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then NETSUDO=\"sudo -n\"\n");
        b.append("else NETSUDO=\"\"; echo \"side-car: no root/sudo -n — cannot unregister root sq-net-agent\"\n");
        b.append("fi\n");
        b.append("net_root() { if [ -n \"$NETSUDO\" ]; then $NETSUDO \"$@\"; elif [ \"$(id -u)\" -eq 0 ]; then \"$@\"; else return 0; fi; }\n");
        if (plan.mac()) {
            b.append("net_root launchctl bootout system/com.studioquack.sq-net-agent >/dev/null 2>&1 || true\n");
            b.append("if [ -x /usr/local/bin/sq-net-agent ]; then\n");
            b.append("  net_root /usr/local/bin/sq-net-agent uninstall-system-daemon >/dev/null 2>&1 || true\n");
            b.append("fi\n");
            b.append("net_root rm -f /Library/LaunchDaemons/com.studioquack.sq-net-agent.plist\n");
            b.append("net_root rm -f /etc/sudoers.d/studioquack-wg\n");
            b.append("net_root rm -rf /var/lib/sq-net-agent /var/run/sq-net-agent \\\n");
            b.append("  /Library/StudioQuack/net-agent /Library/Logs/StudioQuack/sq-net-agent.log \\\n");
            b.append("  /Library/StudioQuack/sq-net-agent.state\n");
            b.append("net_root rm -f /usr/local/bin/sq-net /usr/local/bin/sq-net-agent\n");
            b.append("net_root rmdir /Library/Logs/StudioQuack /Library/StudioQuack >/dev/null 2>&1 || true\n");
        } else {
            b.append("if command -v systemctl >/dev/null 2>&1; then\n");
            b.append("  net_root systemctl disable --now sq-net-agent >/dev/null 2>&1 || true\n");
            b.append("fi\n");
            b.append("net_root rm -f /etc/systemd/system/sq-net-agent.service\n");
            b.append("if command -v systemctl >/dev/null 2>&1; then\n");
            b.append("  net_root systemctl daemon-reload >/dev/null 2>&1 || true\n");
            b.append("fi\n");
            b.append("net_root ip link delete sqnet0 >/dev/null 2>&1 || true\n");
            b.append("net_root rm -rf /var/lib/sq-net-agent /var/run/sq-net-agent /var/lib/sq-net\n");
            b.append("net_root rm -f /etc/sudoers.d/studioquack-wg\n");
            b.append("net_root rm -f /usr/local/sbin/sq-net-agent /usr/local/bin/sq-net /usr/local/bin/sq-net-agent\n");
        }
        b.append("echo \"side-car: sq-net-agent service and key material removed\"\n\n");
    }

    /**
     * Stopping the machine (step c) is always safe; removing the package is not, because Podman is
     * commonly shared with other work on a self-hosted box. That is why this is opt-in and separate.
     */
    private void appendPodmanRemoval(StringBuilder b, Plan plan) {
        if (!plan.removePodman()) {
            b.append("# e) Podman kept (operator did not opt into removing it).\n\n");
            return;
        }
        String sudo = plan.uid() == 0L ? "" : "sudo -n ";
        b.append("# e) Remove Podman itself — the operator opted in on the uninstall dialog.\n");
        b.append("if command -v podman >/dev/null 2>&1; then\n");
        b.append("  podman machine rm -f >/dev/null 2>&1 || true\n");
        if (plan.mac()) {
            b.append("  if command -v brew >/dev/null 2>&1; then\n");
            b.append("    brew uninstall --force podman >/dev/null 2>&1 || true\n");
            b.append("  fi\n");
        } else {
            b.append("  if command -v apt-get >/dev/null 2>&1; then\n");
            b.append("    ").append(sudo).append("apt-get remove -y podman >/dev/null 2>&1 || true\n");
            b.append("  elif command -v dnf >/dev/null 2>&1; then\n");
            b.append("    ").append(sudo).append("dnf remove -y podman >/dev/null 2>&1 || true\n");
            b.append("  elif command -v yum >/dev/null 2>&1; then\n");
            b.append("    ").append(sudo).append("yum remove -y podman >/dev/null 2>&1 || true\n");
            b.append("  elif command -v zypper >/dev/null 2>&1; then\n");
            b.append("    ").append(sudo).append("zypper --non-interactive remove podman >/dev/null 2>&1 || true\n");
            b.append("  elif command -v pacman >/dev/null 2>&1; then\n");
            b.append("    ").append(sudo).append("pacman -Rns --noconfirm podman >/dev/null 2>&1 || true\n");
            b.append("  fi\n");
        }
        b.append("  echo \"side-car: podman removed\"\n");
        b.append("fi\n\n");
    }

    private void appendLaunchdUnload(StringBuilder b, Plan plan) {
        if (plan.launchdLabels().isEmpty()) {
            b.append("# (no launchd label resolved — nothing to unload)\n");
            return;
        }
        for (String label : plan.launchdLabels()) {
            if (!isSafeToken(label)) {
                continue;
            }
            String gui = "gui/" + plan.uid() + "/" + label;
            String user = "user/" + plan.uid() + "/" + label;
            b.append("launchctl disable ").append(shQuote(gui)).append(" 2>/dev/null || true\n");
            b.append("launchctl disable ").append(shQuote(user)).append(" 2>/dev/null || true\n");
            b.append("launchctl bootout ").append(shQuote(gui)).append(" 2>/dev/null || true\n");
            b.append("launchctl bootout ").append(shQuote(user)).append(" 2>/dev/null || true\n");
            Path plist = Path.of(plan.userHome(), "Library", "LaunchAgents", label + ".plist");
            b.append("rm -f ").append(shQuote(plist.toString())).append(" 2>/dev/null || true\n");
        }
    }

    private void appendSystemdUnload(StringBuilder b, Plan plan) {
        ResolvedSystemdUnit unit = plan.systemd();
        if (unit == null || !isSafeToken(unit.name())) {
            b.append("# (no systemd unit resolved — nothing to unload)\n");
            return;
        }
        String svc = unit.name() + ".service";
        // Cover both user + system scope (best-effort); disable --now also stops the running unit.
        b.append("systemctl --user disable --now ").append(shQuote(svc)).append(" 2>/dev/null || true\n");
        b.append("systemctl disable --now ").append(shQuote(svc)).append(" 2>/dev/null || true\n");
        b.append("rm -f ").append(shQuote(Path.of(plan.userHome(), ".config", "systemd", "user", svc).toString()))
                .append(" 2>/dev/null || true\n");
        b.append("rm -f ").append(shQuote("/etc/systemd/system/" + svc)).append(" 2>/dev/null || true\n");
        b.append("systemctl --user daemon-reload 2>/dev/null || true\n");
        b.append("systemctl daemon-reload 2>/dev/null || true\n");
    }

    // ---------------------------------------------------------------------
    // records + helpers
    // ---------------------------------------------------------------------

    /** Resolved teardown inputs. Package-private so {@link #buildScript} tests can construct one. */
    record Plan(boolean mac, long uid, String userHome, List<String> launchdLabels,
                ResolvedSystemdUnit systemd, long pid, String jarStem, List<String> sandboxLabels,
                boolean removeDeps, String depsDir, boolean removePodman, String netCli,
                long graceSec, Path scriptPath, String storeBase,
                List<String> notes) {
        String primaryLabel() {
            if (!launchdLabels.isEmpty()) {
                return launchdLabels.get(0);
            }
            return systemd == null ? "" : systemd.name();
        }
    }

    record ResolvedSystemdUnit(String name, boolean userScope) {
    }

    private static Spawner defaultSpawner() {
        return command -> {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            pb.redirectError(ProcessBuilder.Redirect.DISCARD);
            Process p = pb.start();
            // The launcher backgrounds the side-car and returns immediately; treat a clean exit (or a
            // still-running launcher) as spawned.
            boolean done = p.waitFor(10, TimeUnit.SECONDS);
            return !done || p.exitValue() == 0;
        };
    }

    private static Path defaultScriptDir() {
        String tmp = System.getProperty("java.io.tmpdir", "/tmp");
        return Path.of(tmp);
    }

    private void trySetExecutable(Path path) {
        try {
            path.toFile().setExecutable(true, true);
        } catch (Exception ignored) {
            // Not fatal — we invoke it via `/bin/sh <script>`, not by exec bit.
        }
    }

    private boolean isMac() {
        return hostEnv.osName().toLowerCase(Locale.ROOT).contains("mac");
    }

    private boolean isLinux() {
        String os = hostEnv.osName().toLowerCase(Locale.ROOT);
        return os.contains("linux") || os.contains("unix");
    }

    private boolean isWindows() {
        return hostEnv.osName().toLowerCase(Locale.ROOT).contains("win");
    }

    private static boolean isSafeToken(String token) {
        return token != null && !token.isBlank() && SAFE_UNIT_TOKEN.matcher(token).matches();
    }

    private static String stripServiceSuffix(String name) {
        String n = name.trim();
        return n.endsWith(".service") ? n.substring(0, n.length() - ".service".length()) : n;
    }

    /** POSIX single-quote wrap for safe embedding in the generated script. */
    static String shQuote(String s) {
        return "'" + s.replace("'", "'\\''") + "'";
    }

    private static String trim(String s) {
        return s == null ? "" : s.trim();
    }

    private static String firstNonBlank(String... values) {
        if (values != null) {
            for (String v : values) {
                if (v != null && !v.isBlank()) {
                    return v.trim();
                }
            }
        }
        return "";
    }

    private static String safe(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
