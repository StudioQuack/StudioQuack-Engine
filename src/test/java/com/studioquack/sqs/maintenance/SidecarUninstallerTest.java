package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.maintenance.HostServiceUninstaller.HostEnv;
import com.studioquack.sqs.maintenance.ServiceManagerDetector.ServiceManager;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * + 3 — {@link SidecarUninstaller}. Proves the detached teardown script is generated with the
 * correct ordered steps (stop daemon → unload host unit → stop podman → conditional deps removal →
 * self-delete) and is launched via a detached {@code /bin/sh -c nohup … &} — without ever spawning
 * a real process.
 */
class SidecarUninstallerTest {

    /** Configurable host environment (os / home / uid / env vars) for deterministic resolution. */
    private static final class FakeHostEnv implements HostEnv {
        private final String os;
        private final String home;
        private final long uid;
        private final Map<String, String> env = new HashMap<>();

        FakeHostEnv(String os, String home, long uid) {
            this.os = os;
            this.home = home;
            this.uid = uid;
        }

        @Override public String get(String key) { return env.get(key); }
        @Override public String userHome() { return home; }
        @Override public long userId() { return uid; }
        @Override public String cgroup() { return ""; }
        @Override public String osName() { return os; }
    }

    /** Captures the launch argv instead of spawning; returns a configurable success flag. */
    private static final class CapturingSpawner implements SidecarUninstaller.Spawner {
        final AtomicReference<List<String>> command = new AtomicReference<>();
        boolean result = true;

        @Override
        public boolean spawnDetached(List<String> cmd) {
            command.set(cmd);
            return result;
        }
    }

    private String readScript(SidecarUninstaller.SidecarResult result) throws IOException {
        return Files.readString(Path.of(result.scriptPath()));
    }

    @Test
    void macos_keepDeps_generatesOrderedDetachedTeardown() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-macos");
        Path deps = tmp.resolve("deps");
        FakeHostEnv host = new FakeHostEnv("Mac OS X", tmp.toString(), 501L);
        host.env.put("SQS_SERVICE_LABEL", "com.studioquack.sqs");
        host.env.put("SQS_DEPS_DIR", deps.toString());
        CapturingSpawner spawner = new CapturingSpawner();
        SidecarUninstaller sidecar = new SidecarUninstaller(mock(Environment.class), host, spawner, tmp);

        SidecarUninstaller.SidecarResult result = sidecar.spawn(ServiceManager.LAUNCHD, false, false, 0L);

        assertTrue(result.spawned(), () -> "should spawn: " + result.notes());
        assertEquals("com.studioquack.sqs", result.label());
        // Launched detached (nohup + backgrounded) via a shell so it survives our death.
        List<String> cmd = spawner.command.get();
        assertEquals("/bin/sh", cmd.get(0));
        assertEquals("-c", cmd.get(1));
        assertTrue(cmd.get(2).startsWith("nohup /bin/sh "), cmd.get(2));
        assertTrue(cmd.get(2).endsWith("&"), cmd.get(2));

        String script = readScript(result);
        // Re-exec into a new session (setsid / perl) so launchd bootout cannot kill the side-car.
        assertTrue(script.contains("POSIX::setsid()") || script.contains("exec setsid"), script);
        // b) launchd unit unload sequence.
        assertTrue(script.contains("launchctl disable 'gui/501/com.studioquack.sqs'"), script);
        assertTrue(script.contains("launchctl bootout 'gui/501/com.studioquack.sqs'"), script);
        assertTrue(script.contains("launchctl bootout 'user/501/com.studioquack.sqs'"), script);
        assertTrue(script.contains("LaunchAgents/com.studioquack.sqs.plist"), script);
        // a/a') stop the daemon (graceful then hard) + jar-name safety net.
        assertTrue(script.contains("kill -TERM"), script);
        assertTrue(script.contains("kill -KILL"), script);
        assertTrue(script.contains("pkill -f 'sqs-client-daemon.*\\.jar'"), script);
        // c) podman: remove containers by label + stop the machine on macOS.
        assertTrue(script.contains("podman rm -f"), script);
        assertTrue(script.contains("for L in studioquack.sqs studioquack.sqs.stateful"), script);
        assertTrue(script.contains("label=$L"), script);
        assertTrue(script.contains("podman machine stop"), script);
        // d) deps NOT removed (operator opted out).
        assertFalse(script.contains("rm -rf '" + deps + "'"), "deps must be kept when flag is false");
        // f) self-delete.
        assertTrue(script.contains("rm -f '" + result.scriptPath() + "'"), script);
    }

    /**
     * The embedded store holds the user's Duckbot conversation history and this environment's
     * execution state, so deleting the engine must take it along — unconditionally, unlike the
     * dependency cache. Leaving it behind strands personal content on a machine whose owner
     * believes they removed the product.
     */
    @Test
    void embeddedStore_isAlwaysRemoved_andDepsAreNot() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-store");
        Path deps = tmp.resolve("deps");
        Path data = tmp.resolve("data");
        FakeHostEnv host = new FakeHostEnv("Mac OS X", tmp.toString(), 501L);
        host.env.put("SQS_SERVICE_LABEL", "com.studioquack.sqs");
        host.env.put("SQS_DEPS_DIR", deps.toString());
        host.env.put("SQS_DATA_DIR", data.toString());
        SidecarUninstaller sidecar =
                new SidecarUninstaller(mock(Environment.class), host, new CapturingSpawner(), tmp);

        // removeDeps=false: the store still goes, proving the two are not coupled.
        SidecarUninstaller.SidecarResult result = sidecar.spawn(ServiceManager.LAUNCHD, false, false, 0L);

        String script = readScript(result);
        String storeBase = data.resolve("sqs-engine").toString();
        assertTrue(script.contains("'" + storeBase + ".mv.db'"),
                "the H2 database carrying conversation history must be deleted: " + script);
        assertTrue(script.contains("'" + storeBase + ".trace.db'"), script);
        assertFalse(script.contains("rm -rf '" + deps + "'"),
                "removing the store must not drag the opt-in dependency cache with it");
        // Surgical: the sibling directories under data/ are not swept.
        assertFalse(script.contains("rm -rf '" + data + "'"),
                "only the H2 files are removed, never the whole data dir");
    }

    @Test
    void macos_removeDeps_deletesDependencyCache() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-deps");
        Path deps = tmp.resolve("deps");
        FakeHostEnv host = new FakeHostEnv("Mac OS X", tmp.toString(), 501L);
        host.env.put("SQS_SERVICE_LABEL", "com.studioquack.sqs");
        host.env.put("SQS_DEPS_DIR", deps.toString());
        SidecarUninstaller sidecar =
                new SidecarUninstaller(mock(Environment.class), host, new CapturingSpawner(), tmp);

        SidecarUninstaller.SidecarResult result = sidecar.spawn(ServiceManager.LAUNCHD, true, false, 0L);

        String script = readScript(result);
        assertTrue(script.contains("rm -rf '" + deps + "'"), "deps must be removed when flag is true");
    }

    @Test
    void linux_usesSystemd_andHasNoPodmanMachineStop() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-linux");
        FakeHostEnv host = new FakeHostEnv("Linux", tmp.toString(), 1000L);
        host.env.put("SQS_SERVICE_LABEL", "studioquack-sqs-pro");
        SidecarUninstaller sidecar =
                new SidecarUninstaller(mock(Environment.class), host, new CapturingSpawner(), tmp);

        SidecarUninstaller.SidecarResult result = sidecar.spawn(ServiceManager.SYSTEMD, false, false, 0L);

        assertTrue(result.spawned());
        String script = readScript(result);
        assertTrue(script.contains("systemctl --user disable --now 'studioquack-sqs-pro.service'"), script);
        assertTrue(script.contains("systemctl disable --now 'studioquack-sqs-pro.service'"), script);
        assertTrue(script.contains("/etc/systemd/system/studioquack-sqs-pro.service"), script);
        assertTrue(script.contains("daemon-reload"), script);
        // The service label selects the jar name the side-car stops.
        assertTrue(script.contains("pkill -f 'sqs-pro-client-daemon.*\\.jar'"), script);
        // Linux podman is daemonless/rootless: no machine to stop.
        assertFalse(script.contains("podman machine stop"), "no podman machine on Linux");
        assertTrue(script.contains("podman rm -f"), script);
    }

    // --- Mesh agent teardown ----------------------------------------------------------------------
    // `sq-net up` leaves a logged-in node AND a root KeepAlive/Restart= service. The side-car
    // SIGKILLs the daemon, so @PreDestroy cannot be relied on. Operator CLI: down + logout, no
    // sudo. Privileged removal uses net_root (sudo -n or already-root) so keys/service go too.

    @Test
    void netSessionIsAlwaysTornDown_evenWithNoOptInFlags() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-net");
        FakeHostEnv host = new FakeHostEnv("Mac OS X", tmp.toString(), 501L);
        host.env.put("SQS_SERVICE_LABEL", "com.studioquack.sqs");
        SidecarUninstaller sidecar =
                new SidecarUninstaller(mock(Environment.class), host, new CapturingSpawner(), tmp);

        SidecarUninstaller.SidecarResult result = sidecar.spawn(ServiceManager.LAUNCHD, false, false, 0L);

        String script = readScript(result);
        assertTrue(script.contains("\"$NETCLI\" down"), "sq-net down missing: " + script);
        assertTrue(script.contains("\"$NETCLI\" logout"), "sq-net logout missing: " + script);
        assertTrue(script.contains("side-car: sq-net session torn down"), script);
        assertFalse(script.contains("sudo -n \"$NETCLI\""), "CLI down/logout must not use sudo: " + script);
        assertTrue(script.contains("net_root launchctl bootout system/com.studioquack.sq-net-agent"), script);
        assertTrue(script.contains("uninstall-system-daemon"), script);
        assertTrue(script.contains("/Library/StudioQuack/net-agent"), script);
        assertTrue(script.contains("net_root rm -f /usr/local/bin/sq-net"), script);
        assertTrue(script.contains("side-car: sq-net-agent service and key material removed"), script);
    }

    @Test
    void netTeardownNeverUsesSudoOnTheOperatorCli_evenWhenUnprivileged() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-net-sudo");
        FakeHostEnv user = new FakeHostEnv("Linux", tmp.toString(), 1000L);
        user.env.put("SQS_SERVICE_LABEL", "studioquack-sqs");
        String asUser = readScript(new SidecarUninstaller(mock(Environment.class), user,
                new CapturingSpawner(), tmp).spawn(ServiceManager.SYSTEMD, false, false, 0L));
        assertTrue(asUser.contains("\"$NETCLI\" down"), asUser);
        assertTrue(asUser.contains("\"$NETCLI\" logout"), asUser);
        assertFalse(asUser.contains("sudo -n \"$NETCLI\""),
                "unprivileged CLI teardown must not sudo (daemon is operator): " + asUser);
        assertTrue(asUser.contains("net_root ip link delete sqnet0"), asUser);
        assertTrue(asUser.contains("net_root systemctl disable --now sq-net-agent"), asUser);
        assertTrue(asUser.contains("/usr/local/sbin/sq-net-agent"), asUser);
        assertFalse(asUser.contains("\nip link del"), "bare ip link must not run without net_root: " + asUser);

        FakeHostEnv root = new FakeHostEnv("Linux", tmp.toString(), 0L);
        root.env.put("SQS_SERVICE_LABEL", "studioquack-sqs");
        String asRoot = readScript(new SidecarUninstaller(mock(Environment.class), root,
                new CapturingSpawner(), tmp).spawn(ServiceManager.SYSTEMD, false, false, 0L));
        assertTrue(asRoot.contains("\"$NETCLI\" down"), asRoot);
        assertFalse(asRoot.contains("sudo -n \"$NETCLI\""), asRoot);
        assertTrue(asRoot.contains("net_root systemctl disable --now sq-net-agent"), asRoot);
    }

    // --- Podman removal (opt-in) --------------------------------------------------------------------
    // Stopping Podman is always safe; REMOVING it is not, because a self-hosted box commonly shares it
    // with other work. So it is opt-in and separate from the unconditional container reap.

    @Test
    void podmanIsKeptUnlessOperatorOptsIn() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-podman-keep");
        FakeHostEnv host = new FakeHostEnv("Mac OS X", tmp.toString(), 501L);
        host.env.put("SQS_SERVICE_LABEL", "com.studioquack.sqs");
        SidecarUninstaller sidecar =
                new SidecarUninstaller(mock(Environment.class), host, new CapturingSpawner(), tmp);

        String script = readScript(sidecar.spawn(ServiceManager.LAUNCHD, false, false, 0L));

        assertTrue(script.contains("podman rm -f"), "containers are always reaped");
        assertTrue(script.contains("podman machine stop"), "the machine is always stopped on macOS");
        assertFalse(script.contains("podman machine rm"), "must NOT remove podman without opt-in");
        assertFalse(script.contains("brew uninstall"), "must NOT uninstall podman without opt-in");
    }

    @Test
    void macos_removePodman_uninstallsViaBrewAfterRemovingTheMachine() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-podman-mac");
        FakeHostEnv host = new FakeHostEnv("Mac OS X", tmp.toString(), 501L);
        host.env.put("SQS_SERVICE_LABEL", "com.studioquack.sqs");
        SidecarUninstaller sidecar =
                new SidecarUninstaller(mock(Environment.class), host, new CapturingSpawner(), tmp);

        String script = readScript(sidecar.spawn(ServiceManager.LAUNCHD, false, true, 0L));

        assertTrue(script.contains("podman machine rm -f"), script);
        assertTrue(script.contains("brew uninstall --force podman"), script);
        // Order matters: uninstalling the package while a VM image remains orphans GBs of disk.
        assertTrue(script.indexOf("podman machine rm") < script.indexOf("brew uninstall"),
                "machine must be removed BEFORE the package");
    }

    @Test
    void linux_removePodman_usesThePackageManager() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-podman-linux");
        FakeHostEnv host = new FakeHostEnv("Linux", tmp.toString(), 1000L);
        host.env.put("SQS_SERVICE_LABEL", "studioquack-sqs-pro");
        SidecarUninstaller sidecar =
                new SidecarUninstaller(mock(Environment.class), host, new CapturingSpawner(), tmp);

        String script = readScript(sidecar.spawn(ServiceManager.SYSTEMD, false, true, 0L));

        assertTrue(script.contains("apt-get remove -y podman"), script);
        assertTrue(script.contains("dnf remove -y podman"), script);
        assertFalse(script.contains("brew uninstall"), "brew is not a Linux path");
    }

    @Test
    void windows_isNotSpawned() {
        FakeHostEnv host = new FakeHostEnv("Windows 11", "C:/Users/x", 0L);
        SidecarUninstaller sidecar =
                new SidecarUninstaller(mock(Environment.class), host, new CapturingSpawner(), Path.of("."));

        SidecarUninstaller.SidecarResult result =
                sidecar.spawn(ServiceManager.WINDOWS_SERVICE, false, false, 0L);

        assertFalse(result.spawned());
        assertTrue(result.notes().get(0).toLowerCase().contains("windows"), result.notes().toString());
    }

    @Test
    void launcherFailure_isReportedAsNotSpawned() throws Exception {
        Path tmp = Files.createTempDirectory("sq-sidecar-fail");
        FakeHostEnv host = new FakeHostEnv("Mac OS X", tmp.toString(), 501L);
        host.env.put("SQS_SERVICE_LABEL", "com.studioquack.sqs");
        CapturingSpawner spawner = new CapturingSpawner();
        spawner.result = false; // launcher exited non-zero
        SidecarUninstaller sidecar = new SidecarUninstaller(mock(Environment.class), host, spawner, tmp);

        SidecarUninstaller.SidecarResult result = sidecar.spawn(ServiceManager.LAUNCHD, false, false, 0L);

        assertFalse(result.spawned());
    }
}
