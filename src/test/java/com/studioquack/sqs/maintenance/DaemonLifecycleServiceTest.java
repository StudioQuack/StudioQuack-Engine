package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.maintenance.DaemonLifecycleService.LifecycleResult;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.ProcessResult;
import com.studioquack.sqs.maintenance.ServiceManagerDetector.ServiceManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link DaemonLifecycleService}. Proves the self-restart / stop / reinstall DECISIONS without
 * ending the JVM or shelling out: the process-exit and host-command executions are behind fake
 * seams.
 */
class DaemonLifecycleServiceTest {

    /** Records scheduled exits instead of terminating the JVM. */
    private static final class RecordingExiter implements DaemonLifecycleService.Exiter {
        final List<String> calls = new ArrayList<>();

        @Override
        public void scheduleExit(long graceMillis, int exitCode, String reason) {
            calls.add(reason + ":" + exitCode + ":" + graceMillis);
        }
    }

    /** Returns a canned result or throws, per test wiring. */
    private static final class StubRunner implements DaemonLifecycleService.ProcessRunner {
        ProcessResult result;
        Exception toThrow;
        int invocations;
        final List<List<String>> commands = new ArrayList<>();

        @Override
        public ProcessResult run(List<String> command, long timeoutMillis) throws Exception {
            invocations++;
            commands.add(command);
            if (toThrow != null) {
                throw toThrow;
            }
            return result;
        }
    }

    private ServiceManagerDetector detector;
    private MaintenanceProperties props;
    private RecordingExiter exiter;
    private StubRunner runner;
    private SidecarUninstaller sidecar;
    private DaemonRelauncher relauncher;
    private DaemonLifecycleService service;

    @BeforeEach
    void setUp() {
        detector = mock(ServiceManagerDetector.class);
        props = new MaintenanceProperties();
        props.setRestartGraceMillis(0);
        props.setStopGraceMillis(0);
        exiter = new RecordingExiter();
        runner = new StubRunner();
        // By default the side-car does NOT spawn, so uninstall exercises the legacy in-process
        // host-unload + self-exit fallback these tests assert. The spawned path is covered separately.
        sidecar = mock(SidecarUninstaller.class);
        relauncher = mock(DaemonRelauncher.class);
        when(sidecar.spawn(any(), anyBoolean(), anyBoolean(), anyLong()))
                .thenReturn(SidecarUninstaller.SidecarResult.notSpawned("test: side-car disabled"));
        // Host uninstaller uses the same stub runner; default env has no unit markers.
        HostServiceUninstaller hostUninstaller = new HostServiceUninstaller(runner,
                new HostServiceUninstaller.HostEnv() {
                    @Override public String get(String key) { return null; }
                    @Override public String userHome() { return System.getProperty("user.home", "."); }
                    @Override public long userId() { return 501L; }
                    @Override public String cgroup() { return ""; }
                });
        service = new DaemonLifecycleService(detector, props, exiter, runner, hostUninstaller, sidecar, relauncher);
    }

    // ---- restart --------------------------------------------------------

    @Test
    void restart_schedulesExit_whenServiceManaged() {
        when(detector.detect()).thenReturn(ServiceManager.SYSTEMD);

        LifecycleResult result = service.requestRestart();

        assertTrue(result.accepted(), () -> "restart should be accepted: " + result.message());
        assertEquals(1, exiter.calls.size(), "a self-exit is scheduled");
        // systemd's Restart=on-failure ignores a clean exit — the restart exit code must be non-zero.
        assertTrue(exiter.calls.get(0).startsWith("restart:75:"), exiter.calls.toString());
    }

    /** The unit comes back even when the exit ends 0: a transient timer outside the cgroup starts it. */
    @Test
    void relaunch_underSystemd_armsARelaunchGuardForTheInstalledUnit() throws Exception {
        when(detector.detect()).thenReturn(ServiceManager.SYSTEMD);
        HostServiceUninstaller labelled = new HostServiceUninstaller(runner, new HostServiceUninstaller.HostEnv() {
            @Override public String get(String key) {
                return HostServiceUninstaller.SERVICE_LABEL_ENV.equals(key) ? "studioquack-sqs" : null;
            }
            @Override public String userHome() { return System.getProperty("user.home", "."); }
            @Override public long userId() { return 0L; }
            @Override public String cgroup() { return ""; }
        });
        service = new DaemonLifecycleService(detector, props, exiter, runner, labelled, sidecar, relauncher);
        runner.result = new ProcessResult(0, "");

        service.relaunch(null, null, 0, "update");

        assertEquals(1, runner.commands.size(), runner.commands.toString());
        List<String> argv = runner.commands.get(0);
        assertEquals("systemd-run", argv.get(0));
        assertFalse(argv.contains("--user"), "a root-installed unit is guarded on the system manager");
        assertTrue(argv.contains("--on-active=60s"), argv.toString());
        assertEquals(List.of("systemctl", "start", "studioquack-sqs.service"),
                argv.subList(argv.size() - 3, argv.size()));
        assertTrue(exiter.calls.get(0).startsWith("update:75:"), exiter.calls.toString());
    }

    @Test
    void relaunchGuardCommand_forAUserUnit_targetsTheUserManager() {
        List<String> argv = DaemonLifecycleService.systemdRelaunchGuardCommand("studioquack-sqs-pro", true, 42L);

        assertEquals(List.of("systemd-run", "--user", "--unit=studioquack-sqs-pro-relaunch-42", "--on-active=60s",
                "--timer-property=AccuracySec=1s", "--description=StudioQuack engine relaunch guard",
                "systemctl", "--user", "start", "studioquack-sqs-pro.service"), argv);
    }

    @Test
    void relaunch_underLaunchd_armsNoSystemdGuard() throws Exception {
        when(detector.detect()).thenReturn(ServiceManager.LAUNCHD);

        service.relaunch(null, null, 0, "restart");

        assertTrue(runner.commands.isEmpty(), runner.commands.toString());
        assertTrue(exiter.calls.get(0).startsWith("restart:75:"), exiter.calls.toString());
    }

    @Test
    void restart_underTheWindowsScheduledTask_spawnsTheRelaunchHelper_thenExitsCleanly() throws Exception {
        when(detector.detect()).thenReturn(ServiceManager.WINDOWS_SCHEDULED_TASK);
        when(detector.serviceLabel()).thenReturn("StudioQuack SQS");

        LifecycleResult result = service.requestRestart();

        assertTrue(result.accepted(), () -> "restart should be accepted: " + result.message());
        verify(relauncher).spawn(eq("StudioQuack SQS"), isNull(), isNull());
        assertTrue(exiter.calls.get(0).startsWith("restart:0:"), exiter.calls.toString());
    }

    // Background / foreground installs (and the non-elevated Windows fallback) have no supervisor:
    // the helper starts the launcher beside the jar again.
    @Test
    void restart_withoutASupervisor_handsOverToTheRelaunchHelper_thenExitsCleanly() throws Exception {
        when(detector.detect()).thenReturn(ServiceManager.NONE);

        LifecycleResult result = service.requestRestart();

        assertTrue(result.accepted(), () -> "restart should be accepted: " + result.message());
        verify(relauncher).spawn(eq(""), isNull(), isNull());
        assertTrue(exiter.calls.get(0).startsWith("restart:0:"), exiter.calls.toString());
    }

    @Test
    void restart_refused_whenNothingCouldStartItAgain_andDoesNotExit() throws Exception {
        when(detector.detect()).thenReturn(ServiceManager.NONE);
        doThrow(new IllegalStateException("no supervisor watches this daemon and there is no run.sh"))
                .when(relauncher).spawn(any(), any(), any());

        LifecycleResult result = service.requestRestart();

        assertFalse(result.accepted(), "restart must be refused when nothing would relaunch it");
        assertTrue(result.message().contains("no supervisor watches this daemon"), result.message());
        assertTrue(exiter.calls.isEmpty(), "must NOT self-exit when it cannot come back");
    }

    @Test
    void restart_underSystemd_neverSpawnsTheHelper() throws Exception {
        when(detector.detect()).thenReturn(ServiceManager.SYSTEMD);

        service.requestRestart();

        verify(relauncher, never()).spawn(any(), any(), any());
    }

    @Test
    void restart_honoursServiceManagedOverride_true() {
        when(detector.detect()).thenReturn(ServiceManager.NONE); // auto-detect sees nothing
        props.setServiceManaged(Boolean.TRUE); // operator asserts it IS managed

        LifecycleResult result = service.requestRestart();

        assertTrue(result.accepted(), "explicit service-managed=true allows restart");
        assertEquals(1, exiter.calls.size());
    }

    // ---- uninstall ------------------------------------------------------

    @Test
    void uninstall_disablesLaunchdUnit_thenSchedulesExit() throws Exception {
        when(detector.detect()).thenReturn(ServiceManager.LAUNCHD);
        // Rebuild with an env that exposes a LaunchAgent label + plist.
        Path agents = Files.createTempDirectory("sqs-uninstall-agents");
        Path launchAgents = agents.resolve("Library/LaunchAgents");
        Files.createDirectories(launchAgents);
        Files.writeString(launchAgents.resolve("com.studioquack.sqs.plist"), "<plist/>");
        HostServiceUninstaller hostUninstaller = new HostServiceUninstaller(runner,
                new HostServiceUninstaller.HostEnv() {
                    @Override public String get(String key) {
                        return "XPC_SERVICE_NAME".equals(key) ? "com.studioquack.sqs" : null;
                    }
                    @Override public String userHome() { return agents.toString(); }
                    @Override public long userId() { return 501L; }
                    @Override public String cgroup() { return ""; }
                });
        service = new DaemonLifecycleService(detector, props, exiter, runner, hostUninstaller, sidecar, relauncher);
        runner.result = new ProcessResult(0, "");

        LifecycleResult result = service.requestUninstall();

        assertTrue(result.accepted());
        assertTrue(result.message().contains("disabled=true"), result.message());
        assertTrue(result.message().contains("unitFileRemoved=true"), result.message());
        assertTrue(result.message().contains("deferredUnload=true"), result.message());
        assertFalse(result.message().contains("host uninstall step must remove"),
                "legacy 'host must remove unit' note must be gone when we unload ourselves");
        assertEquals(1, exiter.calls.size());
        assertTrue(exiter.calls.get(0).startsWith("uninstall:0:"));
        // disable gui + disable user + deferred bootout shell (at least)
        assertTrue(runner.invocations >= 2, "expected disable + deferred bootout, got " + runner.invocations);
    }

    @Test
    void uninstall_spawnsSideCar_andDoesNotSelfExit() {
        when(detector.detect()).thenReturn(ServiceManager.LAUNCHD);
        // Side-car spawns successfully → it owns stopping the daemon; we must NOT self-exit (that
        // would race KeepAlive before the side-car disables the unit).
        when(sidecar.spawn(any(), anyBoolean(), anyBoolean(), anyLong()))
                .thenReturn(new SidecarUninstaller.SidecarResult(
                        true, "/tmp/sq-uninstall.sh", "com.studioquack.sqs", java.util.List.of()));

        LifecycleResult result = service.requestUninstall(true);

        assertTrue(result.accepted());
        assertEquals("side-car", result.mechanism());
        assertTrue(result.message().contains("side-car teardown spawned"), result.message());
        assertTrue(result.message().contains("removeDependencies=true"), result.message());
        assertTrue(exiter.calls.isEmpty(),
                "when the side-car owns teardown the daemon must NOT schedule its own exit");
    }
}
