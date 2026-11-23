package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.maintenance.DaemonLifecycleService.ProcessResult;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.ProcessRunner;
import com.studioquack.sqs.maintenance.ServiceManagerDetector.ServiceManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves uninstall disables/removes the host service unit and schedules a deferred bootout/stop
 * so KeepAlive/{@code Restart=} cannot relaunch after Environment Delete.
 */
class HostServiceUninstallerTest {

    private static final class RecordingRunner implements ProcessRunner {
        final List<List<String>> commands = new ArrayList<>();
        int exitCode = 0;

        @Override
        public ProcessResult run(List<String> command, long timeoutMillis) {
            commands.add(List.copyOf(command));
            return new ProcessResult(exitCode, "");
        }
    }

    private static final class MapEnv implements HostServiceUninstaller.HostEnv {
        final Map<String, String> env = new HashMap<>();
        String home;
        long uid = 501L;
        String cgroup = "";
        String osName = "Mac OS X";

        MapEnv(Path home) {
            this.home = home.toString();
        }

        @Override public String get(String key) { return env.get(key); }
        @Override public String userHome() { return home; }
        @Override public long userId() { return uid; }
        @Override public String cgroup() { return cgroup; }
        @Override public String osName() { return osName; }
    }

    @TempDir
    Path temp;

    @Test
    void launchd_disablesUnit_removesPlist_andSchedulesDeferredBootout() throws Exception {
        Path agents = temp.resolve("Library/LaunchAgents");
        Files.createDirectories(agents);
        Path plist = agents.resolve("com.studioquack.sqs.plist");
        Files.writeString(plist, "<?xml version=\"1.0\"?><plist/>");

        RecordingRunner runner = new RecordingRunner();
        MapEnv env = new MapEnv(temp);
        env.env.put("XPC_SERVICE_NAME", "com.studioquack.sqs");
        HostServiceUninstaller uninstaller = new HostServiceUninstaller(runner, env);

        HostServiceUninstaller.UnloadResult result = uninstaller.unload(ServiceManager.LAUNCHD, 5_000L);

        assertTrue(result.serviceDisabled());
        assertTrue(result.unitFileRemoved());
        assertTrue(result.deferredUnloadScheduled());
        assertTrue(result.hostSupervisionCleared());
        assertEquals("com.studioquack.sqs", result.label());
        assertFalse(Files.exists(plist), "plist must be deleted so launchd cannot reload it");
        assertTrue(runner.commands.stream().anyMatch(c ->
                c.equals(List.of("launchctl", "disable", "gui/501/com.studioquack.sqs"))));
        assertTrue(runner.commands.stream().anyMatch(c ->
                c.equals(List.of("launchctl", "disable", "user/501/com.studioquack.sqs"))));
        // Synchronous bootout would kill the JVM mid-ACK — only deferred via nohup/sh.
        assertTrue(runner.commands.stream().noneMatch(c ->
                c.size() >= 2 && "launchctl".equals(c.get(0)) && c.contains("bootout")));
        assertTrue(runner.commands.stream().anyMatch(c ->
                c.size() >= 3 && "/bin/sh".equals(c.get(0)) && "-c".equals(c.get(1))
                        && c.get(2).contains("bootout") && c.get(2).contains("nohup")));
    }

    @Test
    void launchd_prefersSqsServiceLabel_overXpc() throws Exception {
        Path agents = temp.resolve("Library/LaunchAgents");
        Files.createDirectories(agents);
        Files.writeString(agents.resolve("com.studioquack.sqs-pro.plist"), "<plist/>");

        RecordingRunner runner = new RecordingRunner();
        MapEnv env = new MapEnv(temp);
        env.env.put(HostServiceUninstaller.SERVICE_LABEL_ENV, "com.studioquack.sqs-pro");
        env.env.put("XPC_SERVICE_NAME", "com.studioquack.sqs");
        HostServiceUninstaller uninstaller = new HostServiceUninstaller(runner, env);

        HostServiceUninstaller.UnloadResult result = uninstaller.unload(ServiceManager.LAUNCHD);

        assertEquals("com.studioquack.sqs-pro", result.label());
        assertTrue(runner.commands.stream().anyMatch(c ->
                c.contains("gui/501/com.studioquack.sqs-pro")));
    }

    @Test
    void systemd_disablesUnit_withoutNow_schedulesDeferredStop() throws Exception {
        Path userUnits = temp.resolve(".config/systemd/user");
        Files.createDirectories(userUnits);
        Path unitFile = userUnits.resolve("studioquack-sqs-pro.service");
        Files.writeString(unitFile, "[Unit]\nDescription=test\n");

        RecordingRunner runner = new RecordingRunner();
        MapEnv env = new MapEnv(temp);
        env.osName = "Linux";
        env.env.put(HostServiceUninstaller.SERVICE_LABEL_ENV, "studioquack-sqs-pro");
        env.uid = 1000L;
        HostServiceUninstaller uninstaller = new HostServiceUninstaller(runner, env);

        HostServiceUninstaller.UnloadResult result = uninstaller.unload(ServiceManager.SYSTEMD, 4_000L);

        assertTrue(result.serviceDisabled());
        assertTrue(result.unitFileRemoved());
        assertTrue(result.deferredUnloadScheduled());
        assertEquals("studioquack-sqs-pro", result.label());
        assertFalse(Files.exists(unitFile));
        assertEquals(List.of("systemctl", "--user", "disable", "studioquack-sqs-pro"),
                runner.commands.get(0));
        assertTrue(runner.commands.stream().noneMatch(c -> c.contains("--now")),
                "must not stop the unit mid-ACK");
        assertTrue(runner.commands.stream().anyMatch(c ->
                c.size() >= 3 && c.get(2).contains("systemctl --user stop")
                        && c.get(2).contains("nohup")));
    }

    @Test
    void none_onMac_stillAttemptsLaunchdUnload_whenPlistPresent() throws Exception {
        Path agents = temp.resolve("Library/LaunchAgents");
        Files.createDirectories(agents);
        Files.writeString(agents.resolve("com.studioquack.sqs.plist"), "<plist/>");

        RecordingRunner runner = new RecordingRunner();
        MapEnv env = new MapEnv(temp);
        env.osName = "Mac OS X";
        HostServiceUninstaller uninstaller = new HostServiceUninstaller(runner, env);

        HostServiceUninstaller.UnloadResult result = uninstaller.unload(ServiceManager.NONE);

        assertTrue(result.deferredUnloadScheduled() || result.serviceDisabled());
        assertEquals("com.studioquack.sqs", result.label());
    }

    @Test
    void none_onUnknownOs_skipsUnload() {
        RecordingRunner runner = new RecordingRunner();
        MapEnv env = new MapEnv(temp);
        // An OS with no supervisor binding at all. Windows is deliberately NOT the example here any
        // more: it has a scheduled-task path of its own, covered below.
        env.osName = "SunOS";
        HostServiceUninstaller uninstaller = new HostServiceUninstaller(runner, env);

        HostServiceUninstaller.UnloadResult result = uninstaller.unload(ServiceManager.NONE);

        assertFalse(result.serviceDisabled());
        assertFalse(result.deferredUnloadScheduled());
        assertTrue(runner.commands.isEmpty());
    }

    @Test
    void windows_endsAndDeletesTheInstallerScheduledTask() {
        RecordingRunner runner = new RecordingRunner();
        MapEnv env = new MapEnv(temp);
        env.osName = "Windows 11";
        env.env.put(HostServiceUninstaller.SERVICE_LABEL_ENV, "StudioQuackSQS");
        HostServiceUninstaller uninstaller = new HostServiceUninstaller(runner, env);

        HostServiceUninstaller.UnloadResult result = uninstaller.unload(ServiceManager.WINDOWS_SERVICE);

        assertEquals("StudioQuackSQS", result.label());
        assertTrue(result.serviceDisabled());
        assertTrue(result.hostSupervisionCleared());
        // /End before /Delete so the task's restart policy cannot treat our exit as a crash.
        assertEquals(List.of("schtasks", "/End", "/TN", "StudioQuackSQS"), runner.commands.get(0));
        assertEquals(List.of("schtasks", "/Delete", "/TN", "StudioQuackSQS", "/F"), runner.commands.get(1));
        assertTrue(runner.commands.stream().anyMatch(c -> c.equals(List.of("sc.exe", "stop", "sq-net-agent"))),
                runner.commands.toString());
        assertTrue(runner.commands.stream().anyMatch(c -> c.equals(List.of("sc.exe", "delete", "sq-net-agent"))),
                runner.commands.toString());
        assertTrue(result.notes().stream().anyMatch(n -> n.contains("mesh agent")), result.notes().toString());
    }

    @Test
    void windows_reportsWhenTheTaskSurvives() {
        RecordingRunner runner = new RecordingRunner();
        runner.exitCode = 1; // schtasks refuses (not elevated / task already gone)
        MapEnv env = new MapEnv(temp);
        env.osName = "Windows 11";
        env.env.put(HostServiceUninstaller.SERVICE_LABEL_ENV, "StudioQuackSQS");
        HostServiceUninstaller uninstaller = new HostServiceUninstaller(runner, env);

        HostServiceUninstaller.UnloadResult result = uninstaller.unload(ServiceManager.WINDOWS_SERVICE);

        assertFalse(result.serviceDisabled());
        assertTrue(result.notes().stream().anyMatch(n -> n.contains("may relaunch the daemon")));
    }

    @Test
    void parseSystemdUnitFromCgroup_prefersAppUnit() {
        Optional<String> unit = HostServiceUninstaller.parseSystemdUnitFromCgroup(
                "0::/user.slice/user-1000.slice/user@1000.service/app.slice/studioquack-sqs-pro.service");
        assertEquals(Optional.of("studioquack-sqs-pro.service"), unit);
    }

    @Test
    void shellSingleQuote_escapesEmbeddedQuotes() {
        assertEquals("'foo'\\''bar'", HostServiceUninstaller.shellSingleQuote("foo'bar"));
    }
}
