package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.maintenance.ServiceManagerDetector.ServiceManager;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ServiceManagerDetector}. Proves the best-effort detection of the supervising service
 * manager (which decides how restart and update bring the daemon back) and that the installer
 * override always wins.
 */
class ServiceManagerDetectorTest {

    private final ServiceManagerDetector detector = new ServiceManagerDetector();

    @Test
    void systemd_detectedViaInvocationId() {
        assertEquals(ServiceManager.SYSTEMD,
                detector.detect(Map.of("INVOCATION_ID", "9f8e"), "Linux"));
        assertEquals(ServiceManager.SYSTEMD,
                detector.detect(Map.of("JOURNAL_STREAM", "8:12345"), "Linux"));
    }

    @Test
    void launchd_detectedViaXpcServiceName_onMac() {
        assertEquals(ServiceManager.LAUNCHD,
                detector.detect(Map.of("XPC_SERVICE_NAME", "com.studioquack.sqs"), "Mac OS X"));
    }

    @Test
    void launchd_notDetected_forInteractiveShellSentinel() {
        // launchd reports XPC_SERVICE_NAME="0" for a plain interactive shell.
        assertEquals(ServiceManager.NONE,
                detector.detect(Map.of("XPC_SERVICE_NAME", "0"), "Mac OS X"));
    }

    @Test
    void none_whenNoSignalsPresent() {
        assertEquals(ServiceManager.NONE, detector.detect(Map.of(), "Linux"));
        assertEquals(ServiceManager.NONE, detector.detect(Map.of(), "Windows 11"));
    }

    @Test
    void explicitOverride_winsOverAutoDetect() {
        // Override to NONE even though systemd markers are present.
        assertEquals(ServiceManager.NONE,
                detector.detect(Map.of("SQVM_SERVICE_MANAGER", "none", "INVOCATION_ID", "x"), "Linux"));
        // Override to windows-service (no JVM-readable signal otherwise).
        assertEquals(ServiceManager.WINDOWS_SERVICE,
                detector.detect(Map.of("SQVM_SERVICE_MANAGER", "windows-service"), "Windows 11"));
        // Override to systemd on any OS.
        assertEquals(ServiceManager.SYSTEMD,
                detector.detect(Map.of("SQVM_SERVICE_MANAGER", "systemd"), "Mac OS X"));
    }

    // 2026-11-04: a Windows engine refused "restart" with detected=NONE although the installer's
    // scheduled task supervises it — the task is named in SQS_SERVICE_LABEL.
    @Test
    void windowsWithTheInstallersRegisteredTask_isTheScheduledTask() {
        Map<String, String> env = Map.of("SQS_SERVICE_LABEL", "StudioQuack SQS");
        assertEquals(ServiceManager.WINDOWS_SCHEDULED_TASK,
                detector.detect(env, "Windows 11", "", "StudioQuack SQS"::equals));
        assertEquals(ServiceManager.NONE, detector.detect(Map.of(), "Windows 11", "", task -> true));
        assertEquals(ServiceManager.NONE, detector.detect(env, "Linux", "", task -> true));
    }

    // The non-elevated installer falls back to a background process but still exports
    // SQS_SERVICE_LABEL; starting that never-registered task after an update left the engine down.
    @Test
    void windowsBackgroundInstall_withoutARegisteredTask_isUnsupervised() {
        assertEquals(ServiceManager.NONE, detector.detect(
                Map.of("SQS_SERVICE_LABEL", "StudioQuack SQS"), "Windows 11", "", task -> false));
    }

    // 2026-11-05: a root systemd unit's update died with "no supervisor recorded in the host install
    // marker" — the installer wrote that file under the operator's home, the daemon looked in root's.
    @Test
    void systemdUnit_isDetectedFromTheProcessItself() {
        assertEquals(ServiceManager.SYSTEMD, detector.detect(
                Map.of("INVOCATION_ID", "9f8e"), "Linux", "studioquack-sqs.service", task -> false));
    }

    @Test
    void systemdVariablesInheritedByATerminalShell_areNotASupervisor() {
        assertEquals(ServiceManager.NONE, detector.detect(
                Map.of("INVOCATION_ID", "9f8e", "JOURNAL_STREAM", "8:1"), "Linux",
                "vte-spawn-1c2d.scope", task -> false));
    }

    @Test
    void launchd_notDetected_forATerminalAppsShell() {
        assertEquals(ServiceManager.NONE, detector.detect(
                Map.of("XPC_SERVICE_NAME", "application.com.googlecode.iterm2.4711"), "Mac OS X"));
    }

    @Test
    void unknownOverride_fallsBackToAutoDetect() {
        assertEquals(ServiceManager.SYSTEMD,
                detector.detect(Map.of("SQVM_SERVICE_MANAGER", "bogus", "INVOCATION_ID", "x"), "Linux"));
    }

    @Test
    void relaunchesOnExit_isTrueForManagers_falseForNone() {
        assertTrue(ServiceManager.SYSTEMD.relaunchesOnExit());
        assertTrue(ServiceManager.LAUNCHD.relaunchesOnExit());
        assertTrue(ServiceManager.WINDOWS_SERVICE.relaunchesOnExit());
        assertFalse(ServiceManager.NONE.relaunchesOnExit());
    }
}
