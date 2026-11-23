package com.studioquack.sqs.maintenance;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Windows update hand-over: the helper runs as its own one-shot scheduled task, outside the
 * engine's task. 2026-11-10: a self-hosted Windows engine went offline after "Update engine" and
 * never came back — the helper lived in the engine task's process tree, where a stopped task takes
 * it down and a still-Running instance makes Start-ScheduledTask a silent no-op.
 */
class DaemonRelauncherTest {

    private static Map<String, String> helperArgs(String staged) {
        Map<String, String> args = new LinkedHashMap<>();
        args.put("PidToWait", "17288");
        args.put("TaskName", "StudioQuack SQS");
        args.put("Jar", "C:\\Users\\Mert\\AppData\\Local\\StudioQuack\\sqs\\sqs-client-daemon-1.0.0.jar");
        args.put("Staged", staged);
        args.put("Port", "9876");
        args.put("Launcher", "C:\\Users\\Mert\\AppData\\Local\\StudioQuack\\sqs\\run.cmd");
        args.put("Log", "C:\\Users\\Mert\\AppData\\Local\\StudioQuack\\sqs\\logs\\daemon.log");
        args.put("Notes", "C:\\Users\\Mert\\AppData\\Local\\StudioQuack\\sqs\\data\\logs\\daemon.log");
        return args;
    }

    private static String registration(String staged) {
        return DaemonRelauncher.windowsTaskRegistration("StudioQuack SQS", "StudioQuack SQS relaunch",
                Path.of("C:\\Users\\Mert\\AppData\\Local\\Temp\\sqs-relaunch-1.ps1"), helperArgs(staged));
    }

    @Test
    void registersTheHelperAsItsOwnTaskWithTheEngineTasksPrincipal() {
        String ps = registration("C:\\Users\\Mert\\AppData\\Local\\StudioQuack\\sqs\\sqs-client-daemon-1.0.0.jar.new");

        assertTrue(ps.contains("$main = Get-ScheduledTask -TaskName 'StudioQuack SQS'"), ps);
        assertTrue(ps.contains("Register-ScheduledTask -TaskName 'StudioQuack SQS relaunch' -Action $action "
                + "-Principal $main.Principal"), ps);
        assertTrue(ps.contains("-MultipleInstances IgnoreNew"), ps);
        assertTrue(ps.contains("Start-ScheduledTask -TaskName 'StudioQuack SQS relaunch'"), ps);
    }

    @Test
    void passesNamedHelperParametersAndTheTaskItRunsIn() {
        String ps = registration("C:\\x\\sqs.jar.new");

        assertTrue(ps.contains("-File \"C:\\Users\\Mert\\AppData\\Local\\Temp\\sqs-relaunch-1.ps1\""), ps);
        assertTrue(ps.contains("-PidToWait \"17288\""), ps);
        assertTrue(ps.contains("-TaskName \"StudioQuack SQS\""), ps);
        assertTrue(ps.contains("-Staged \"C:\\x\\sqs.jar.new\""), ps);
        assertTrue(ps.contains("-SelfTask \"StudioQuack SQS relaunch\""), ps);
    }

    /** A plain restart has no staged jar: an empty value through -File could shift every later one. */
    @Test
    void leavesOutEmptyParameters() {
        String ps = registration("");

        assertFalse(ps.contains("-Staged"), ps);
        assertTrue(ps.contains("-Port \"9876\""), ps);
    }

    /** A second helper would race the first over the same jar; the caller then keeps the child path. */
    @Test
    void refusesWhileAPreviousHelperStillRunsAndUnregistersOneThatNeverStarted() {
        String ps = registration("");

        assertTrue(ps.contains("if ($old -and $old.State -eq 'Running')"), ps);
        assertTrue(ps.contains("exit 2"), ps);
        int startCheck = ps.indexOf("Start-ScheduledTask -TaskName 'StudioQuack SQS relaunch'");
        int unregister = ps.indexOf("Unregister-ScheduledTask -TaskName 'StudioQuack SQS relaunch'");
        assertTrue(startCheck > 0 && unregister > startCheck, ps);
        assertTrue(ps.contains("exit 3"), ps);
    }

    @Test
    void quotesSingleQuotesInPowerShellLiterals() {
        String ps = DaemonRelauncher.windowsTaskRegistration("O'Neil SQS", "O'Neil SQS relaunch",
                Path.of("C:\\Users\\O'Neil\\x.ps1"), Map.of("PidToWait", "1"));

        assertTrue(ps.contains("-TaskName 'O''Neil SQS'"), ps);
        assertTrue(ps.contains("-File \"C:\\Users\\O''Neil\\x.ps1\""), ps);
    }

    @Test
    void doublesBackslashesBeforeAClosingQuote() {
        String ps = DaemonRelauncher.windowsTaskRegistration("SQS", "SQS relaunch",
                Path.of("x.ps1"), Map.of("Log", "C:\\logs\\"));

        assertTrue(ps.contains("-Log \"C:\\logs\\\\\""), ps);
    }

    @Test
    void refusesAValueThatCannotBeQuoted() {
        assertThrows(IllegalArgumentException.class, () -> DaemonRelauncher.windowsTaskRegistration(
                "SQS", "SQS relaunch", Path.of("x.ps1"), Map.of("Log", "C:\\a\"b")));
    }

    @Test
    void encodesTheCommandAsUtf16LeBase64() {
        String encoded = DaemonRelauncher.encodeCommand("Write-Output 'ü'");

        assertEquals("Write-Output 'ü'",
                new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_16LE));
    }

    /**
     * 2026-11-11: every Windows engine stayed offline after "Update engine". Windows PowerShell 5.1
     * read the BOM-less helper in the ANSI code page; the em dash's last UTF-8 byte (0x94) became a
     * curly closing quote, the script did not parse and never ran.
     */
    @Test
    void helperScriptIsAsciiOnly() {
        String s = DaemonRelauncher.WINDOWS_SCRIPT;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            assertTrue(c == '\n' || c == '\t' || (c >= 0x20 && c < 0x7F),
                    "non-ASCII U+" + Integer.toHexString(c) + " at " + i + ": "
                            + s.substring(Math.max(0, i - 40), Math.min(s.length(), i + 10)));
        }
    }

    @Test
    void helperFileIsWrittenWithAUtf8Bom() {
        byte[] bytes = DaemonRelauncher.windowsScriptBytes();

        assertEquals((byte) 0xEF, bytes[0]);
        assertEquals((byte) 0xBB, bytes[1]);
        assertEquals((byte) 0xBF, bytes[2]);
        assertEquals(DaemonRelauncher.WINDOWS_SCRIPT,
                new String(bytes, 3, bytes.length - 3, StandardCharsets.UTF_8));
    }

    @Test
    void helperStopsALingeringInstanceOnlyFromItsOwnTaskAndAlwaysRemovesThatTask() {
        String s = DaemonRelauncher.WINDOWS_SCRIPT;

        assertTrue(s.contains("[string]$SelfTask"), "helper takes the task it runs in");
        assertTrue(s.contains("if ($SelfTask -and (Get-TaskState) -eq 'Running')"),
                "a lingering instance is stopped only from the helper's own task");
        int tryAt = s.indexOf("try {\n  Say \"waiting for daemon pid");
        int finallyAt = s.indexOf("} finally {");
        int unregister = s.indexOf("Unregister-ScheduledTask -TaskName $SelfTask -Confirm:$false");
        assertTrue(tryAt >= 0 && finallyAt > tryAt && unregister > finallyAt, s);
        assertTrue(s.contains("did not start (state="), "an ignored start is logged, not silent");
    }
}
