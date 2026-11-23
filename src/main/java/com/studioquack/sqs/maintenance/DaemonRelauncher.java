package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.engine.update.DaemonUpdateService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Brings the daemon back from outside its own process after it has exited — for a restart, or for
 * an in-place update (then the new jar comes up). Used wherever no supervisor starts the launcher
 * again by itself: A Windows scheduled task.
 */
@Slf4j
@Component
public class DaemonRelauncher {

    /** Name suffix of the one-shot task the Windows helper runs in, beside the engine's own task. */
    static final String RELAUNCH_TASK_SUFFIX = " relaunch";

    /** Registering and starting the helper task: PowerShell plus the ScheduledTasks module. */
    private static final long REGISTER_TIMEOUT_SECONDS = 60L;

    private final int healthPort;

    public DaemonRelauncher(@Value("${server.port:9876}") int healthPort) {
        this.healthPort = healthPort;
    }

    /**
     * The launcher beside the boot jar — {@code run.cmd} on Windows, {@code run.sh} elsewhere.
     *
     * @throws IllegalStateException when it is missing: nothing could start this process again
     */
    public static Path launcher(Path jar) {
        Path launcher = jar.resolveSibling(isWindows() ? "run.cmd" : "run.sh");
        if (!Files.isRegularFile(launcher)) {
            throw new IllegalStateException("no supervisor watches this daemon and there is no "
                    + launcher.getFileName() + " beside " + jar + " to start it again");
        }
        return launcher;
    }

    /**
     * Spawns the detached helper for this process.
     *
     * @param windowsTask The scheduled task to start; {@code ""} starts the launcher beside the jar
     * @param jar
     * @param staged On macOS/Linux {@code run.sh} swaps {@code <jar>.new} in itself.
     */
    public void spawn(String windowsTask, Path jar, Path staged) throws IOException {
        Path bootJar = jar != null ? jar : DaemonUpdateService.runningJar();
        String task = windowsTask == null ? "" : windowsTask.trim();
        Path launcher = task.isEmpty() ? launcher(bootJar) : bootJar.resolveSibling("run.cmd");
        Path log = bootJar.resolveSibling("logs").resolve("daemon.log");
        // Step lines go to the file the daemon itself logs to (run.sh exports LOGGING_FILE_NAME) —
        // the one "Fetch recent logs" tails — not only to the engine's stdout log beside the jar.
        String notesEnv = System.getenv("LOGGING_FILE_NAME");
        String notes = notesEnv == null || notesEnv.isBlank() ? log.toString() : notesEnv.trim();
        String pid = Long.toString(ProcessHandle.current().pid());
        String port = Integer.toString(healthPort);
        if (isWindows()) {
            Path script = Files.createTempFile("sqs-relaunch-", ".ps1");
            Files.write(script, windowsScriptBytes());
            Map<String, String> helperArgs = new LinkedHashMap<>();
            helperArgs.put("PidToWait", pid);
            helperArgs.put("TaskName", task);
            helperArgs.put("Jar", bootJar.toString());
            helperArgs.put("Staged", staged == null ? "" : staged.toString());
            helperArgs.put("Port", port);
            helperArgs.put("Launcher", launcher.toString());
            helperArgs.put("Log", log.toString());
            helperArgs.put("Notes", notes);
            if (!task.isEmpty() && startAsOwnTask(task, script, helperArgs)) {
                return;
            }
            new ProcessBuilder("cmd.exe", "/c", "start", "\"sqs-relaunch\"", "powershell.exe",
                    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script.toString(),
                    pid, task, bootJar.toString(), staged == null ? "" : staged.toString(), port,
                    launcher.toString(), log.toString(), notes)
                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start();
            return;
        }
        // Own session where setsid exists (Linux): the helper must outlive this process AND the
        // session/process group it was started in — a terminal closing or a session teardown.
        java.util.List<String> argv = new java.util.ArrayList<>();
        for (String setsid : new String[]{"/usr/bin/setsid", "/bin/setsid"}) {
            if (Files.isExecutable(Path.of(setsid))) {
                argv.add(setsid);
                break;
            }
        }
        argv.addAll(java.util.List.of("/bin/sh", "-c", POSIX_SCRIPT, "sqs-relaunch",
                pid, launcher.toString(), port, log.toString(), notes));
        new ProcessBuilder(argv)
                .redirectInput(ProcessBuilder.Redirect.from(new File("/dev/null")))
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    /**
     * Registers and starts {@code "<mainTask> relaunch"} running the helper; true once that task is
     * running. False — the caller spawns the detached child instead — when it cannot be registered or
     * started, or a helper from an earlier hand-over is still running in it.
     */
    private boolean startAsOwnTask(String mainTask, Path script, Map<String, String> helperArgs) {
        String selfTask = mainTask + RELAUNCH_TASK_SUFFIX;
        String registration;
        try {
            registration = windowsTaskRegistration(mainTask, selfTask, script, helperArgs);
        } catch (IllegalArgumentException unquotable) {
            log.warn("maintenance: relaunch helper runs as a child process: {}", unquotable.getMessage());
            return false;
        }
        Path out = null;
        try {
            out = Files.createTempFile("sqs-relaunch-task-", ".log");
            Process p = new ProcessBuilder("powershell.exe", "-NoProfile", "-NonInteractive",
                    "-ExecutionPolicy", "Bypass", "-EncodedCommand", encodeCommand(registration))
                    .redirectErrorStream(true)
                    .redirectOutput(out.toFile())
                    .start();
            if (!p.waitFor(REGISTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                log.warn("maintenance: registering the relaunch task timed out; the helper runs as a child process");
                return false;
            }
            if (p.exitValue() == 0) {
                log.info("maintenance: relaunch helper running as its own scheduled task '{}'", selfTask);
                return true;
            }
            log.warn("maintenance: relaunch task '{}' not started (exit {}): {}; the helper runs as a child process",
                    selfTask, p.exitValue(), Files.readString(out).trim());
            return false;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        } catch (IOException ex) {
            log.warn("maintenance: relaunch task not started: {}; the helper runs as a child process", ex.toString());
            return false;
        } finally {
            if (out != null) {
                try {
                    Files.deleteIfExists(out);
                } catch (IOException ignored) {
                    // a leftover temp log is harmless
                }
            }
        }
    }

    /**
     * The PowerShell that registers {@code selfTask} with {@code mainTask}'s principal (the
     * installer's S4U, highest-privilege user), starts it and waits until it runs. Exit 0 started; 2 a
     * previous helper still runs there (a second helper would race it over the same jar); 3 it never
     * started (it is unregistered again, so it cannot start later beside the fallback child); 1 any
     * error.
     *
     * @throws IllegalArgumentException For a value that cannot be quoted for {@code -File}
     */
    static String windowsTaskRegistration(String mainTask, String selfTask, Path script,
                                          Map<String, String> helperArgs) {
        StringBuilder arg = new StringBuilder("-NoProfile -ExecutionPolicy Bypass -File ")
                .append(fileArg(script.toString()));
        Map<String, String> named = new LinkedHashMap<>(helperArgs);
        named.put("SelfTask", selfTask);
        named.forEach((name, value) -> {
            if (value != null && !value.isEmpty()) {
                arg.append(" -").append(name).append(' ').append(fileArg(value));
            }
        });
        String main = psLiteral(mainTask);
        String self = psLiteral(selfTask);
        return String.join("\n",
                "$ErrorActionPreference = 'Stop'",
                "try {",
                "  $main = Get-ScheduledTask -TaskName " + main,
                "  $old = Get-ScheduledTask -TaskName " + self + " -ErrorAction SilentlyContinue",
                "  if ($old -and $old.State -eq 'Running') { Write-Output 'a relaunch helper is still running'; exit 2 }",
                "  $action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument " + psLiteral(arg.toString()),
                "  $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries "
                        + "-ExecutionTimeLimit (New-TimeSpan -Hours 1) -MultipleInstances IgnoreNew",
                "  Register-ScheduledTask -TaskName " + self + " -Action $action -Principal $main.Principal "
                        + "-Settings $settings -Force | Out-Null",
                "  Start-ScheduledTask -TaskName " + self,
                "  for ($i = 0; $i -lt 10; $i++) {",
                "    if ((Get-ScheduledTask -TaskName " + self + ").State -eq 'Running') { exit 0 }",
                "    Start-Sleep -Seconds 1",
                "  }",
                "  Unregister-ScheduledTask -TaskName " + self + " -Confirm:$false",
                "  Write-Output 'the relaunch task did not start'",
                "  exit 3",
                "} catch {",
                "  Write-Output $_.Exception.Message",
                "  exit 1",
                "}");
    }

    /**
     * The helper file as Windows PowerShell 5.1 must receive it: with a UTF-8 BOM. Without one it
     * reads the file in the ANSI code page, where the last byte of a UTF-8 em dash is a curly closing
     * quote — the string ended early, the script did not parse, and no Windows engine came back after
     * an update.
     */
    static byte[] windowsScriptBytes() {
        byte[] body = WINDOWS_SCRIPT.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[body.length + 3];
        out[0] = (byte) 0xEF;
        out[1] = (byte) 0xBB;
        out[2] = (byte) 0xBF;
        System.arraycopy(body, 0, out, 3, body.length);
        return out;
    }

    /** {@code -EncodedCommand} form: Base64 of the UTF-16LE script — no quoting through the Windows argv. */
    static String encodeCommand(String script) {
        return Base64.getEncoder().encodeToString(script.getBytes(StandardCharsets.UTF_16LE));
    }

    /** A PowerShell single-quoted literal. */
    private static String psLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * A double-quoted {@code -File} argument; Windows paths and task names never contain {@code "}.
     * Backslashes right before the closing quote are doubled, or they would escape it.
     */
    private static String fileArg(String value) {
        if (value.indexOf('"') >= 0) {
            throw new IllegalArgumentException("value contains a double quote: " + value);
        }
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '\\') {
            end--;
        }
        return "\"" + value + value.substring(end) + "\"";
    }

    /**
     * $1 pid to wait for, $2 run.sh, $3 health port, $4 log the relaunched daemon appends to. The
     * engine is started from a subshell so it is not this helper's child: a crashed child would stay
     * a zombie that {@code kill -0} still reports alive.
     */
    static final String POSIX_SCRIPT = """
            trap '' HUP
            pid="$1"; launcher="$2"; port="$3"; log="$4"; notes="${5:-$4}"
            mkdir -p "$(dirname "$log")" "$(dirname "$notes")" 2>/dev/null
            say() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) relauncher: $*" >>"$notes" 2>/dev/null; }
            say "waiting for daemon pid $pid to exit"
            i=0
            while kill -0 "$pid" 2>/dev/null; do
              i=$((i + 1))
              if [ "$i" -gt 180 ]; then kill -9 "$pid" 2>/dev/null; sleep 1; break; fi
              sleep 1
            done
            say "daemon exited; starting engine (launcher=$launcher port=$port)"
            cd "$(dirname "$launcher")" || { say "FATAL: cannot cd to the launcher dir — engine stays down"; exit 1; }
            start_engine() { child=$( (nohup /bin/bash "$launcher" >>"$log" 2>&1 </dev/null & echo $!) ); }
            if ! command -v curl >/dev/null 2>&1; then start_engine; say "started without health verification (no curl on host)"; exit 0; fi
            for attempt in 1 2 3 4; do
              start_engine
              say "start attempt $attempt (child $child); probing /sqs/v1/health"
              n=0
              while [ "$n" -lt 60 ] && kill -0 "$child" 2>/dev/null; do
                if curl -fsS -m 3 -o /dev/null "http://127.0.0.1:$port/sqs/v1/health" 2>/dev/null; then say "engine healthy after attempt $attempt"; exit 0; fi
                sleep 3
                n=$((n + 1))
              done
              say "attempt $attempt did not come up; killing and retrying"
              kill "$child" 2>/dev/null; sleep 5; kill -9 "$child" 2>/dev/null
            done
            say "FATAL: engine did not come up after 4 attempts — check $log and the host"
            """;

    static final String WINDOWS_SCRIPT = """
            param([string]$PidToWait, [string]$TaskName, [string]$Jar, [string]$Staged, [string]$Port,
                  [string]$Launcher, [string]$Log, [string]$Notes, [string]$SelfTask)
            $ErrorActionPreference = 'SilentlyContinue'
            if (-not $Notes) { $Notes = $Log }
            function Say([string]$m) {
              $stamp = Get-Date -Format 'yyyy-MM-ddTHH:mm:ssZ'
              Add-Content -Path $Notes -Value "$stamp relauncher: $m" -ErrorAction SilentlyContinue
            }
            function Test-EngineHealth {
              for ($i = 0; $i -lt 60; $i++) {
                try {
                  $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 "http://127.0.0.1:$Port/sqs/v1/health"
                  if ($r.StatusCode -eq 200) { return $true }
                } catch { }
                Start-Sleep -Seconds 3
              }
              return $false
            }
            function Get-TaskState {
              $t = Get-ScheduledTask -TaskName $TaskName
              if ($t) { return [string]$t.State }
              return ''
            }
            function Wait-TaskStopped([int]$Seconds) {
              for ($i = 0; $i -lt $Seconds; $i++) {
                if ((Get-TaskState) -ne 'Running') { return }
                Start-Sleep -Seconds 1
              }
            }
            $script:engine = $null
            function Start-Engine {
              if ($TaskName) {
                # A start while an instance still runs is ignored without an error: say so.
                Start-ScheduledTask -TaskName $TaskName
                for ($i = 0; $i -lt 15; $i++) {
                  if ((Get-TaskState) -eq 'Running') { return }
                  Start-Sleep -Seconds 1
                }
                Say "task '$TaskName' did not start (state=$(Get-TaskState))"
                return
              }
              New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Log) | Out-Null
              $script:engine = Start-Process -FilePath $env:ComSpec -WindowStyle Hidden -PassThru `
                -WorkingDirectory (Split-Path -Parent $Launcher) `
                -ArgumentList "/c `"`"$Launcher`" >> `"$Log`" 2>&1`""
            }
            function Stop-Engine {
              if ($TaskName) { Stop-ScheduledTask -TaskName $TaskName; Wait-TaskStopped 30; return }
              if ($script:engine) { taskkill.exe /PID $script:engine.Id /T /F | Out-Null }
            }
            try {
              Say "waiting for daemon pid $PidToWait to exit"
              Wait-Process -Id ([int]$PidToWait) -Timeout 180
              Stop-Process -Id ([int]$PidToWait) -Force
              if ($TaskName) {
                Wait-TaskStopped 90
                # Only a helper in its own task may stop a lingering instance; inside the engine's
                # task, stopping it would end this helper too.
                if ($SelfTask -and (Get-TaskState) -eq 'Running') {
                  Say "task '$TaskName' still running after the daemon exited; stopping it so the start takes"
                  Stop-Engine
                }
              }
              Say "daemon exited; proceeding (task='$TaskName' staged='$Staged' own-task='$SelfTask')"
              $swapped = $false
              if ($Staged -and (Test-Path $Staged)) {
                for ($i = 0; $i -lt 60; $i++) {
                  try {
                    Move-Item -Force -Path $Jar -Destination "$Jar.previous" -ErrorAction Stop
                    Move-Item -Force -Path $Staged -Destination $Jar -ErrorAction Stop
                    $swapped = $true
                    break
                  } catch { Start-Sleep -Seconds 1 }
                }
              }
              Say "swap done: $swapped"
              # The swapped-in jar is on probation until its first heartbeat (UpdateProbation),
              # the same marker run.sh writes on macOS/Linux.
              if ($swapped) { Set-Content -Path "$Jar.update-pending" -Value "0" -ErrorAction SilentlyContinue }
              Start-Engine
              Say "engine started; probing /sqs/v1/health"
              $healthy = Test-EngineHealth
              Say "new build healthy: $healthy"
              if (-not $healthy -and $swapped -and (Test-Path "$Jar.previous")) {
                Say "new build did not come up - rolling back to the previous jar"
                Stop-Engine
                Start-Sleep -Seconds 3
                Move-Item -Force -Path "$Jar.previous" -Destination $Jar
                Remove-Item -Force -Path "$Jar.update-pending" -ErrorAction SilentlyContinue
                Start-Engine
                $restored = Test-EngineHealth
                if ($restored) {
                  Say "previous build restored and healthy"
                } else {
                  Say "FATAL: the previous build also did not come up - start the daemon on the host and check $Log"
                }
              }
            } finally {
              if ($SelfTask) { Unregister-ScheduledTask -TaskName $SelfTask -Confirm:$false }
            }
            """;
}
