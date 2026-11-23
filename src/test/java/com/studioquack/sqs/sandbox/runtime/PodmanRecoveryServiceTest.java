package com.studioquack.sqs.sandbox.runtime;

import com.studioquack.sqs.sandbox.IsolationTier;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PodmanRecoveryService} — focuses on the detection logic
 * (pattern matching) rather than actual process execution.
 */
class PodmanRecoveryServiceTest {

    @Test
    void isConnectionFailure_detectsHandshakeError() {
        var svc = new PodmanRecoveryService(true); // force Windows mode
        var result = new CliRunner.Result(125, "",
                "unable to connect to Podman socket: failed to connect: ssh: handshake failed: "
                        + "read tcp 127.0.0.1:54032->127.0.0.1:54033: wsarecv: An existing connection "
                        + "was forcibly closed by the remote host.", false, 1200);
        assertTrue(svc.isConnectionFailure(result));
    }

    @Test
    void isConnectionFailure_detectsConnectionRefused() {
        var svc = new PodmanRecoveryService(true);
        var result = new CliRunner.Result(125, "", "connection refused", false, 500);
        assertTrue(svc.isConnectionFailure(result));
    }

    @Test
    void isConnectionFailure_detectsBrokenPipe() {
        var svc = new PodmanRecoveryService(true);
        var result = new CliRunner.Result(1, "", "write: broken pipe", false, 100);
        assertTrue(svc.isConnectionFailure(result));
    }

    @Test
    void isConnectionFailure_ignoresNormalErrors() {
        var svc = new PodmanRecoveryService(true);
        var result = new CliRunner.Result(1, "", "image not found: no-such-image:latest", false, 200);
        assertFalse(svc.isConnectionFailure(result));
    }

    @Test
    void isConnectionFailure_ignoresSuccessfulResult() {
        var svc = new PodmanRecoveryService(true);
        var result = new CliRunner.Result(0, "abc123\n", "", false, 300);
        assertFalse(svc.isConnectionFailure(result));
    }

    @Test
    void isConnectionFailure_noopOnNonWindows() {
        var svc = new PodmanRecoveryService(false); // non-Windows
        var result = new CliRunner.Result(125, "",
                "ssh: handshake failed: connection forcibly closed", false, 500);
        assertFalse(svc.isConnectionFailure(result));
    }

    @Test
    void isActive_trueOnWindows() {
        var svc = new PodmanRecoveryService(true);
        assertTrue(svc.isActive());
    }

    @Test
    void isActive_falseOnNonWindows() {
        var svc = new PodmanRecoveryService(false);
        assertFalse(svc.isActive());
    }

    @Test
    void attemptRecovery_refusesOnNonWindows() {
        var svc = new PodmanRecoveryService(false);
        // Should immediately return false without attempting anything
        assertFalse(svc.attemptRecovery(null));
    }

    // --- first install stopped at "restart Windows": no machine was ever created ---------------

    @Test
    void missingMachine_isCreatedWithInstallerSizingAndStarted_withoutWslShutdown() {
        var cli = new ScriptedCli();
        cli.machineLists.add(ok(""));                        // before init: nothing registered
        cli.machineLists.add(ok("podman-machine-default\n")); // after init: machine exists
        var runtime = runtime(true);
        var svc = new PodmanRecoveryService(true, cli, Runnable::run, Duration.ZERO);

        // Creation runs on the create executor, so the caller is told "not yet".
        assertFalse(svc.attemptRecovery(runtime));

        List<String> init = cli.call("machine", "init");
        assertNotNull(init, "a missing machine must be created");
        if (System.getenv("SQ_PODMAN_MACHINE_MEMORY_MIB") == null) {
            assertEquals("8192", init.get(init.indexOf("--memory") + 1));
        }
        if (System.getenv("SQ_PODMAN_MACHINE_CPUS") == null) {
            assertEquals("4", init.get(init.indexOf("--cpus") + 1));
        }
        assertEquals("C:\\podman.exe", init.get(0), "uses the CLI the runtime resolved");
        assertNotNull(cli.call("machine", "start"));
        assertNull(cli.call("--shutdown"), "restarting WSL cannot create a machine");
        verify(runtime).redetect();
        assertFalse(svc.restartRequired());
    }

    @Test
    void missingMachine_wslNeedsRestart_flagsItAndBacksOffQuietly() {
        var cli = new ScriptedCli();
        cli.machineLists.add(ok(""));
        cli.machineLists.add(ok(""));
        cli.init = new CliRunner.Result(125, "",
                "Importing operating system into WSL\nError: Wsl/Service/CreateInstance/E_FAIL", false, 9000);
        var svc = new PodmanRecoveryService(true, cli, Runnable::run, Duration.ZERO);

        assertFalse(svc.attemptRecovery(runtime(false)));
        assertTrue(svc.restartRequired());
        assertNull(cli.call("machine", "start"), "starting a machine that was not created is pointless");

        int callsAfterFirst = cli.calls.size();
        assertFalse(svc.attemptRecovery(runtime(false)));
        assertEquals(callsAfterFirst, cli.calls.size(), "back-off: no CLI call until the retry window");
    }

    @Test
    void existingMachine_takesTheWslRestartPath() {
        var cli = new ScriptedCli();
        cli.machineLists.add(ok("podman-machine-default\n"));
        var svc = new PodmanRecoveryService(true, cli, Runnable::run, Duration.ZERO);

        assertTrue(svc.attemptRecovery(runtime(true)));
        assertNotNull(cli.call("--shutdown"));
        assertNotNull(cli.call("machine", "start"));
        assertNull(cli.call("machine", "init"), "an existing machine is never re-created here");
    }

    @Test
    void summarize_keepsTheLastErrorLineOfNoisyOutput() {
        var r = new CliRunner.Result(125, "Copying blob sha256:abc\nCopying blob sha256:def",
                "Error: first\nnoise\nError: the real one", false, 10);
        assertEquals("Error: the real one", PodmanRecoveryService.summarize(r));
    }

    private static CliRunner.Result ok(String stdout) {
        return new CliRunner.Result(0, stdout, "", false, 5);
    }

    private static ContainerRuntime runtime(boolean availableAfterRecovery) {
        ContainerRuntime runtime = mock(ContainerRuntime.class);
        when(runtime.cli()).thenReturn("C:\\podman.exe");
        when(runtime.redetect()).thenReturn(new CapabilityReport(availableAfterRecovery,
                OciRuntimeKind.PODMAN, "C:\\podman.exe", "5.6.0", List.of(), false, true, null,
                IsolationTier.INTERMEDIATE, IsolationTier.INTERMEDIATE, "", ""));
        return runtime;
    }

    /** Answers podman/wsl calls by subcommand and records every argv. */
    private static final class ScriptedCli implements PodmanRecoveryService.Cli {
        final List<List<String>> calls = new ArrayList<>();
        final Deque<CliRunner.Result> machineLists = new ArrayDeque<>();
        CliRunner.Result init = ok("");

        @Override
        public CliRunner.Result run(List<String> argv, Duration timeout) {
            calls.add(argv);
            if (argv.contains("list")) {
                return machineLists.isEmpty() ? ok("") : machineLists.poll();
            }
            return argv.contains("init") ? init : ok("");
        }

        /** First recorded argv containing every token, or null. */
        List<String> call(String... tokens) {
            return calls.stream().filter(a -> a.containsAll(List.of(tokens))).findFirst().orElse(null);
        }
    }
}

