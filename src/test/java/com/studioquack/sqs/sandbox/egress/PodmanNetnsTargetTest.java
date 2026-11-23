package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The macOS / Windows rule transport. The batch interpreter runs under a real {@code sh} against
 * a fake {@code iptables}/{@code ip}, so its check / skip / tolerate / stop behaviour is the
 * shell's, not a re-implementation of it.
 */
class PodmanNetnsTargetTest {

    @TempDir
    Path tmp;

    @Test
    void quoting_survives_spaces_quotes_and_substitutions() {
        assertEquals("'a b' 'it'\\''s' '$(x)'", PodmanNetnsTarget.quoteAll(List.of("a b", "it's", "$(x)")));
    }

    @Test
    void batch_mirrors_the_manager_loop_and_stops_at_the_first_hard_failure() throws Exception {
        Path log = tmp.resolve("calls.log");
        PodmanNetnsTarget t = PodmanNetnsTarget.machine("podman", "m", false, "sqs-jobs", localShell(log));

        String failure = t.applyAll(List.of(
                List.of("iptables", "-N", "EXISTING"),                                   // tolerated
                List.of("iptables", "-C", "FORWARD", "-s", "10.0.0.0/24", "-j", "X"),     // present
                List.of("iptables", "-I", "FORWARD", "-s", "10.0.0.0/24", "-j", "X"),     // skipped
                List.of("iptables", "-C", "INPUT", "-s", "10.0.0.0/24", "-j", "Y"),       // absent
                List.of("iptables", "-I", "INPUT", "-s", "10.0.0.0/24", "-j", "Y"),       // runs
                List.of("ip", "rule", "del", "pref", "1"),                                // ignored
                List.of("iptables", "-A", "BROKEN", "-j", "DROP"),                        // hard fail
                List.of("iptables", "-A", "AFTER", "-j", "DROP")));                       // never runs

        assertEquals("iptables v1.8: Couldn't load target", failure);
        assertEquals(List.of(
                "iptables -N EXISTING",
                "iptables -C FORWARD -s 10.0.0.0/24 -j X",
                "iptables -C INPUT -s 10.0.0.0/24 -j Y",
                "iptables -I INPUT -s 10.0.0.0/24 -j Y",
                "ip rule del pref 1",
                "iptables -A BROKEN -j DROP"), Files.readAllLines(log));
    }

    @Test
    void batch_reports_success() {
        PodmanNetnsTarget t = PodmanNetnsTarget.machine("podman", "m", false, "sqs-jobs",
                localShell(tmp.resolve("calls.log")));
        assertNull(t.applyAll(List.of(List.of("iptables", "-N", "FRESH"), List.of("iptables", "-A", "FRESH", "-j", "DROP"))));
    }

    @Test
    void single_command_returns_exit_code_and_error_line() {
        PodmanNetnsTarget t = PodmanNetnsTarget.machine("podman", "m", false, "sqs-jobs",
                localShell(tmp.resolve("calls.log")));
        SandboxEgressManager.ExecResult r = t.run(List.of("iptables", "-C", "INPUT", "-j", "Y"));
        assertFalse(r.ok());
        assertEquals(1, r.exitCode());
        assertTrue(r.stderr().contains("does a matching rule exist"));
        assertTrue(t.run(List.of("iptables", "-N", "FRESH")).ok());
    }

    @Test
    void rootless_scripts_run_in_the_rootless_netns_and_rootful_ones_under_sudo() {
        AtomicReference<List<String>> seen = new AtomicReference<>();
        PodmanNetnsTarget.Runner capture = (argv, stdin, timeout) -> {
            seen.set(argv);
            return new CliRunner.Result(0, PodmanNetnsTarget.OK_MARK, "", false, 1);
        };
        PodmanNetnsTarget.machine("podman", "m", false, "sqs-jobs", capture).applyAll(List.of());
        assertEquals(List.of("podman", "machine", "ssh", "m", "podman unshare --rootless-netns sh -s"), seen.get());
        PodmanNetnsTarget.machine("podman", "m", true, "sqs-jobs", capture).applyAll(List.of());
        assertEquals(List.of("podman", "machine", "ssh", "m", "sudo -n sh -s"), seen.get());
    }

    @Test
    void an_unreachable_machine_is_a_failure_never_a_success() {
        PodmanNetnsTarget t = PodmanNetnsTarget.machine("podman", "m", false, "sqs-jobs",
                (argv, stdin, timeout) -> new CliRunner.Result(255, "", "Error: machine is not running", false, 1));
        assertEquals("Podman machine m unreachable: Error: machine is not running",
                t.applyAll(List.of(List.of("iptables", "-N", "X"))));
        assertFalse(t.run(List.of("iptables", "-N", "X")).ok());
    }

    @Test
    void detect_needs_a_running_well_named_machine_with_iptables() {
        assertNotNull(detectWith("podman-machine-default|running|false", 0));
        assertEquals("Podman machine podman-machine-default",
                detectWith("podman-machine-default|running|false", 0).describe());
        assertNull(detectWith("podman-machine-default|stopped|false", 0));
        assertNull(detectWith("bad;name|running|false", 0));
        assertNull(detectWith("podman-machine-default|running|false", 127));
        assertNull(detectWith("", 0));
    }

    @Test
    void local_rootless_podman_runs_scripts_in_its_own_netns_and_needs_the_anchor() {
        List<List<String>> calls = new ArrayList<>();
        PodmanNetnsTarget t = PodmanNetnsTarget.detectLocalRootless("podman", "sqs-jobs", (argv, stdin, timeout) -> {
            calls.add(argv);
            if (argv.contains("info")) {
                return new CliRunner.Result(0, "true\n", "", false, 1);
            }
            if (argv.contains("exists")) {
                return new CliRunner.Result(1, "", "", false, 1);
            }
            return new CliRunner.Result(0, PodmanNetnsTarget.RC_MARK + "0\n", "", false, 1);
        });
        assertNotNull(t);
        assertEquals("rootless Podman", t.describe());
        assertEquals(List.of("podman", "unshare", "--rootless-netns", "sh", "-s"), calls.get(1));
        assertNull(t.ensureAnchor());
        assertTrue(calls.stream().anyMatch(c -> c.contains("create")));

        assertNull(PodmanNetnsTarget.detectLocalRootless("podman", "sqs-jobs", (argv, stdin, timeout) ->
                new CliRunner.Result(0, "false\n", "", false, 1)), "rootful Podman: host rules apply");
    }

    @Test
    void anchor_is_created_on_the_jobs_network_with_the_reaped_label_and_started() {
        List<List<String>> calls = new ArrayList<>();
        PodmanNetnsTarget t = PodmanNetnsTarget.machine("podman", "m", false, "sqs-jobs", (argv, stdin, timeout) -> {
            calls.add(argv);
            boolean exists = argv.contains("exists");
            return new CliRunner.Result(exists ? 1 : 0, "", "", false, 1);
        });
        assertNull(t.ensureAnchor());
        assertEquals(List.of("podman", "pod", "create", "--name", PodmanNetnsTarget.ANCHOR_POD,
                "--network", "sqs-jobs", "--label", "studioquack.sqs=egress-anchor"), calls.get(1));
        assertEquals(List.of("podman", "pod", "start", PodmanNetnsTarget.ANCHOR_POD), calls.get(2));

        calls.clear();
        assertNull(PodmanNetnsTarget.machine("podman", "m", true, "sqs-jobs", (argv, stdin, timeout) -> {
            calls.add(argv);
            return new CliRunner.Result(0, "", "", false, 1);
        }).ensureAnchor());
        assertTrue(calls.isEmpty(), "a rootful machine's namespace is the VM's own and needs no anchor");
    }

    @Test
    void anchor_failure_is_reported() {
        PodmanNetnsTarget t = PodmanNetnsTarget.machine("podman", "m", false, "sqs-jobs", (argv, stdin, timeout) ->
                new CliRunner.Result(argv.contains("create") || argv.contains("exists") ? 125 : 0, "",
                        "Error: network sqs-jobs not found", false, 1));
        assertEquals("anchor pod create failed: Error: network sqs-jobs not found", t.ensureAnchor());
    }

    private static PodmanNetnsTarget detectWith(String inspectLine, int iptablesRc) {
        return PodmanNetnsTarget.detectMachine("podman", "sqs-jobs", (argv, stdin, timeout) -> {
            if (argv.contains("inspect")) {
                return new CliRunner.Result(0, inspectLine + "\n", "", false, 1);
            }
            return new CliRunner.Result(0, PodmanNetnsTarget.RC_MARK + iptablesRc + "\n", "", false, 1);
        });
    }

    /** Runs the script the target would send over SSH in a local {@code sh}, with fake tools on PATH. */
    private PodmanNetnsTarget.Runner localShell(Path log) {
        Path bin = tmp.resolve("bin");
        try {
            Files.createDirectories(bin);
            fakeTool(bin.resolve("iptables"), """
                    #!/bin/sh
                    echo "iptables $*" >> "$SQ_LOG"
                    case "$*" in
                      *"-C FORWARD -s 10.0.0.0/24"*) exit 0 ;;
                      *-C*) echo "iptables: Bad rule (does a matching rule exist in that chain?)." >&2; exit 1 ;;
                      *"-N EXISTING"*)
                        # Inside podman unshare this message never reaches a redirected file.
                        [ -p /dev/fd/2 ] && echo "iptables: Chain already exists." >&2
                        exit 1 ;;
                      *"-A BROKEN"*) echo "iptables v1.8: Couldn't load target" >&2; exit 2 ;;
                    esac
                    exit 0
                    """);
            fakeTool(bin.resolve("ip"), """
                    #!/bin/sh
                    echo "ip $*" >> "$SQ_LOG"
                    echo "RTNETLINK answers: No such file or directory" >&2
                    exit 2
                    """);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return (argv, stdin, timeout) -> {
            try {
                ProcessBuilder pb = new ProcessBuilder("sh", "-s");
                pb.environment().put("PATH", bin + ":" + System.getenv("PATH"));
                pb.environment().put("SQ_LOG", log.toString());
                Process p = pb.start();
                p.getOutputStream().write(stdin);
                p.getOutputStream().close();
                String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                String err = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
                return new CliRunner.Result(p.waitFor(), out, err, false, 1);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
    }

    private static void fakeTool(Path path, String body) throws IOException {
        Files.writeString(path, body);
        path.toFile().setExecutable(true);
    }
}
