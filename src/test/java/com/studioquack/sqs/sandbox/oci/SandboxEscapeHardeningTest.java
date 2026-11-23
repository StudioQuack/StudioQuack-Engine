package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.stateful.StatefulRunArgs;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Container-escape penetration suite (attacker perspective). Pins the argv invariants that keep a
 * hostile image or hostile workload inside the sandbox.
 */
class SandboxEscapeHardeningTest {

    /** Flags that widen privilege, widen the mount table, or share a host namespace. */
    private static final Set<String> FORBIDDEN_FLAGS = Set.of(
            "--privileged", "--cap-add", "--device", "--mount", "--pid", "--ipc", "--uts",
            "--cgroupns", "--userns", "--sysctl", "--env-host", "--hostuser", "--rootfs");

    /** Syscalls every published escape chain reaches for — must stay on the baseline deny-list. */
    private static final Set<String> REQUIRED_SECCOMP_DENIES = Set.of(
            "mount", "umount", "umount2", "pivot_root", "move_mount", "open_tree",
            "ptrace", "process_vm_readv", "process_vm_writev", "kcmp",
            "kexec_load", "kexec_file_load", "init_module", "finit_module", "delete_module",
            "create_module", "bpf", "perf_event_open", "keyctl", "add_key", "request_key",
            "userfaultfd", "unshare", "open_by_handle_at", "name_to_handle_at",
            "io_uring_setup", "io_uring_enter", "io_uring_register",
            "reboot", "swapon", "swapoff", "acct", "sethostname", "setdomainname",
            "ioperm", "iopl", "quotactl", "sysfs");

    /* ------------------------------------------------------------------ */
    /* image-slot flag injection (fail closed)                            */
    /* ------------------------------------------------------------------ */

    @Test
    void image_ref_with_leading_dash_is_refused_before_podman_can_parse_it_as_a_flag() {
        // Podman parses flags until the first positional: a ref like "--privileged" in the image
        // slot would be consumed as a runtime flag. The builders must refuse it fail-closed.
        for (String bad : List.of("--privileged", "--cap-add", "-v", "--network=host", "-", "")) {
            assertThrows(IllegalArgumentException.class,
                    () -> HardeningArgs.builder("podman", SandboxLanguage.PYTHON, bad)
                            .containerCommand(List.of("sh", "-c", "true"))
                            .build()
                            .toArgv(),
                    "ephemeral builder must refuse image ref '" + bad + "'");
            assertThrows(IllegalArgumentException.class,
                    () -> StatefulRunArgs.builder("podman", bad, "c1").build().toArgv(),
                    "stateful builder must refuse image ref '" + bad + "'");
        }
    }

    @Test
    void legitimate_image_refs_are_accepted() {
        for (String ok : List.of(
                "python:3.11-slim",
                "docker.io/library/alpine:3.20",
                "localhost/sq-local/node-x:1",
                "sq-local/sq-builtin-node-library:1.0.0",
                "registry.example.com/team/img@sha256:"
                        + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")) {
            assertEquals(ok, HardeningArgs.requireSafeImageRef(ok), "must accept " + ok);
        }
    }

    /* ------------------------------------------------------------------ */
    /* ephemeral argv — the full containment invariant set                */
    /* ------------------------------------------------------------------ */

    @Test
    void ephemeral_argv_carries_no_privilege_widening_flag_ever() {
        List<String> argv = fullEphemeralArgv();
        for (String tok : argv) {
            String flag = tok.contains("=") ? tok.substring(0, tok.indexOf('=')) : tok;
            assertFalse(FORBIDDEN_FLAGS.contains(flag),
                    "forbidden flag '" + flag + "' present in sandbox argv: " + argv);
        }
        // --security-opt may only ever carry these two values.
        for (int i = 0; i + 1 < argv.size(); i++) {
            if ("--security-opt".equals(argv.get(i))) {
                String v = argv.get(i + 1);
                assertTrue("no-new-privileges".equals(v) || v.startsWith("seccomp="),
                        "unexpected --security-opt value: " + v);
            }
        }
    }

    @Test
    void ephemeral_argv_pins_the_containment_baseline() {
        List<String> argv = fullEphemeralArgv();
        assertTrue(argv.contains("--read-only"), "rootfs must be read-only");
        assertTrue(argv.contains("--init"), "a real PID 1 reaps zombies / forwards signals");
        assertTrue(pair(argv, "--cap-drop", "ALL"), "all capabilities dropped");
        assertTrue(pair(argv, "--security-opt", "no-new-privileges"), "setuid/file-cap escalation off");
        assertTrue(pair(argv, "--user", "65534:65534"), "never root in the container");
        assertTrue(argv.stream().anyMatch(a -> a.startsWith("seccomp=")), "seccomp baseline applied");
        assertTrue(pair(argv, "--runtime", "runsc"), "best tier pins gVisor");
        assertTrue(pair(argv, "--memory", "256m") && pair(argv, "--memory-swap", "256m"),
                "memory cap with swap disabled");
        assertTrue(pair(argv, "--cpus", "1"), "cpu cap");
        assertTrue(pair(argv, "--pids-limit", "512"), "fork bomb stays inside the container pid cgroup");
    }

    @Test
    void the_only_host_path_mounted_is_the_dep_cache_and_it_is_read_only() {
        List<String> argv = fullEphemeralArgv();
        int mounts = 0;
        for (int i = 0; i + 1 < argv.size(); i++) {
            if ("-v".equals(argv.get(i)) || "--volume".equals(argv.get(i))) {
                mounts++;
                String spec = argv.get(i + 1);
                assertTrue(spec.startsWith("/var/lib/sq/dep-cache/python:/opt/sq/deps/python"),
                        "only the dep-cache bucket may be bind-mounted: " + spec);
                assertTrue(spec.endsWith(":ro"), "dep-cache mount must be read-only: " + spec);
            }
        }
        assertEquals(1, mounts, "exactly one bind mount (the dep cache) is expected");
        // And with no dep cache configured there must be NO host path in the container at all.
        List<String> bare = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .containerCommand(List.of("sh", "-c", "true"))
                .build()
                .toArgv();
        assertFalse(bare.contains("-v") || bare.contains("--volume") || bare.contains("--mount"),
                "no dep cache → no host path mounts at all: " + bare);
    }

    @Test
    void writable_space_is_exactly_the_three_tmpfs_mounts_nosuid_nodev() {
        List<String> argv = fullEphemeralArgv();
        List<String> tmpfs = new ArrayList<>();
        for (int i = 0; i + 1 < argv.size(); i++) {
            if ("--tmpfs".equals(argv.get(i))) {
                tmpfs.add(argv.get(i + 1));
            }
        }
        assertEquals(3, tmpfs.size(), "exactly /tmp, /run, /sandbox are writable: " + tmpfs);
        for (String spec : tmpfs) {
            assertTrue(spec.contains("nosuid") && spec.contains("nodev"),
                    "tmpfs must be nosuid,nodev: " + spec);
            assertTrue(spec.contains("size="), "tmpfs must be size-bounded: " + spec);
        }
        assertTrue(tmpfs.stream().anyMatch(s -> s.startsWith("/sandbox:")), "/sandbox scratch present");
    }

    /* ------------------------------------------------------------------ */
    /* stateful argv — the documented relaxation must not erode further   */
    /* ------------------------------------------------------------------ */

    @Test
    void stateful_argv_keeps_the_non_negotiables() {
        List<String> argv = StatefulRunArgs.builder("podman", "localhost/sq-local/app:1", "inst-1")
                .runtimeFlag("runsc")
                .seccompProfilePath("/tmp/sqs/seccomp-baseline.json")
                .volumeName("sq-vol-1")
                .build()
                .toArgv();
        assertTrue(pair(argv, "--cap-drop", "ALL"), "stateful keeps cap-drop ALL");
        assertTrue(pair(argv, "--security-opt", "no-new-privileges"), "stateful keeps no-new-privileges");
        assertTrue(argv.stream().anyMatch(a -> a.startsWith("seccomp=")), "stateful keeps seccomp");
        assertTrue(pair(argv, "--runtime", "runsc"), "stateful honours the gVisor tier flag");
        for (String tok : argv) {
            String flag = tok.contains("=") ? tok.substring(0, tok.indexOf('=')) : tok;
            assertFalse(FORBIDDEN_FLAGS.contains(flag), "forbidden flag in stateful argv: " + flag);
        }
        // The only -v is the daemon-named persistent volume — never a host path.
        for (int i = 0; i + 1 < argv.size(); i++) {
            if ("-v".equals(argv.get(i))) {
                String source = argv.get(i + 1).split(":", 2)[0];
                assertFalse(source.startsWith("/") || source.startsWith("."),
                        "stateful volume must be a named volume, not a host path: " + argv.get(i + 1));
            }
        }
    }

    @Test
    void stateful_publish_is_loopback_pinned_by_construction() {
        List<String> argv = StatefulRunArgs.builder("podman", "img", "c1")
                .publishPort(5001, 5001)
                .build()
                .toArgv();
        assertTrue(pair(argv, "-p", "127.0.0.1:5001:5001"),
                "the only publish shape is loopback-pinned: " + argv);
    }

    /* ------------------------------------------------------------------ */
    /* seccomp baseline content — the deny-list IS the filter (no stacking) */
    /* ------------------------------------------------------------------ */

    @Test
    void seccomp_baseline_denies_every_syscall_the_published_escape_chains_use() throws Exception {
        JsonNode root;
        try (InputStream in = getClass().getResourceAsStream("/sandbox/seccomp-baseline.json")) {
            root = new ObjectMapper().readTree(
                    new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        Set<String> denied = new HashSet<>();
        for (JsonNode block : root.withArray("syscalls")) {
            if ("SCMP_ACT_ERRNO".equals(block.path("action").asText())) {
                for (JsonNode name : block.withArray("names")) {
                    denied.add(name.asText());
                }
            }
        }
        Set<String> missing = new HashSet<>(REQUIRED_SECCOMP_DENIES);
        missing.removeAll(denied);
        assertTrue(missing.isEmpty(),
                "seccomp baseline no longer denies " + missing + " — since seccomp=<path> REPLACES "
                        + "the runtime default profile, an unlisted syscall is an ALLOWED syscall");
    }

    /* ------------------------------------------------------------------ */

    private static List<String> fullEphemeralArgv() {
        return HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .runtimeFlag("runsc")
                .egress(EgressPolicy.OPEN)
                .limits(SandboxLimits.of(256, 5000))
                .cpus(1)
                .seccompProfilePath("/tmp/sqs/seccomp-baseline.json")
                .depCacheHostDir("/var/lib/sq/dep-cache/python")
                .ownership(SandboxOwnership.of("own1", "boot1"))
                .containerCommand(List.of("sh", "-c", "true"))
                .build()
                .toArgv();
    }

    private static boolean pair(List<String> argv, String a, String b) {
        for (int i = 0; i + 1 < argv.size(); i++) {
            if (argv.get(i).equals(a) && argv.get(i + 1).equals(b)) {
                return true;
            }
        }
        return false;
    }
}
