package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts the EXACT per-job hardening argv. The sandbox security posture is code that must be
 * tested, not trusted: every flag (read-only rootfs, tmpfs scratch, cap-drop, seccomp, non-root
 * uid, resource caps, egress) is pinned here so a regression fails the build.
 */
class HardeningArgsTest {

    @Test
    void python_open_egress_produces_the_full_hardening_argv_in_order() {
        List<String> argv = HardeningArgs.builder("/usr/bin/docker", SandboxLanguage.PYTHON, "python:3.11-slim")
                .runtimeFlag("")                       // intermediate tier (runc default)
                .egress(EgressPolicy.OPEN)
                .limits(SandboxLimits.of(256, 5000))   // ramMb=256, wallMs=5000
                .seccompProfilePath("/tmp/sqs/seccomp-baseline.json")
                .ownership(SandboxOwnership.of("own1", "boot1"))
                .role(HardeningArgs.ROLE_JOB)
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .env("SQ_HELPER_SOCKET", "/tmp/sq-helper.sock")
                .env("SQ_HELPER_BEARER", "h_job1")
                .build()
                .toArgv();

        assertEquals(List.of(
                "/usr/bin/docker", "run", "--rm", "-i", "--init",
                "--label", "studioquack.sqs=1",
                "--label", "studioquack.sqs.lang=python",
                "--label", "studioquack.sqs.owner=own1",
                "--label", "studioquack.sqs.boot=boot1",
                "--label", "studioquack.sqs.role=job",
                // Open egress: cloud-metadata DNS nullification + sqs-jobs (no host-gateway alias)
                "--add-host", "metadata.google.internal:127.0.0.1",
                "--add-host", "metadata.goog:127.0.0.1",
                "--add-host", "instance-data:127.0.0.1",
                "--add-host", "instance-data.ec2.internal:127.0.0.1",
                "--network", "sqs-jobs",
                // Pinned resolvers (daemon-owned, before the image): without them the container
                // resolves via the bridge gateway's forwarder into the host's DNS.
                "--dns", "9.9.9.9",
                "--dns", "1.1.1.1",
                // read-only rootfs + tmpfs scratch
                "--read-only",
                "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m,mode=1777",
                "--tmpfs", "/run:rw,nosuid,nodev,size=8m,mode=1777",
                "--tmpfs", "/sandbox:rw,nosuid,nodev,size=64m,mode=1777",
                // privilege drop + seccomp
                "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges",
                "--security-opt", "seccomp=/tmp/sqs/seccomp-baseline.json",
                // non-root uid + resource caps
                "--user", "65534:65534",
                "--memory", "256m",
                "--memory-swap", "256m",
                "--cpus", "1",
                "--pids-limit", "512",
                // working dir + env + image + command
                "-w", "/sandbox",
                "-e", "SQ_HELPER_SOCKET",
                "-e", "SQ_HELPER_BEARER",
                "python:3.11-slim",
                "sh", "-c", "BOOT"),
                argv);
    }

    @Test
    void egress_none_uses_network_none_and_no_add_host() {
        List<String> argv = HardeningArgs.builder("docker", SandboxLanguage.PYTHON, "python:3.11-slim")
                .egress(EgressPolicy.NONE)
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .build()
                .toArgv();

        assertTrue(indexOfPair(argv, "--network", "none") >= 0, "egress none → --network none");
        assertFalse(argv.contains("host.containers.internal:host-gateway"), "no host-gateway on --network none");
        assertFalse(argv.stream().anyMatch(a -> a.startsWith("metadata.google")), "no metadata add-host");
        // No DNS exists to pin on --network none, and podman rejects --dns there.
        assertFalse(argv.contains("--dns"), "no --dns pins under --network none: " + argv);
        // hardening flags still present even with no network
        assertTrue(argv.contains("--read-only"));
        assertTrue(indexOfPair(argv, "--cap-drop", "ALL") >= 0);
        assertTrue(indexOfPair(argv, "--user", "65534:65534") >= 0);
        assertFalse(argv.stream().anyMatch(a -> a.startsWith("seccomp=")),
                "empty seccomp path must omit the flag: " + argv);
    }

    @Test
    void isolation_enforce_empty_seccomp_fails_closed() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> HardeningArgs.builder("docker", SandboxLanguage.PYTHON, "python:3.11-slim")
                        .seccompRequired(true)
                        .containerCommand(List.of("sh", "-c", "BOOT"))
                        .build()
                        .toArgv());
    }

    @Test
    void best_tier_selects_runsc_runtime_and_cpus_and_depcache_are_emitted() {
        List<String> argv = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .runtimeFlag("runsc")
                .cpus(1.5)
                .depCacheHostDir("/opt/sq/deps/python")
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .build()
                .toArgv();

        assertTrue(indexOfPair(argv, "--runtime", "runsc") >= 0, "best tier → --runtime runsc");
        assertTrue(indexOfPair(argv, "--cpus", "1.5") >= 0, "cpu cap emitted");
        assertTrue(indexOfPair(argv, "-v", "/opt/sq/deps/python:/opt/sq/deps/python:ro") >= 0,
                "dep-cache RO bind mount emitted");
    }

    @Test
    void silent_limits_emit_ram_and_cpu_floors() {
        List<String> argv = HardeningArgs.builder("docker", SandboxLanguage.JAVASCRIPT, "node:26.12.1-slim")
                .limits(SandboxLimits.unbounded())
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .build()
                .toArgv();
        assertTrue(indexOfPair(argv, "--memory", "512m") >= 0, "D-07 RAM floor");
        assertTrue(indexOfPair(argv, "--memory-swap", "512m") >= 0);
        assertTrue(indexOfPair(argv, "--cpus", "1") >= 0, "D-07 CPU floor");
        assertTrue(indexOfPair(argv, "--label", "studioquack.sqs.lang=javascript") >= 0);
    }

    @Test
    void detached_warm_container_uses_dashd_not_dashi() {
        List<String> argv = HardeningArgs.builder("docker", SandboxLanguage.PYTHON, "python:3.11-slim")
                .detached(true)
                .containerCommand(List.of("sleep", "86400"))
                .build()
                .toArgv();
        assertTrue(argv.contains("-d"), "warm slot runs detached");
        assertFalse(argv.contains("-i"), "warm slot is not interactive");
        assertEquals("sleep", argv.get(argv.size() - 2));
        assertEquals("86400", argv.get(argv.size() - 1));
    }

    @Test
    void provenance_labels_carry_owner_boot_and_role_so_a_leftover_is_attributable() {
        List<String> warm = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .detached(true)
                .ownership(SandboxOwnership.of("ownerA", "bootA"))
                .role(HardeningArgs.ROLE_WARM)
                .containerCommand(List.of("sleep", "86400"))
                .build()
                .toArgv();

        assertTrue(indexOfPair(warm, "--label", "studioquack.sqs.owner=ownerA") >= 0,
                "owner id identifies WHICH installed engine opened this container");
        assertTrue(indexOfPair(warm, "--label", "studioquack.sqs.boot=bootA") >= 0,
                "boot id identifies WHICH run of it, so a leftover of a dead run is recognisable");
        assertTrue(indexOfPair(warm, "--label", "studioquack.sqs.role=warm") >= 0);

        // Nothing opted in: the container is still reapable by the blanket uninstall filter, but no
        // live daemon will claim it as its own.
        List<String> unowned = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .build()
                .toArgv();
        assertTrue(indexOfPair(unowned, "--label", "studioquack.sqs=1") >= 0);
        assertTrue(indexOfPair(unowned, "--label", "studioquack.sqs.owner=unknown") >= 0);
        assertTrue(indexOfPair(unowned, "--label", "studioquack.sqs.role=job") >= 0,
                "job is the safe default role: only ever reclaimed once its creating run is gone");
    }

    @Test
    void open_egress_never_emits_host_containers_internal() {
        // Job sandboxes use the UDS helper broker; no host.containers.internal alias on either the
        // job or stateful path.
        List<String> argv = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .egress(EgressPolicy.OPEN)
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .build()
                .toArgv();

        assertFalse(argv.stream().anyMatch(a -> a.startsWith("host.containers.internal:")),
                "no --add-host may be emitted for the helper alias: " + argv);
        assertTrue(indexOfPair(argv, "--network", SandboxNetworks.JOBS) >= 0);
        assertTrue(indexOfPair(argv, "--add-host", "metadata.google.internal:127.0.0.1") >= 0);
    }

    @Test
    void sandbox_tmpfs_mb_sizes_the_writable_sandbox_scratch() {
        // Disc → /sandbox tmpfs size (rootfs is --read-only; no portable rootfs quota).
        List<String> argv = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .limits(SandboxLimits.of(256, 0L))
                .cpus(1.5)
                .sandboxTmpfsMb(128)
                .detached(true)
                .containerCommand(List.of("sleep", "86400"))
                .build()
                .toArgv();

        assertTrue(indexOfPair(argv, "--memory", "256m") >= 0, "ram → --memory");
        assertTrue(indexOfPair(argv, "--memory-swap", "256m") >= 0, "swap disabled");
        assertTrue(indexOfPair(argv, "--cpus", "1.5") >= 0, "cpu → --cpus");
        assertTrue(indexOfPair(argv, "--tmpfs",
                        "/sandbox:rw,nosuid,nodev,size=128m,mode=1777") >= 0,
                "diskMb → /sandbox tmpfs size: " + argv);
    }

    /**
     * (acceptance line: "internal DNS names do not resolve from inside a sandbox") — truth
     * statement: on a podman custom network with NO {@code --dns}, the container's resolv.conf
     * points at the bridge gateway's aardvark-dns/dnsmasq, which forwards to the HOST's resolver —
     * on a LAN that answers for internal names. The daemon therefore pins public resolvers on every
     * networked job/warm argv, BEFORE the image, so the workload (which controls only the
     * post-image tail) can never choose its resolver (podman: last --dns wins — and a tail
     * injection is refused by {@code SandboxNetworkGuard}).
     */
    @Test
    void open_egress_pins_the_default_public_resolvers_before_the_image() {
        List<String> argv = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .egress(EgressPolicy.OPEN)
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .build()
                .toArgv();

        int dns1 = indexOfPair(argv, "--dns", "9.9.9.9");
        int dns2 = indexOfPair(argv, "--dns", "1.1.1.1");
        int image = argv.indexOf("python:3.11-slim");
        assertTrue(dns1 >= 0 && dns2 >= 0, "both default resolvers pinned: " + argv);
        assertTrue(dns1 < image && dns2 < image,
                "--dns pins must precede the image so the workload tail cannot own them");
        assertEquals(2, argv.stream().filter("--dns"::equals).count(),
                "exactly the two default pins, no more");
    }

    @Test
    void configured_resolvers_replace_the_default_set() {
        List<String> argv = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .egress(EgressPolicy.OPEN)
                .dnsServers(com.studioquack.sqs.sandbox.egress.SandboxDns.parse("1.0.0.1, 9.9.9.9"))
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .build()
                .toArgv();

        assertTrue(indexOfPair(argv, "--dns", "1.0.0.1") >= 0, "configured resolver pinned");
        assertTrue(indexOfPair(argv, "--dns", "9.9.9.9") >= 0, "second configured resolver pinned");
        assertEquals(2, argv.stream().filter("--dns"::equals).count(), "no default left behind");
    }

    @Test
    void a_malformed_dns_pin_fails_closed_at_build_time() {
        // A garbage/hostname entry must NEVER reach podman: fail loud instead of silently
        // degrading to the bridge gateway resolver (which would re-open the internal-DNS leak).
        HardeningArgs.Builder b = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "img");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> b.dnsServers(List.of("nas.lan")));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> com.studioquack.sqs.sandbox.egress.SandboxDns.parse("9.9.9.9,--network,host"));
        // Blank config → the documented public defaults, never the platform default.
        assertEquals(List.of("9.9.9.9", "1.1.1.1"),
                com.studioquack.sqs.sandbox.egress.SandboxDns.parse("  "));
        assertEquals(List.of("9.9.9.9", "1.1.1.1"),
                com.studioquack.sqs.sandbox.egress.SandboxDns.validate(null));
        assertEquals(List.of("9.9.9.9", "1.1.1.1"),
                com.studioquack.sqs.sandbox.egress.SandboxDns.validate(List.of()));
        // IPv6 literal accepted; 4-octet range enforced.
        assertEquals(List.of("2606:4700:4700::1111"),
                com.studioquack.sqs.sandbox.egress.SandboxDns.parse("2606:4700:4700::1111"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> com.studioquack.sqs.sandbox.egress.SandboxDns.parse("999.1.1.1"));
    }

    @Test
    void keep_image_entrypoint_omits_command_and_can_omit_rm() {
        List<String> argv = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .detached(true)
                .autoRemove(false)
                .build()
                .toArgv();
        assertFalse(argv.contains("--rm"), "autoRemove=false keeps the container for inspect");
        assertTrue(argv.contains("-d"), "detached");
        assertEquals("python:3.11-slim", argv.get(argv.size() - 1),
                "empty containerCommand keeps the image ENTRYPOINT: argv must end at the image");
    }

    @Test
    void clear_entrypoint_resets_the_image_entrypoint_so_the_idle_command_actually_runs() {
        // A parked/idle container for a custom image must run the hardening sleep, not the image's
        // (possibly one-shot) ENTRYPOINT: otherwise the container exits at create and rm deletes it
        // before the first exec ("no such container").
        List<String> argv = HardeningArgs.builder("podman", null, "localhost/sq-local/node-x:1")
                .detached(true)
                .clearEntrypoint(true)
                .containerCommand(List.of("sleep", "86400"))
                .build()
                .toArgv();

        int entrypoint = indexOfPair(argv, "--entrypoint", "");
        int image = argv.indexOf("localhost/sq-local/node-x:1");
        assertTrue(entrypoint >= 0, "entrypoint image create must reset ENTRYPOINT: " + argv);
        assertTrue(image > 0 && entrypoint < image, "--entrypoint \"\" sits in the trusted pre-image region: " + argv);
        assertEquals("sleep", argv.get(argv.size() - 2));
        assertEquals("86400", argv.get(argv.size() - 1));
        // Language-less custom image carries the "custom" label, and no dep-cache mount.
        assertTrue(indexOfPair(argv, "--label", "studioquack.sqs.lang=custom") >= 0);
    }

    @Test
    void script_mode_create_leaves_the_entrypoint_flag_out() {
        List<String> argv = HardeningArgs.builder("podman", SandboxLanguage.PYTHON, "python:3.11-slim")
                .detached(true)
                .containerCommand(List.of("sleep", "86400"))
                .build()
                .toArgv();
        assertFalse(argv.contains("--entrypoint"),
                "script-mode images have no overriding ENTRYPOINT to clear: " + argv);
    }

    /** Index of a consecutive [a,b] flag pair, or -1. */
    private static int indexOfPair(List<String> argv, String a, String b) {
        for (int i = 0; i + 1 < argv.size(); i++) {
            if (argv.get(i).equals(a) && argv.get(i + 1).equals(b)) {
                return i;
            }
        }
        return -1;
    }
}
