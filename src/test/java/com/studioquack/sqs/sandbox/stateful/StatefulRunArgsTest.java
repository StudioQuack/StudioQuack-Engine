package com.studioquack.sqs.sandbox.stateful;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the EXACT stateful {@code podman run} argv (the documented relaxed profile). Like {@code
 * HardeningArgsTest} for the ephemeral path, the security posture is code that must be tested.
 */
class StatefulRunArgsTest {

    @Test
    void never_emits_host_containers_internal_add_host() {
        List<String> argv = StatefulRunArgs.builder("/usr/bin/podman", "myenv/pg:1", "sq-stateful-inst1")
                .caps(new StatefulCaps(1, 512, 1024, null))
                .build()
                .toArgv();

        assertFalse(argv.stream().anyMatch(a -> a.startsWith("host.containers.internal:")),
                "no --add-host host.containers.internal:… pair is ever emitted");
        // Egress hardening is unaffected: metadata nullification + stateful network still apply.
        assertTrue(pair(argv, "--network",
                        com.studioquack.sqs.sandbox.oci.SandboxNetworks.STATEFUL) >= 0,
                "stateful network egress still applied");
    }

    @Test
    void stateful_run_argv_is_the_relaxed_profile_named_persistent_not_rm() {
        StatefulRunArgs built = StatefulRunArgs.builder("/usr/bin/podman", "myenv/pg:1", "sq-stateful-inst1")
                .volumeName("sq-stateful-vol-inst1")
                .volumeMountPath("/data")
                .restartPolicy("unless-stopped")
                .caps(new StatefulCaps(2, 1024, 4096, null))
                .seccompProfilePath("/tmp/sqs/seccomp-baseline.json")
                .envId("env-1")
                .imageRef("myenv/pg:1")
                .instanceId("inst1")
                .env("SQ_STATEFUL_RUNNER", "1")
                .env("SQ_APP_PORT", "8080")
                .build();
        List<String> argv = built.toArgv();

        // Long-lived: detached, NAMED, restart policy, and crucially NOT --rm.
        assertEquals("/usr/bin/podman", argv.get(0));
        assertEquals("run", argv.get(1));
        assertTrue(argv.contains("-d"), "stateful instance runs detached");
        assertFalse(argv.contains("--rm"), "stateful instance is persistent — never --rm");
        assertTrue(pair(argv, "--name", "sq-stateful-inst1") >= 0, "named container");
        assertTrue(pair(argv, "--restart", "unless-stopped") >= 0, "restart policy applied");

        // Named persistent volume + writable rootfs (the RELAXATION vs the ephemeral tmpfs-only).
        assertTrue(pair(argv, "-v", "sq-stateful-vol-inst1:/data") >= 0, "named persistent volume mounted");
        assertFalse(argv.contains("--read-only"), "stateful rootfs is writable (relaxed)");
        assertFalse(argv.contains("65534:65534"), "stateful uses the image-default user (relaxed)");

        // KEPT hardening (still applied under the relaxed profile).
        assertTrue(pair(argv, "--cap-drop", "ALL") >= 0, "cap-drop ALL kept");
        assertTrue(pair(argv, "--security-opt", "no-new-privileges") >= 0, "no-new-privileges kept");
        assertTrue(pair(argv, "--security-opt", "seccomp=/tmp/sqs/seccomp-baseline.json") >= 0, "seccomp kept");

        // Egress kept: metadata DNS nullification + sqs-stateful; NO helper host alias.
        assertFalse(argv.stream().anyMatch(a -> a.startsWith("host.containers.internal:")),
                "no helper alias --add-host on the stateful path");
        assertTrue(pair(argv, "--network",
                        com.studioquack.sqs.sandbox.oci.SandboxNetworks.STATEFUL) >= 0,
                "stateful network egress kept");

        // Pinned resolvers (daemon-owned, before the image): a stateful instance must never resolve
        // via the bridge gateway's forwarder into the host's internal DNS.
        int dns1 = pair(argv, "--dns", "9.9.9.9");
        int dns2 = pair(argv, "--dns", "1.1.1.1");
        int image = argv.indexOf("myenv/pg:1");
        assertTrue(dns1 >= 0 && dns2 >= 0, "default public resolvers pinned: " + argv);
        assertTrue(dns1 < image && dns2 < image,
                "--dns pins precede the image so a command tail cannot override them");

        // Resource caps kept.
        assertTrue(pair(argv, "--memory", "1024m") >= 0, "ram cap kept");
        assertTrue(pair(argv, "--memory-swap", "1024m") >= 0, "swap disabled → OOM not swap");
        assertTrue(pair(argv, "--cpus", "2") >= 0, "cpu cap kept");
        assertTrue(pair(argv, "--storage-opt", "size=4096m") >= 0, "disk cap wired");

        // Env + labels + image.
        assertTrue(pair(argv, "-e", "SQ_STATEFUL_RUNNER") >= 0, "stateful runner signal name injected");
        assertFalse(argv.stream().anyMatch(a -> a.contains("=1") && a.startsWith("SQ_")),
                "env values stay off argv");
        assertEquals("1", built.containerEnv().get("SQ_STATEFUL_RUNNER"));
        assertEquals("8080", built.containerEnv().get("SQ_APP_PORT"));
        assertFalse(argv.stream().anyMatch(a -> "8080".equals(a) || a.contains("SQ_APP_PORT=")),
                "port value stays off argv");
        assertTrue(pair(argv, "--pids-limit", "1024") >= 0, "pid cap always present");
        assertTrue(pair(argv, "--label", "studioquack.sqs.stateful=1") >= 0, "provenance label");
        assertTrue(pair(argv, "--label", "studioquack.sqs.instance=inst1") >= 0, "instance label");
        assertFalse(argv.stream().anyMatch(a -> a.startsWith(StatefulRunArgs.REVERSE_PORT_LABEL + "=")),
                "no reverse label when reversePort is unset");
        assertFalse(argv.stream().anyMatch(a -> a.startsWith(StatefulRunArgs.DISPATCH_LABEL + "=")),
                "no dispatch label when dispatchCapable is unset (unlabeled argv stays byte-identical)");
        assertEquals("myenv/pg:1", argv.get(argv.size() - 1), "image is the last token (no command override)");
    }

    /**
     * A daemon-managed instance (e.g. Docling) that the DAEMON PROCESS itself calls into needs its
     * port published, loopback-only.
     */
    @Test
    void publishPort_emits_a_loopback_only_port_mapping() {
        List<String> argv = StatefulRunArgs.builder("podman", "docling/serve:1", "sq-stateful-docling")
                .publishPort(5001, 5001)
                .build()
                .toArgv();
        assertTrue(pair(argv, "-p", "127.0.0.1:5001:5001") >= 0, "loopback-only port publish");
    }

    @Test
    void no_port_published_when_publishPort_is_not_called() {
        List<String> argv = StatefulRunArgs.builder("podman", "myenv/pg:1", "sq-stateful-inst1")
                .build()
                .toArgv();
        assertFalse(argv.contains("-p"), "user stateful instances are not port-published by default");
    }

    @Test
    void reversePort_stamps_label_for_daemon_restart_recovery() {
        List<String> argv = StatefulRunArgs.builder("podman", "myenv/pg:1", "sq-stateful-inst1")
                .instanceId("inst1")
                .reversePort(5432)
                .build()
                .toArgv();
        assertTrue(pair(argv, "--label", StatefulRunArgs.REVERSE_PORT_LABEL + "=5432") >= 0,
                "reverse port must be labeled so a restart can re-publish");
    }

    @Test
    void dispatchCapable_true_stamps_label_value_1() {
        List<String> argv = StatefulRunArgs.builder("podman", "myenv/pg:1", "sq-stateful-inst1")
                .instanceId("inst1")
                .dispatchCapable(true)
                .build()
                .toArgv();
        assertTrue(pair(argv, "--label", StatefulRunArgs.DISPATCH_LABEL + "=1") >= 0,
                "dispatch-capable instances stamp studioquack.sqs.dispatch=1 for D109 reconcile");
        assertFalse(argv.contains(StatefulRunArgs.DISPATCH_LABEL + "=true"),
                "capable value is always 1, never true");
    }

    @Test
    void dispatchCapable_false_omits_dispatch_label() {
        List<String> argv = StatefulRunArgs.builder("podman", "myenv/pg:1", "sq-stateful-inst1")
                .dispatchCapable(false)
                .build()
                .toArgv();
        assertFalse(argv.stream().anyMatch(a -> a.startsWith(StatefulRunArgs.DISPATCH_LABEL + "=")),
                "false must emit nothing so unlabeled images stay byte-identical");
    }

    @Test
    void isDispatchLabelTruthy_accepts_1_and_true_ignoring_case_and_trim() {
        assertTrue(StatefulRunArgs.isDispatchLabelTruthy("1"));
        assertTrue(StatefulRunArgs.isDispatchLabelTruthy("true"));
        assertTrue(StatefulRunArgs.isDispatchLabelTruthy("TRUE"));
        assertTrue(StatefulRunArgs.isDispatchLabelTruthy("True"));
        assertTrue(StatefulRunArgs.isDispatchLabelTruthy(" 1 "));
        assertTrue(StatefulRunArgs.isDispatchLabelTruthy(" true "));
        assertFalse(StatefulRunArgs.isDispatchLabelTruthy(null));
        assertFalse(StatefulRunArgs.isDispatchLabelTruthy(""));
        assertFalse(StatefulRunArgs.isDispatchLabelTruthy("   "));
        assertFalse(StatefulRunArgs.isDispatchLabelTruthy("0"));
        assertFalse(StatefulRunArgs.isDispatchLabelTruthy("false"));
        assertFalse(StatefulRunArgs.isDispatchLabelTruthy("yes"));
    }

    @Test
    void gpu_cap_and_runtime_flag_are_emitted_when_set() {
        List<String> argv = StatefulRunArgs.builder("podman", "img", "c1")
                .caps(new StatefulCaps(1, 512, 1024, 2))
                .runtimeFlag("runsc")
                .build()
                .toArgv();
        assertTrue(pair(argv, "--gpus", "2") >= 0, "gpu cap emitted when requested");
        assertTrue(pair(argv, "--runtime", "runsc") >= 0, "runsc runtime flag emitted");
    }

    @Test
    void isolation_enforce_empty_seccomp_fails_closed() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> StatefulRunArgs.builder("podman", "img", "c1")
                        .seccompRequired(true)
                        .build()
                        .toArgv());
    }

    /** An operator-configured resolver set replaces the defaults on the stateful path. */
    @Test
    void configured_resolvers_replace_the_default_set() {
        List<String> argv = StatefulRunArgs.builder("podman", "myenv/pg:1", "sq-stateful-inst1")
                .dnsServers(com.studioquack.sqs.sandbox.egress.SandboxDns.parse("1.0.0.1"))
                .build()
                .toArgv();
        assertTrue(pair(argv, "--dns", "1.0.0.1") >= 0, "configured resolver pinned");
        assertEquals(1, argv.stream().filter("--dns"::equals).count(), "no default left behind");

        // Garbage fails closed at build time — never a silent fall-through to bridge DNS.
        StatefulRunArgs.Builder b = StatefulRunArgs.builder("podman", "img", "c2");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> b.dnsServers(List.of("printer.home.arpa")));
    }

    @Test
    void uncapped_memory_still_emits_pids_limit_and_no_swap() {
        List<String> argv = StatefulRunArgs.builder("podman", "img", "c")
                .caps(StatefulCaps.uncapped())
                .build()
                .toArgv();
        assertTrue(pair(argv, "--pids-limit", "1024") >= 0, "pid cap is not optional");
        assertFalse(argv.contains("--memory"), "uncapped RAM omits --memory");
        assertFalse(argv.contains("--memory-swap"));
    }

    /** The built-in node library runs with no limits, even when caps are passed in. */
    @Test
    void unlimited_emits_no_resource_limit_at_all() {
        List<String> argv = StatefulRunArgs.builder("podman", "img", "c")
                .caps(StatefulCaps.defaults())
                .pidsLimit(0)
                .unlimited(true)
                .build()
                .toArgv();
        for (String flag : List.of("--memory", "--memory-swap", "--cpus", "--storage-opt", "--pids-limit")) {
            assertFalse(argv.contains(flag), flag + " must not be emitted");
        }
    }

    @Test
    void zero_pids_limit_is_refused() {
        StatefulRunArgs args = StatefulRunArgs.builder("podman", "img", "c").pidsLimit(0).build();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, args::toArgv);
    }

    /** Index of a consecutive [a,b] pair in argv, or -1. */
    private static int pair(List<String> argv, String a, String b) {
        for (int i = 0; i + 1 < argv.size(); i++) {
            if (argv.get(i).equals(a) && argv.get(i + 1).equals(b)) {
                return i;
            }
        }
        return -1;
    }
}
