package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.oci.HardeningArgs;
import com.studioquack.sqs.sandbox.oci.SandboxNetworks;
import com.studioquack.sqs.sandbox.stateful.StatefulRunArgs;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attacker-perspective unit tests for {@link SandboxNetworkGuard}. The security posture is code
 * that must be tested: every refuse path (host net, aliases, ns sharing, non-loopback publish,
 * missing network, command-tail smuggle) and the real builders' pass path are pinned.
 */
class SandboxNetworkGuardTest {

    @Test
    void network_host_is_refused_with_operator_readable_reason() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(runArgv("--network", "host")));
        assertTrue(ex.getMessage().contains("host"), "message names the value");
        assertTrue(ex.getMessage().toLowerCase().contains("host networking is never permitted"),
                "message states host networking is never permitted: " + ex.getMessage());
    }

    @Test
    void network_host_inline_form_is_refused() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(runArgv("--network=host")));
        assertTrue(ex.getMessage().contains("host"));
        assertTrue(ex.getMessage().toLowerCase().contains("never permitted"));
    }

    @Test
    void net_alias_is_refused() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(List.of(
                        "podman", "run", "--net", "host", "img")));
        assertTrue(ex.getMessage().contains("--net"), "message names the --net alias");
    }

    @Test
    void forbidden_network_shapes_are_refused() {
        for (String bad : List.of(
                "ns:/var/run/netns/foo",
                "container:abc123",
                "bridge",
                "slirp4netns:port=53",
                "pasta:--ipv4-only",
                "private",
                "path:/run/netns/x")) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> SandboxNetworkGuard.validate(runArgv("--network", bad)),
                    "expected refuse for --network " + bad);
            assertTrue(ex.getMessage().contains(bad), "message names " + bad + ": " + ex.getMessage());
        }
    }

    @Test
    void missing_network_is_refused_fail_closed() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(List.of("podman", "run", "--rm", "img")));
        assertTrue(ex.getMessage().contains("missing --network"), ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("fail closed"), ex.getMessage());
    }

    @Test
    void pod_and_share_are_refused() {
        IllegalArgumentException pod = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(runArgv(
                        "--network", SandboxNetworks.JOBS, "--pod", "new:foo")));
        assertTrue(pod.getMessage().contains("--pod"), pod.getMessage());

        IllegalArgumentException share = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(runArgv(
                        "--network", SandboxNetworks.JOBS, "--share", "net")));
        assertTrue(share.getMessage().contains("--share"), share.getMessage());
    }

    @Test
    void non_loopback_publish_refused_loopback_accepted() {
        IllegalArgumentException p = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(runArgv(
                        "--network", SandboxNetworks.STATEFUL, "-p", "8080:80")));
        assertTrue(p.getMessage().toLowerCase().contains("loopback")
                        || p.getMessage().contains("8080:80"),
                p.getMessage());

        IllegalArgumentException pub = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(runArgv(
                        "--network", SandboxNetworks.STATEFUL,
                        "--publish", "0.0.0.0:8080:80")));
        assertTrue(pub.getMessage().contains("0.0.0.0:8080:80")
                        || pub.getMessage().toLowerCase().contains("loopback"),
                pub.getMessage());

        assertDoesNotThrow(() -> SandboxNetworkGuard.validate(runArgv(
                "--network", SandboxNetworks.STATEFUL, "-p", "127.0.0.1:5001:5001")));
    }

    @Test
    void malicious_container_command_tail_cannot_smuggle_network_host() {
        // Tokens after the image must still be scanned — a later consumer must not re-interpret.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(List.of(
                        "podman", "run", "--network", SandboxNetworks.JOBS,
                        "img", "sh", "-c", "true", "--network", "host")));
        assertTrue(ex.getMessage().contains("host")
                        || ex.getMessage().contains("multiple --network"),
                ex.getMessage());
    }

    /**
     * The DNS-hardening guard: a workload must not append its own {@code --dns} (or the {@code
     * --dns-opt}/{@code --dns-search} family) in the command tail — podman's last-flag precedence
     * would let it override the daemon's pins and re-point resolution at an attacker resolver or
     * back at the bridge gateway (reopening the internal-namespace leak).
     */
    @Test
    void tail_injected_dns_flags_are_refused_in_every_form() {
        // Space form, after the image (the HardeningArgs call shape: full argv + image index).
        List<String> argv = new ArrayList<>(List.of(
                "podman", "run", "--network", SandboxNetworks.JOBS,
                "--dns", "9.9.9.9", "--dns", "1.1.1.1"));
        int imageIndex = argv.size();
        argv.add("img");
        argv.addAll(List.of("sh", "-c", "true", "--dns", "10.89.0.1"));
        IllegalArgumentException space = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(argv, imageIndex));
        assertTrue(space.getMessage().contains("--dns"), space.getMessage());

        // = form, and the opt/search family.
        for (String bad : List.of("--dns=10.89.0.1", "--dns-opt=ndots:1", "--dns-search",
                "--dns-search=lan")) {
            List<String> a = new ArrayList<>(List.of(
                    "podman", "run", "--network", SandboxNetworks.JOBS, "--dns", "9.9.9.9"));
            int idx = a.size();
            a.add("img");
            a.add(bad);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> SandboxNetworkGuard.validate(a, idx),
                    "expected refusal of tail " + bad);
            assertTrue(ex.getMessage().contains("DNS"), ex.getMessage());
        }
    }

    @Test
    void daemon_dns_pins_before_the_image_pass_and_legacy_validate_refuses_any_dns() {
        // The daemon's own pins sit BEFORE the image: they must pass the indexed validate…
        List<String> daemonArgv = HardeningArgs.builder("/usr/bin/podman", SandboxLanguage.PYTHON, "img")
                .egress(EgressPolicy.OPEN)
                .containerCommand(List.of("sh", "-c", "BOOT"))
                .build()
                .toArgv();
        assertTrue(daemonArgv.contains("--dns"), "pins present by default");
        // …(toArgv already ran the guard internally — reaching here proves the pass path).

        // The legacy no-index validate (callers without an attacker tail) refuses --dns
        // ANYWHERE — fail closed when the boundary is unknown.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validate(List.of(
                        "podman", "run", "--network", SandboxNetworks.JOBS,
                        "--dns", "9.9.9.9", "img")));
        assertTrue(ex.getMessage().contains("--dns"), ex.getMessage());
    }

    @Test
    void real_hardening_args_builders_pass_for_open_and_none() {
        assertDoesNotThrow(() -> HardeningArgs.builder("/usr/bin/podman", SandboxLanguage.PYTHON, "img")
                .egress(EgressPolicy.OPEN)
                .build()
                .toArgv());
        assertDoesNotThrow(() -> HardeningArgs.builder("/usr/bin/podman", SandboxLanguage.PYTHON, "img")
                .egress(EgressPolicy.NONE)
                .build()
                .toArgv());
    }

    @Test
    void real_stateful_run_args_pass_including_loopback_publish() {
        assertDoesNotThrow(() -> StatefulRunArgs.builder("/usr/bin/podman", "img", "c1")
                .build()
                .toArgv());
        assertDoesNotThrow(() -> StatefulRunArgs.builder("/usr/bin/podman", "img", "c1")
                .publishPort(5001, 5001)
                .build()
                .toArgv());
    }

    @Test
    void build_network_allows_sqs_build_and_none_refuses_jobs() {
        assertDoesNotThrow(() -> SandboxNetworkGuard.validateBuildNetwork(List.of(
                "podman", "build", "--network", SandboxNetworks.BUILD, "-t", "r", ".")));
        assertDoesNotThrow(() -> SandboxNetworkGuard.validateBuildNetwork(List.of(
                "podman", "build", "--network", "none", "-t", "r", ".")));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validateBuildNetwork(List.of(
                        "podman", "build", "--network", SandboxNetworks.JOBS, "-t", "r", ".")));
        assertTrue(ex.getMessage().contains(SandboxNetworks.JOBS), ex.getMessage());
    }

    @Test
    void build_network_degraded_requires_no_network_token() {
        // DEGRADED host (rootless podman machine): the build rides the implicit default
        // bridge — the argv must state NO network at all, and any stated network is refused.
        assertDoesNotThrow(() -> SandboxNetworkGuard.validateBuildNetwork(List.of(
                "podman", "build", "--memory", "2048m", "--dns", "9.9.9.9", "-t", "r", "."), true));
        assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validateBuildNetwork(List.of(
                        "podman", "build", "--network", SandboxNetworks.BUILD, "-t", "r", "."), true));
        assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validateBuildNetwork(List.of(
                        "podman", "build", "--network=none", "-t", "r", "."), true));
        assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validateBuildNetwork(List.of(
                        "podman", "build", "--net", "none", "-t", "r", "."), true));
        assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validateBuildNetwork(List.of(), true));
        // degraded=false keeps the strict fail-closed shape (missing --network refused).
        assertThrows(IllegalArgumentException.class,
                () -> SandboxNetworkGuard.validateBuildNetwork(List.of(
                        "podman", "build", "-t", "r", "."), false));
    }

    /** Minimal run-shaped argv with the given tokens inserted before the image. */
    private static List<String> runArgv(String... mid) {
        List<String> a = new ArrayList<>();
        a.add("podman");
        a.add("run");
        a.addAll(List.of(mid));
        a.add("img");
        return a;
    }
}
