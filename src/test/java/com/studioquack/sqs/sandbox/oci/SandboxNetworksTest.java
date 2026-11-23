package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Network name constants + idempotent ensure argv (no real podman). */
class SandboxNetworksTest {

    @Test
    void jobs_and_stateful_networks_are_distinct_stable_names() {
        assertEquals("sqs-jobs", SandboxNetworks.JOBS);
        assertEquals("sqs-stateful", SandboxNetworks.STATEFUL);
        assertNotEquals(SandboxNetworks.JOBS, SandboxNetworks.STATEFUL,
                "jobs and stateful must not share an L2 network");
    }

    @Test
    void ensure_is_noop_when_network_already_isolated() {
        List<List<String>> calls = new ArrayList<>();
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-jobs-test-exists", (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            assertTrue(timeout.toSeconds() > 0);
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("--format")) {
                String fmt = argv.get(argv.size() - 1);
                if (fmt.contains("Subnets")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "10.89.0.0/24;10.89.0.1", "");
                }
                if (fmt.contains("DNSEnabled")) {
                    // Already created --disable-dns: no migration, cacheable.
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "false", "");
                }
                return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
            }
            return new SandboxNetworkEnsure.ExecResult(false, 1, "unexpected");
        });
        assertEquals(4, calls.size(), "exists + subnet inspect + isolate inspect + dns inspect");
        assertEquals(List.of("/usr/bin/podman", "network", "exists", "sqs-jobs-test-exists"), calls.get(0));

        // Process-local cache: second ensure must not re-exec.
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-jobs-test-exists", (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            return new SandboxNetworkEnsure.ExecResult(false, 1, "should not be called");
        });
        assertEquals(4, calls.size(), "cached ensure skips CLI");
    }

    @Test
    void ensure_creates_with_isolate_opt_when_missing() {
        AtomicInteger existsCalls = new AtomicInteger();
        List<List<String>> creates = new ArrayList<>();
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-jobs-test-create", (argv, timeout) -> {
            if (argv.contains("exists")) {
                existsCalls.incrementAndGet();
                return new SandboxNetworkEnsure.ExecResult(false, 1, "no such network");
            }
            assertTrue(argv.contains("create"));
            creates.add(List.copyOf(argv));
            return new SandboxNetworkEnsure.ExecResult(true, 0, "");
        });
        assertEquals(1, existsCalls.get());
        assertEquals(1, creates.size());
        assertEquals(
                List.of("/usr/bin/podman", "network", "create", "--opt", "isolate=true",
                        // No aardvark on sandbox networks: the --dns pins must BE the container's
                        // resolv.conf, not mere upstreams behind the gateway.
                        "--disable-dns", "sqs-jobs-test-create"),
                creates.get(0));

        // Race path: create reports already exists → adopt via inspect (isolated + dns-disabled).
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-jobs-test-race", (argv, timeout) -> {
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(false, 1, "no such network");
            }
            if (argv.contains("create")) {
                return new SandboxNetworkEnsure.ExecResult(false, 125, "network already exists");
            }
            if (argv.contains("--format")) {
                String fmt = argv.get(argv.size() - 1);
                if (fmt.contains("Subnets")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "10.90.0.0/24;10.90.0.1", "");
                }
                if (fmt.contains("DNSEnabled")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "false", "");
                }
                return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
            }
            return new SandboxNetworkEnsure.ExecResult(false, 1, "unexpected");
        });
    }

    /**
     * A network (isolated but dns_enabled=true) forces the aardvark gateway resolver into every
     * container's resolv.conf, diluting the {@code --dns} pins into mere upstreams. Ensure must
     * migrate it (rm + create --disable-dns) exactly like a non-isolated one; when rm fails
     * (containers attached) ensure succeeds WITHOUT caching so the next spawn retries the
     * migration.
     */
    @Test
    void ensure_migrates_a_dns_enabled_network_and_never_caches_a_failed_migration() {
        List<List<String>> calls = new ArrayList<>();
        // First pass: dns ENABLED → rm + recreate with --disable-dns.
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-jobs-test-dnsmig", (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("--format")) {
                String fmt = argv.get(argv.size() - 1);
                if (fmt.contains("Subnets")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "10.91.0.0/24;10.91.0.1", "");
                }
                if (fmt.contains("DNSEnabled")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
                }
                return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
            }
            return new SandboxNetworkEnsure.ExecResult(true, 0, "");
        });
        assertTrue(calls.contains(
                        List.of("/usr/bin/podman", "network", "rm", "sqs-jobs-test-dnsmig")),
                "dns-enabled network is removed for recreation: " + calls);
        assertTrue(calls.contains(
                        List.of("/usr/bin/podman", "network", "create", "--opt", "isolate=true",
                                "--disable-dns", "sqs-jobs-test-dnsmig")),
                "recreated with --disable-dns: " + calls);

        // Second pass: rm FAILS (containers attached) → no throw, no cache → next ensure retries.
        AtomicInteger ensureCalls = new AtomicInteger();
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-jobs-test-dnsbusy", (argv, timeout) -> {
            ensureCalls.incrementAndGet();
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("--format")) {
                String fmt = argv.get(argv.size() - 1);
                if (fmt.contains("Subnets")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "10.92.0.0/24;10.92.0.1", "");
                }
                if (fmt.contains("DNSEnabled")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
                }
                return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
            }
            if (argv.contains("rm")) {
                return new SandboxNetworkEnsure.ExecResult(false, 125,
                        "network is being used by containers");
            }
            return new SandboxNetworkEnsure.ExecResult(false, 1, "unexpected");
        });
        int firstPassCalls = ensureCalls.get();
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-jobs-test-dnsbusy", (argv, timeout) -> {
            ensureCalls.incrementAndGet();
            return new SandboxNetworkEnsure.ExecResult(true, 0, "");
        });
        assertTrue(firstPassCalls > 0);
        assertTrue(ensureCalls.get() > firstPassCalls,
                "failed migration must NOT be cached — the next ensure re-execs");
    }

    @Test
    void ensure_fails_loudly_when_create_fails() {
        assertThrows(IllegalStateException.class, () ->
                SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-jobs-test-fail", (argv, timeout) -> {
                    if (argv.contains("exists")) {
                        return new SandboxNetworkEnsure.ExecResult(false, 1, "missing");
                    }
                    return new SandboxNetworkEnsure.ExecResult(false, 125, "permission denied");
                }));
    }

    @Test
    void ensure_rejects_blank_cli_or_name() {
        SandboxNetworkEnsure.Exec noop = (argv, timeout) ->
                new SandboxNetworkEnsure.ExecResult(true, 0, "");
        assertThrows(IllegalStateException.class, () ->
                SandboxNetworkEnsure.ensure("", "sqs-jobs", noop));
        assertThrows(IllegalStateException.class, () ->
                SandboxNetworkEnsure.ensure("/usr/bin/podman", "", noop));
    }
}
