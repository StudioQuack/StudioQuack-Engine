package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Isolate=true create, migration of non-isolated networks, inspect parsing (fake Exec). */
class SandboxNetworkEnsureIsolationTest {

    @Test
    void create_argv_includes_isolate_opt() {
        List<List<String>> calls = new ArrayList<>();
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-create", (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(false, 1, "missing");
            }
            return new SandboxNetworkEnsure.ExecResult(true, 0, "");
        });
        assertEquals(2, calls.size());
        assertEquals(
                List.of("/usr/bin/podman", "network", "create", "--opt", "isolate=true",
                        "--disable-dns", "sqs-iso-create"),
                calls.get(1));
    }

    @Test
    void pre_netavark_podman_degrades_to_plain_create_and_caches() {
        List<List<String>> calls = new ArrayList<>();
        SandboxNetworkEnsure.Exec exec = (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(false, 1, "missing");
            }
            if (argv.contains("--opt")) {
                // CNI-era podman (Ubuntu 22.04's 3.4.x) rejects the netavark-only flags.
                return new SandboxNetworkEnsure.ExecResult(
                        false, 125, "Error: unsupported option isolate");
            }
            return new SandboxNetworkEnsure.ExecResult(true, 0, "");
        };
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-degraded", exec);
        assertEquals(3, calls.size());
        assertEquals(List.of("/usr/bin/podman", "network", "create", "sqs-iso-degraded"),
                calls.get(2), "fallback create drops isolate/--disable-dns");

        int afterFirst = calls.size();
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-degraded", exec);
        assertEquals(afterFirst, calls.size(), "degraded network is cached (ENSURED_DEGRADED)");
    }

    @Test
    void pre_netavark_migration_after_rm_also_degrades_to_plain_create() {
        List<List<String>> calls = new ArrayList<>();
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-degraded-migrate", (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("rm")) {
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("--format")) {
                String fmt = argv.get(argv.size() - 1);
                if (fmt.contains("Subnets")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "10.84.0.0/24;10.84.0.1", "");
                }
                // CNI-era inspect: no Options.isolate, no DNSEnabled field.
                return new SandboxNetworkEnsure.ExecResult(false, 125, "template error");
            }
            if (argv.contains("--opt")) {
                return new SandboxNetworkEnsure.ExecResult(
                        false, 125, "Error: unsupported option isolate");
            }
            return new SandboxNetworkEnsure.ExecResult(true, 0, "");
        });
        assertTrue(calls.stream().anyMatch(a -> a.contains("rm")));
        assertTrue(calls.stream().anyMatch(a ->
                a.contains("create") && !a.contains("--opt") && !a.contains("--disable-dns")));
    }

    @Test
    void non_option_create_failure_still_throws() {
        List<List<String>> calls = new ArrayList<>();
        try {
            SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-real-failure", (argv, timeout) -> {
                calls.add(List.copyOf(argv));
                if (argv.contains("exists")) {
                    return new SandboxNetworkEnsure.ExecResult(false, 1, "missing");
                }
                return new SandboxNetworkEnsure.ExecResult(false, 1, "permission denied");
            });
            org.junit.jupiter.api.Assertions.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("permission denied"));
        }
        assertEquals(2, calls.size(), "no plain-create retry for non-option failures");
    }

    @Test
    void cni_conflist_1_0_0_is_downgraded_to_0_4_0() throws Exception {
        java.nio.file.Path conflist = java.nio.file.Files.createTempFile("sqs-jobs", ".conflist");
        java.nio.file.Files.writeString(conflist, """
                {
                  "cniVersion": "1.0.0",
                  "name": "sqs-jobs",
                  "plugins": [ {"type": "bridge"}, {"type": "firewall"} ]
                }
                """);
        SandboxNetworkEnsure.downgradeCniConflistVersion(conflist);
        String fixed = java.nio.file.Files.readString(conflist);
        assertTrue(fixed.contains("\"cniVersion\": \"0.4.0\""));
        assertFalse(fixed.contains("1.0.0"));
        java.nio.file.Files.deleteIfExists(conflist);
    }

    @Test
    void cni_conflist_already_0_4_0_or_absent_is_untouched() throws Exception {
        java.nio.file.Path conflist = java.nio.file.Files.createTempFile("sqs-jobs", ".conflist");
        String original = "{ \"cniVersion\": \"0.4.0\", \"name\": \"sqs-jobs\" }";
        java.nio.file.Files.writeString(conflist, original);
        SandboxNetworkEnsure.downgradeCniConflistVersion(conflist);
        assertEquals(original, java.nio.file.Files.readString(conflist));
        java.nio.file.Files.deleteIfExists(conflist);

        // Absent file (netavark host writes no conflist) — no-op, no throw.
        SandboxNetworkEnsure.downgradeCniConflistVersion(
                java.nio.file.Path.of("/nonexistent/sqs-jobs.conflist"));
    }

    @Test
    void existing_isolated_network_is_cached_without_rm() {
        List<List<String>> calls = new ArrayList<>();
        SandboxNetworkEnsure.Exec exec = (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("rm")) {
                return new SandboxNetworkEnsure.ExecResult(false, 1, "should not rm");
            }
            if (argv.contains("--format")) {
                String fmt = argv.get(argv.size() - 1);
                if (fmt.contains("Subnets")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "10.89.0.0/24;10.89.0.1", "");
                }
                if (fmt.contains("DNSEnabled")) {
                    // "false" → dnsDisabled=true (posture present).
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "false", "");
                }
                return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
            }
            return new SandboxNetworkEnsure.ExecResult(false, 1, "unexpected");
        };
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-already", exec);
        assertTrue(calls.stream().noneMatch(a -> a.contains("rm")));
        assertTrue(calls.stream().noneMatch(a -> a.contains("create")));
        int afterFirst = calls.size();

        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-already", (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            return new SandboxNetworkEnsure.ExecResult(false, 1, "cached");
        });
        assertEquals(afterFirst, calls.size(), "second ensure uses ENSURED cache");
    }

    @Test
    void non_isolated_network_attempts_rm_and_create() {
        List<List<String>> calls = new ArrayList<>();
        AtomicBoolean removed = new AtomicBoolean();
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-migrate", (argv, timeout) -> {
            calls.add(List.copyOf(argv));
            if (argv.contains("exists")) {
                return new SandboxNetworkEnsure.ExecResult(!removed.get(), removed.get() ? 1 : 0, "");
            }
            if (argv.contains("rm")) {
                removed.set(true);
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("create")) {
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("--format")) {
                String fmt = argv.get(argv.size() - 1);
                if (fmt.contains("Subnets")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "10.87.0.0/24;10.87.0.1", "");
                }
                // Not isolated yet.
                return new SandboxNetworkEnsure.ExecResult(true, 0, "false", "");
            }
            return new SandboxNetworkEnsure.ExecResult(false, 1, "unexpected");
        });
        assertTrue(calls.stream().anyMatch(a -> a.contains("rm")));
        assertTrue(calls.stream().anyMatch(a ->
                a.contains("create") && a.contains("--opt") && a.contains("isolate=true")));
    }

    @Test
    void rm_failure_still_succeeds_but_is_not_cached() {
        List<Integer> ensureWaves = new ArrayList<>();
        SandboxNetworkEnsure.Exec exec = (argv, timeout) -> {
            if (argv.contains("exists")) {
                ensureWaves.add(1);
                return new SandboxNetworkEnsure.ExecResult(true, 0, "");
            }
            if (argv.contains("rm")) {
                return new SandboxNetworkEnsure.ExecResult(false, 125, "network is in use");
            }
            if (argv.contains("--format")) {
                String fmt = argv.get(argv.size() - 1);
                if (fmt.contains("Subnets")) {
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "10.86.0.0/24;10.86.0.1", "");
                }
                return new SandboxNetworkEnsure.ExecResult(true, 0, "", ""); // not "true"
            }
            return new SandboxNetworkEnsure.ExecResult(false, 1, "unexpected");
        };
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-rm-fail", exec);
        int firstWave = ensureWaves.size();
        assertTrue(firstWave >= 1);

        // Not cached → second ensure re-runs exists (+ inspect + rm).
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-iso-rm-fail", exec);
        assertTrue(ensureWaves.size() > firstWave, "uncached ensure re-runs CLI");
    }

    @Test
    void inspect_parses_subnet_gateway_and_isolate() {
        SandboxNetworkEnsure.SandboxNetworkInfo info = SandboxNetworkEnsure.inspect(
                "/usr/bin/podman", "sqs-iso-inspect", (argv, timeout) -> {
                    assertTrue(argv.contains("inspect"));
                    assertTrue(argv.contains("--format"));
                    String fmt = argv.get(argv.size() - 1);
                    if (fmt.contains("Subnets")) {
                        assertEquals(SandboxNetworkEnsure.SUBNET_FORMAT, fmt);
                        return new SandboxNetworkEnsure.ExecResult(
                                true, 0, "10.89.1.0/24;10.89.1.1", "");
                    }
                    if (fmt.contains("DNSEnabled")) {
                        assertEquals(SandboxNetworkEnsure.DNS_ENABLED_FORMAT, fmt);
                        return new SandboxNetworkEnsure.ExecResult(true, 0, "false", "");
                    }
                    assertEquals(SandboxNetworkEnsure.ISOLATE_FORMAT, fmt);
                    return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
                });
        assertNotNull(info);
        assertEquals("sqs-iso-inspect", info.name());
        assertEquals("10.89.1.0/24", info.subnetCidr());
        assertEquals("10.89.1.1", info.gatewayIp());
        assertTrue(info.isolated());
    }

    @Test
    void inspect_returns_null_when_absent_or_unparseable() {
        assertNull(SandboxNetworkEnsure.inspect("/usr/bin/podman", "missing", (argv, timeout) ->
                new SandboxNetworkEnsure.ExecResult(false, 1, "no such network")));

        assertNull(SandboxNetworkEnsure.inspect("/usr/bin/podman", "bad", (argv, timeout) -> {
            if (argv.get(argv.size() - 1).contains("Subnets")) {
                return new SandboxNetworkEnsure.ExecResult(true, 0, "not-a-pair", "");
            }
            return new SandboxNetworkEnsure.ExecResult(true, 0, "true", "");
        }));

        assertNull(SandboxNetworkEnsure.inspect("", "x", (argv, timeout) ->
                new SandboxNetworkEnsure.ExecResult(true, 0, "")));
    }

    @Test
    void inspect_isolate_probe_error_falls_back_to_not_isolated() {
        SandboxNetworkEnsure.SandboxNetworkInfo info = SandboxNetworkEnsure.inspect(
                "/usr/bin/podman", "sqs-iso-old-podman", (argv, timeout) -> {
                    String fmt = argv.get(argv.size() - 1);
                    if (fmt.contains("Subnets")) {
                        return new SandboxNetworkEnsure.ExecResult(
                                true, 0, "10.85.0.0/24;10.85.0.1", "");
                    }
                    return new SandboxNetworkEnsure.ExecResult(false, 125, "template error");
                });
        assertNotNull(info);
        assertFalse(info.isolated());
        assertEquals("10.85.0.0/24", info.subnetCidr());
        assertEquals("10.85.0.1", info.gatewayIp());
    }
}
