package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Self-hosted macOS / Windows: the egress rules are enforced inside the Podman machine, so a
 * networked sandbox is allowed exactly when they are in place — and refused while they are not.
 */
class SandboxEgressManagerVmTest {

    private static final List<SandboxNet> NETS = List.of(new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"));

    @Test
    void rules_applied_in_the_machine_make_the_host_enforced() {
        FakeMachine vm = new FakeMachine();
        SandboxEgressManager m = manager(() -> vm.target());

        assertEquals(EgressReadiness.State.UNAVAILABLE, m.readiness().state(), "pending until the overlay is up");
        m.overlayUp("100.90.1.2/32");

        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.ENFORCED, r.state());
        assertTrue(r.internetAllowed());
        assertEquals("overlay-egress enforced inside Podman machine podman-machine-default", r.reason());
        assertEquals(1, vm.batches.get(), "the whole rule set goes in one round trip");
        assertTrue(vm.lastBatch.get().contains("'iptables' '-A' 'SQS_SBX_FWD' '-d' '192.168.0.0/16' '-j' 'DROP'"));
        assertTrue(vm.anchorStarts.get() >= 1, "the namespace is held open before the rules go in");
    }

    @Test
    void no_running_machine_is_degraded_and_refuses_networked_starts() {
        SandboxEgressManager m = manager(() -> null);
        m.overlayUp("100.90.1.2/32");

        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.DEGRADED, r.state());
        assertFalse(r.internetAllowed());
        assertTrue(r.reason().startsWith("overlay-egress needs a running Podman machine"));
    }

    @Test
    void a_machine_that_starts_later_is_picked_up_by_the_verify_tick() {
        FakeMachine vm = new FakeMachine();
        AtomicBoolean machineUp = new AtomicBoolean(false);
        SandboxEgressManager m = manager(() -> machineUp.get() ? vm.target() : null);
        m.overlayUp("100.90.1.2/32");
        assertEquals(EgressReadiness.State.DEGRADED, m.readiness().state());

        machineUp.set(true);
        m.verifyNetnsEgress();
        assertEquals(EgressReadiness.State.ENFORCED, m.readiness().state());
    }

    @Test
    void lost_rules_are_reapplied_and_present_rules_left_alone() {
        FakeMachine vm = new FakeMachine();
        SandboxEgressManager m = manager(() -> vm.target());
        m.overlayUp("100.90.1.2/32");

        m.verifyNetnsEgress();
        assertEquals(1, vm.batches.get(), "rules present: nothing re-applied");
        assertTrue(vm.lastSingle.get().contains("'-C' 'FORWARD' '-s' '10.89.0.0/24' '-j' 'SQS_SBX_FWD'"));

        vm.rulesPresent.set(false);
        m.verifyNetnsEgress();
        assertEquals(2, vm.batches.get(), "machine restarted: the whole set goes back in");
        assertEquals(EgressReadiness.State.ENFORCED, m.readiness().state());
    }

    @Test
    void the_grant_chain_is_flushed_inside_the_machine_at_init_and_after_recreation() {
        FakeMachine vm = new FakeMachine();
        SandboxEgressManager m = manager(() -> vm.target());
        m.overlayUp("100.90.1.2/32");
        assertTrue(flushesGrantChain(vm.lastBatch.get()), "daemon start empties the chain in the machine");

        vm.rulesPresent.set(false);
        m.verifyNetnsEgress();
        assertEquals(2, vm.batches.get());
        assertTrue(flushesGrantChain(vm.lastBatch.get()), "and so does a recreated namespace");
    }

    /** Created, then flushed, in the same batch that keeps the FWD jump into it. */
    private static boolean flushesGrantChain(String batch) {
        int create = batch.indexOf("'iptables' '-N' 'SQS_SBX_PERBOX'");
        int flush = batch.indexOf("'iptables' '-F' 'SQS_SBX_PERBOX'");
        return create >= 0 && flush > create
                && batch.contains("'iptables' '-A' 'SQS_SBX_FWD' '-j' 'SQS_SBX_PERBOX'");
    }

    @Test
    void a_failed_reapply_fails_closed() {
        FakeMachine vm = new FakeMachine();
        SandboxEgressManager m = manager(() -> vm.target());
        m.overlayUp("100.90.1.2/32");

        vm.rulesPresent.set(false);
        vm.reachable.set(false);
        m.verifyNetnsEgress();
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.UNAVAILABLE, r.state());
        assertFalse(r.internetAllowed());
    }

    @Test
    void a_latch_set_elsewhere_is_not_lifted_by_the_verify_tick() {
        FakeMachine vm = new FakeMachine();
        SandboxEgressManager m = manager(() -> vm.target());
        m.overlayUp("100.90.1.2/32");
        m.overlayDown("handshake-failed");

        vm.rulesPresent.set(false);
        m.verifyNetnsEgress();
        assertEquals(EgressReadiness.State.UNAVAILABLE, m.readiness().state());
        assertEquals("handshake-failed", m.readiness().reason());
        assertEquals(1, vm.batches.get());
    }

    @Test
    void operator_opt_out_never_touches_the_machine() {
        FakeMachine vm = new FakeMachine();
        SandboxEgressManager m = SandboxEgressManager.forNetnsTest("off", true, true, "sqnet0", cli -> NETS, () -> vm.target());
        m.overlayUp("100.90.1.2/32");
        m.verifyNetnsEgress();
        assertTrue(m.readiness().operatorOptOut());
        assertEquals(0, vm.batches.get());
    }

    @Test
    void discoverNetworks_preEnsuresMissingSqsJobs_soAnchorPodCanAttach() {
        AtomicBoolean jobsCreated = new AtomicBoolean(false);
        com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure.Exec runner = (argv, timeout) -> {
            if (argv.contains("exists") && argv.contains("sqs-jobs")) {
                return new com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure.ExecResult(
                        jobsCreated.get(), jobsCreated.get() ? 0 : 1, "", "");
            }
            if (argv.contains("create") && argv.contains("sqs-jobs")) {
                jobsCreated.set(true);
                return new com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure.ExecResult(true, 0, "", "");
            }
            if (argv.contains("inspect") && argv.contains("sqs-jobs")) {
                if (!jobsCreated.get()) {
                    return new com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure.ExecResult(
                            false, 1, "", "Error: network sqs-jobs not found");
                }
                String fmt = argv.get(argv.size() - 1);
                String out = fmt.contains("Subnet") ? "10.89.0.0/24;10.89.0.1"
                        : fmt.contains("isolate") ? "true" : "false";
                return new com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure.ExecResult(true, 0, out, "");
            }
            return new com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure.ExecResult(false, 1, "", "not found");
        };

        List<SandboxNet> nets = SandboxEgressManager.discoverNetworks("podman", runner);
        assertTrue(jobsCreated.get(), "sqs-jobs must be created during discovery when absent");
        assertEquals(List.of(new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1")), nets);
    }

    private static SandboxEgressManager manager(java.util.function.Supplier<PodmanNetnsTarget> locator) {
        return SandboxEgressManager.forNetnsTest("auto", true, true, "sqnet0", cli -> NETS, locator);
    }

    /** A rootless machine whose rules / reachability the test flips. */
    private static final class FakeMachine {
        final AtomicInteger batches = new AtomicInteger();
        final AtomicInteger anchorStarts = new AtomicInteger();
        final AtomicBoolean rulesPresent = new AtomicBoolean(true);
        final AtomicBoolean reachable = new AtomicBoolean(true);
        final AtomicReference<String> lastBatch = new AtomicReference<>("");
        final AtomicReference<String> lastSingle = new AtomicReference<>("");

        PodmanNetnsTarget target() {
            return PodmanNetnsTarget.machine("podman", "podman-machine-default", false, "sqs-jobs", (argv, stdin, timeout) -> {
                if (argv.contains("pod")) {
                    if (argv.contains("start")) {
                        anchorStarts.incrementAndGet();
                    }
                    return new CliRunner.Result(0, "", "", false, 1);
                }
                if (!reachable.get()) {
                    return new CliRunner.Result(255, "", "Error: machine is not running", false, 1);
                }
                String script = new String(stdin, StandardCharsets.UTF_8);
                if (script.contains("c0()")) {
                    batches.incrementAndGet();
                    lastBatch.set(script);
                    rulesPresent.set(true);
                    return new CliRunner.Result(0, PodmanNetnsTarget.OK_MARK + "\n", "", false, 1);
                }
                lastSingle.set(script);
                int rc = script.contains("'-C' 'FORWARD'") && !rulesPresent.get() ? 1 : 0;
                return new CliRunner.Result(0, PodmanNetnsTarget.RC_MARK + rc + "\n", "", false, 1);
            });
        }
    }
}
