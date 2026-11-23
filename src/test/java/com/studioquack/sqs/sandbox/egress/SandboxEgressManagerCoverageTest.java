package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.ExecResult;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.Platform;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A sandbox network created after the rules were built (Podman creates {@code sqs-jobs} with the
 * first job that needs internet) must be filtered before its first container — never left as an
 * unfiltered bridge until the next overlay bring-up.
 */
class SandboxEgressManagerCoverageTest {

    private static final SandboxNet BUILD = new SandboxNet("sqs-build", "10.89.0.0/24", "10.89.0.1");
    private static final SandboxNet STATEFUL = new SandboxNet("sqs-stateful", "10.89.1.0/24", "10.89.1.1");
    private static final SandboxNet JOBS = new SandboxNet("sqs-jobs", "10.89.2.0/24", "10.89.2.1");
    private static final String IFACE = "sqnet0";
    private static final List<String> JOBS_INPUT_HOOK =
            List.of("iptables", "-I", "INPUT", "-s", "10.89.2.0/24", "-j", OverlayEgressRules.CHAIN_IN);

    private final List<List<String>> ran = new ArrayList<>();
    private List<SandboxNet> present = List.of(BUILD, STATEFUL);
    private boolean insertsFail;
    private SandboxEgressManager manager;

    @BeforeEach
    void setUp() {
        manager = manager();
        manager.networkUp(IFACE, IFACE, "100.64.1.7/32", OverlayEgressRules.OVERLAY_CIDR, null);
        assertEquals(EgressReadiness.State.ENFORCED, manager.readiness().state());
        ran.clear();
    }

    @Test
    void aNetworkCreatedAfterBringUp_isHookedBeforeItsFirstContainer_withoutAFlush() {
        present = List.of(BUILD, STATEFUL, JOBS);

        manager.coverSandboxNetwork("sqs-jobs");

        assertTrue(ran.contains(JOBS_INPUT_HOOK));
        assertTrue(ran.contains(List.of("iptables", "-I", "FORWARD", "-s", "10.89.2.0/24",
                "-j", OverlayEgressRules.CHAIN_FWD)));
        assertTrue(ran.contains(List.of("iptables", "-t", "nat", "-I", "POSTROUTING", "-s", "10.89.2.0/24",
                "-j", OverlayEgressRules.CHAIN_NAT)));
        assertTrue(ran.stream().noneMatch(argv -> argv.contains("-F")),
                "sandboxes already running keep their rules: nothing is flushed");
    }

    @Test
    void aNetworkTheRulesAlreadyCover_changesNothing() {
        manager.coverSandboxNetwork("sqs-stateful");

        assertTrue(ran.isEmpty());
    }

    @Test
    void rulesThatCannotBeApplied_refuseTheNetwork_andTheNextAttemptHooksIt() {
        present = List.of(BUILD, STATEFUL, JOBS);
        insertsFail = true;

        assertThrows(IllegalStateException.class, () -> manager.coverSandboxNetwork("sqs-jobs"));

        insertsFail = false;
        ran.clear();
        manager.coverSandboxNetwork("sqs-jobs");
        assertTrue(ran.contains(JOBS_INPUT_HOOK));
    }

    @Test
    void beforeAnyRuleSet_thereIsNothingToHookInto() {
        present = List.of(BUILD, STATEFUL, JOBS);

        manager().coverSandboxNetwork("sqs-jobs");

        assertTrue(ran.isEmpty(), "the bring-up that builds the rules discovers the network itself");
    }

    private SandboxEgressManager manager() {
        SandboxEgressManager.CommandExec exec = (argv, timeout) -> {
            ran.add(List.copyOf(argv));
            if (argv.contains("-C")) {
                return new ExecResult(false, 1, "Bad rule");
            }
            if (insertsFail && argv.contains("-I")) {
                return new ExecResult(false, 4, "iptables: Resource temporarily unavailable.");
            }
            return new ExecResult(true, 0, "");
        };
        return SandboxEgressManager.forTest("auto", true, true, IFACE, Platform.supported(),
                "/usr/bin/podman", exec, cli -> present);
    }
}
