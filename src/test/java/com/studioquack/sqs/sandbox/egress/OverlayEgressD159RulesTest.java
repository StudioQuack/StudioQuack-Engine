package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.NetworkPlan;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MASQUERADE argv contract: sandbox subnets jump POSTROUTING → SQS_SBX_NAT, local/overlay dests
 * RETURN, remainder is {@code -j MASQUERADE} with no {@code --to-source}.
 */
class OverlayEgressD159RulesTest {

    @Test
    void masqueradeArgv_excludesOverlayAndRfc1918() {
        NetworkPlan plan = new NetworkPlan(
                "sqnet0", "sqnet0", "100.64.1.7", OverlayEgressRules.OVERLAY_CIDR,
                OverlayEgressRules.TABLE_ID, OverlayEgressRules.STEER_PREF_BASE);
        List<List<String>> cmds = OverlayEgressRules.reconcileCommands(plan, List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1")));

        assertEquals(List.of("iptables", "-t", "nat", "-A", "SQS_SBX_NAT", "-j", "MASQUERADE"),
                cmds.stream().filter(c -> c.contains("MASQUERADE")).findFirst().orElseThrow());
        assertTrue(cmds.contains(List.of(
                "iptables", "-t", "nat", "-I", "POSTROUTING", "-s", "10.89.0.0/24", "-j", "SQS_SBX_NAT")));
        for (String dest : OverlayEgressRules.NAT_EXCLUDE_DESTS) {
            assertTrue(cmds.contains(List.of(
                    "iptables", "-t", "nat", "-A", "SQS_SBX_NAT", "-d", dest, "-j", "RETURN")),
                    "NAT must RETURN " + dest);
        }
        assertFalse(cmds.stream().anyMatch(c -> c.contains("--to-source")));
        assertEquals(OverlayEgressRules.OVERLAY_CIDR, "100.64.0.0/10");
        assertTrue(cmds.contains(List.of(
                "iptables", "-A", "SQS_SBX_FWD", "-d", "100.64.0.0/10", "-j", "DROP")));
        assertTrue(cmds.contains(List.of(
                "iptables", "-A", "SQS_SBX_DMNF",
                "-d", "100.64.0.0/10", "-p", "tcp", "--dport", "9876",
                "-m", "comment", "--comment", "deny-sandbox-to-daemon-peer-listener",
                "-j", "DROP")));
        assertTrue(cmds.contains(List.of(
                "iptables", "-A", "SQS_SBX_DMNF",
                "-d", "100.64.0.0/10", "-p", "tcp", "--dport", "8443",
                "-m", "comment", "--comment", "deny-sandbox-to-daemon-execute-endpoint",
                "-j", "DROP")));
    }
}
