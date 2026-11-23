package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.NetworkPlan;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Golden ordering for the host-egress builder: overlay range is 100.64.0.0/10, internet is
 * MASQUERADE on the host default route, no 10.88 hairpin SNAT, local denies include the
 * overlay,:9876:8443 stay denied.
 */
class OverlayEgressRulesTest {

    private static final String OVERLAY_IP = "100.64.1.7";
    private static final String IFACE = "sqnet0";

    private static final NetworkPlan PRIMARY = new NetworkPlan(
            IFACE, IFACE, OVERLAY_IP, OverlayEgressRules.OVERLAY_CIDR,
            OverlayEgressRules.TABLE_ID, OverlayEgressRules.STEER_PREF_BASE);

    private static List<SandboxNet> sampleNets() {
        return List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"),
                new SandboxNet("sqs-stateful", "10.89.1.0/24", "10.89.1.1"),
                new SandboxNet("sqs-build", "10.89.2.0/24", "10.89.2.1"));
    }

    @Test
    void overlayCidr_isCgnat_notFabric1088() {
        assertEquals("100.64.0.0/10", OverlayEgressRules.OVERLAY_CIDR);
    }

    @Test
    void addSubnet_hooksOnlyTheNewSubnet_withoutFlushingTheSharedChains() {
        SandboxNet jobs = new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1");
        List<List<String>> cmds = OverlayEgressRules.addSubnetCommands(List.of(jobs), sampleNets());

        assertTrue(cmds.contains(List.of("iptables", "-I", "INPUT", "-s", "10.89.0.0/24", "-j", "SQS_SBX_IN")));
        assertTrue(cmds.contains(List.of("iptables", "-I", "FORWARD", "-s", "10.89.0.0/24", "-j", "SQS_SBX_FWD")));
        assertTrue(cmds.contains(List.of("iptables", "-t", "nat", "-I", "POSTROUTING",
                "-s", "10.89.0.0/24", "-j", "SQS_SBX_NAT")));
        // Cross-sandbox DROPs both ways against every other sandbox subnet, at the FWD head.
        assertTrue(cmds.contains(List.of("iptables", "-I", "SQS_SBX_FWD", "1",
                "-s", "10.89.0.0/24", "-d", "10.89.1.0/24", "-j", "DROP")));
        assertTrue(cmds.contains(List.of("iptables", "-I", "SQS_SBX_FWD", "1",
                "-s", "10.89.2.0/24", "-d", "10.89.0.0/24", "-j", "DROP")));
        // Every insert is preceded by its check, and nothing live is flushed or appended to.
        for (int i = 0; i < cmds.size(); i += 2) {
            assertTrue(cmds.get(i).contains("-C"), "check before insert: " + cmds.get(i));
            assertTrue(cmds.get(i + 1).contains("-I"), "insert after check: " + cmds.get(i + 1));
        }
        assertFalse(cmds.stream().anyMatch(c -> c.contains("-F") || c.contains("-A")));
        assertFalse(cmds.stream().anyMatch(c -> c.contains("10.89.1.0/24") && c.contains("INPUT")),
                "subnets already hooked are left alone");
    }

    @Test
    void reconcile_emits_create_or_flush_then_ordered_security_rules() {
        List<List<String>> cmds = OverlayEgressRules.reconcileCommands(PRIMARY, sampleNets());

        assertEquals(List.of("iptables", "-N", "SQS_SBX_IN"), cmds.get(0));
        assertEquals(List.of("iptables", "-F", "SQS_SBX_IN"), cmds.get(1));
        assertTrue(cmds.contains(List.of("iptables", "-F", "SQS_SBX_FWD")));
        assertTrue(cmds.contains(List.of("iptables", "-t", "nat", "-F", "SQS_SBX_NAT")));
        assertTrue(cmds.contains(List.of("iptables", "-N", "SQS_SBX_PERBOX")));
        assertFalse(cmds.contains(List.of("iptables", "-F", "SQS_SBX_PERBOX")),
                "PERBOX must never be flushed — a re-apply would wipe live containers' grants");

        int dmnJump = indexOfExact(cmds, List.of("iptables", "-A", "SQS_SBX_IN", "-j", "SQS_SBX_DMN"));
        int drop9876 = indexOfSubsequence(cmds, List.of(
                "iptables", "-A", "SQS_SBX_DMN",
                "-d", OVERLAY_IP, "-p", "tcp", "--dport", "9876"));
        int loopbackDrop = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_IN", "-d", "127.0.0.0/8", "-j", "DROP"));
        int finalInDrop = indexOfExact(cmds, List.of("iptables", "-A", "SQS_SBX_IN", "-j", "DROP"));
        assertTrue(dmnJump >= 0, "the INPUT chain must jump the per-network daemon denies first");
        assertTrue(drop9876 >= 0, "explicit :9876 DROP required");
        assertTrue(loopbackDrop > dmnJump, "loopback DROP after the daemon denies");
        assertTrue(finalInDrop > loopbackDrop, "the fail-closed DROP terminates SQS_SBX_IN");

        assertFalse(cmds.stream().anyMatch(c -> c.contains("--dport") && c.contains("53")
                        && c.contains("ACCEPT")),
                "no gateway DNS ACCEPT may remain anywhere in the rule set: " + cmds);

        int established = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_FWD",
                "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT"));
        int dmnfJump = indexOfExact(cmds, List.of("iptables", "-A", "SQS_SBX_FWD", "-j", "SQS_SBX_DMNF"));
        int dropPeer9876 = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_DMNF",
                "-d", "100.64.0.0/10", "-p", "tcp", "--dport", "9876",
                "-m", "comment", "--comment", "deny-sandbox-to-daemon-peer-listener",
                "-j", "DROP"));
        int dropExec8443 = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_DMNF",
                "-d", "100.64.0.0/10", "-p", "tcp", "--dport", "8443",
                "-m", "comment", "--comment", "deny-sandbox-to-daemon-execute-endpoint",
                "-j", "DROP"));
        int perboxJump = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_FWD", "-j", "SQS_SBX_PERBOX"));
        int dropOverlay = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_FWD", "-d", "100.64.0.0/10", "-j", "DROP"));
        int drop10 = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_FWD", "-d", "10.0.0.0/8", "-j", "DROP"));
        int outJump = indexOfExact(cmds, List.of("iptables", "-A", "SQS_SBX_FWD", "-j", "SQS_SBX_OUT"));
        int hairpinDrop = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_OUT", "-o", IFACE,
                "-m", "comment", "--comment", "sq-no-overlay-hairpin", "-j", "DROP"));
        int hostEgress = indexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_FWD",
                "-m", "comment", "--comment", "sq-host-egress", "-j", "ACCEPT"));
        int finalFwdDrop = lastIndexOfExact(cmds, List.of(
                "iptables", "-A", "SQS_SBX_FWD",
                "-m", "comment", "--comment", "sq-sbx-fail-closed", "-j", "DROP"));

        assertTrue(established >= 0, "established must head the FORWARD chain");
        assertTrue(dmnfJump > established, "daemon-listener DROPs right after established");
        assertTrue(dropPeer9876 >= 0 && dropExec8443 >= 0, "daemon-listener DROPs required");
        assertTrue(perboxJump > dmnfJump, "PERBOX jump below the daemon-listener DROPs");
        assertTrue(dropOverlay > perboxJump, "100.64.0.0/10 DROP below the PERBOX jump");
        assertTrue(drop10 > perboxJump, "RFC1918 10/8 DROP below the PERBOX jump");
        assertTrue(outJump > dropOverlay, "hairpin-deny jump after local DROPs");
        assertTrue(hairpinDrop >= 0, "overlay-iface hairpin DROP lives in SQS_SBX_OUT");
        assertTrue(hostEgress > outJump, "host-egress ACCEPT after the hairpin jump");
        assertTrue(finalFwdDrop > hostEgress, "fail-closed DROP is last");

        assertTrue(cmds.contains(List.of("iptables", "-t", "nat", "-A", "SQS_SBX_NAT", "-j", "MASQUERADE")),
                "internet path is MASQUERADE on the host default egress");
        assertTrue(cmds.contains(List.of(
                "iptables", "-t", "nat", "-A", "SQS_SBX_NAT", "-d", "100.64.0.0/10", "-j", "RETURN")));
        assertTrue(cmds.contains(List.of(
                "iptables", "-t", "nat", "-A", "SQS_SBX_NAT", "-d", "10.0.0.0/8", "-j", "RETURN")));
        assertTrue(cmds.contains(List.of(
                "iptables", "-t", "nat", "-A", "SQS_SBX_NAT", "-d", "172.16.0.0/12", "-j", "RETURN")));
        assertTrue(cmds.contains(List.of(
                "iptables", "-t", "nat", "-A", "SQS_SBX_NAT", "-d", "192.168.0.0/16", "-j", "RETURN")));
        assertTrue(cmds.contains(List.of(
                "iptables", "-t", "nat", "-A", "SQS_SBX_NAT", "-d", "169.254.0.0/16", "-j", "RETURN")));
        assertTrue(indexOfSubsequence(cmds, List.of(
                "iptables", "-t", "nat", "-I", "POSTROUTING",
                "-s", "10.89.0.0/24", "-j", "SQS_SBX_NAT")) >= 0);

        assertFalse(cmds.stream().anyMatch(c -> c.contains("--to-source")),
                "no SNAT --to-source overlay IP (that was the hairpin)");
        assertFalse(cmds.stream().anyMatch(c ->
                        c.equals(List.of("ip", "route", "replace", "default", "dev", IFACE, "table", "88"))),
                "no policy-routing default via the overlay iface");
        assertFalse(cmds.stream().anyMatch(c -> c.contains("main")),
                "no main-table route is ever emitted");
    }

    @Test
    void addNetwork_is_daemon_denies_and_hairpin_only() {
        NetworkPlan extra = new NetworkPlan(
                "sqnet1", "sqnet1", "100.64.2.9", OverlayEgressRules.OVERLAY_CIDR,
                OverlayEgressRules.TABLE_ID + 1, OverlayEgressRules.STEER_PREF_BASE + 1);
        List<List<String>> add = OverlayEgressRules.addNetworkCommands(extra, sampleNets());

        assertFalse(add.stream().anyMatch(c -> c.contains("-F")), "additive form never flushes");
        assertFalse(add.stream().anyMatch(c -> c.contains("-N")), "additive form never creates chains");
        assertFalse(add.stream().anyMatch(c -> c.contains("--to-source") || c.contains("MASQUERADE")
                        || c.contains("SNAT")),
                "additive form never SNATs or MASQUERADEs");
        assertFalse(add.stream().anyMatch(c -> c.contains("route") && c.contains("default")),
                "additive form never installs a default via overlay");
        assertTrue(add.contains(List.of(
                "iptables", "-A", "SQS_SBX_DMN", "-d", "100.64.2.9", "-p", "tcp", "--dport", "9876",
                "-m", "comment", "--comment", "deny-sandbox-to-overlay-reverse-listener",
                "-j", "DROP")));
        assertTrue(add.contains(List.of(
                "iptables", "-A", "SQS_SBX_OUT", "-o", "sqnet1",
                "-m", "comment", "--comment", "sq-no-overlay-hairpin", "-j", "DROP")));

        List<List<String>> remove = OverlayEgressRules.removeNetworkCommands(extra, sampleNets());
        for (List<String> del : remove) {
            if (del.get(0).equals("iptables")) {
                assertTrue(del.contains("-D"), "removal emits only delete forms: " + del);
            }
        }
    }

    private static int indexOfExact(List<List<String>> cmds, List<String> target) {
        return cmds.indexOf(target);
    }

    private static int lastIndexOfExact(List<List<String>> cmds, List<String> target) {
        return cmds.lastIndexOf(target);
    }

    private static int indexOfSubsequence(List<List<String>> cmds, List<String> prefix) {
        for (int i = 0; i < cmds.size(); i++) {
            List<String> c = cmds.get(i);
            if (c.size() >= prefix.size() && c.subList(0, prefix.size()).equals(prefix)) {
                return i;
            }
        }
        return -1;
    }
}
