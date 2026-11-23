package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.ExecResult;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.Platform;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-network host egress. A second iface (leftover drain API) is additive daemon-denies +
 * hairpin DROP only: no SNAT, no default via overlay.
 */
class SandboxEgressManagerDrainTest {

    private static final List<SandboxNet> NETS = List.of(
            new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"));
    private static final String IFACE = "sqnet0";

    private OverlayPeerDirectory directory;
    private NetworkRuleStore store;
    private List<List<String>> ran;
    private SandboxEgressManager manager;

    @BeforeEach
    void setUp() {
        directory = new OverlayPeerDirectory();
        store = NetworkRuleStore.inMemory();
        ran = new ArrayList<>();
        SandboxEgressManager.CommandExec exec = (argv, timeout) -> {
            ran.add(List.copyOf(argv));
            if (argv.contains("-C")) {
                return new ExecResult(false, 1, "Bad rule");
            }
            return new ExecResult(true, 0, "");
        };
        manager = SandboxEgressManager.forTest("auto", true, true, IFACE, Platform.supported(),
                "/usr/bin/podman", exec, cli -> NETS, directory, store);
        manager.networkUp(IFACE, IFACE, "100.64.1.7/32", OverlayEgressRules.OVERLAY_CIDR, null);
        assertEquals(EgressReadiness.State.ENFORCED, manager.readiness().state());
        ran.clear();
    }

    @Test
    void overlayUp_isHostMasquerade_notHairpin() {
        List<List<String>> first = new ArrayList<>();
        SandboxEgressManager fresh = SandboxEgressManager.forTest("auto", true, true, IFACE,
                Platform.supported(), "/usr/bin/podman",
                (argv, timeout) -> {
                    first.add(List.copyOf(argv));
                    if (argv.contains("-C")) {
                        return new ExecResult(false, 1, "Bad rule");
                    }
                    return new ExecResult(true, 0, "");
                }, cli -> NETS, directory, store);
        fresh.overlayUp("100.64.1.7/32");
        assertTrue(first.stream().anyMatch(a -> a.contains("MASQUERADE")));
        assertFalse(first.stream().anyMatch(a -> a.contains("--to-source")));
        assertFalse(first.stream().anyMatch(a -> a.contains("route") && a.contains("default")
                        && a.contains(IFACE)),
                "must not install a policy-routing default via the overlay");
    }

    @Test
    void secondIface_isAdditive_noDefaultViaOverlay() {
        manager.networkUp("sqnet1", "sqnet1", "100.64.2.9/32", OverlayEgressRules.OVERLAY_CIDR, null);

        assertTrue(ran.stream().noneMatch(a -> a.contains("-F")),
                "the second iface must never flush the first's chains");
        assertFalse(ran.stream().anyMatch(a -> a.contains("--to-source")));
        assertFalse(ran.stream().anyMatch(a -> a.equals(List.of(
                "ip", "route", "replace", "default", "dev", "sqnet1", "table", "89"))));
        assertEquals("100.64.1.7", manager.readiness().overlayIp());
    }

    @Test
    void addressChurn_rewritesDaemonDeny_notSnat() {
        manager.networkUp(IFACE, IFACE, "100.64.1.99/32", OverlayEgressRules.OVERLAY_CIDR, null);

        assertTrue(ran.stream().anyMatch(a -> a.contains("-D") && a.contains("100.64.1.7")),
                "the stale /32's daemon deny is removed exactly");
        assertTrue(ran.stream().anyMatch(a -> a.contains("-A") && a.contains("100.64.1.99")
                        && a.contains("9876")),
                "the new /32 is denied on :9876");
        assertFalse(ran.stream().anyMatch(a -> a.contains("--to-source")));
        assertEquals("100.64.1.99", manager.readiness().overlayIp());
    }

    @Test
    void containerGrants_resolveOnTheContainersOwnNetwork() {
        manager.networkUp("sqnet1", "sqnet1", "100.64.2.9/32", OverlayEgressRules.OVERLAY_CIDR, null);
        store.replaceAll(List.of(), List.of(
                new NetworkRuleStore.OutboundRule("engine-b", 2222),
                new NetworkRuleStore.OutboundRule("engine-c", 5432)));
        directory.replace(IFACE, Map.of("engine-b", "100.64.1.20", "engine-c", "100.64.1.30"));
        // Mid-drain: engine-b already answers on the new mesh, engine-c does not yet.
        directory.replace("sqnet1", Map.of("engine-b", "100.64.2.20"));
        manager.assignContainerNetworkTo("10.89.0.6", IFACE);
        manager.assignContainerNetworkTo("10.89.0.7", "sqnet1");
        ran.clear();

        assertTrue(manager.applyContainerGrants("10.89.0.6"));
        assertTrue(manager.applyContainerGrants("10.89.0.7"));

        List<List<String>> oldNet = grantsOf("10.89.0.6");
        assertEquals(2, oldNet.size(), "the old-network container keeps both grants");
        assertTrue(oldNet.stream().allMatch(a -> a.contains(IFACE)), "pinned to its own iface");
        assertTrue(oldNet.stream().anyMatch(a -> a.contains("100.64.1.20/32") && a.contains("2222")));
        assertTrue(oldNet.stream().anyMatch(a -> a.contains("100.64.1.30/32") && a.contains("5432")));

        List<List<String>> newNet = grantsOf("10.89.0.7");
        assertEquals(1, newNet.size(), "only what the new network's mesh resolves");
        assertTrue(newNet.get(0).contains("100.64.2.20/32") && newNet.get(0).contains("2222")
                && newNet.get(0).contains("sqnet1"));
        assertTrue(newNet.stream().noneMatch(a -> a.contains("100.64.1.30/32") || a.contains(IFACE)),
                "a grant never widens to the other tunnel's mesh");
    }

    @Test
    void containerAssignment_isBookkeepingOnly() {
        manager.networkUp("sqnet1", "sqnet1", "100.64.2.9/32", OverlayEgressRules.OVERLAY_CIDR, null);
        manager.setDefaultNetwork("sqnet1");
        List<String> released = new ArrayList<>();
        manager.setContainerReleaseListener(released::add);
        ran.clear();

        assertEquals("sqnet1", manager.assignContainerNetwork("10.89.0.6"),
                "a new container rides the current default network");
        assertEquals(IFACE, manager.assignContainerNetworkTo("10.89.0.7", IFACE),
                "a surviving instance can stay on the old network");
        assertEquals("sqnet1", manager.networkOfContainer("10.89.0.6"));
        assertEquals(IFACE, manager.networkOfContainer("10.89.0.7"));

        manager.releaseContainerNetwork("10.89.0.6");
        manager.releaseContainerNetwork("10.89.0.7");

        assertNull(manager.networkOfContainer("10.89.0.6"));
        assertEquals(List.of("10.89.0.6", "10.89.0.7"), released,
                "every release nudges the drain manager's occupancy");
        assertTrue(ran.isEmpty(), "D159 assignment and release program no host rule: " + ran);
    }

    private List<List<String>> grantsOf(String containerIp) {
        return ran.stream()
                .filter(a -> a.contains("-A") && a.contains(OverlayEgressRules.CHAIN_PERBOX)
                        && a.contains(containerIp))
                .toList();
    }
}
