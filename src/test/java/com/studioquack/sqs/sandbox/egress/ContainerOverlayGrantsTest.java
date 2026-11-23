package com.studioquack.sqs.sandbox.egress;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Network Access Control, caller side — the per-container grant argv: one ACCEPT per granted
 * (destination /32, port), never the overlay range, pinned to the container and the interface of
 * its own network; no grant, no rule.
 */
class ContainerOverlayGrantsTest {

    private static final ContainerOverlayGrants.OverlayGrant GRANT_B_2222 =
            new ContainerOverlayGrants.OverlayGrant("100.64.1.9", 2222);
    private static final ContainerOverlayGrants.OverlayGrant GRANT_C_5432 =
            new ContainerOverlayGrants.OverlayGrant("100.64.1.10", 5432);

    @Test
    void oneAcceptPerGrantedDestinationAndPort() {
        List<List<String>> rules = ContainerOverlayGrants.rulesFor(
                "10.89.0.7", "sqnet0", List.of(GRANT_B_2222, GRANT_C_5432));

        assertEquals(2, rules.size(), "one ACCEPT per granted (destination, port) pair");
        for (List<String> rule : rules) {
            assertEquals("iptables", rule.get(0));
            assertEquals("-A", rule.get(1));
            assertEquals(OverlayEgressRules.CHAIN_PERBOX, rule.get(2));
            assertTrue(rule.contains("-s") && rule.contains("10.89.0.7"));
            assertTrue(rule.contains("-p") && rule.contains("tcp"), "overlay grants are TCP");
            assertTrue(rule.contains("-o") && rule.contains("sqnet0"),
                    "the accept must stay pinned to the container's overlay iface");
            assertEquals("ACCEPT", rule.get(rule.size() - 1));
        }
        assertTrue(rules.stream().anyMatch(r -> r.contains("100.64.1.9/32") && r.contains("2222")));
        assertTrue(rules.stream().anyMatch(r -> r.contains("100.64.1.10/32") && r.contains("5432")));
        assertFalse(rules.stream().anyMatch(r -> r.contains(OverlayEgressRules.OVERLAY_CIDR)),
                "never a whole-range ACCEPT — least privilege is per (engine, port)");
    }

    @Test
    void noGrantsEmitNoRule_failClosed() {
        assertTrue(ContainerOverlayGrants.rulesFor("10.89.0.7", "sqnet0", List.of()).isEmpty(),
                "without a Network Access Control grant the container reaches nothing on the overlay");
        assertTrue(ContainerOverlayGrants.rulesFor("10.89.0.7", "sqnet0", null).isEmpty(),
                "a null grant list is the same fail-closed answer");
    }

    @Test
    void duplicateGrantsCollapseToOneRule() {
        List<List<String>> rules = ContainerOverlayGrants.rulesFor(
                "10.89.0.7", "sqnet0", List.of(GRANT_B_2222, GRANT_B_2222));

        assertEquals(1, rules.size(), "duplicate grants must not stack duplicate iptables rules");
    }

    @Test
    void sameGrantsYieldTheSameArgvInAnyOrder() {
        assertEquals(
                ContainerOverlayGrants.rulesFor("10.89.0.7", "sqnet0", List.of(GRANT_B_2222, GRANT_C_5432)),
                ContainerOverlayGrants.rulesFor("10.89.0.7", "sqnet0", List.of(GRANT_C_5432, GRANT_B_2222)),
                "the manager diffs live rules by exact argv, so order must not depend on the projection");
    }

    @Test
    void invalidGrantIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new ContainerOverlayGrants.OverlayGrant(" ", 2222));
        assertThrows(IllegalArgumentException.class,
                () -> new ContainerOverlayGrants.OverlayGrant("100.64.0.9", 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ContainerOverlayGrants.OverlayGrant("100.64.0.9", 70000));
    }

    @Test
    void blankContainerIpIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ContainerOverlayGrants.rulesFor(" ", "sqnet0", List.of(GRANT_B_2222)));
    }
}
