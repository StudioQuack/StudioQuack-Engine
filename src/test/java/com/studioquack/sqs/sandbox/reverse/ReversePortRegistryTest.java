package com.studioquack.sqs.sandbox.reverse;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Port space is per engine: the second claim on a port must fail loudly and name the holder, and
 * the daemon's own overlay listeners are never claimable.
 */
class ReversePortRegistryTest {

    private final ReversePortRegistry registry = new ReversePortRegistry();

    @Test
    void secondClaimByAnotherNodeFailsLoudly_namingTheHolder() {
        registry.claim(2222, "instance-db-one");

        ReversePortRegistry.ReversePortException ex = assertThrows(
                ReversePortRegistry.ReversePortException.class,
                () -> registry.claim(2222, "instance-db-two"));

        assertTrue(ex.getMessage().contains("instance-db-one"),
                "the conflict message must name the node that holds the port: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("2222"), ex.getMessage());
    }

    @Test
    void sameClaimantRepublishIsIdempotent() {
        registry.claim(2222, "instance-db-one");
        assertDoesNotThrow(() -> registry.claim(2222, "instance-db-one"));
        assertEquals("instance-db-one", registry.claimantOf(2222).orElseThrow());
    }

    @Test
    void daemonReservedPortsAreNeverClaimable() {
        for (int reserved : ReversePortRegistry.RESERVED_PORTS) {
            ReversePortRegistry.ReversePortException ex = assertThrows(
                    ReversePortRegistry.ReversePortException.class,
                    () -> registry.claim(reserved, "instance-x"));
            assertTrue(ex.getMessage().contains("reserved"), ex.getMessage());
        }
    }

    @Test
    void outOfRangePortsAreRefused() {
        assertThrows(ReversePortRegistry.ReversePortException.class, () -> registry.claim(0, "a"));
        assertThrows(ReversePortRegistry.ReversePortException.class, () -> registry.claim(65536, "a"));
    }

    @Test
    void releaseFreesThePortForAnotherClaimant() {
        registry.claim(2222, "instance-db-one");
        registry.release(2222, "instance-db-one");

        assertTrue(registry.claimantOf(2222).isEmpty());
        assertDoesNotThrow(() -> registry.claim(2222, "instance-db-two"));
    }

    @Test
    void releaseByNonHolderKeepsTheClaim() {
        registry.claim(2222, "instance-db-one");
        registry.release(2222, "instance-db-two");

        assertEquals("instance-db-one", registry.claimantOf(2222).orElseThrow());
    }
}
