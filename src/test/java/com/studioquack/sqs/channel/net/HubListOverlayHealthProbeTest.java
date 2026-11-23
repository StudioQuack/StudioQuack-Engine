package com.studioquack.sqs.channel.net;

import com.studioquack.sqs.channel.OverlayHealthProbe;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Handshake v2 hub list: {@link HubList} walks in order; {@link OverlayHealthProbe#httpsReachable}
 * is the HTTPS gate (a closed port / blank host stays dark).
 */
class HubListOverlayHealthProbeTest {

    @Test
    void replace_dropsUnusableAndClearsInUseWhenGone() {
        HubList hubs = new HubList();
        HubAddress a = new HubAddress("g1", "203.0.113.10", 443);
        HubAddress b = new HubAddress("g2", "203.0.113.11", 443);
        hubs.replace(List.of(a, b, new HubAddress("bad", "", 0)));

        assertEquals(2, hubs.snapshot().size());
        hubs.markInUse(a);
        assertEquals("g1", hubs.inUse().gatewayId());
        assertFalse(hubs.currentGone());

        // Current hub leaves the list → walk must start over.
        hubs.replace(List.of(b));
        assertTrue(hubs.currentGone());
        assertNull(hubs.inUse());
    }

    @Test
    void currentGone_whenListNonEmptyAndNothingMarked() {
        HubList hubs = new HubList();
        assertFalse(hubs.currentGone(), "empty list: nothing to walk");
        hubs.replace(List.of(new HubAddress("g1", "203.0.113.10", 443)));
        assertTrue(hubs.currentGone(), "a list with no in-use hub is a walk");
    }

    @Test
    void httpsReachable_falseForBlankOrClosedPort() throws Exception {
        assertFalse(OverlayHealthProbe.httpsReachable("", 443, null, 200));
        assertFalse(OverlayHealthProbe.httpsReachable("127.0.0.1", 0, null, 200));
        int closed;
        try (ServerSocket s = new ServerSocket(0)) {
            closed = s.getLocalPort();
        }
        assertFalse(OverlayHealthProbe.httpsReachable("127.0.0.1", closed, null, 250),
                "a closed port must not count as a reachable hub");
    }
}
