package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.overlay.OverlayPeerListener;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Bind safety for the daemon→daemon peer listener. The whole security argument of the overlay
 * endpoint is "reachable implies came through the WireGuard gateway", and that sentence is only
 * true while the socket binds the overlay IP.
 */
class OverlayPeerListenerBindSafetyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void productionBindHostResolution_neverReturnsWildcard() {
        // The production path: blank override → the overlay IP from the handshake assignment's CIDR.
        String resolved = SqsChannelBootstrap.resolveOverlayBindHost("", "100.64.0.5/32");
        assertEquals("100.64.0.5", resolved);
        assertFalse(isWildcard(resolved), "the overlay CIDR host part must never be a wildcard");

        String resolvedNullOverride = SqsChannelBootstrap.resolveOverlayBindHost(null, "100.64.0.7/32");
        assertEquals("100.64.0.7", resolvedNullOverride);
        assertFalse(isWildcard(resolvedNullOverride));

        // A CIDR-less host part (already bare) stays bare.
        assertEquals("100.64.0.9", SqsChannelBootstrap.resolveOverlayBindHost("", "100.64.0.9"));
    }

    @Test
    void listenerRefusesToStart_withoutAnOverlayAddress() {
        OverlayPeerListener blank = new OverlayPeerListener("", 0, null, null, MAPPER);
        assertThrows(IllegalStateException.class, blank::start,
                "blank bind host → refuse (fail closed, never a wildcard fallback)");
        assertFalse(blank.isRunning(), "a refused start leaves the listener down");

        OverlayPeerListener nullHost = new OverlayPeerListener(null, 0, null, null, MAPPER);
        assertThrows(IllegalStateException.class, nullHost::start);
        assertFalse(nullHost.isRunning());
    }

    @Test
    void listenerRefusesToStart_onWildcardAddress() {
        OverlayPeerListener v4Any = new OverlayPeerListener("0.0.0.0", 0, null, null, MAPPER);
        assertThrows(IllegalStateException.class, v4Any::start,
                "0.0.0.0 would expose the peer endpoint to the client LAN");
        assertFalse(v4Any.isRunning());

        OverlayPeerListener v6Any = new OverlayPeerListener("::", 0, null, null, MAPPER);
        assertThrows(IllegalStateException.class, v6Any::start);
        assertFalse(v6Any.isRunning());
    }

    @Test
    void bootstrapGate_peerListener_mirrorsReverseListener_minusMtls() {
        // Happy path: enabled + wg up + real backend + stats up → starts (null). No mTLS input at
        // all: two daemons share no trust anchor, so enrolment is irrelevant to this transport.
        assertNull(SqsChannelBootstrap.overlayPeerSkipReason(true, true, "userspace", true));

        assertEquals("disabled",
                SqsChannelBootstrap.overlayPeerSkipReason(false, true, "kernel", true));
        assertEquals("no-data-plane",
                SqsChannelBootstrap.overlayPeerSkipReason(true, true, null, true));
        assertEquals("no-data-plane",
                SqsChannelBootstrap.overlayPeerSkipReason(true, true, "  ", true));
        assertEquals("wg-not-up",
                SqsChannelBootstrap.overlayPeerSkipReason(true, false, "kernel", false));
        assertEquals("wg-down",
                SqsChannelBootstrap.overlayPeerSkipReason(true, true, "kernel", false));
    }

    private static boolean isWildcard(String host) {
        return host == null || host.isBlank() || "0.0.0.0".equals(host) || "::".equals(host);
    }
}
