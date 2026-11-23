package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.net.OverlayAddressChangedEvent;
import com.studioquack.sqs.channel.overlay.OverlayPeerListener;
import com.studioquack.sqs.channel.overlay.PeerCallAuthenticator;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OverlayPeerListener#rebind} must stop+start on the new host and must never bind the
 * wildcard. HTTP test constructor ({@code ssl == null}) is enough here.
 */
class OverlayPeerListenerRebindTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void rebind_stopsAndStarts_withoutBindingWildcard() throws Exception {
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, null, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            assertTrue(listener.isRunning());
            assertEquals("127.0.0.1", listener.bindHost());
            int firstPort = listener.boundPort();
            assertTrue(firstPort > 0);

            String otherLoopback = otherLoopbackIfBindable();
            if (otherLoopback != null) {
                listener.rebind(otherLoopback);
                assertTrue(listener.isRunning(), "rebind must leave the listener serving");
                assertEquals(otherLoopback, listener.bindHost());
                assertTrue(listener.boundPort() > 0);
            } else {
                // Same-host rebind is a no-op when already running — still a valid smoke.
                listener.rebind("127.0.0.1");
                assertTrue(listener.isRunning());
                assertEquals("127.0.0.1", listener.bindHost());
            }

            assertThrows(IllegalStateException.class, () -> listener.rebind("0.0.0.0"),
                    "rebind must refuse the wildcard before unbinding");
            assertTrue(listener.isRunning(), "a refused rebind must leave the previous bind up");
            assertFalse("0.0.0.0".equals(listener.bindHost()));
        } finally {
            listener.stop();
        }
    }

    @Test
    void addressChangedEvent_rebindsWhenOverrideBlank() throws Exception {
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, null, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.start();
        try {
            String other = otherLoopbackIfBindable();
            if (other == null) {
                listener.onOverlayAddressChanged(new OverlayAddressChangedEvent(this, "10.1.1.1", "127.0.0.1"));
                assertTrue(listener.isRunning());
                return;
            }
            listener.onOverlayAddressChanged(new OverlayAddressChangedEvent(this, "127.0.0.1", other));
            assertEquals(other, listener.bindHost());
            assertTrue(listener.isRunning());
        } finally {
            listener.stop();
        }
    }

    @Test
    void addressChangedEvent_ignoredWhenBindOverrideSet() throws Exception {
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, null, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.setBindOverride("127.0.0.1");
        listener.start();
        try {
            listener.onOverlayAddressChanged(new OverlayAddressChangedEvent(this, "127.0.0.1", "127.0.0.2"));
            assertEquals("127.0.0.1", listener.bindHost(), "pinned override must not follow the agent IP");
            assertTrue(listener.isRunning());
        } finally {
            listener.stop();
        }
    }

    @Test
    void addressChangedEvent_ignoredWhenNotRunning() {
        OverlayPeerListener listener = new OverlayPeerListener(
                "127.0.0.1", 0, null, PeerCallAuthenticator.allowAll(), MAPPER);
        listener.onOverlayAddressChanged(new OverlayAddressChangedEvent(this, "10.0.0.1", "10.0.0.2"));
        assertEquals("127.0.0.1", listener.bindHost());
        assertFalse(listener.isRunning());
    }

    /** {@code 127.0.0.2} on most hosts; {@code null} when the OS will not bind it. */
    private static String otherLoopbackIfBindable() {
        try (ServerSocket s = new ServerSocket()) {
            s.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.2"), 0));
            return "127.0.0.2";
        } catch (Exception ignored) {
            return null;
        }
    }
}
