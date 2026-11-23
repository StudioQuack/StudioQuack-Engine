package com.studioquack.sqs.channel.net;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OverlayReverseLookupProvider}: JDK HttpsServer reverse-resolves every client before its TLS
 * reply; for private-network callers that must not reach the OS (NetBIOS on Windows: ~4.6 s).
 */
class OverlayReverseLookupProviderTest {

    @Test
    void overlayRanges() {
        assertTrue(OverlayReverseLookupProvider.isOverlay(new byte[]{100, 64, 0, 1}));
        assertTrue(OverlayReverseLookupProvider.isOverlay(new byte[]{100, 88, 119, 2}), "the gateway");
        assertTrue(OverlayReverseLookupProvider.isOverlay(new byte[]{100, 127, (byte) 255, (byte) 255}));
        assertFalse(OverlayReverseLookupProvider.isOverlay(new byte[]{100, (byte) 128, 0, 1}));
        assertFalse(OverlayReverseLookupProvider.isOverlay(new byte[]{100, 63, (byte) 255, (byte) 255}));
        assertFalse(OverlayReverseLookupProvider.isOverlay(new byte[]{10, 0, 0, 1}));
        byte[] overlay6 = new byte[16];
        overlay6[0] = (byte) 0xfd;
        overlay6[1] = 0x7a;
        overlay6[2] = 0x11;
        overlay6[3] = 0x5c;
        overlay6[4] = (byte) 0xa1;
        overlay6[5] = (byte) 0xe0;
        overlay6[15] = 1;
        assertTrue(OverlayReverseLookupProvider.isOverlay(overlay6));
        overlay6[5] = (byte) 0xe1;
        assertFalse(OverlayReverseLookupProvider.isOverlay(overlay6));
    }

    @Test
    void overlayReverseLookups_neverReachThePlatformResolver_othersDo() throws Exception {
        List<String> platformCalls = new ArrayList<>();
        InetAddressResolver platform = new InetAddressResolver() {
            @Override
            public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) {
                platformCalls.add("name:" + host);
                return Stream.empty();
            }

            @Override
            public String lookupByAddress(byte[] addr) {
                platformCalls.add("addr");
                return "lan-host";
            }
        };
        InetAddressResolver resolver = OverlayReverseLookupProvider.resolver(platform);

        assertThrows(UnknownHostException.class, () -> resolver.lookupByAddress(new byte[]{100, 88, 119, 2}));
        assertTrue(platformCalls.isEmpty(), "the OS resolver is never asked about an overlay address");
        assertEquals("lan-host", resolver.lookupByAddress(new byte[]{(byte) 192, (byte) 168, 1, 10}));
        resolver.lookupByName("api.studioquack.com", null);
        assertEquals(List.of("addr", "name:api.studioquack.com"), platformCalls);
    }

    /** Registered for the JDK: after install(), an overlay caller's host name is its literal address. */
    @Test
    void installedProvider_answersTheJdkReverseLookupWithTheLiteralAddress() throws Exception {
        OverlayReverseLookupProvider.install();

        assertTrue(OverlayReverseLookupProvider.active(), "META-INF/services registration must be picked up");
        assertEquals("100.88.119.2",
                InetAddress.getByAddress(new byte[]{100, 88, 119, 2}).getHostName());
    }
}
