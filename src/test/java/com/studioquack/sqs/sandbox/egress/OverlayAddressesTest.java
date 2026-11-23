package com.studioquack.sqs.sandbox.egress;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OverlayAddressesTest {

    @Test
    void cgnat_range_membership() {
        assertTrue(OverlayAddresses.isOverlayAddress("100.64.0.1"));
        assertTrue(OverlayAddresses.isOverlayAddress("100.100.7.7"));
        assertTrue(OverlayAddresses.isOverlayAddress("100.127.255.254"));
        assertFalse(OverlayAddresses.isOverlayAddress("100.63.255.255"));
        assertFalse(OverlayAddresses.isOverlayAddress("100.128.0.0"));
        assertFalse(OverlayAddresses.isOverlayAddress("10.0.0.1"));
        assertFalse(OverlayAddresses.isOverlayAddress("169.254.169.254"));
        assertFalse(OverlayAddresses.isOverlayAddress("127.0.0.1"));
        assertFalse(OverlayAddresses.isOverlayAddress("0.0.0.0"));
        assertFalse(OverlayAddresses.isOverlayAddress("100.64.0.1; rm -rf /"));
        assertFalse(OverlayAddresses.isOverlayAddress("100.064.0.1"));
        assertFalse(OverlayAddresses.isOverlayAddress(null));
    }

    @Test
    void ipv6_overlay_prefix() {
        assertTrue(OverlayAddresses.isOverlayAddress("fd7a:115c:a1e0::1"));
        assertTrue(OverlayAddresses.isOverlayAddress("FD7A:115C:A1E0:ab00::7"));
        assertFalse(OverlayAddresses.isOverlayAddress("fd7a:115c:a1e1::1"));
        assertFalse(OverlayAddresses.isOverlayAddress("::1"));
        assertFalse(OverlayAddresses.isOverlayAddress("fd7a:115c:a1e0::1%eth0"));
    }

    @Test
    void cidr_scope_must_be_inside_overlay_and_no_broader() {
        assertTrue(OverlayAddresses.isOverlayCidr("100.64.0.0/10"));
        assertTrue(OverlayAddresses.isOverlayCidr("100.64.1.7/32"));
        assertTrue(OverlayAddresses.isOverlayCidr("100.64.1.7"));
        assertFalse(OverlayAddresses.isOverlayCidr("0.0.0.0/0"));
        assertFalse(OverlayAddresses.isOverlayCidr("100.64.0.0/8"));
        assertFalse(OverlayAddresses.isOverlayCidr("10.0.0.0/8"));
        assertFalse(OverlayAddresses.isOverlayCidr("100.64.0.0/x"));
        assertTrue(OverlayAddresses.isOverlayCidr("fd7a:115c:a1e0::/48"));
        assertFalse(OverlayAddresses.isOverlayCidr("fd7a:115c:a1e0::/32"));
        assertFalse(OverlayAddresses.isOverlayCidr(""));
    }
}
