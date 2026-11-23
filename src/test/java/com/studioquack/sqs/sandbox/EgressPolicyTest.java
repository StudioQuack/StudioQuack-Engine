package com.studioquack.sqs.sandbox;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Pins wire parsing after deleted the misleading {@code RESTRICTED} enum constant. */
class EgressPolicyTest {

    @Test
    void fromWire_maps_legacy_restricted_to_open() {
        assertEquals(EgressPolicy.OPEN, EgressPolicy.fromWire("restricted"));
        assertEquals(EgressPolicy.OPEN, EgressPolicy.fromWire("RESTRICTED"));
    }

    @Test
    void fromWire_open_and_none() {
        assertEquals(EgressPolicy.OPEN, EgressPolicy.fromWire("open"));
        assertEquals(EgressPolicy.NONE, EgressPolicy.fromWire("none"));
        assertEquals(EgressPolicy.DEFAULT, EgressPolicy.fromWire(null));
        assertEquals(EgressPolicy.DEFAULT, EgressPolicy.fromWire("bogus"));
    }
}
