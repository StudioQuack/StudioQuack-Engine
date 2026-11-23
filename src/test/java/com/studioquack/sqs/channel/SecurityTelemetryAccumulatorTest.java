package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Allowlist, drain-on-read, and bound behaviour for heartbeat counters. */
class SecurityTelemetryAccumulatorTest {

    @Test
    void allowlists_keys_and_ignores_unknown() {
        SecurityTelemetryAccumulator acc = new SecurityTelemetryAccumulator();
        acc.record("ingress_tls");
        acc.record("not_a_real_class");
        acc.record(null);

        Map<String, Integer> drained = acc.drain();
        assertEquals(Map.of("ingress_tls", 1), drained);
    }

    @Test
    void drain_resets_so_second_call_is_empty() {
        SecurityTelemetryAccumulator acc = new SecurityTelemetryAccumulator();
        acc.record("ingress_rate");
        acc.record("ingress_rate");

        assertEquals(2, acc.drain().get("ingress_rate"));
        assertTrue(acc.drain().isEmpty(), "drain-on-read must reset the interval delta");
    }

    @Test
    void clamps_each_count_at_integer_max() {
        SecurityTelemetryAccumulator acc = new SecurityTelemetryAccumulator();
        acc.forceCountForTest("ingress_tls", Integer.MAX_VALUE);
        acc.record("ingress_tls");
        assertEquals(Integer.MAX_VALUE, acc.drain().get("ingress_tls"));
        assertTrue(SecurityTelemetryAccumulator.ALLOWED_CLASSES.size()
                <= SecurityTelemetryAccumulator.MAX_CLASSES);
    }

    @Test
    void replay_subclasses_share_one_reason_class_via_caller_mapping() {
        // Accumulator itself only knows the catalogue key; mapping is the guard's job.
        SecurityTelemetryAccumulator acc = new SecurityTelemetryAccumulator();
        acc.record("ingress_replay");
        acc.record("ingress_replay");
        assertEquals(2, acc.drain().get("ingress_replay"));
    }
}
