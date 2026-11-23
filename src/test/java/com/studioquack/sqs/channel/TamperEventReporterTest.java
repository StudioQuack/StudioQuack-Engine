package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TamperEventReporterTest {

    @Test
    void reported_events_drain_in_order_with_timestamps() {
        TamperEventReporter reporter = new TamperEventReporter();
        reporter.report("secret.ref.mismatch", "node=n1 refs=[sec_a:v1]");
        reporter.report("bearer.file.tampered", null);

        List<TamperEventReporter.TamperEvent> events = reporter.drain();

        assertEquals(2, events.size());
        assertEquals("secret.ref.mismatch", events.get(0).type());
        assertEquals("node=n1 refs=[sec_a:v1]", events.get(0).detail());
        assertNotNull(events.get(0).observedAt());
        assertEquals(0, reporter.queued(), "drain empties the queue");
    }

    @Test
    void blank_types_are_dropped() {
        TamperEventReporter reporter = new TamperEventReporter();
        reporter.report(" ", "detail");
        reporter.report(null, "detail");
        assertEquals(0, reporter.queued());
    }

    @Test
    void the_queue_is_bounded_drop_oldest() {
        TamperEventReporter reporter = new TamperEventReporter();
        for (int i = 0; i < TamperEventReporter.MAX_QUEUED + 10; i++) {
            reporter.report("secret.ref.mismatch", "event-" + i);
        }
        List<TamperEventReporter.TamperEvent> events = reporter.drain();
        assertEquals(TamperEventReporter.MAX_QUEUED, events.size());
        assertEquals("event-10", events.get(0).detail(), "the oldest events were dropped first");
    }

    @Test
    void oversized_details_are_truncated() {
        TamperEventReporter reporter = new TamperEventReporter();
        reporter.report("t", "x".repeat(TamperEventReporter.MAX_DETAIL_LENGTH * 3));
        String detail = reporter.drain().get(0).detail();
        assertTrue(detail.length() <= TamperEventReporter.MAX_DETAIL_LENGTH);
    }
}
