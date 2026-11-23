package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.TamperEventReporter;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code status} carries the tamper-event backlog: while the daemon beats, events ride the
 * heartbeat body; once the cadence goes OVERLAY_SILENT the beat stops and this reply is the
 * security signal's prompt path home. Same send-or-lose drain semantics as the beat — an event is
 * never replayed after a reply shipped it.
 */
class StatusCommandHandlerTamperTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private StatusCommandHandler handler(TamperEventReporter reporter) {
        return new StatusCommandHandler(mapper, null, "base", null, null, null, null, reporter,
                null);
    }

    @Test
    void status_drains_the_tamper_backlog_into_the_reply() throws Exception {
        TamperEventReporter reporter = new TamperEventReporter();
        reporter.report("secret.binding-mismatch", "node=n7 ref=vault/db");
        StatusCommandHandler h = handler(reporter);

        DaemonCommandOutcome out = h.handle(
                new DaemonCommand("s1", "status", "2026-08-28T00:00:00Z", null));

        assertTrue(out.succeeded());
        JsonNode body = mapper.readTree(out.output());
        assertEquals("secret.binding-mismatch", body.at("/tamperEvents/0/type").asText());
        assertEquals("node=n7 ref=vault/db", body.at("/tamperEvents/0/detail").asText());
        assertEquals(0, reporter.queued(), "drained — the reply shipped them");

        JsonNode second = mapper.readTree(h.handle(
                new DaemonCommand("s2", "status", "2026-08-28T00:00:01Z", null)).output());
        assertTrue(second.path("tamperEvents").isMissingNode(), "no replay after the drain");
    }

    @Test
    void status_without_a_reporter_omits_the_block() throws Exception {
        StatusCommandHandler h = handler(null);
        JsonNode body = mapper.readTree(h.handle(
                new DaemonCommand("s3", "status", "2026-08-28T00:00:02Z", null)).output());
        assertTrue(body.path("tamperEvents").isMissingNode(),
                "no reporter wired (slice test) — the block is absent, never an empty array");
    }
}
