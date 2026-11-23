package com.studioquack.sqs.sandbox.helperchan;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stateful launcher's idle reaper reads {@link HelperJob#lastActivityNanos()}: a helper request
 * that is still open (an LLM generation, a parked execute) must read as live activity however long
 * it runs, and a closed one must not.
 */
class HelperJobActivityTest {

    @Test
    void anOpenRequestCountsAsActivityForAsLongAsItStaysOpen() throws Exception {
        HelperJob job = HelperJob.builder("job-activity", "python").executionContext(Map.of()).build();
        job.beginRequest();
        long opened = System.nanoTime();
        Thread.sleep(60);
        assertTrue(job.lastActivityNanos() - opened >= 50_000_000L, "an open request reads as live activity");

        job.endRequest();
        long closedAt = job.lastActivityNanos();
        Thread.sleep(60);
        assertEquals(closedAt, job.lastActivityNanos(), "once closed, the clock stays at the last frame");
    }
}
