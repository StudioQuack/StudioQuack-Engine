package com.studioquack.sqs.sandbox.helperchan;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Revoke listeners are how a parked call learns its caller is gone without polling: each runs
 * exactly once, at once when registered after the revoke, and never once removed.
 */
class HelperJobRevokeListenerTest {

    private static HelperJob job() {
        return HelperJob.builder("job-revoke", "python").executionContext(Map.of()).build();
    }

    @Test
    void aListenerRunsOnceWhenTheJobIsRevoked() {
        HelperJob job = job();
        AtomicInteger runs = new AtomicInteger();
        job.onRevoke(runs::incrementAndGet);

        job.markRevoked();
        job.markRevoked();
        assertEquals(1, runs.get());
    }

    @Test
    void aListenerAddedAfterTheRevokeRunsAtOnce() {
        HelperJob job = job();
        job.markRevoked();
        AtomicInteger runs = new AtomicInteger();
        job.onRevoke(runs::incrementAndGet);
        assertEquals(1, runs.get());
    }

    @Test
    void aRemovedListenerNeverRuns() {
        HelperJob job = job();
        AtomicInteger runs = new AtomicInteger();
        Runnable listener = runs::incrementAndGet;
        job.onRevoke(listener);
        job.removeRevokeListener(listener);
        job.markRevoked();
        assertEquals(0, runs.get());
    }

    @Test
    void aFailingListenerDoesNotStopTheOthers() {
        HelperJob job = job();
        AtomicInteger runs = new AtomicInteger();
        job.onRevoke(() -> {
            throw new IllegalStateException("boom");
        });
        job.onRevoke(runs::incrementAndGet);
        job.markRevoked();
        assertEquals(1, runs.get());
    }
}
