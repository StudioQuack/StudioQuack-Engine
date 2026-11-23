package com.studioquack.sqs.sandbox.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The idle reaper sleeps until the process exits or the idle window can have run out — it reaps a
 * silent process on time, never one whose activity clock keeps moving, and returns as soon as a
 * process ends on its own.
 */
class CliRunnerIdleReaperTest {

    private static final int CAP = 64 * 1024;

    @Test
    void aSilentProcessIsReapedWhenTheIdleWindowRunsOut() {
        long silentSince = System.nanoTime();
        CliRunner.Result r = CliRunner.runWithIdleReaper(List.of("sleep", "30"), null,
                Duration.ofMillis(300), () -> silentSince, CAP, CAP);
        assertTrue(r.timedOut());
        assertTrue(r.elapsedMs() < 5_000, "reaped near the 300 ms window, took " + r.elapsedMs() + " ms");
    }

    @Test
    void aProcessWithLiveActivityIsNeverReaped() {
        CliRunner.Result r = CliRunner.runWithIdleReaper(List.of("sleep", "1"), null,
                Duration.ofMillis(200), System::nanoTime, CAP, CAP);
        assertFalse(r.timedOut(), "an open helper request reads as activity the whole time");
        assertTrue(r.ok());
    }

    @Test
    void activityThatStopsMidRunStillGetsReapedOneWindowLater() {
        AtomicLong last = new AtomicLong(System.nanoTime());
        Thread activity = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < 5; i++) {
                last.set(System.nanoTime());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        CliRunner.Result r = CliRunner.runWithIdleReaper(List.of("sleep", "30"), null,
                Duration.ofMillis(300), last::get, CAP, CAP);
        activity.interrupt();
        assertTrue(r.timedOut());
        assertTrue(r.elapsedMs() >= 500, "not reaped while activity was still moving: " + r.elapsedMs() + " ms");
    }

    @Test
    void aProcessThatEndsOnItsOwnReturnsAtOnce() {
        CliRunner.Result r = CliRunner.runWithIdleReaper(List.of("true"), null,
                Duration.ofMinutes(10), System::nanoTime, CAP, CAP);
        assertTrue(r.ok());
        assertTrue(r.elapsedMs() < 5_000, "no fixed wait after exit: " + r.elapsedMs() + " ms");
    }
}
