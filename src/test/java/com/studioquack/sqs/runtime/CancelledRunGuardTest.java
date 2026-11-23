package com.studioquack.sqs.runtime;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Targeted unit tests for {@link CancelledRunGuard}: bounded TTL suppression of late logs/status
 * for a just-cancelled run, lazy self-cleaning past TTL, and safe handling of blank/unknown ids.
 */
class CancelledRunGuardTest {

    @Test
    void markCancelled_thenIsCancelled_trueWithinTtl() {
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);

        guard.markCancelled("wfx-1");

        assertThat(guard.isCancelled("wfx-1")).isTrue();
    }

    @Test
    void isCancelled_falseForUnknownId() {
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);

        assertThat(guard.isCancelled("never-cancelled")).isFalse();
    }

    @Test
    void isCancelled_falseAfterTtlExpires_andSelfCleans() throws InterruptedException {
        // Minimum enforced TTL is 1s; sleep past it.
        CancelledRunGuard guard = new CancelledRunGuard(1L);

        guard.markCancelled("wfx-2");
        assertThat(guard.size()).isEqualTo(1);

        Thread.sleep(1100);

        assertThat(guard.isCancelled("wfx-2")).isFalse();
        // Lazy cleanup on read past TTL.
        assertThat(guard.size()).isZero();
    }

    @Test
    void sweep_clearsExpiredEntriesNobodyReadAgain() throws InterruptedException {
        CancelledRunGuard guard = new CancelledRunGuard(1L);
        guard.markCancelled("wfx-3");

        Thread.sleep(1100);
        guard.sweep();

        assertThat(guard.size()).isZero();
    }

    @Test
    void markCancelled_blankOrNullId_isNoop() {
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);

        guard.markCancelled("");
        guard.markCancelled(null);

        assertThat(guard.size()).isZero();
        assertThat(guard.isCancelled("")).isFalse();
        assertThat(guard.isCancelled(null)).isFalse();
    }
}
