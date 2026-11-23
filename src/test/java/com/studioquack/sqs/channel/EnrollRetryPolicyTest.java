package com.studioquack.sqs.channel;

import com.studioquack.sqs.channel.config.HttpsEndpointPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * First-boot enroll retry policy. The loop itself lives on the existing {@code
 * sqs-channel-bootstrap} daemon thread; these tests pin the decisions (retry vs terminal, delay +
 * jitter, budget) so a cloud VM whose first enroll fails still retries, while / a non-HTTPS base
 * URL never do.
 */
class EnrollRetryPolicyTest {

    @Test
    void envNotFound_isTerminal_andNeverRetryable() {
        EnrollmentClient.EnrollmentException envGone =
                new EnrollmentClient.EnrollmentException("enroll: status 404 Error_EnvNotFound");
        assertTrue(SqsChannelBootstrap.isEnvDeletedOnBackend(envGone));
        assertFalse(SqsChannelBootstrap.isRetryableBootstrapFailure(envGone));

        EnrollmentClient.EnrollmentException status404 =
                new EnrollmentClient.EnrollmentException("enroll: status 404 body=");
        assertTrue(SqsChannelBootstrap.isEnvDeletedOnBackend(status404));
        assertFalse(SqsChannelBootstrap.isRetryableBootstrapFailure(status404));
    }

    @Test
    void httpsRefusal_isTerminal_andNeverRetryable() {
        IllegalStateException refusal = new IllegalStateException(
                "channel refused: " + HttpsEndpointPolicy.REFUSAL);
        assertTrue(SqsChannelBootstrap.isHttpsRefusal(refusal));
        assertFalse(SqsChannelBootstrap.isRetryableBootstrapFailure(refusal));
        assertFalse(SqsChannelBootstrap.isEnvDeletedOnBackend(refusal));
    }

    @Test
    void connectAndTimeoutFailures_areRetryable() {
        assertTrue(SqsChannelBootstrap.isRetryableBootstrapFailure(
                new ConnectException("Connection refused")));
        assertTrue(SqsChannelBootstrap.isRetryableBootstrapFailure(
                new IOException("timed out")));
        assertTrue(SqsChannelBootstrap.isRetryableBootstrapFailure(
                new EnrollmentClient.EnrollmentException("enroll: status 503 body=")));
        assertTrue(SqsChannelBootstrap.isRetryableBootstrapFailure(
                new EnrollmentClient.EnrollmentException("enroll: status 401 body=")));
    }

    @Test
    void delay_is60sPlusJitterInRange() {
        assertEquals(60_000L, SqsChannelBootstrap.enrollRetryDelayMs(0, bound -> 0));
        assertEquals(61_234L, SqsChannelBootstrap.enrollRetryDelayMs(5_000, bound -> {
            assertEquals(5_000, bound);
            return 1_234;
        }));
        long sampled = SqsChannelBootstrap.enrollRetryDelayMs();
        assertTrue(sampled >= SqsChannelBootstrap.ENROLL_RETRY_INTERVAL_MS);
        assertTrue(sampled < SqsChannelBootstrap.ENROLL_RETRY_INTERVAL_MS
                + SqsChannelBootstrap.ENROLL_RETRY_JITTER_MS);
    }

    @Test
    void budget_expiresAtDeadline_andZeroCapIsImmediatelyExhausted() {
        assertTrue(SqsChannelBootstrap.enrollRetryBudgetRemaining(100L, 99L));
        assertFalse(SqsChannelBootstrap.enrollRetryBudgetRemaining(100L, 100L));
        assertFalse(SqsChannelBootstrap.enrollRetryBudgetRemaining(100L, 101L));
        // cap 0 → deadline == start → the first failure leaves the fast phase.
        long start = 50L;
        assertFalse(SqsChannelBootstrap.enrollRetryBudgetRemaining(start, start));
    }

    @Test
    void slowPhaseDelay_is5mPlusJitterInRange() {
        // After the fast-phase cap the bootstrap keeps retrying forever at this cadence —
        // an exhausted budget must never mean a gave-up daemon.
        long sampled = SqsChannelBootstrap.enrollSlowPhaseDelayMs();
        assertTrue(sampled >= SqsChannelBootstrap.ENROLL_SLOW_PHASE_INTERVAL_MS);
        assertTrue(sampled < SqsChannelBootstrap.ENROLL_SLOW_PHASE_INTERVAL_MS + 30_000L);
    }
}
