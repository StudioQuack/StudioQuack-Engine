package com.studioquack.sqs.entitlement;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link EntitlementsHolder} unit tests: YAML-seeded boot default (base {@code 1} and unlimited
 * {@code -1}), live snapshot swap on apply, null guard, and thread-safety of concurrent apply/read
 * (the heartbeat thread writes while the retention job reads).
 */
class EntitlementsHolderTest {

    @Test
    void seedsFromYamlDefault_beforeAnyPush() {
        EntitlementsHolder holder = new EntitlementsHolder(EnvironmentEntitlements.DEFAULT_LOG_RETENTION_DAYS);

        assertNotNull(holder.current(), "holder is seeded (never null) at boot");
        assertEquals(1, holder.logRetentionDays(), "base tier seeds the 1-day YAML floor");
        assertFalse(holder.current().monitoringEnabled(), "everything-off baseline before a push");
        assertFalse(holder.current().retentionUnlimited());
        assertEquals(1, holder.defaultLogRetentionDays());
    }

    @Test
    void seedsUnlimited_whenYamlDefaultIsNegative() {
        // A negative YAML default means unlimited retention.
        EntitlementsHolder holder = new EntitlementsHolder(-1);

        assertEquals(-1, holder.logRetentionDays());
        assertTrue(holder.current().retentionUnlimited(), "<= 0 means unlimited");
    }

    @Test
    void apply_swapsLiveSnapshotAtomically() {
        EntitlementsHolder holder = new EntitlementsHolder(1);

        holder.apply(new EnvironmentEntitlements(30, true, true, true));

        assertEquals(30, holder.logRetentionDays());
        assertTrue(holder.current().monitoringEnabled());
        assertTrue(holder.current().webhookRelay());
        assertTrue(holder.current().doclingEnabled());
    }

    @Test
    void apply_ignoresNull_keepsCurrentSnapshot() {
        EntitlementsHolder holder = new EntitlementsHolder(7);

        holder.apply(null);

        assertEquals(7, holder.logRetentionDays(), "a null apply must never unseat the current snapshot");
    }

    // --- freshness guard (stale-snapshot replay) ----------------------------------------------

    @Test
    void applyIfFresh_ignoresStrictlyOlder_appliesNewerAndEqual() {
        EntitlementsHolder holder = new EntitlementsHolder(1);

        assertTrue(holder.applyIfFresh(new EnvironmentEntitlements(30, true, true, true),
                java.time.Instant.parse("2026-08-26T01:49:10Z")));
        assertEquals(30, holder.logRetentionDays());

        assertFalse(holder.applyIfFresh(new EnvironmentEntitlements(1, false, false, false),
                java.time.Instant.parse("2026-08-26T01:48:00Z")),
                "a strictly older stamp is rejected");
        assertEquals(30, holder.logRetentionDays(), "a rejected snapshot mutates nothing");
        assertTrue(holder.current().doclingEnabled());

        assertTrue(holder.applyIfFresh(new EnvironmentEntitlements(15, false, false, false),
                java.time.Instant.parse("2026-08-26T01:49:10Z")),
                "an equal stamp is not older — it applies (duplicate delivery of the same push)");
        assertEquals(15, holder.logRetentionDays());
    }

    @Test
    void applyIfFresh_nullStampFailsOpen() {
        EntitlementsHolder holder = new EntitlementsHolder(1);

        assertTrue(holder.applyIfFresh(new EnvironmentEntitlements(20, false, false, false), null),
                "an unstamped caller must never be blocked by the guard");

        assertEquals(20, holder.logRetentionDays());
    }

    // A pushed retention above the ceiling, including unlimited (-1), is clamped.

    @Test
    void baseTier_clampsAnOverlongPushedRetention() {
        EntitlementsHolder holder = new EntitlementsHolder(1, 30);

        holder.apply(new EnvironmentEntitlements(365, false, false, false, List.of()));

        assertEquals(30, holder.logRetentionDays(), "a pushed 365 days must clamp to the tier ceiling");
    }

    @Test
    void baseTier_clampsPushedUnlimitedToTheCeiling() {
        // -1 means "never reap". On a capped tier that is precisely what must not be honoured.
        EntitlementsHolder holder = new EntitlementsHolder(1, 30);

        holder.apply(new EnvironmentEntitlements(-1, false, false, false, List.of()));

        assertEquals(30, holder.logRetentionDays());
        assertFalse(holder.current().retentionUnlimited(), "a capped tier can never become unlimited");
    }

    @Test
    void baseTier_leavesAValueUnderTheCeilingAlone_andKeepsOtherEntitlements() {
        EntitlementsHolder holder = new EntitlementsHolder(1, 30);

        holder.apply(new EnvironmentEntitlements(7, true, true, true, List.of("hash")));

        assertEquals(7, holder.logRetentionDays());
        // Clamping must rewrite ONLY the retention field.
        assertTrue(holder.current().webhookRelay());
        assertEquals(1, holder.apiKeyHashes().size());
    }

    @Test
    void unlimitedRetention_hasNoCeiling() {
        // A ceiling of -1 leaves unlimited retention unchanged.
        EntitlementsHolder holder = new EntitlementsHolder(-1, -1);

        holder.apply(new EnvironmentEntitlements(-1, false, false, false, List.of()));

        assertTrue(holder.current().retentionUnlimited(), "unlimited retention stays unlimited");
    }

    @Test
    void baseTier_clampsTheSeededDefaultToo() {
        // Defence in depth: a misconfigured YAML default must not out-rank the ceiling either.
        EntitlementsHolder holder = new EntitlementsHolder(9999, 30);

        assertEquals(30, holder.logRetentionDays());
    }

    @Test
    void concurrentApplyAndRead_isThreadSafe() throws Exception {
        EntitlementsHolder holder = new EntitlementsHolder(1);
        int threads = 8;
        int iterations = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int retention = t + 1; // 1..threads, always > 0
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < iterations; i++) {
                    holder.apply(new EnvironmentEntitlements(retention, false, true, false));
                    // A read must never observe a null / torn snapshot.
                    EnvironmentEntitlements snap = holder.current();
                    assertNotNull(snap);
                    assertTrue(snap.logRetentionDays() >= 1 && snap.logRetentionDays() <= threads);
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS); // rethrows any worker assertion failure
        }
        pool.shutdownNow();

        int finalDays = holder.logRetentionDays();
        assertTrue(finalDays >= 1 && finalDays <= threads,
                "final value is one of the concurrently-applied snapshots");
    }
}
