package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The beat loop reads its resource snapshot through this cache, so the contract under test is the
 * one the incident proved necessary: a wedged or slow probe (Postgres / Podman after a host sleep)
 * must never block the beat — the last good snapshot rides until a newer one lands.
 */
class HeartbeatResourceCacheTest {

    /** A probe that answers promptly with a distinguishable, incrementing snapshot. */
    private static final class CountingProvider implements HeartbeatResourceProvider {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public Map<String, Object> resources() {
            return Map.of("call", calls.incrementAndGet());
        }
    }

    /** A probe that wedges until released — the hung-Postgres stand-in. */
    private static final class WedgedProvider implements HeartbeatResourceProvider {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public Map<String, Object> resources() {
            entered.countDown();
            try {
                release.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return Map.of("wedged", false);
        }
    }

    @Test
    void firstReadCollectsSynchronouslyWithRealNumbers() {
        CountingProvider provider = new CountingProvider();
        HeartbeatResourceCache cache = (HeartbeatResourceCache) HeartbeatResourceCache.wrap(provider);
        try {
            Map<String, Object> snapshot = cache.resources();
            assertNotNull(snapshot, "first beat must not be empty when the probe is healthy");
            assertTrue(snapshot.containsKey("call"), "the delegate's snapshot is served, not a placeholder");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void wedgedProbeNeverBlocksTheBeatBeyondTheFirstCallBound() throws Exception {
        WedgedProvider wedged = new WedgedProvider();
        HeartbeatResourceCache cache = (HeartbeatResourceCache) HeartbeatResourceCache.wrap(wedged);
        try {
            // Cold read: pays the bounded first-collect wait once, then serves empty — not forever.
            long start = System.nanoTime();
            Map<String, Object> snapshot = cache.resources();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            assertTrue(wedged.entered.await(2, TimeUnit.SECONDS), "the probe really was hit");
            assertTrue(elapsedMs < HeartbeatResourceCache.FIRST_COLLECT_TIMEOUT_MS + 2_000L,
                    "cold read stayed within the bound (" + elapsedMs + "ms)");
            assertNotNull(snapshot, "an empty map, not a block, while the probe is wedged");

            // Steady state: subsequent reads are immediate even though the collector is still wedged.
            long start2 = System.nanoTime();
            cache.resources();
            long elapsed2Ms = (System.nanoTime() - start2) / 1_000_000L;
            assertTrue(elapsed2Ms < 1_000L, "steady-state read never waits on the wedged probe");
        } finally {
            wedged.release.countDown();
            cache.shutdown();
        }
    }

    @Test
    void lastGoodSnapshotRidesWhenALaterProbeFails() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        HeartbeatResourceProvider flaky = () -> {
            if (calls.incrementAndGet() == 1) {
                return Map.of("cpu", 42);
            }
            throw new RuntimeException("probe exploded");
        };
        HeartbeatResourceCache cache = (HeartbeatResourceCache) HeartbeatResourceCache.wrap(flaky);
        try {
            assertEquals(42, cache.resources().get("cpu"), "first collect lands");
            // Force a failing refresh directly through the read path's staleness trigger.
            long deadline = System.currentTimeMillis() + 3_000L;
            while (calls.get() < 2 && System.currentTimeMillis() < deadline) {
                cache.resources();
                Thread.sleep(50L);
            }
            assertTrue(calls.get() >= 2, "a later refresh ran and failed");
            assertEquals(42, cache.resources().get("cpu"), "the last good snapshot rides the failure");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void wrapIsNullSafeAndNeverStacks() {
        assertNull(HeartbeatResourceCache.wrap(null), "no provider stays no provider");
        HeartbeatResourceProvider once = HeartbeatResourceCache.wrap(Map::of);
        HeartbeatResourceProvider twice = HeartbeatResourceCache.wrap(once);
        assertSame(once, twice, "wrapping a cache again must not stack a second collector");
        ((HeartbeatResourceCache) once).shutdown();
    }
}
