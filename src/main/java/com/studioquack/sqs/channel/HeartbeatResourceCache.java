package com.studioquack.sqs.channel;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Non-blocking {@link HeartbeatResourceProvider} facade: the beat loop reads the last successfully collected
 * snapshot from a volatile, while ONE collector thread does the real probing in the background. The beat must
 * never wait on the snapshot: probes can hit a wedged runtime (a Podman machine that did not survive a host
 * sleep).
 */
@Slf4j
public final class HeartbeatResourceCache implements HeartbeatResourceProvider {

    /**
     * Bound on the one synchronous collect a cold reader pays.
     */
    static final long FIRST_COLLECT_TIMEOUT_MS = 5_000L;
    /**
     * Background refresh cadence that keeps the snapshot warm while the daemon idles.
     */
    static final long PERIOD_MS = 15_000L;
    /**
     * Minimum age before a read triggers an async refresh (coalesced — never one probe per beat).
     */
    static final long MIN_REFRESH_INTERVAL_MS = 1_500L;

    private final HeartbeatResourceProvider delegate;
    private final ScheduledExecutorService collector;
    private final AtomicBoolean collectRunning = new AtomicBoolean(false);
    /**
     * The bounded synchronous collect happens on the FIRST read only — never twice.
     */
    private final AtomicBoolean firstCollectDone = new AtomicBoolean(false);
    private volatile Map<String, Object> lastGood;
    /**
     * Start time of the most recent collect attempt (drives the staleness check).
     */
    private volatile long lastCollectStartedMs;

    private HeartbeatResourceCache(HeartbeatResourceProvider delegate) {
        this.delegate = delegate;
        this.collector = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sqs-resource-collector");
            t.setDaemon(true);
            t.setContextClassLoader(HeartbeatResourceCache.class.getClassLoader());
            return t;
        });
        this.collector.scheduleWithFixedDelay(this::requestRefresh, 0L, PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Wraps {@code delegate} in the cache; {@code null} stays {@code null} (channel-disabled callers) and an
     * existing cache is returned as-is so double-wrapping never stacks threads.
     */
    public static HeartbeatResourceProvider wrap(HeartbeatResourceProvider delegate) {
        if (delegate == null || delegate instanceof HeartbeatResourceCache) {
            return delegate;
        }
        return new HeartbeatResourceCache(delegate);
    }

    /**
     * The snapshot for this beat: last good, never blocking. The first-ever call collects synchronously with a
     * hard bound so the initial beats are not empty; afterwards a missing or stale snapshot only schedules a
     * background refresh — a wedged probe is never paid for twice.
     */
    @Override
    public Map<String, Object> resources() {
        Map<String, Object> snapshot = lastGood;
        if (snapshot == null && firstCollectDone.compareAndSet(false, true)) {
            snapshot = collectFirstBounded();
        }
        if (System.currentTimeMillis() - lastCollectStartedMs >= MIN_REFRESH_INTERVAL_MS) {
            requestRefresh();
        }
        return snapshot == null ? Map.of() : snapshot;
    }

    /**
     * Idempotent shutdown of the collector (owning client's stop path).
     */
    public void shutdown() {
        collector.shutdownNow();
    }

    /**
     * The cold-start collect: runs on the collector thread but awaited with a hard bound, so a wedged probe costs
     * the first reader {@link #FIRST_COLLECT_TIMEOUT_MS} once — never the beat loop on every beat.
     */
    private Map<String, Object> collectFirstBounded() {
        Future<Map<String, Object>> first = collector.submit(this::collectOnce);
        try {
            return first.get(FIRST_COLLECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            log.warn("channel/resources: first collect exceeded {}ms — beats ship an empty snapshot "
                    + "until the probe answers", FIRST_COLLECT_TIMEOUT_MS);
            return null;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception ex) {
            log.debug("channel/resources: first collect failed: {}", ex.toString());
            return null;
        }
    }

    /**
     * Coalesced async refresh: at most one collect in flight; a wedged one simply stays in flight.
     */
    private void requestRefresh() {
        if (!collectRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            collector.execute(() -> {
                try {
                    collectOnce();
                } finally {
                    collectRunning.set(false);
                }
            });
        } catch (RuntimeException rejected) {
            collectRunning.set(false);
        }
    }

    /**
     * One probe pass. A failure keeps the previous snapshot; only a real result replaces it.
     */
    private Map<String, Object> collectOnce() {
        lastCollectStartedMs = System.currentTimeMillis();
        try {
            Map<String, Object> fresh = delegate.resources();
            if (fresh != null && !fresh.isEmpty()) {
                lastGood = fresh;
            }
        } catch (RuntimeException ex) {
            log.debug("channel/resources: collect failed, keeping last good snapshot: {}", ex.toString());
        }
        return lastGood;
    }
}
