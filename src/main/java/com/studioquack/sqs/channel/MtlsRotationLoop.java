package com.studioquack.sqs.channel;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Daily mTLS leaf rotation driver. The enrolment leaf (1-year TTL) must never expire in service: when its
 * remaining validity drops below {@link #ROTATION_THRESHOLD_SECONDS} the loop re-runs {@code enrol-mtls} and a
 * failed attempt is retried hourly forever — an engine is never lost to a certificate lifecycle.
 */
@Slf4j
public final class MtlsRotationLoop {

    /**
     * Steady-state cadence: re-check the leaf once a day.
     */
    static final long CHECK_INTERVAL_MS = 86_400_000L;
    /**
     * After a failed/unavailable rotation attempt: retry every hour, forever.
     */
    static final long RETRY_INTERVAL_MS = 3_600_000L;
    /**
     * Rotate when &lt;30 days remain — matches the BE's leaf-rotation threshold.
     */
    static final long ROTATION_THRESHOLD_SECONDS = 30L * 86_400L;
    /**
     * First check soon after boot: a leaf loaded from disk may already be near expiry.
     */
    static final long INITIAL_DELAY_MS = 60_000L;

    private final MtlsIdentity identity;
    /**
     * One {@code enrol-mtls} attempt; built fresh per call so it presents the CURRENT credential.
     */
    private final Callable<Boolean> enrolment;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    /**
     * Log-rate marker: repeated identical unavailability answers drop to DEBUG.
     */
    private volatile boolean unavailableLogged;

    public MtlsRotationLoop(MtlsIdentity identity, Callable<Boolean> enrolment) {
        this(identity, enrolment, Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sqs-mtls-rotation");
            t.setDaemon(true);
            return t;
        }));
    }

    /**
     * Test seam: caller-owned scheduler.
     */
    MtlsRotationLoop(MtlsIdentity identity, Callable<Boolean> enrolment,
                     ScheduledExecutorService scheduler) {
        this.identity = identity;
        this.enrolment = enrolment;
        this.scheduler = scheduler;
    }

    /**
     * Starts the loop (idempotent). First check runs {@link #INITIAL_DELAY_MS} after start.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        scheduler.schedule(this::tickSafely, INITIAL_DELAY_MS, TimeUnit.MILLISECONDS);
        log.info("channel/mtls-rotation: loop started (daily check, <{}d threshold, hourly retry)",
                ROTATION_THRESHOLD_SECONDS / 86_400L);
    }

    /**
     * Stops the loop (idempotent); scheduled ticks no-op once stopped.
     */
    public void stop() {
        running.set(false);
        scheduler.shutdownNow();
    }

    private void tickSafely() {
        try {
            tick();
        } catch (Exception ex) {
            log.debug("channel/mtls-rotation: tick failed ({})", ex.toString());
        }
    }

    /**
     * One check: rotate when due, then re-arm — daily after success/not-due, hourly after a failure. Never throws,
     * never gives up.
     */
    void tick() {
        if (!running.get()) {
            return;
        }
        long next = CHECK_INTERVAL_MS;
        try {
            if (identity.rotationDue(ROTATION_THRESHOLD_SECONDS)) {
                boolean enrolledBefore = identity.isEnrolled();
                if (enrolment.call()) {
                    unavailableLogged = false;
                    log.info("channel/mtls-rotation: {} (leaf expires {})",
                            enrolledBefore ? "leaf rotated" : "mTLS enrolled after BE cert service appeared",
                            identity.leafCert() == null ? "?" : identity.leafCert().getNotAfter());
                } else {
                    // 503 — BE has no cert service. Stay bearer-only; retry hourly forever.
                    if (unavailableLogged) {
                        log.debug("channel/mtls-rotation: enrol-mtls still unavailable — retry in 1h");
                    } else {
                        unavailableLogged = true;
                        log.info("channel/mtls-rotation: enrol-mtls unavailable (BE has no cert "
                                + "service) — retrying hourly until it appears");
                    }
                    next = RETRY_INTERVAL_MS;
                }
            }
        } catch (Exception ex) {
            // The current leaf keeps working until its expiry; the loop retries hourly forever.
            log.warn("channel/mtls-rotation: rotation attempt failed ({}) — retrying in 1h; "
                    + "the current leaf stays in service", ex.toString());
            next = RETRY_INTERVAL_MS;
        }
        if (running.get()) {
            scheduler.schedule(this::tickSafely, next, TimeUnit.MILLISECONDS);
        }
    }
}
