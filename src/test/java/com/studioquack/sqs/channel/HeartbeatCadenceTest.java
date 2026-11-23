package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HeartbeatCadence} — WHEN the daemon beats. Offline by construction: no network, no Spring,
 * no clock.
 */
class HeartbeatCadenceTest {

    private static final long RESPONSIVE_MS = 2_000L;
    private static final long LONG_POLL_MS = 20_000L;
    private static final long KEEPALIVE_MS = 60_000L;
    private static final long FALLBACK_START_MS = 300_000L;
    private static final long FALLBACK_MIN_MS = 30_000L;

    private final AtomicBoolean overlayHealthy = new AtomicBoolean(false);
    private final AtomicBoolean runActive = new AtomicBoolean(false);

    private HeartbeatCadence cadence() {
        return new HeartbeatCadence(RESPONSIVE_MS, LONG_POLL_MS, KEEPALIVE_MS,
                FALLBACK_START_MS, FALLBACK_MIN_MS, overlayHealthy::get, runActive::get);
    }

    /**
     * The very first resolve counts as a transition, so enrolment is reported without waiting out a
     * cadence. Every other test settles the mode through this first call.
     */
    private HeartbeatCadence settled() {
        HeartbeatCadence c = cadence();
        assertEquals(0L, c.nextDelayMs(), "the first beat after start is the enrolment transition");
        return c;
    }

    @Test
    void unconfirmed_reachability_never_yields_keepalive_even_with_healthy_local_signals() {
        overlayHealthy.set(true);
        HeartbeatCadence c = settled();

        assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, c.mode(),
                "local reverse-listener health alone must not enter keepalive");
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs(), "unconfirmed settles at the fallback floor");
        // Next rung is 60s (coincidentally equal to keepalive) but mode must stay fallback.
        assertEquals(60_000L, c.nextDelayMs());
        assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, c.mode());
        assertEquals(0L, c.longPollWaitMs());
    }

    @Test
    void confirmed_plus_healthy_yields_keepalive() {
        overlayHealthy.set(true);
        HeartbeatCadence c = settled();
        c.noteOverlayReachable(true);

        assertEquals(0L, c.nextDelayMs(), "confirmation is a mode change — beat now");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());
        assertEquals(KEEPALIVE_MS, c.nextDelayMs(), "keepalive interval is liveness only");
        assertEquals(KEEPALIVE_MS, c.nextDelayMs(), "the keepalive does not decay");
        assertEquals(0L, c.longPollWaitMs(),
                "long-polling a keepalive cadence would hold the connection open continuously");
    }

    @Test
    void absent_overlayReachable_behaves_as_false() {
        overlayHealthy.set(true);
        HeartbeatCadence c = settled();
        c.noteOverlayReachable(true);
        assertEquals(0L, c.nextDelayMs());
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());

        // null = unconfirmed (older backends omit the field) — identical to false.
        c.noteOverlayReachable(null);
        assertEquals(0L, c.nextDelayMs(), "losing confirmation drops out of keepalive immediately");
        assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, c.mode());
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs());

        HeartbeatCadence neverConfirmed = settled();
        neverConfirmed.noteOverlayReachable(null);
        assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, neverConfirmed.mode());
        assertEquals(FALLBACK_MIN_MS, neverConfirmed.nextDelayMs());
    }

    @Test
    void entering_fallback_polls_at_floor_immediately() {
        overlayHealthy.set(false);
        HeartbeatCadence c = settled();

        assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, c.mode());
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs(),
                "first settled fallback beat is the floor (30s), not the 300s ceiling");
    }

    @Test
    void prolonged_idle_relaxes_upward_and_activity_returns_to_floor() {
        overlayHealthy.set(false);
        HeartbeatCadence c = settled();

        assertEquals(30_000L, c.nextDelayMs());
        assertEquals(60_000L, c.nextDelayMs());
        assertEquals(120_000L, c.nextDelayMs());
        assertEquals(240_000L, c.nextDelayMs());
        assertEquals(300_000L, c.nextDelayMs(), "doubled 240s clamps at the ceiling");
        assertEquals(300_000L, c.nextDelayMs(), "ceiling is a stop, never a step past it");
        assertEquals(300_000L, c.nextDelayMs());

        c.noteActivity();
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs(),
                "activity returns the ladder to the floor for the next idle stretch");
        assertEquals(60_000L, c.nextDelayMs(), "and relaxation starts over from there");
    }

    @Test
    void an_active_run_keeps_the_responsive_interval() {
        overlayHealthy.set(true);
        runActive.set(true);
        HeartbeatCadence c = settled();
        c.noteOverlayReachable(true);

        // Run wins over confirmed+healthy overlay.
        assertEquals(HeartbeatCadence.Mode.RESPONSIVE, c.mode());
        assertEquals(RESPONSIVE_MS, c.nextDelayMs(), "status/result feeding must not be starved");
        assertEquals(LONG_POLL_MS, c.longPollWaitMs(),
                "a run in flight long-polls so a cancel is picked up sub-second");
    }

    @Test
    void a_state_transition_reports_send_now() {
        overlayHealthy.set(true);
        HeartbeatCadence c = settled();
        c.noteOverlayReachable(true);
        assertEquals(0L, c.nextDelayMs());
        assertEquals(KEEPALIVE_MS, c.nextDelayMs());

        c.sendNow("run-terminal");
        assertEquals(0L, c.nextDelayMs(), "a reported transition beats immediately, whatever the mode");
        assertEquals(KEEPALIVE_MS, c.nextDelayMs(), "and the cadence resumes, not resets");

        // Mode-change beat-now: overlay down.
        overlayHealthy.set(false);
        assertEquals(0L, c.nextDelayMs(), "overlay down is a transition — reported immediately");
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs());

        // Mode-change beat-now: run starts.
        runActive.set(true);
        assertEquals(0L, c.nextDelayMs(), "run start is a transition — reported immediately");
        assertEquals(RESPONSIVE_MS, c.nextDelayMs());
    }

    @Test
    void a_wakeup_that_already_beat_does_not_schedule_a_second_one() {
        overlayHealthy.set(true);
        HeartbeatCadence c = settled();
        c.noteOverlayReachable(true);
        assertEquals(0L, c.nextDelayMs());
        assertEquals(KEEPALIVE_MS, c.nextDelayMs());

        // What beatNow() does: flag the request, then interrupt the sleep so the loop beats at once.
        c.sendNow("overlay-recovered");
        c.beginBeat();
        assertEquals(KEEPALIVE_MS, c.nextDelayMs(),
                "the beat the request asked for already happened — no redundant follow-up");
    }

    @Test
    void a_failed_beat_backs_off_from_the_responsive_period_and_never_uses_the_keepalive() {
        overlayHealthy.set(true);
        HeartbeatCadence c = settled();
        c.noteOverlayReachable(true);
        assertEquals(0L, c.nextDelayMs());
        assertEquals(KEEPALIVE_MS, c.nextDelayMs());

        // A beat that failed is evidence about the transport the keepalive claim rests on, so the
        // retry must be prompt enough for BackendBaseResolver to accumulate its consecutive failures.
        assertEquals(2_000L, c.nextDelayAfterFailedBeatMs());
        assertEquals(4_000L, c.nextDelayAfterFailedBeatMs());
        assertEquals(8_000L, c.nextDelayAfterFailedBeatMs());
        assertEquals(16_000L, c.nextDelayAfterFailedBeatMs());
        assertEquals(FALLBACK_MIN_MS, c.nextDelayAfterFailedBeatMs(), "capped at the fallback floor");
        assertEquals(FALLBACK_MIN_MS, c.nextDelayAfterFailedBeatMs());

        // A completed round-trip is what clears the ladder.
        assertEquals(KEEPALIVE_MS, c.nextDelayMs());
        assertEquals(2_000L, c.nextDelayAfterFailedBeatMs(), "success reset the retry backoff");
    }

    @Test
    void a_run_starting_resets_a_relaxed_fallback_interval() {
        overlayHealthy.set(false);
        HeartbeatCadence c = settled();
        // Walk the ladder up toward the ceiling.
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs());
        assertEquals(60_000L, c.nextDelayMs());
        assertEquals(120_000L, c.nextDelayMs());

        runActive.set(true);
        assertEquals(0L, c.nextDelayMs(), "the mode changed, so the beat goes out now");
        assertEquals(RESPONSIVE_MS, c.nextDelayMs());

        // Back to no run and still no reverse channel: the ladder restarts at the floor.
        runActive.set(false);
        assertEquals(0L, c.nextDelayMs(), "run terminal is a transition — reported immediately");
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs(), "decay state reset to the floor with the mode change");
    }

    @Test
    void a_recovered_overlay_does_not_inherit_the_stale_fallback_interval() {
        overlayHealthy.set(false);
        HeartbeatCadence c = settled();
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs());
        assertEquals(60_000L, c.nextDelayMs());

        overlayHealthy.set(true);
        c.noteOverlayReachable(true);
        assertEquals(0L, c.nextDelayMs(), "overlay up + confirmed is a transition — the BE hears it at once");
        assertEquals(KEEPALIVE_MS, c.nextDelayMs());

        overlayHealthy.set(false);
        assertEquals(0L, c.nextDelayMs(), "overlay down is a transition too");
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs(), "the ladder restarts from the floor");
    }

    @Test
    void a_run_ending_inside_the_grace_window_stays_responsive() {
        overlayHealthy.set(true);
        HeartbeatCadence c = new HeartbeatCadence(RESPONSIVE_MS, LONG_POLL_MS, KEEPALIVE_MS,
                FALLBACK_START_MS, FALLBACK_MIN_MS, 60_000L, overlayHealthy::get, runActive::get);
        assertEquals(0L, c.nextDelayMs(), "the first beat after start is the enrolment transition");
        c.noteOverlayReachable(true);
        runActive.set(true);
        assertEquals(0L, c.nextDelayMs(), "run start is a transition — reported immediately");
        assertEquals(HeartbeatCadence.Mode.RESPONSIVE, c.mode());

        runActive.set(false);
        assertEquals(RESPONSIVE_MS, c.nextDelayMs(),
                "run end inside the grace is NOT a demotion — the next chat message must not wait "
                        + "out a 60s keepalive when the overlay push is missed");
        assertEquals(HeartbeatCadence.Mode.RESPONSIVE, c.mode());
        assertEquals(RESPONSIVE_MS, c.nextDelayMs(), "the grace holds for the whole window");
        assertEquals(HeartbeatCadence.Mode.RESPONSIVE, c.mode());
    }

    @Test
    void the_grace_expiry_demotes_to_keepalive() throws InterruptedException {
        overlayHealthy.set(true);
        HeartbeatCadence c = new HeartbeatCadence(RESPONSIVE_MS, LONG_POLL_MS, KEEPALIVE_MS,
                FALLBACK_START_MS, FALLBACK_MIN_MS, 60L, overlayHealthy::get, runActive::get);
        c.nextDelayMs();
        c.noteOverlayReachable(true);
        runActive.set(true);
        c.nextDelayMs();
        runActive.set(false);
        assertEquals(RESPONSIVE_MS, c.nextDelayMs(), "inside the grace: still responsive");

        Thread.sleep(90L);
        assertEquals(0L, c.nextDelayMs(),
                "grace expiry is a transition to keepalive — announced immediately, not a beat late");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());
        assertEquals(KEEPALIVE_MS, c.nextDelayMs(), "a truly idle engine keeps the ~30x poll saving");
    }

    @Test
    void a_zero_grace_demotes_the_moment_the_run_ends() {
        overlayHealthy.set(true);
        HeartbeatCadence c = settled();
        c.noteOverlayReachable(true);
        runActive.set(true);
        assertEquals(0L, c.nextDelayMs());
        assertEquals(HeartbeatCadence.Mode.RESPONSIVE, c.mode());

        runActive.set(false);
        assertEquals(0L, c.nextDelayMs(), "grace disabled: run end demotes immediately (legacy)");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());
    }

    @Test
    void collected_work_reopens_the_grace_window() {
        overlayHealthy.set(true);
        HeartbeatCadence c = new HeartbeatCadence(RESPONSIVE_MS, LONG_POLL_MS, KEEPALIVE_MS,
                FALLBACK_START_MS, FALLBACK_MIN_MS, 60_000L, overlayHealthy::get, runActive::get);
        c.nextDelayMs();
        c.noteOverlayReachable(true);
        assertEquals(0L, c.nextDelayMs());
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode(),
                "no activity yet — a fresh daemon with a confirmed overlay keepalives");

        c.noteActivity();
        assertEquals(0L, c.nextDelayMs(), "activity is a transition out of keepalive — beat now");
        assertEquals(HeartbeatCadence.Mode.RESPONSIVE, c.mode());
        assertEquals(RESPONSIVE_MS, c.nextDelayMs(), "and the grace window holds it responsive");
    }

    @Test
    void the_grace_never_masks_the_fallback_ladder() {
        // No confirmed overlay: the fallback floor is the pickup latency and must not be
        // pre-empted by session activity — its 30s floor is the considered trade-off there.
        overlayHealthy.set(false);
        HeartbeatCadence c = new HeartbeatCadence(RESPONSIVE_MS, LONG_POLL_MS, KEEPALIVE_MS,
                FALLBACK_START_MS, FALLBACK_MIN_MS, 60_000L, overlayHealthy::get, runActive::get);
        c.nextDelayMs();
        c.noteActivity();
        assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, c.mode());
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs(), "activity resets the ladder to the floor, not to responsive");
    }

    @Test
    void the_always_responsive_cadence_preserves_the_historical_fixed_timing() {
        HeartbeatCadence c = HeartbeatCadence.alwaysResponsive(RESPONSIVE_MS, LONG_POLL_MS);
        assertEquals(LONG_POLL_MS, c.longPollWaitMs(), "long-poll before the first resolve");
        assertEquals(0L, c.nextDelayMs(), "first beat is still a transition");
        assertEquals(HeartbeatCadence.Mode.RESPONSIVE, c.mode());
        assertEquals(RESPONSIVE_MS, c.nextDelayMs());
        assertEquals(LONG_POLL_MS, c.longPollWaitMs());
    }

    @Test
    void a_floor_above_the_start_collapses_instead_of_climbing() {
        // Misconfiguration: min > start would otherwise invert the ladder.
        HeartbeatCadence c = new HeartbeatCadence(RESPONSIVE_MS, LONG_POLL_MS, KEEPALIVE_MS,
                10_000L, 60_000L, () -> false, () -> false);
        assertEquals(0L, c.nextDelayMs());
        long first = c.nextDelayMs();
        assertEquals(60_000L, first, "start is raised to the floor");
        assertTrue(c.nextDelayMs() <= first, "the ladder can never climb past the collapsed ceiling");
    }

    // ---- OVERLAY_SILENT -----------------------------------------------------

    /** A silence-capable cadence (the production wiring); grace disabled to keep modes legible. */
    private HeartbeatCadence silentCadence(long freshnessMs, long checkMs) {
        return new HeartbeatCadence(RESPONSIVE_MS, LONG_POLL_MS, KEEPALIVE_MS,
                FALLBACK_START_MS, FALLBACK_MIN_MS, 0L, freshnessMs, checkMs,
                overlayHealthy::get, runActive::get);
    }

    /** Settles into WG_KEEPALIVE: overlay healthy, backend-confirmed, enrolment announced. */
    private HeartbeatCadence keepaliveSettled(HeartbeatCadence c) {
        overlayHealthy.set(true);
        assertEquals(0L, c.nextDelayMs(), "the first beat after start is the enrolment transition");
        c.noteOverlayReachable(true);
        assertEquals(0L, c.nextDelayMs(), "confirmation is a mode change — beat now");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());
        return c;
    }

    @Test
    void silence_requires_the_backend_liveness_capability_and_fresh_inbound_traffic() {
        HeartbeatCadence c = keepaliveSettled(silentCadence(120_000L, 5_000L));

        c.noteInboundOverlayTraffic();
        assertEquals(KEEPALIVE_MS, c.nextDelayMs(),
                "inbound traffic alone is not enough — a backend that never advertised "
                        + "overlay-liveness still liveness-checks the beat");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());

        c.noteOverlayLiveness(true);
        assertEquals(5_000L, c.nextDelayMs(),
                "capability + fresh inbound + healthy overlay: going quiet schedules the "
                        + "stale-check clock, NOT an announcement beat");
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());
        assertEquals(0L, c.longPollWaitMs(), "a silent daemon holds no long-poll open");
    }

    @Test
    void silence_never_enters_without_observed_inbound_traffic() {
        HeartbeatCadence c = keepaliveSettled(silentCadence(120_000L, 5_000L));

        c.noteOverlayLiveness(true);
        assertEquals(KEEPALIVE_MS, c.nextDelayMs(),
                "the capability alone proves nothing about THIS daemon's inbound path — "
                        + "only an observed inbound command does");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());
    }

    @Test
    void fresh_inbound_traffic_holds_the_silence_on_the_check_clock() {
        HeartbeatCadence c = keepaliveSettled(silentCadence(120_000L, 5_000L));
        c.noteOverlayLiveness(true);
        c.noteInboundOverlayTraffic();
        c.nextDelayMs();
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());

        assertEquals(5_000L, c.nextSilentCheckMs(), "still silent: re-resolve, no beat");
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());
        c.noteInboundOverlayTraffic();
        assertEquals(5_000L, c.nextSilentCheckMs(), "each observed poll re-proofs the path");
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());
    }

    @Test
    void stale_inbound_breaks_the_silence_back_to_keepalive() throws InterruptedException {
        HeartbeatCadence c = keepaliveSettled(silentCadence(60L, 5_000L));
        c.noteOverlayLiveness(true);
        c.noteInboundOverlayTraffic();
        c.nextDelayMs();
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());

        Thread.sleep(90L);
        assertEquals(0L, c.nextSilentCheckMs(),
                "the backend's polls stopped arriving — the daemon re-establishes contact itself");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode(),
                "overlay still healthy + confirmed: keepalive, not the fallback ladder");
    }

    @Test
    void a_local_overlay_drop_breaks_the_silence_to_the_fallback_floor() {
        HeartbeatCadence c = keepaliveSettled(silentCadence(120_000L, 5_000L));
        c.noteOverlayLiveness(true);
        c.noteInboundOverlayTraffic();
        c.nextDelayMs();
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());

        overlayHealthy.set(false);
        assertEquals(0L, c.nextSilentCheckMs(), "tunnel down is detected locally — beat at once");
        assertEquals(HeartbeatCadence.Mode.FALLBACK_DECAY, c.mode());
        assertEquals(FALLBACK_MIN_MS, c.nextDelayMs(),
                "queued-work pickup resumes at the floor, not at a relaxed rung");
    }

    @Test
    void a_send_now_breaks_the_silence_for_one_beat_then_resumes_it() {
        HeartbeatCadence c = keepaliveSettled(silentCadence(120_000L, 5_000L));
        c.noteOverlayLiveness(true);
        c.noteInboundOverlayTraffic();
        c.nextDelayMs();
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());

        c.sendNow("run-terminal");
        assertEquals(0L, c.nextSilentCheckMs(),
                "a state the backend must have at once breaks the silence immediately");
        // The beat went out; the loop asks for the next delay and the still-confirmed overlay
        // takes the daemon straight back to silence.
        c.beginBeat();
        assertEquals(5_000L, c.nextDelayMs(), "the announcement beat done, silence resumes");
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());
    }

    @Test
    void a_run_with_a_confirmed_overlay_stays_silent() {
        HeartbeatCadence c = keepaliveSettled(silentCadence(120_000L, 5_000L));
        c.noteOverlayLiveness(true);
        c.noteInboundOverlayTraffic();
        c.nextDelayMs();
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());

        runActive.set(true);
        assertEquals(5_000L, c.nextSilentCheckMs(),
                "a run changes nothing: status/logs ride their direct posts, a cancel pushes "
                        + "down the reverse channel — the beat would add nothing");
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());
    }

    @Test
    void losing_the_liveness_capability_breaks_the_silence() {
        HeartbeatCadence c = keepaliveSettled(silentCadence(120_000L, 5_000L));
        c.noteOverlayLiveness(true);
        c.noteInboundOverlayTraffic();
        c.nextDelayMs();
        assertEquals(HeartbeatCadence.Mode.OVERLAY_SILENT, c.mode());

        c.noteOverlayLiveness(false);
        assertEquals(0L, c.nextSilentCheckMs(),
                "a backend that no longer advertises overlay-liveness must see beats again");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());
    }

    @Test
    void constructors_that_predate_the_mode_never_go_silent() {
        HeartbeatCadence c = keepaliveSettled(cadence());

        c.noteOverlayLiveness(true);
        c.noteInboundOverlayTraffic();
        assertEquals(KEEPALIVE_MS, c.nextDelayMs(),
                "the 6-arg constructor has no freshness window — silence stays disabled");
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, c.mode());
        assertEquals(0L, c.nextSilentCheckMs(), "not silent: the loop beats instead");
    }

    @Test
    void advertisedBeatGap_isTheFifteenSecondPeriod_byDefault() {
        // The backend judges this engine's silence against what every beat advertises.
        HeartbeatCadence c = new HeartbeatCadence(0L, 0L, 0L, 0L, 0L, () -> false, () -> false);
        assertEquals(15L, c.maxBeatGapSeconds());
    }

    @Test
    void non_positive_configuration_falls_back_to_the_documented_defaults() {
        HeartbeatCadence c = new HeartbeatCadence(0L, 0L, 0L, 0L, 0L, () -> false, () -> false);
        assertEquals(0L, c.nextDelayMs());
        assertEquals(HeartbeatCadence.DEFAULT_FALLBACK_MIN_SECONDS * 1_000L, c.nextDelayMs(),
                "fallback starts at the floor default, not the ceiling");

        HeartbeatCadence healthy = new HeartbeatCadence(0L, 0L, 0L, 0L, 0L, () -> true, () -> false);
        // Local healthy alone is insufficient — confirm reachability to exercise the keepalive default.
        healthy.noteOverlayReachable(true);
        assertEquals(0L, healthy.nextDelayMs());
        assertEquals(HeartbeatCadence.Mode.WG_KEEPALIVE, healthy.mode());
        assertEquals(HeartbeatCadence.DEFAULT_WG_KEEPALIVE_SECONDS * 1_000L, healthy.nextDelayMs());
    }
}
