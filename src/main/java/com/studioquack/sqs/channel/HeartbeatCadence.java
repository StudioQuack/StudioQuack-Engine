package com.studioquack.sqs.channel;

import lombok.extern.slf4j.Slf4j;

import java.util.function.BooleanSupplier;

/**
 * Decides when the daemon sends its next heartbeat. Extracted from {@link HeartbeatClient} so the policy is
 * one small object with no I/O rather than a set of {@code if}s inside the beat loop.
 */
@Slf4j
public final class HeartbeatCadence {


    static final long DEFAULT_RESPONSIVE_MS = 2_000L;
    /**
     * Keepalive on a backend-confirmed healthy overlay. Matches the backend's 15s status poll so the engine's
     * status and resources are never more than one period old, whichever side is carrying them (2026-11-05:
     * operators saw an offline engine minutes late).
     */
    static final long DEFAULT_WG_KEEPALIVE_SECONDS = 15L;
    /**
     * Ceiling of the no-reverse-channel ladder. Equal to the floor: with no reverse path the beat IS the
     * status/resources report, and it must keep the same 15s period as the overlay poll.
     */
    static final long DEFAULT_FALLBACK_START_SECONDS = 15L;
    /**
     * Floor of the ladder (also the cap on failed-beat retry backoff).
     */
    static final long DEFAULT_FALLBACK_MIN_SECONDS = 15L;
    /**
     * How long an inbound overlay command stays proof of end-to-end reachability. The backend's status polls
     * arrive every 15s; two missed polls end the silence, so a daemon whose inbound path died starts beating
     * before the backend's 45s silence bound would call it offline.
     */
    static final long DEFAULT_WG_SILENT_FRESHNESS_SECONDS = 30L;
    /**
     * How often the beat loop re-resolves while {@link Mode#OVERLAY_SILENT}. No traffic leaves the host — this is
     * only the local clock that notices the freshness window lapsed.
     */
    static final long DEFAULT_WG_SILENT_CHECK_SECONDS = 10L;

    /**
     * Which cadence applies right now. Public so the loop can log it and tests can assert on it.
     */
    public enum Mode {
        /**
         * A run is in flight: keep the fast beat + long-poll so a cancel or a result is not stalled.
         */
        RESPONSIVE,
        /**
         * Overlay reverse channel healthy and backend confirmed inbound reachability, nothing running: slow liveness
         * keepalive only.
         */
        WG_KEEPALIVE,
        /**
         * Overlay confirmed both ways — local listener+resolver healthy, inbound overlay traffic observed inside the
         * freshness window — and the backend advertised it derives liveness from its own overlay polls (
         * {@code overlayLiveness} on the beat response): no heartbeat at all. Commands arrive over the reverse
         * channel, run status/logs ride their direct posts, and the backend's status polls carry telemetry home.
         */
        OVERLAY_SILENT,
        /**
         * No confirmed reverse channel: the daemon must poll, starting at the floor and relaxing upward toward the
         * ceiling on prolonged idleness, to pick up queued work.
         */
        FALLBACK_DECAY
    }

    private final long responsiveMs;
    private final long responsiveLongPollMs;
    private final long wgKeepaliveMs;
    private final long fallbackStartMs;
    private final long fallbackMinMs;
    /**
     * Inbound-overlay freshness window for {@link Mode#OVERLAY_SILENT}. {@code 0} disables silence entirely
     * (constructors that predate the mode keep the historical three-mode behaviour).
     */
    private final long silentFreshnessMs;
    /**
     * Re-resolve interval while silent — a local clock only, no traffic leaves the host.
     */
    private final long silentCheckMs;
    /**
     * Whether {@link Mode#OVERLAY_SILENT} is reachable at all. Old constructors pass {@code silentFreshnessMs = 0}
     * and keep the historical three-mode behaviour even though the freshness default is filled in — an explicit
     * zero is "feature absent", not "use the default".
     */
    private final boolean silentEnabled;
    /**
     * Post-activity grace before an idle daemon may drop to {@link Mode#WG_KEEPALIVE}. Interactive sessions
     * (Duckbot chat, designer runs) are bursty: the next trigger typically lands within a minute or two of the
     * last run.
     */
    private final long wgKeepaliveGraceMs;

    private final BooleanSupplier overlayReverseHealthy;
    private final BooleanSupplier runActive;

    /**
     * Wall-clock of the last observed run activity / collected work; {@code 0} until the first.
     */
    private long lastActivityMs;

    /**
     * Null until the first resolve, so the first beat after start is always treated as a transition.
     */
    private Mode mode;
    /**
     * The current rung of the fallback ladder; reset to the floor on every mode change and on activity.
     */
    private long fallbackMs;
    /**
     * The current rung of the failed-beat retry ladder; reset by any completed round-trip.
     */
    private long retryMs;
    private boolean pendingSend;
    /**
     * Backend's most recent confirmation that it can reach THIS daemon over the overlay. Fail-safe: false until
     * explicitly confirmed — never keepalive without it.
     */
    private boolean backendOverlayReachable;
    /**
     * Backend's advertised capability to derive engine liveness from its own overlay status polls (
     * {@code overlayLiveness} on the beat response). Fail-safe: false until advertised — a backend that still
     * liveness-checks the heartbeat must never see this daemon go silent.
     */
    private boolean backendOverlayLiveness;
    /**
     * Wall-clock of the last inbound overlay command the reverse listener saw; {@code 0} until the first.
     */
    private long lastInboundOverlayMs;

    /**
     * @param responsiveMs beat interval while a run is in flight ({@code sqs.channel.heartbeat.period-ms})
     * @param responsiveLongPollMs long-poll budget to use in {@link Mode#RESPONSIVE} only
     * @param wgKeepaliveMs beat interval while the overlay reverse channel is healthy, backend-confirmed, and idle
     * @param fallbackStartMs ceiling of the no-reverse-channel ladder (prolonged-idle relax target)
     * @param fallbackMinMs floor of that ladder (also the cap on failed-beat retry backoff)
     * @param overlayReverseHealthy sampled per beat — see {@code SqsChannelBootstrap} for what proves it
     * @param runActive sampled per beat — true while this daemon has a dispatch in flight
     */
    public HeartbeatCadence(long responsiveMs, long responsiveLongPollMs, long wgKeepaliveMs,
                            long fallbackStartMs, long fallbackMinMs,
                            BooleanSupplier overlayReverseHealthy, BooleanSupplier runActive) {
        this(responsiveMs, responsiveLongPollMs, wgKeepaliveMs, fallbackStartMs, fallbackMinMs,
                0L, overlayReverseHealthy, runActive);
    }

    /**
     * As the 7-arg form, plus the post-activity keepalive grace (
     * {@code sqs.channel.heartbeat.wg-keepalive-grace-seconds}).
     *
     * @param wgKeepaliveGraceMs how long after the last observed activity an otherwise keepalive-eligible daemon
     * stays {@link Mode#RESPONSIVE}; {@code 0} disables (demote immediately when the run ends)
     */
    public HeartbeatCadence(long responsiveMs, long responsiveLongPollMs, long wgKeepaliveMs,
                            long fallbackStartMs, long fallbackMinMs, long wgKeepaliveGraceMs,
                            BooleanSupplier overlayReverseHealthy, BooleanSupplier runActive) {
        this(responsiveMs, responsiveLongPollMs, wgKeepaliveMs, fallbackStartMs, fallbackMinMs,
                wgKeepaliveGraceMs, 0L, 0L, overlayReverseHealthy, runActive);
    }

    /**
     * As the 8-arg form, plus the overlay-silence knobs ({@code sqs.channel.heartbeat.wg-silent-freshness-seconds}
     * / {@code wg-silent-check-seconds}).
     *
     * @param silentFreshnessMs how long an inbound overlay command stays proof of end-to-end reachability;
     * {@code 0} disables {@link Mode#OVERLAY_SILENT}
     * @param silentCheckMs how often the loop re-resolves while silent (local clock only)
     */
    public HeartbeatCadence(long responsiveMs, long responsiveLongPollMs, long wgKeepaliveMs,
                            long fallbackStartMs, long fallbackMinMs, long wgKeepaliveGraceMs,
                            long silentFreshnessMs, long silentCheckMs,
                            BooleanSupplier overlayReverseHealthy, BooleanSupplier runActive) {
        this.responsiveMs = responsiveMs > 0 ? responsiveMs : DEFAULT_RESPONSIVE_MS;
        this.responsiveLongPollMs = Math.max(0L, responsiveLongPollMs);
        this.wgKeepaliveMs = wgKeepaliveMs > 0 ? wgKeepaliveMs : DEFAULT_WG_KEEPALIVE_SECONDS * 1_000L;
        this.wgKeepaliveGraceMs = Math.max(0L, wgKeepaliveGraceMs);
        this.silentFreshnessMs = silentFreshnessMs > 0 ? silentFreshnessMs
                : DEFAULT_WG_SILENT_FRESHNESS_SECONDS * 1_000L;
        this.silentCheckMs = silentCheckMs > 0 ? silentCheckMs : DEFAULT_WG_SILENT_CHECK_SECONDS * 1_000L;
        // The freshness window is the on/off switch: an explicit 0 from an old constructor means
        // "feature absent", so silence stays disabled there even though the default is filled in.
        this.silentEnabled = silentFreshnessMs > 0;
        long start = fallbackStartMs > 0 ? fallbackStartMs : DEFAULT_FALLBACK_START_SECONDS * 1_000L;
        long min = fallbackMinMs > 0 ? fallbackMinMs : DEFAULT_FALLBACK_MIN_SECONDS * 1_000L;
        this.fallbackStartMs = Math.max(start, min);
        // A floor above the ceiling would invert the ladder, so a misconfigured pair is collapsed
        // rather than obeyed.
        this.fallbackMinMs = Math.min(min, this.fallbackStartMs);
        this.overlayReverseHealthy = overlayReverseHealthy == null ? () -> false : overlayReverseHealthy;
        this.runActive = runActive == null ? () -> false : runActive;
        // Ladder starts at the floor so the first idle stretch collects work promptly.
        this.fallbackMs = this.fallbackMinMs;
        this.retryMs = this.responsiveMs;
    }

    /**
     * The historical cadence: always {@link Mode#RESPONSIVE}, i.e. a fixed period and the configured long-poll on
     * every beat. Used by callers that have no overlay/run signals to offer (the offline unit tests and any
     * static-base embedding), so wiring the cadence in cannot change their timing.
     */
    public static HeartbeatCadence alwaysResponsive(long periodMs, long longPollWaitMs) {
        return new HeartbeatCadence(periodMs, longPollWaitMs, 0L, 0L, 0L, () -> false, () -> true);
    }

    /**
     * The long-poll budget for the beat this cadence just scheduled ({@code 0} disables it). Only
     * {@link Mode#RESPONSIVE} long-polls.
     */
    public synchronized long longPollWaitMs() {
        return mode == null || mode == Mode.RESPONSIVE ? responsiveLongPollMs : 0L;
    }

    /**
     * Called by the loop immediately before it beats: consumes any pending immediate-send request and returns the
     * long-poll budget for this beat. The flag has to be cleared here rather than on the next
     * {@link #nextDelayMs()}.
     */
    public synchronized long beginBeat() {
        pendingSend = false;
        return longPollWaitMs();
    }

    /**
     * The mode the last resolve settled on ({@code null} before the first beat).
     */
    public synchronized Mode mode() {
        return mode;
    }

    /**
     * The longest gap this daemon leaves between beats whenever its beat is what carries liveness (keepalive or
     * the no-reverse-channel ladder). Published on every beat as {@code resources.beatCadenceSeconds} so the
     * backend calls THIS engine offline after a small multiple of its own promise instead of the slowest cadence
     * any daemon generation shipped.
     */
    public long maxBeatGapSeconds() {
        return Math.max(1L, Math.max(wgKeepaliveMs, fallbackStartMs) / 1_000L);
    }

    /**
     * Forces the next beat to go out immediately, whatever the cadence. For state the loop cannot observe on its
     * own: enrolment completing, a run reporting terminal, an operator-visible change the backend should not learn
     * about a keepalive period late.
     */
    public synchronized void sendNow(String reason) {
        pendingSend = true;
        log.debug("channel/heartbeat: immediate beat requested (reason={})", reason);
    }

    /**
     * Backend's most recent confirmation that it can reach THIS daemon over the overlay. {@code null} or
     * {@code false} = unconfirmed — treat identically (fail-safe: never keepalive).
     */
    public synchronized void noteOverlayReachable(Boolean confirmed) {
        this.backendOverlayReachable = confirmed != null && confirmed;
    }

    /**
     * Backend's advertised capability to derive engine liveness from its own overlay status polls. {@code null} or
     * {@code false} = not advertised — treat identically (fail-safe: never silent).
     */
    public synchronized void noteOverlayLiveness(Boolean advertised) {
        this.backendOverlayLiveness = advertised != null && advertised;
    }

    /**
     * The reverse listener saw an inbound overlay command. Fired for EVERY request including poll types: the
     * request arriving at all is the proof that the backend can reach this daemon inbound — the confirmation
     * {@link Mode#OVERLAY_SILENT} rests on once beat-carried {@code overlayReachable} stops arriving.
     */
    public synchronized void noteInboundOverlayTraffic() {
        this.lastInboundOverlayMs = System.currentTimeMillis();
    }

    /**
     * True while an inbound overlay command was seen inside the freshness window. The window lapses when the
     * backend's polls stop reaching this daemon — the signal that breaks the silence.
     */
    private boolean inboundOverlayFresh() {
        return silentEnabled && lastInboundOverlayMs > 0L
                && System.currentTimeMillis() - lastInboundOverlayMs < silentFreshnessMs;
    }

    /**
     * Whether the daemon may go silent right now: overlay healthy locally, backend advertised overlay-derived
     * liveness, and inbound overlay traffic is fresh. Takes priority over every other mode — with the reverse
     * channel confirmed both ways the beat carries nothing the overlay does not already deliver (commands push
     * down it, run status/logs post directly, telemetry rides the status replies).
     */
    private boolean silentEligible() {
        return overlayReverseHealthy.getAsBoolean() && backendOverlayLiveness && inboundOverlayFresh();
    }

    /**
     * Called by the loop INSTEAD of beating while {@link Mode#OVERLAY_SILENT}: re-resolves from the live signals
     * and returns the stale-check interval while silence may continue, or {@code 0} when the loop must beat at
     * once (inbound went stale, the local overlay dropped, the backend stopped advertising overlay liveness, or a
     * {@link #sendNow(String)} landed mid-silence).
     */
    public synchronized long nextSilentCheckMs() {
        if (mode != Mode.OVERLAY_SILENT) {
            return 0L;
        }
        if (!pendingSend && silentEligible()) {
            return silentCheckMs;
        }
        // Exiting silence: resolve the mode the beat after this one will continue with and demand
        // it immediately. pendingSend stays set — beginBeat() consumes it on the beat itself.
        Mode resolved = runActive.getAsBoolean() ? Mode.RESPONSIVE
                : !(overlayReverseHealthy.getAsBoolean() && backendOverlayReachable) ? Mode.FALLBACK_DECAY
                : withinKeepaliveGrace() ? Mode.RESPONSIVE
                : Mode.WG_KEEPALIVE;
        Mode previous = mode;
        mode = resolved;
        fallbackMs = fallbackMinMs;
        log.info("channel/heartbeat: cadence {} -> {} (beating now; interval {}ms)",
                previous, resolved, intervalForMode());
        return 0L;
    }

    /**
     * Queued work was collected or other activity occurred. Returns the fallback ladder to its floor so the next
     * idle stretch starts responsive again rather than inheriting a relaxed interval, and (re)opens the keepalive
     * grace window so a follow-up trigger inside an interactive session is collected at the responsive interval
     * instead of waiting out a 60s keepalive.
     */
    public synchronized void noteActivity() {
        fallbackMs = fallbackMinMs;
        lastActivityMs = System.currentTimeMillis();
    }

    /**
     * True while the post-activity grace window is open. Only ever upgrades a keepalive-eligible beat to
     * {@link Mode#RESPONSIVE} — it never touches the no-reverse-channel fallback ladder, whose floor is its own
     * considered trade-off.
     */
    private boolean withinKeepaliveGrace() {
        return wgKeepaliveGraceMs > 0L && lastActivityMs > 0L
                && System.currentTimeMillis() - lastActivityMs < wgKeepaliveGraceMs;
    }

    /**
     * How long to sleep after a successful beat. Re-resolves the mode from the live signals, resets the ladder to
     * the floor when mode changed, and returns {@code 0} when a transition means the next beat must go out at
     * once.
     */
    public synchronized long nextDelayMs() {
        // A completed round-trip is the proof the failure ladder was waiting for.
        retryMs = responsiveMs;
        boolean runNow = runActive.getAsBoolean();
        if (runNow) {
            // A live run IS activity: it keeps the grace window open across the run's whole length,
            // so the message the user sends right after the answer still lands in a responsive beat.
            lastActivityMs = System.currentTimeMillis();
        }
        // Local healthy alone is NEVER enough for keepalive — backend must confirm inbound reachability.
        // The grace only ever upgrades a keepalive-eligible beat; the fallback ladder is untouched.
        // Silence outranks every beating mode: with the overlay confirmed both ways the beat adds
        // nothing the reverse channel and the direct posts do not already carry.
        Mode resolved = silentEligible() ? Mode.OVERLAY_SILENT
                : runNow ? Mode.RESPONSIVE
                : !(overlayReverseHealthy.getAsBoolean() && backendOverlayReachable) ? Mode.FALLBACK_DECAY
                : withinKeepaliveGrace() ? Mode.RESPONSIVE
                : Mode.WG_KEEPALIVE;
        if (resolved != mode) {
            // A recovered link (or a run starting) must never inherit a relaxed interval, and the
            // backend must hear about the change now rather than next cycle. Entering SILENCE is the
            // one exception: demanding an immediate beat for it would defeat the mode — the backend
            // learns the daemon is healthy from its own overlay polls, not from a farewell beat.
            Mode previous = mode;
            mode = resolved;
            fallbackMs = fallbackMinMs;
            if (resolved != Mode.OVERLAY_SILENT) {
                pendingSend = true;
            }
            log.info("channel/heartbeat: cadence {} -> {} ({}; interval {}ms)",
                    previous == null ? "start" : previous, resolved,
                    resolved == Mode.OVERLAY_SILENT ? "going quiet" : "beating now", intervalForMode());
        }
        if (pendingSend) {
            pendingSend = false;
            return 0L;
        }
        if (mode == Mode.FALLBACK_DECAY) {
            long current = fallbackMs;
            // Prolonged idleness relaxes UPWARD toward the ceiling; activity resets to the floor.
            fallbackMs = Math.min(fallbackStartMs, Math.max(fallbackMinMs, fallbackMs * 2));
            return current;
        }
        return intervalForMode();
    }

    /**
     * How long to sleep after a failed beat: bounded exponential backoff from the responsive period, capped at the
     * fallback floor. The keepalive is never used here.
     */
    public synchronized long nextDelayAfterFailedBeatMs() {
        long current = retryMs;
        retryMs = Math.min(fallbackMinMs, retryMs * 2);
        return current;
    }

    private long intervalForMode() {
        return switch (mode) {
            case RESPONSIVE -> responsiveMs;
            case WG_KEEPALIVE -> wgKeepaliveMs;
            // The first sleep after entering silence; subsequent re-resolves go through
            // nextSilentCheckMs() without a beat ever leaving the host.
            case OVERLAY_SILENT -> silentCheckMs;
            case FALLBACK_DECAY -> fallbackMs;
        };
    }
}
