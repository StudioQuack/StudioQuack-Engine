package com.studioquack.sqs.sandbox.warmpool;

import com.studioquack.sqs.sandbox.oci.HardeningArgs;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * An IMAGE-keyed reservoir of pre-warmed slots (/). The pool's identity key is the resolved OCI
 * image ref; language survives only as slot metadata for runner/broker eligibility and as a
 * status-row hint.
 */
@Slf4j
public final class WarmPool {

    /** Atomic aggregate consumed by heartbeat telemetry. */
    public record Counts(int built, int target) {
    }

    /**
     * Languages the operator can warm from the Runtime UI / that seed script-mode default images.
     * Still the gate for script edge-transforms ({@code EdgeTransformPreStep}); the pool itself is
     * image-keyed and accepts any OCI image.
     */
    public static List<String> warmableLanguages() {
        return List.of("python", "javascript", "typescript", "ruby");
    }

    /**
     * Summary of one {@link #reconcile()} pass.
     *
     * @param removed   dead idle slots dropped
     * @param trimmed   live idle slots cooled because the image was over its target
     * @param reclaimed owned containers the reservoir had lost track of, removed off the host
     */
    public record ReconcileReport(int removed, int trimmed, int reclaimed,
                                  Map<String, Integer> refills, List<String> errors) {
    }

    /**
     * Operator-facing per-image row used by status and set-warm-target results.
     * {@code imageRef} is the identity; {@code language} is a retained hint (null when unknown —
     * Jackson still serialises the key).
     */
    public record LanguageStatus(
            String imageRef,
            String language,
            int preBuilt,
            int target,
            int defaultTarget,
            boolean warmable,
            String warmerKind,
            WarmTargetEntry.Caps caps,
            List<SlotStatus> slots) {
    }

    /** One idle slot currently waiting in the warm reservoir. */
    public record SlotStatus(
            String containerId,
            String kind,
            String image,
            String warmedAt) {
    }

    private final Warmer warmer;
    /** Desired reservoir size per image ref (trimmed; OCI tags are case-sensitive — not lowercased). */
    private final Map<String, Integer> target = new ConcurrentHashMap<>();
    private final Map<String, Deque<WarmSlot>> idle = new ConcurrentHashMap<>();
    /**
     * Slots handed out by {@link #acquire} and not yet {@link #release}d, keyed by container id.
     * These are the containers with a live job inside them.
     */
    private final Map<String, WarmSlot> leased = new ConcurrentHashMap<>();
    /** Warms currently in flight per image (async refills) so we never over-/under-provision. */
    private final Map<String, Integer> inFlight = new ConcurrentHashMap<>();
    /** imageRef → language hint (runner/broker eligibility + status). */
    private final Map<String, String> languageByImage = new ConcurrentHashMap<>();
    /** Seeded factory defaults for status display (python's default image → 6, others → 0). */
    private final Map<String, Integer> defaultTargetByImage = new ConcurrentHashMap<>();
    /** Per-image caps; absent / removed = uncapped. */
    private final Map<String, WarmTargetEntry.Caps> caps = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    /** Runs the parallel FIFO refill after a slot is consumed. Direct (caller-runs) in unit tests. */
    private final Executor refillExecutor;
    /** This daemon run's boot id; a container stamped with any other one outlived its creator. */
    private final String bootId;
    /** Set by {@link #shutdown()}: the pool is done and must never open another container. */
    private volatile boolean closed;

    public WarmPool(Warmer warmer, Map<String, Integer> targets) {
        this(warmer, targets, Runnable::run, SandboxOwnership.unowned());
    }

    public WarmPool(Warmer warmer, Map<String, Integer> targets, Executor refillExecutor) {
        this(warmer, targets, refillExecutor, SandboxOwnership.unowned());
    }

    public WarmPool(Warmer warmer, Map<String, Integer> targets, Executor refillExecutor,
                    SandboxOwnership ownership) {
        this.warmer = warmer == null ? new NoopWarmer() : warmer;
        if (targets != null) {
            targets.forEach((k, v) -> {
                String key = normaliseKey(k);
                if (!key.isEmpty() && v != null) {
                    target.put(key, Math.max(0, v));
                }
            });
        }
        this.refillExecutor = refillExecutor == null ? Runnable::run : refillExecutor;
        this.bootId = (ownership == null ? SandboxOwnership.unowned() : ownership).bootId();
    }

    /** The active warmer's kind ({@code oci}/{@code noop}). */
    public String warmerKind() {
        return warmer.kind();
    }

    /**
     * Seeds a status row + language hint + factory default for a known image (the four warmable
     * languages' images). Does not raise the live target — that stays at whatever
     * {@link #setTarget} / boot defaults / persistence applied.
     */
    public void registerImage(String imageRef, String language, int defaultTarget) {
        String key = normaliseKey(imageRef);
        if (key.isEmpty()) {
            return;
        }
        if (language != null && !language.isBlank()) {
            languageByImage.putIfAbsent(key, language.trim().toLowerCase(java.util.Locale.ROOT));
        }
        defaultTargetByImage.putIfAbsent(key, Math.max(0, defaultTarget));
    }

    /**
     * Sets the desired reservoir size for an IMAGE, with its language hint and per-image caps
     * ({@code null} caps = uncapped, clears any stored caps).
     */
    public void setTarget(String imageRef, String language, int n, WarmTargetEntry.Caps caps) {
        if (closed) {
            return;
        }
        String key = normaliseKey(imageRef);
        if (key.isEmpty()) {
            return;
        }
        if (language != null && !language.isBlank()) {
            languageByImage.putIfAbsent(key, language.trim().toLowerCase(java.util.Locale.ROOT));
        }
        if (caps == null || caps.isEmpty()) {
            this.caps.remove(key);
        } else {
            this.caps.put(key, caps);
        }
        target.put(key, Math.max(0, n));
    }

    /** Legacy shape: image (or previously language) key with no language hint / caps. */
    public void setTarget(String key, int n) {
        setTarget(key, null, n, null);
    }

    /** The desired reservoir size for an image (0 when unset). */
    public int target(String imageRef) {
        return target.getOrDefault(normaliseKey(imageRef), 0);
    }

    /** An immutable copy of the full per-image target map (used by status / diagnostics). */
    public Map<String, Integer> targets() {
        return new LinkedHashMap<>(target);
    }

    /** Per-image caps, or {@code null} when uncapped / unknown. */
    public WarmTargetEntry.Caps capsFor(String imageRef) {
        return caps.get(normaliseKey(imageRef));
    }

    /**
     * Every known image as a persistable entry: union of targets, caps, and registered seeds.
     * Seeds appear first so a round-trip preserves the factory-default row order.
     */
    public List<WarmTargetEntry> entries() {
        LinkedHashSet<String> keys = new LinkedHashSet<>();
        keys.addAll(defaultTargetByImage.keySet());
        keys.addAll(target.keySet());
        keys.addAll(caps.keySet());
        keys.addAll(languageByImage.keySet());
        List<WarmTargetEntry> out = new ArrayList<>(keys.size());
        for (String key : keys) {
            out.add(new WarmTargetEntry(key, languageByImage.get(key), target(key), caps.get(key)));
        }
        return out;
    }

    /** The current idle reservoir size for an image. */
    public int size(String imageRef) {
        Deque<WarmSlot> d = idle.get(normaliseKey(imageRef));
        return d == null ? 0 : d.size();
    }

    /** How many slots are currently out with a dispatch (containers with a live job in them). */
    public int leasedCount() {
        return leased.size();
    }

    /** @return one consistent aggregate of idle built slots and desired slots. */
    public Counts counts() {
        synchronized (lock) {
            int built = idle.values().stream().mapToInt(Deque::size).sum();
            int desired = target.values().stream().mapToInt(Integer::intValue).sum();
            return new Counts(built, desired);
        }
    }

    /**
     * Stable per-image snapshot for the server-side environment runtime UI. Row set = registered seeds
     * ∪ target keys ∪ idle keys (seeds first).
     */
    public List<LanguageStatus> status() {
        synchronized (lock) {
            Map<String, Boolean> images = new LinkedHashMap<>();
            defaultTargetByImage.keySet().forEach(k -> images.put(k, true));
            target.keySet().forEach(k -> images.put(k, true));
            idle.keySet().forEach(k -> images.put(k, true));

            List<LanguageStatus> rows = new ArrayList<>();
            for (String imageRef : images.keySet()) {
                Deque<WarmSlot> slots = idle.getOrDefault(imageRef, new ArrayDeque<>());
                List<SlotStatus> slotRows = slots.stream()
                        .map(slot -> new SlotStatus(
                                emptyToNull(slot.containerId()),
                                slot.kind(),
                                emptyToNull(slot.image()),
                                slot.warmedAt() == null ? null : slot.warmedAt().toString()))
                        .toList();
                rows.add(new LanguageStatus(
                        imageRef,
                        languageByImage.get(imageRef),
                        slots.size(),
                        target(imageRef),
                        defaultTargetByImage.getOrDefault(imageRef, 0),
                        true,
                        warmerKind(),
                        caps.get(imageRef),
                        slotRows));
            }
            return rows;
        }
    }

    /** Hands out a warm slot for {@code imageRef} with no language hint / job caps. */
    public Optional<WarmSlot> acquire(String imageRef) {
        return acquire(imageRef, null, null);
    }

    /**
     * Hands out a warm slot for {@code imageRef}: pops the first live idle slot, else warms a fresh
     * one on demand. Records {@code languageHint} (putIfAbsent) when non-blank.
     */
    public Optional<WarmSlot> acquire(String imageRef, String languageHint, WarmTargetEntry.Caps jobCaps) {
        if (closed) {
            return Optional.empty();
        }
        String key = normaliseKey(imageRef);
        if (key.isEmpty()) {
            return Optional.empty();
        }
        if (languageHint != null && !languageHint.isBlank()) {
            languageByImage.putIfAbsent(key, languageHint.trim().toLowerCase(java.util.Locale.ROOT));
        }
        WarmSlot popped = null;
        synchronized (lock) {
            Deque<WarmSlot> d = idle.get(key);
            while (d != null && !d.isEmpty()) {
                WarmSlot slot = d.pollFirst();
                if (warmer.alive(slot)) {
                    popped = slot;
                    break;
                }
                // Stale/dead slot — discard and try the next.
                warmer.cool(slot);
            }
        }
        // Single-use containers (Go the control plane parity): the consumed slot is torn down after the
        // job, so warm its replacement NOW, off-thread — consumption and creation overlap (FIFO), keeping
        // a live, preloaded slot ready for the next request without the dispatch waiting.
        scheduleRefill(key);
        if (popped != null) {
            return Optional.of(lease(popped));
        }
        // Reservoir empty (cold burst) — warm on demand so the dispatch never blocks on the pool.
        try {
            return Optional.of(lease(warmer.warm(warmSpecForAcquire(key, jobCaps))));
        } catch (Warmer.WarmException e) {
            log.debug("warmpool.acquire.warm-failed image={}: {}", key, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Tears down a consumed slot's container (single-use, Go the control plane parity): a dispatched
     * container is never reused, so destroying it after the job guarantees per-job isolation. {@link
     * #acquire} already scheduled the replacement warm, so the reservoir stays at target.
     */
    public void release(WarmSlot slot) {
        if (slot == null || !slot.hasContainer()) {
            return; // token-only noop slot: nothing to destroy
        }
        leased.remove(slot.containerId());
        warmer.cool(slot);
    }

    /**
     * Cancel-instance teardown: kills + removes a LEASED job container by id, through the warmer's own
     * {@link Warmer#remove(String)} (the same call {@link #release} makes) — never by returning it to
     * {@link #idle}, so a killed container can never be resurrected/handed out again. Clears the
     * {@link #leased} bookkeeping proactively so a later {@link #release} of the same slot is a
     * harmless (idempotent) no-op double-teardown.
     */
    public void forceKill(String containerId) {
        if (containerId == null || containerId.isBlank()) {
            return;
        }
        leased.remove(containerId);
        warmer.remove(containerId);
    }

    /** Records a slot as out with a dispatch, so shutdown and reclamation can still account for it. */
    private WarmSlot lease(WarmSlot slot) {
        if (slot != null && slot.hasContainer()) {
            leased.put(slot.containerId(), slot);
        }
        return slot;
    }

    /** Warms one replacement slot for {@code imageRef} off-thread (the parallel FIFO refill). */
    private void scheduleRefill(String imageRef) {
        final String key = normaliseKey(imageRef);
        if (closed || target(key) <= 0) {
            return; // shut down, or the image isn't pre-warmed → nothing to refill
        }
        try {
            refillExecutor.execute(() -> refillOne(key));
        } catch (RuntimeException reject) {
            log.debug("warmpool.refill.reject image={}: {}", key, reject.getMessage());
        }
    }

    /** Warms a single slot when the reservoir (idle + in-flight) is below target; never throws. */
    private void refillOne(String key) {
        synchronized (lock) {
            if (closed || size(key) + inFlight.getOrDefault(key, 0) >= target(key)) {
                return;
            }
            inFlight.merge(key, 1, Integer::sum);
        }
        WarmSlot fresh = null;
        try {
            fresh = warmer.warm(warmSpecFor(key));
        } catch (Warmer.WarmException e) {
            log.debug("warmpool.refill.warm-failed image={}: {}", key, e.getMessage());
        } finally {
            boolean strandedByClose;
            synchronized (lock) {
                strandedByClose = fresh != null && closed;
                if (fresh != null && !strandedByClose) {
                    idle.computeIfAbsent(key, k -> new ArrayDeque<>()).addLast(fresh);
                }
                inFlight.merge(key, -1, Integer::sum);
            }
            // The pool closed while this warm was in flight. The reservoir has already been drained,
            // so putting the slot in it would strand a container nothing will ever look at again.
            if (strandedByClose) {
                warmer.cool(fresh);
            }
        }
    }

    /** Self-heal pass. Safe to call on a schedule (Go: every 30s after a 10s startup delay). */
    public ReconcileReport reconcile() {
        if (closed) {
            return new ReconcileReport(0, 0, 0, Map.of(), List.of());
        }
        int removed = 0;
        int trimmed = 0;
        Map<String, Integer> refills = new LinkedHashMap<>();
        List<String> errors = new ArrayList<>();

        for (String key : reconcilableImages()) {
            int want = target(key);
            // 1) prune dead idle slots
            List<WarmSlot> live = new ArrayList<>();
            synchronized (lock) {
                Deque<WarmSlot> d = idle.getOrDefault(key, new ArrayDeque<>());
                for (WarmSlot s : d) {
                    if (warmer.alive(s)) {
                        live.add(s);
                    } else {
                        warmer.cool(s);
                    }
                }
            }
            int prunedCount;
            synchronized (lock) {
                Deque<WarmSlot> d = idle.computeIfAbsent(key, k -> new ArrayDeque<>());
                prunedCount = d.size() - live.size();
                d.clear();
                d.addAll(live);
            }
            removed += Math.max(0, prunedCount);
            // 2) cool the surplus when the reservoir is OVER target — the operator lowering a target
            //    (or removing an image) has to release the containers, not just stop replacing them.
            //    Oldest first: that is the slot the age reap would take next anyway.
            trimmed += trimSurplus(key, want);
            // 3) top up to target (counting async refills already in flight so we don't over-provision)
            int refilled = 0;
            while (size(key) + inFlight.getOrDefault(key, 0) < want) {
                try {
                    WarmSlot fresh = warmer.warm(warmSpecFor(key));
                    synchronized (lock) {
                        idle.computeIfAbsent(key, k -> new ArrayDeque<>()).addLast(fresh);
                    }
                    refilled++;
                } catch (Warmer.WarmException ex) {
                    errors.add(key + ": " + ex.getMessage());
                    break; // don't spin on a persistently-failing warmer
                }
            }
            if (refilled > 0) {
                refills.put(key, refilled);
            }
        }
        int reclaimed = reclaimAbandoned();
        if (removed > 0 || trimmed > 0 || reclaimed > 0 || !refills.isEmpty() || !errors.isEmpty()) {
            log.info("warmpool.reconcile removed={} trimmed={} reclaimed={} refills={} errors={}",
                    removed, trimmed, reclaimed, refills, errors);
        }
        return new ReconcileReport(removed, trimmed, reclaimed, refills, errors);
    }

    /**
     * Every image reconcile has to look at: the ones with a target AND the ones merely holding
     * slots. An image whose target was dropped from the map entirely still has containers open, and
     * iterating targets alone would never see them again.
     */
    private List<String> reconcilableImages() {
        synchronized (lock) {
            LinkedHashSet<String> images = new LinkedHashSet<>(target.keySet());
            images.addAll(idle.keySet());
            return List.copyOf(images);
        }
    }

    /** Cools idle slots beyond {@code want} for one image. @return how many were cooled. */
    private int trimSurplus(String key, int want) {
        List<WarmSlot> surplus = new ArrayList<>();
        synchronized (lock) {
            Deque<WarmSlot> d = idle.getOrDefault(key, new ArrayDeque<>());
            while (d.size() > Math.max(0, want)) {
                WarmSlot s = d.pollFirst();
                if (s == null) {
                    break;
                }
                surplus.add(s);
            }
        }
        surplus.forEach(warmer::cool);
        return surplus.size();
    }

    /**
     * Removes containers this daemon owns that the reservoir cannot account for. Two kinds qualify,
     * and nothing else.
     */
    private int reclaimAbandoned() {
        List<Warmer.OwnedContainer> owned;
        try {
            owned = warmer.ownedContainers();
        } catch (RuntimeException e) {
            log.debug("warmpool.reclaim.list-failed: {}", e.toString());
            return 0;
        }
        if (owned == null || owned.isEmpty()) {
            return 0;
        }
        java.util.Set<String> known = new java.util.HashSet<>();
        boolean warmSettled;
        synchronized (lock) {
            known.addAll(leased.keySet());
            for (Deque<WarmSlot> d : idle.values()) {
                d.forEach(s -> known.add(s.containerId()));
            }
            warmSettled = inFlight.values().stream().mapToInt(Integer::intValue).sum() == 0;
        }
        int reclaimed = 0;
        for (Warmer.OwnedContainer c : owned) {
            if (c == null || c.containerId() == null || c.containerId().isBlank()
                    || known.contains(c.containerId())) {
                continue;
            }
            boolean creatorGone = !bootId.equals(c.bootId());
            boolean strayWarmSlot = warmSettled && HardeningArgs.ROLE_WARM.equals(c.role());
            if (!creatorGone && !strayWarmSlot) {
                continue;
            }
            warmer.remove(c.containerId());
            reclaimed++;
            log.warn("warmpool.reclaim container={} role={} boot={} reason={}",
                    shortId(c.containerId()), c.role(), c.bootId(),
                    creatorGone ? "creating-daemon-run-is-gone" : "warm-slot-not-in-reservoir");
        }
        return reclaimed;
    }

    /**
     * Orphan reap: cools + drops every idle slot older than {@code maxAge} (Go: 5m). Returns how many
     * were reaped.
     */
    public int reapIdle(Duration maxAge) {
        Instant cutoff = Instant.now().minus(maxAge);
        int reaped = 0;
        synchronized (lock) {
            for (Deque<WarmSlot> d : idle.values()) {
                List<WarmSlot> keep = new ArrayList<>();
                for (WarmSlot s : d) {
                    if (s.warmedAt().isBefore(cutoff)) {
                        warmer.cool(s);
                        reaped++;
                    } else {
                        keep.add(s);
                    }
                }
                d.clear();
                d.addAll(keep);
            }
        }
        if (reaped > 0) {
            log.debug("warmpool.reap reaped={} olderThan={}", reaped, maxAge);
        }
        return reaped;
    }

    /**
     * Cool every IDLE slot whose pool key is {@code imageRef}. Does not change the target, does not
     * schedule a refill, does not touch leased in-flight slots (single-use: they will not be handed
     * out again).
     *
     * @return How many idle slots were cooled.
     */
    public int invalidateImage(String imageRef) {
        try {
            if (closed) {
                return 0;
            }
            String key = normaliseKey(imageRef);
            if (key.isEmpty()) {
                return 0;
            }
            List<WarmSlot> toCool = new ArrayList<>();
            synchronized (lock) {
                Deque<WarmSlot> d = idle.get(key);
                if (d != null) {
                    WarmSlot slot;
                    while ((slot = d.pollFirst()) != null) {
                        toCool.add(slot);
                    }
                }
            }
            for (WarmSlot slot : toCool) {
                try {
                    warmer.cool(slot);
                } catch (RuntimeException e) {
                    log.debug("warmpool.invalidate.cool-failed image={}: {}", key, e.getMessage());
                }
            }
            if (!toCool.isEmpty()) {
                log.info("warmpool.invalidate image={} cooled={}", key, toCool.size());
            }
            return toCool.size();
        } catch (RuntimeException e) {
            log.debug("warmpool.invalidate.failed: {}", e.getMessage());
            return 0;
        }
    }

    /** Cools every idle slot and every leased one, with no wait (warmer swap / uninstall). */
    public int drain() {
        return drain(Duration.ZERO);
    }

    /**
     * Stops every container this pool is holding open and returns how many it stopped. Idle slots go
     * immediately — nothing is running inside them.
     */
    public int drain(Duration grace) {
        int stopped = 0;
        List<WarmSlot> idleSlots = new ArrayList<>();
        synchronized (lock) {
            for (Deque<WarmSlot> d : idle.values()) {
                idleSlots.addAll(d);
                d.clear();
            }
        }
        for (WarmSlot s : idleSlots) {
            warmer.cool(s);
            stopped++;
        }
        awaitLeaseReturn(grace);
        for (WarmSlot s : List.copyOf(leased.values())) {
            if (leased.remove(s.containerId()) != null) {
                warmer.cool(s);
                stopped++;
            }
        }
        stopped += removeRemainingOwned();
        if (stopped > 0) {
            log.info("warmpool.drain stopped={} graceMs={}", stopped, grace == null ? 0 : grace.toMillis());
        }
        return stopped;
    }

    /**
     * Terminal stop for an uninstall: the engine is being removed, so there is nothing to wait for and
     * nothing may be re-opened. Zeroing the targets and latching {@link #closed} BEFORE the drain is
     * what stops the reconcile loop from re-warming the reservoir in the seconds between the purge and
     * the process actually exiting.
     */
    public int shutdown() {
        closed = true;
        target.clear();
        return drain(Duration.ZERO);
    }

    /** True once {@link #shutdown()} has run: the pool will not open another container. */
    public boolean isClosed() {
        return closed;
    }

    /** Waits (bounded) for dispatches to hand their slots back. Never throws on interrupt. */
    private void awaitLeaseReturn(Duration grace) {
        long waitNanos = grace == null ? 0L : Math.max(0L, grace.toNanos());
        if (waitNanos == 0L || leased.isEmpty()) {
            return;
        }
        log.info("warmpool.drain waiting up to {}ms for {} in-flight slot(s)",
                grace.toMillis(), leased.size());
        long deadline = System.nanoTime() + waitNanos;
        while (!leased.isEmpty() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Removes whatever this daemon still owns once the reservoir has been emptied — in practice the
     * cold single-job containers, which the reservoir never sees, plus anything a failed warm or an
     * earlier killed run left behind. The daemon is going away, so there is no live consumer left for
     * any of them.
     */
    private int removeRemainingOwned() {
        List<Warmer.OwnedContainer> owned;
        try {
            owned = warmer.ownedContainers();
        } catch (RuntimeException e) {
            log.debug("warmpool.drain.list-failed: {}", e.toString());
            return 0;
        }
        if (owned == null || owned.isEmpty()) {
            return 0;
        }
        int stopped = 0;
        for (Warmer.OwnedContainer c : owned) {
            if (c == null || c.containerId() == null || c.containerId().isBlank()) {
                continue;
            }
            warmer.remove(c.containerId());
            stopped++;
        }
        return stopped;
    }

    /**
     * WarmSpec for reservoir refill/reconcile: registered per-image caps only (zeros = uncapped).
     */
    private Warmer.WarmSpec warmSpecFor(String key) {
        WarmTargetEntry.Caps c = caps.get(key);
        return new Warmer.WarmSpec(key, languageByImage.get(key),
                c == null ? 0.0 : c.cpusOrZero(),
                c == null ? 0 : c.ramMbOrZero(),
                c == null ? 0 : c.diskMbOrZero(),
                c == null ? 0 : c.gpuOrZero());
    }

    /**
     * WarmSpec for on-demand acquire: registered per-image caps win over ad-hoc job caps; both
     * absent → zeros (uncapped / operator defaults inside the warmer).
     */
    private Warmer.WarmSpec warmSpecForAcquire(String key, WarmTargetEntry.Caps jobCaps) {
        WarmTargetEntry.Caps registered = caps.get(key);
        WarmTargetEntry.Caps effective = registered != null ? registered : jobCaps;
        return new Warmer.WarmSpec(key, languageByImage.get(key),
                effective == null ? 0.0 : effective.cpusOrZero(),
                effective == null ? 0 : effective.ramMbOrZero(),
                effective == null ? 0 : effective.diskMbOrZero(),
                effective == null ? 0 : effective.gpuOrZero());
    }

    /** Trim only — OCI image tags are case-sensitive, so we never lowercase the key. */
    private static String normaliseKey(String imageRef) {
        return imageRef == null ? "" : imageRef.trim();
    }

    private static String shortId(String id) {
        return id == null ? "" : id.substring(0, Math.min(12, id.length()));
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
