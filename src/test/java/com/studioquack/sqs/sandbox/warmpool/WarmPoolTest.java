package com.studioquack.sqs.sandbox.warmpool;

import com.studioquack.sqs.sandbox.oci.HardeningArgs;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The warm-pool reservoir logic (acquire / release / reconcile / reap) against a deterministic fake
 * warmer, so it runs with no container runtime. The OCI warmer path is covered by a Docker-gated
 * integration test. Pool identity is the OCI image ref.
 */
class WarmPoolTest {

    private static final String PYTHON_IMG = "python:3.11-slim";
    private static final String NODE_IMG = "node:26.12.1-slim";
    /** Second reservoir key with no language default image — treated as a bare imageRef. */
    private static final String RUBY_IMG = "ruby";

    private static final SandboxOwnership OWNERSHIP = SandboxOwnership.of("owner-1", "boot-1");

    /** A pool whose boot id matches the one {@link FakeWarmer} stamps on the containers it creates. */
    private static WarmPool pool(Warmer warmer, Map<String, Integer> targets) {
        return new WarmPool(warmer, targets, Runnable::run, OWNERSHIP);
    }

    /**
     * Deterministic in-memory stand-in for Podman: hands out fake container ids, tracks liveness, and
     * — crucially — keeps its own record of what is RUNNING on the "host" independently of the pool's
     * bookkeeping, so a container the pool has lost track of is visible to the test exactly as a
     * leftover would be to {@code podman ps}.
     */
    private static final class FakeWarmer implements Warmer {
        final AtomicInteger spawns = new AtomicInteger();
        final AtomicInteger cools = new AtomicInteger();
        final Set<String> live = ConcurrentHashMap.newKeySet();
        /** Everything on the "host" carrying this daemon's owner label, in creation order. */
        final Map<String, OwnedContainer> host = new LinkedHashMap<>();
        /** WarmSpecs received, in call order — used to assert cap precedence. */
        final List<WarmSpec> specs = new java.util.ArrayList<>();
        volatile boolean failNext = false;

        @Override
        public String kind() {
            return "fake";
        }

        @Override
        public WarmSlot warm(WarmSpec spec) throws WarmException {
            if (failNext) {
                throw new WarmException("warmer down");
            }
            specs.add(spec);
            String cid = "c" + spawns.incrementAndGet();
            live.add(cid);
            host.put(cid, new OwnedContainer(cid, HardeningArgs.ROLE_WARM, OWNERSHIP.bootId()));
            // Slot image = the requested key — the identity the pool now guarantees.
            return new WarmSlot(spec.language(), "w_" + cid, cid, kind(), spec.imageRef(), Instant.now());
        }

        @Override
        public void cool(WarmSlot slot) {
            cools.incrementAndGet();
            if (slot != null) {
                remove(slot.containerId());
            }
        }

        @Override
        public void remove(String containerId) {
            live.remove(containerId);
            host.remove(containerId);
        }

        @Override
        public List<OwnedContainer> ownedContainers() {
            return List.copyOf(host.values());
        }

        @Override
        public boolean alive(WarmSlot slot) {
            return slot != null && live.contains(slot.containerId());
        }

        void kill(String containerId) {
            live.remove(containerId);
        }

        /** Plants a container on the "host" that the pool has no slot for. */
        void plantOrphan(String containerId, String role, String bootId) {
            live.add(containerId);
            host.put(containerId, new OwnedContainer(containerId, role, bootId));
        }
    }

    @Test
    void reconcile_fills_reservoir_then_acquire_pops_and_parallel_refills() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 3));

        WarmPool.ReconcileReport report = pool.reconcile();
        assertEquals(3, pool.size(PYTHON_IMG), "reservoir filled to target");
        assertEquals(new WarmPool.Counts(3, 3), pool.counts(), "telemetry sees built and target totals");
        assertEquals(3, report.refills().getOrDefault(PYTHON_IMG, 0));
        assertEquals(3, warmer.spawns.get());

        // Single-use model: acquire pops an idle slot AND warms its replacement in parallel (the test's
        // direct executor runs the refill inline), so the reservoir stays at target and a fresh
        // container is spawned — "consume one, warm a fresh one".
        Optional<WarmSlot> acquired = pool.acquire(PYTHON_IMG);
        assertTrue(acquired.isPresent());
        assertTrue(acquired.get().hasContainer());
        assertTrue(warmer.alive(acquired.get()), "handed-out slot is live");
        assertEquals(3, pool.size(PYTHON_IMG), "reservoir refilled to target in parallel");
        assertEquals(4, warmer.spawns.get(), "acquire triggered exactly one replacement warm");

        // Reconcile is a no-op now that the reservoir is already full.
        pool.reconcile();
        assertEquals(3, pool.size(PYTHON_IMG));
        assertEquals(4, warmer.spawns.get());
    }

    @Test
    void acquire_on_empty_reservoir_warms_on_demand() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of()); // no targets, empty reservoir

        Optional<WarmSlot> slot = pool.acquire(NODE_IMG);
        assertTrue(slot.isPresent(), "cold acquire warms on demand");
        assertEquals(1, warmer.spawns.get());
    }

    @Test
    void acquire_skips_dead_idle_slots_and_cools_them() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 2));
        pool.reconcile();
        assertEquals(2, pool.size(PYTHON_IMG));

        // Kill both idle containers behind the pool's back; acquire must skip them + warm fresh.
        warmer.live.forEach(warmer::kill);
        Optional<WarmSlot> slot = pool.acquire(PYTHON_IMG);
        assertTrue(slot.isPresent());
        assertTrue(warmer.alive(slot.get()), "handed-out slot is a live, freshly-warmed one");
        assertTrue(warmer.cools.get() >= 2, "dead idle slots were cooled");
    }

    @Test
    void release_is_single_use_and_destroys_the_container() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 1));
        pool.reconcile(); // 1 idle slot

        // acquire pops the idle slot; the parallel refill warms its replacement.
        WarmSlot a = pool.acquire(PYTHON_IMG).orElseThrow();
        assertTrue(a.hasContainer());
        assertEquals(1, pool.size(PYTHON_IMG), "reservoir refilled after the pop");

        // Single-use: releasing a consumed slot TEARS DOWN its container (never returns it to the pool).
        int coolsBefore = warmer.cools.get();
        pool.release(a);
        assertEquals(coolsBefore + 1, warmer.cools.get(), "released container is cooled");
        assertFalse(warmer.alive(a), "released container is destroyed, not reused");
        assertEquals(1, pool.size(PYTHON_IMG), "reservoir still holds its fresh replacement");
    }

    @Test
    void reap_idle_cools_slots_older_than_max_age() throws Exception {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 2));
        pool.reconcile();
        assertEquals(2, pool.size(PYTHON_IMG));

        Thread.sleep(20);
        int reaped = pool.reapIdle(Duration.ofMillis(5));
        assertEquals(2, reaped, "both idle slots older than 5ms reaped");
        assertEquals(0, pool.size(PYTHON_IMG), "reservoir drained by reap");
        assertTrue(warmer.cools.get() >= 2, "reaped slots were cooled");
    }

    /**
     * The defect: the operator lowers the warm target and the containers above it stay open. Reconcile
     * only ever counted UP to the target, so nothing released the surplus and the live pool sat above
     * its configured size indefinitely.
     */
    @Test
    void reconcile_converges_downwards_when_the_reservoir_is_over_target() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 6));
        pool.reconcile();
        assertEquals(6, pool.size(PYTHON_IMG));
        assertEquals(6, warmer.host.size(), "six containers open on the host");

        pool.setTarget(PYTHON_IMG, 2);
        WarmPool.ReconcileReport report = pool.reconcile();

        assertEquals(2, pool.size(PYTHON_IMG), "reservoir converged on the lowered target");
        assertEquals(4, report.trimmed(), "the four surplus slots were cooled, not merely un-replaced");
        assertEquals(2, warmer.host.size(), "and the surplus containers are gone from the host");
    }

    /** An image dropped to zero must release everything, not park its containers forever. */
    @Test
    void reconcile_drains_a_language_whose_target_went_to_zero() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 3, RUBY_IMG, 2));
        pool.reconcile();
        assertEquals(5, warmer.host.size());

        pool.setTarget(RUBY_IMG, 0);
        pool.reconcile();

        assertEquals(0, pool.size(RUBY_IMG));
        assertEquals(3, warmer.host.size(), "only the python reservoir is left open");
    }

    /**
     * A slot handed to a dispatch that never comes back — the daemon was killed mid-run, or the
     * dispatch died before its {@code finally} — leaves a container the reservoir has no slot for.
     * Nothing used to look for it; reconcile now asks the host what it owns and reclaims the
     * difference.
     */
    @Test
    void reconcile_reclaims_a_leaked_container_the_reservoir_lost_track_of() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 1));
        pool.reconcile();

        // A warm slot from THIS run that is in neither the reservoir nor a lease: unreachable.
        warmer.plantOrphan("leaked-warm", HardeningArgs.ROLE_WARM, OWNERSHIP.bootId());
        // A job container left behind by a run of this daemon that was SIGKILLed.
        warmer.plantOrphan("leaked-job", HardeningArgs.ROLE_JOB, "boot-0");

        WarmPool.ReconcileReport report = pool.reconcile();

        assertEquals(2, report.reclaimed());
        assertFalse(warmer.host.containsKey("leaked-warm"), "stray warm container removed off the host");
        assertFalse(warmer.host.containsKey("leaked-job"), "dead run's job container removed off the host");
        assertEquals(1, pool.size(PYTHON_IMG), "the healthy reservoir is untouched");
    }

    /** A job container of the CURRENT run may be mid-run; only its creator knows, so reconcile leaves it. */
    @Test
    void reconcile_never_reclaims_a_job_container_of_the_current_run() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of());
        warmer.plantOrphan("cold-job-in-flight", HardeningArgs.ROLE_JOB, OWNERSHIP.bootId());

        WarmPool.ReconcileReport report = pool.reconcile();

        assertEquals(0, report.reclaimed());
        assertTrue(warmer.host.containsKey("cold-job-in-flight"),
                "a live cold dispatch owns this container's lifecycle; reconcile must not race it");
    }

    /** A leased slot IS mid-run, so it must survive a reconcile even though it is not in the reservoir. */
    @Test
    void reconcile_never_reclaims_a_slot_that_is_out_with_a_dispatch() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 1));
        pool.reconcile();
        WarmSlot inFlight = pool.acquire(PYTHON_IMG).orElseThrow();
        assertEquals(1, pool.leasedCount());

        WarmPool.ReconcileReport report = pool.reconcile();

        assertEquals(0, report.reclaimed());
        assertTrue(warmer.alive(inFlight), "the container running the job is still up");
    }

    /**
     * Graceful shutdown: idle slots go at once, and a dispatch is given the grace window to hand its
     * slot back before the pool forces it down. Nothing the daemon was holding stays open.
     */
    @Test
    void drain_stops_idle_immediately_and_waits_for_a_dispatch_before_forcing_it() throws Exception {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 3));
        pool.reconcile();
        WarmSlot inFlight = pool.acquire(PYTHON_IMG).orElseThrow();
        assertEquals(1, pool.leasedCount());

        // The dispatch finishes just inside the grace window and returns its slot itself.
        Thread dispatch = new Thread(() -> {
            try {
                Thread.sleep(120);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            pool.release(inFlight);
        });
        dispatch.start();

        int stopped = pool.drain(Duration.ofSeconds(5));
        dispatch.join();

        assertTrue(stopped >= 3, "every idle slot was stopped");
        assertEquals(0, pool.leasedCount());
        assertEquals(0, pool.size(PYTHON_IMG));
        assertTrue(warmer.host.isEmpty(), "the daemon left no container open: " + warmer.host.keySet());
    }

    /** A dispatch that never returns its slot must not keep the container alive past the grace. */
    @Test
    void drain_forces_down_a_slot_that_is_never_handed_back() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 2));
        pool.reconcile();
        WarmSlot abandoned = pool.acquire(PYTHON_IMG).orElseThrow();

        pool.drain(Duration.ofMillis(50));

        assertEquals(0, pool.leasedCount());
        assertFalse(warmer.alive(abandoned), "the abandoned lease was forced down after the grace");
        assertTrue(warmer.host.isEmpty(), "nothing left open: " + warmer.host.keySet());
    }

    /** Cold, single-job containers never enter the reservoir, so the drain has to sweep them by ownership. */
    @Test
    void drain_also_stops_cold_job_containers_the_reservoir_never_saw() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 1));
        pool.reconcile();
        warmer.plantOrphan("cold-job", HardeningArgs.ROLE_JOB, OWNERSHIP.bootId());

        pool.drain(Duration.ZERO);

        assertTrue(warmer.host.isEmpty(), "the cold job container went down with the daemon");
    }

    /**
     * Uninstall is terminal and forceful. Latching the pool shut is what stops the 30s reconcile loop
     * from warming a fresh reservoir in the seconds between the purge and the process exiting.
     */
    @Test
    void shutdown_leaves_nothing_open_and_the_pool_never_warms_again() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 4));
        pool.reconcile();
        assertEquals(4, warmer.host.size());
        pool.acquire(PYTHON_IMG);

        int stopped = pool.shutdown();

        assertTrue(stopped >= 4);
        assertTrue(pool.isClosed());
        assertTrue(warmer.host.isEmpty(), "uninstall left containers behind: " + warmer.host.keySet());

        // Everything that could re-open a container is now inert.
        int spawnsAtShutdown = warmer.spawns.get();
        pool.setTarget(PYTHON_IMG, 6);
        pool.reconcile();
        assertTrue(pool.acquire(PYTHON_IMG).isEmpty(), "a closed pool hands out nothing");
        assertEquals(spawnsAtShutdown, warmer.spawns.get(), "a closed pool warmed a container anyway");
        assertTrue(warmer.host.isEmpty());
    }

    @Test
    void reconcile_records_warmer_errors_without_spinning() {
        FakeWarmer warmer = new FakeWarmer();
        warmer.failNext = true;
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 3));

        WarmPool.ReconcileReport report = pool.reconcile();
        assertEquals(0, pool.size(PYTHON_IMG), "no slots warmed while warmer is down");
        assertFalse(report.errors().isEmpty(), "warmer error surfaced");
    }

    @Test
    void status_rows_are_image_keyed_and_retain_language_and_caps() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of());
        pool.registerImage(PYTHON_IMG, "python", 6);
        String customImg = "registry.example/acme/app:1.2";
        pool.setTarget(customImg, null, 2, new WarmTargetEntry.Caps(1.5, 256, 128));

        pool.reconcile();
        List<WarmPool.LanguageStatus> rows = pool.status();

        WarmPool.LanguageStatus pythonRow = rows.stream()
                .filter(r -> PYTHON_IMG.equals(r.imageRef()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing python image row: " + rows));
        assertEquals("python", pythonRow.language());
        assertEquals(6, pythonRow.defaultTarget());

        WarmPool.LanguageStatus customRow = rows.stream()
                .filter(r -> customImg.equals(r.imageRef()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing custom image row: " + rows));
        assertNull(customRow.language());
        assertEquals(2, customRow.target());
        assertNotNull(customRow.caps());
        assertEquals(1.5, customRow.caps().cpu());
        assertEquals(256, customRow.caps().ramMb());
        assertEquals(128, customRow.caps().diskMb());
    }

    @Test
    void entries_roundtrip_covers_targets_and_caps() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of());
        pool.registerImage(PYTHON_IMG, "python", 6);
        String customImg = "registry.example/acme/app:1.2";
        pool.setTarget(customImg, null, 2, new WarmTargetEntry.Caps(1.5, 256, 128));

        List<WarmTargetEntry> entries = pool.entries();

        WarmTargetEntry python = entries.stream()
                .filter(e -> PYTHON_IMG.equals(e.imageRef()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing python entry: " + entries));
        assertEquals("python", python.language());
        assertEquals(0, python.target(), "registerImage alone does not raise the live target");
        assertNull(python.caps());

        WarmTargetEntry custom = entries.stream()
                .filter(e -> customImg.equals(e.imageRef()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing custom entry: " + entries));
        assertNull(custom.language());
        assertEquals(2, custom.target());
        assertNotNull(custom.caps());
        assertEquals(1.5, custom.caps().cpu());
        assertEquals(256, custom.caps().ramMb());
        assertEquals(128, custom.caps().diskMb());
    }

    @Test
    void reconcile_warm_spec_carries_registered_per_image_caps() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of());
        String customImg = "registry.example/acme/app:caps";
        pool.setTarget(customImg, null, 1, new WarmTargetEntry.Caps(1.5, 256, 128));

        pool.reconcile();

        assertEquals(1, warmer.specs.size());
        Warmer.WarmSpec spec = warmer.specs.get(0);
        assertEquals(customImg, spec.imageRef());
        assertEquals(1.5, spec.cpus());
        assertEquals(256, spec.ramMb());
        assertEquals(128, spec.diskMb());
    }

    @Test
    void acquire_registered_caps_win_over_adhoc_job_caps() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of());
        String customImg = "registry.example/acme/app:d56";
        // target 0 → empty reservoir; acquire warms on demand via warmSpecForAcquire.
        pool.setTarget(customImg, null, 0, new WarmTargetEntry.Caps(2.0, 512, 256));

        assertTrue(pool.acquire(customImg, null,
                new WarmTargetEntry.Caps(9.0, 99, 9)).isPresent());

        assertEquals(1, warmer.specs.size());
        Warmer.WarmSpec spec = warmer.specs.get(0);
        assertEquals(2.0, spec.cpus(), "registered cpu wins (D56)");
        assertEquals(512, spec.ramMb(), "registered ram wins (D56)");
        assertEquals(256, spec.diskMb(), "registered disk wins (D56)");
    }

    @Test
    void acquire_uses_job_caps_when_image_has_no_registered_caps() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of());
        String customImg = "registry.example/acme/app:job-caps";

        assertTrue(pool.acquire(customImg, null,
                new WarmTargetEntry.Caps(1.25, 128, 64)).isPresent());

        assertEquals(1, warmer.specs.size());
        Warmer.WarmSpec spec = warmer.specs.get(0);
        assertEquals(1.25, spec.cpus());
        assertEquals(128, spec.ramMb());
        assertEquals(64, spec.diskMb());
    }

    /**
     * Image rebuild: drain idle slots for one ref only. Target stays, the other image is
     * untouched, and invalidate itself must not spawn replacements (next acquire warms).
     */
    @Test
    void invalidateImage_cools_one_image_idle_slots_without_refill_or_target_change() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 3, NODE_IMG, 2));
        pool.reconcile();
        assertEquals(3, pool.size(PYTHON_IMG));
        assertEquals(2, pool.size(NODE_IMG));
        assertEquals(5, warmer.spawns.get());
        int coolsBefore = warmer.cools.get();

        int cooled = pool.invalidateImage(PYTHON_IMG);

        assertEquals(3, cooled);
        assertEquals(0, pool.size(PYTHON_IMG), "python idle reservoir drained");
        assertEquals(2, pool.size(NODE_IMG), "other image idle slots untouched");
        assertEquals(3, pool.target(PYTHON_IMG), "target is unchanged");
        assertEquals(2, pool.target(NODE_IMG));
        assertEquals(coolsBefore + 3, warmer.cools.get(), "cooler called for the python idle slots");
        assertEquals(5, warmer.spawns.get(), "invalidate must not spawn replacements");

        Optional<WarmSlot> next = pool.acquire(PYTHON_IMG);
        assertTrue(next.isPresent(), "subsequent acquire warms on demand");
        assertTrue(warmer.spawns.get() > 5, "acquire after invalidate spawned; we did not eagerly refill");
    }

    @Test
    void invalidateImage_unknown_or_blank_ref_is_zero() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 1));
        pool.reconcile();
        int coolsBefore = warmer.cools.get();
        int spawnsBefore = warmer.spawns.get();

        assertEquals(0, pool.invalidateImage("registry.example/never-seen:1"));
        assertEquals(0, pool.invalidateImage(null));
        assertEquals(0, pool.invalidateImage("   "));
        assertEquals(1, pool.size(PYTHON_IMG));
        assertEquals(coolsBefore, warmer.cools.get());
        assertEquals(spawnsBefore, warmer.spawns.get());
    }

    /**
     * Direct executor refills idle to target on acquire. Invalidate must cool those idle
     * replacements and leave the leased in-flight container alone.
     */
    @Test
    void invalidateImage_leaves_leased_slot_alive() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 3));
        pool.reconcile();
        WarmSlot leased = pool.acquire(PYTHON_IMG).orElseThrow();
        assertEquals(1, pool.leasedCount());
        assertEquals(3, pool.size(PYTHON_IMG), "direct executor refilled idle to target after acquire");
        int spawnsAfterAcquire = warmer.spawns.get();

        int cooled = pool.invalidateImage(PYTHON_IMG);

        assertEquals(3, cooled, "idle replacements cooled; leased slot is not idle");
        assertEquals(0, pool.size(PYTHON_IMG));
        assertEquals(1, pool.leasedCount());
        assertTrue(warmer.alive(leased), "leased container still alive");
        assertEquals(3, pool.target(PYTHON_IMG));
        assertEquals(spawnsAfterAcquire, warmer.spawns.get(), "invalidate must not spawn");
    }

    @Test
    void invalidateImage_closed_pool_is_zero() {
        FakeWarmer warmer = new FakeWarmer();
        WarmPool pool = pool(warmer, Map.of(PYTHON_IMG, 2));
        pool.reconcile();
        pool.shutdown();

        assertEquals(0, pool.invalidateImage(PYTHON_IMG));
    }
}
