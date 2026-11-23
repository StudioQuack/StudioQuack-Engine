package com.studioquack.sqs.sandbox.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Install-scoped DurableJobQueue: listing/counts/lease only see jobs stamped with the current
 * {@code environmentId}. Locks decisions so reinstall-with-preserved-data cannot surface or run
 * prior-install / legacy-unstamped WAL rows.
 */
class DurableJobQueueInstallScopeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Scoping must never become a reason the daemon stops working. {@code sqs.channel.env-id}
     * defaults to empty and is supplied by enrollment, so a daemon running without it would
     * accept work and then lease none of it — healthy-looking and completely stalled. Hiding
     * unattributable rows from a list is free; refusing to execute them is not, so an install
     * with no identity keeps leasing.
     */
    @Test
    void withNoInstallIdentity_leasingStillWorks(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("jobs.wal");

        DurableJobQueue unidentified = DurableJobQueue.openAt(wal, mapper, "");
        unidentified.submit("job-x", "python", "print(1)");

        Optional<DurableJobQueue.Job> leased = unidentified.lease();
        assertTrue(leased.isPresent(), "a daemon with no enrolled env id must still execute work");
        assertEquals("job-x", leased.get().id);
        assertEquals(JobState.RUNNING, leased.get().state);
        unidentified.close();
    }

    /**
     * A: open as env-A, submit, close; reopen same WAL as env-B → foreign job hidden from
     * snapshot/counts/lease. get/leaseById still resolve by id on the full map.
     */
    @Test
    void foreign_env_job_hidden_from_snapshot_counts_and_lease(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("jobs.wal");

        DurableJobQueue envA = DurableJobQueue.openAt(wal, mapper, "env-A");
        envA.submit("job-a", "python", "print(1)");
        assertEquals(1, envA.counts().total(), "submitter install sees its own job");
        envA.close();

        DurableJobQueue envB = DurableJobQueue.openAt(wal, mapper, "env-B");
        assertTrue(envB.snapshot().stream().noneMatch(j -> "job-a".equals(j.id)),
                "foreign env must not appear in snapshot");
        assertEquals(0, envB.counts().total(),
                "counts.total for env-B must not include env-A jobs");
        assertEquals(0, envB.queuedCount(), "queuedCount is install-scoped");
        assertTrue(envB.lease().isEmpty(), "lease must not return a foreign-env job");

        // Unscoped-by-id ops still work on the full map (in-flight / explicit id).
        assertTrue(envB.get("job-a").isPresent(), "get(id) is not install-scoped");
        Optional<DurableJobQueue.Job> byId = envB.leaseById("job-a");
        assertTrue(byId.isPresent(), "leaseById is not install-scoped");
        assertEquals(JobState.RUNNING, byId.get().state);
        envB.close();
    }

    /**
     * B: finish under env-A, reopen same WAL as env-A → terminal history preserved in snapshot.
     */
    @Test
    void same_env_reopen_preserves_terminal_history_in_snapshot(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("jobs.wal");

        DurableJobQueue first = DurableJobQueue.openAt(wal, mapper, "env-A");
        first.submit("job-hist", "python", "print(1)");
        first.lease();
        first.finish("job-hist", JobState.SUCCEEDED, 0, "ok\n", "", null, Map.of());
        first.close();

        DurableJobQueue again = DurableJobQueue.openAt(wal, mapper, "env-A");
        List<DurableJobQueue.Job> snap = again.snapshot();
        assertEquals(1, snap.size(), "same-env re-enrollment must keep history visible");
        DurableJobQueue.Job job = snap.get(0);
        assertEquals("job-hist", job.id);
        assertEquals(JobState.SUCCEEDED, job.state,
                "terminal SUCCEEDED must survive same-env reopen");
        assertEquals("env-A", job.environmentId);
        again.close();
    }

    /**
     * C: manually append a legacy submit with environmentId=null; open as env-A → hidden, not adopted.
     */
    @Test
    void legacy_unstamped_wal_rows_are_hidden_not_adopted(@TempDir Path dir) throws IOException {
        Path walPath = dir.resolve("jobs.wal");
        long now = System.currentTimeMillis();

        // Pre-existing shape: submit without environmentId (null → omitted / legacy).
        try (WriteAheadLog wal = new WriteAheadLog(walPath, mapper)) {
            wal.append(new WalRecord(
                    WalRecord.OP_SUBMIT, "legacy-job", "python", "x", JobState.QUEUED,
                    null, null, null, null, null, now, null, null, null, null));
        }

        DurableJobQueue q = DurableJobQueue.openAt(walPath, mapper, "env-A");
        assertTrue(q.snapshot().isEmpty(),
                "legacy unstamped rows are hidden, not adopted");
        assertEquals(0, q.counts().total(),
                "legacy unstamped rows are hidden, not adopted");
        assertTrue(q.lease().isEmpty(),
                "legacy unstamped rows are hidden, not adopted");
        // Still present in the full map for explicit get.
        assertTrue(q.get("legacy-job").isPresent(),
                "legacy job remains in full map; only scoped views hide it");
        q.close();
    }

    /**
     * D: N foreign + M current → snapshot.size() == M (never returns foreign; no unbounded index).
     */
    @Test
    void snapshot_size_equals_current_env_jobs_only(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("jobs.wal");
        final int foreign = 5;
        final int current = 3;

        DurableJobQueue foreignQ = DurableJobQueue.openAt(wal, mapper, "env-foreign");
        for (int i = 0; i < foreign; i++) {
            foreignQ.submit("foreign-" + i, "python", "x");
        }
        foreignQ.close();

        DurableJobQueue currentQ = DurableJobQueue.openAt(wal, mapper, "env-current");
        for (int i = 0; i < current; i++) {
            currentQ.submit("current-" + i, "python", "x");
        }

        List<DurableJobQueue.Job> snap = currentQ.snapshot();
        assertEquals(current, snap.size(),
                "snapshot size must equal current-env jobs only (foreign must not inflate)");
        Set<String> ids = snap.stream().map(j -> j.id).collect(Collectors.toSet());
        for (int i = 0; i < current; i++) {
            assertTrue(ids.contains("current-" + i));
        }
        for (int i = 0; i < foreign; i++) {
            assertFalse(ids.contains("foreign-" + i), "foreign job must not appear in snapshot");
        }
        assertEquals(current, currentQ.counts().total(),
                "counts.total must not count foreign-env jobs");
        currentQ.close();
    }

    /**
     * E: blank currentEnvironmentId → fail-closed empty snapshot even when WAL has stamped jobs.
     */
    @Test
    void blank_current_env_fail_closed_empty_snapshot(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("jobs.wal");

        DurableJobQueue stamped = DurableJobQueue.openAt(wal, mapper, "env-A");
        stamped.submit("job-a", "python", "x");
        stamped.close();

        // openAt(path, mapper) defaults to blank env (fail-closed).
        DurableJobQueue blank = DurableJobQueue.openAt(wal, mapper);
        assertTrue(blank.snapshot().isEmpty(),
                "blank current env → empty snapshot (fail closed)");
        assertEquals(0, blank.counts().total());
        assertTrue(blank.lease().isEmpty(),
                "blank current env must not lease stamped jobs");
        assertTrue(blank.get("job-a").isPresent(),
                "get(id) still finds the stamped job on the full map");
        blank.close();
    }
}
