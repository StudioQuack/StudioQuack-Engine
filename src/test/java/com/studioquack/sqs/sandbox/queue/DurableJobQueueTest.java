package com.studioquack.sqs.sandbox.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The durable WAL job queue: submit → lease → finish, and (crucially) a fresh queue that {@link
 * DurableJobQueue#openAt replays} the same WAL file reconstructs the terminal state (no-loss across
 * restarts). Also covers idempotent finish, cancel, and torn-tail tolerance.
 */
class DurableJobQueueTest {

    private final ObjectMapper mapper = new ObjectMapper();
    /** Non-blank env so existing list/lease/count assertions stay meaningful under install scoping. */
    private static final String TEST_ENV = "test-env";

    @Test
    void submit_lease_finish_then_replay_reconstructs_terminal_state(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("jobs.wal");

        DurableJobQueue q = DurableJobQueue.openAt(wal, mapper, TEST_ENV);
        q.submit("job-1", "python", "print('hi')");
        assertEquals(1, q.queuedCount());

        Optional<DurableJobQueue.Job> leased = q.lease();
        assertTrue(leased.isPresent());
        assertEquals(JobState.RUNNING, leased.get().state);
        assertEquals("python", leased.get().language);

        q.finish("job-1", JobState.SUCCEEDED, 0, "hi\n", "", null, Map.of("out", "42"));
        DurableJobQueue.Job done = q.get("job-1").orElseThrow();
        assertEquals(JobState.SUCCEEDED, done.state);
        assertEquals(0, done.exitCode);
        assertEquals("42", done.outputs.get("out"));
        q.close();

        // A brand-new queue replaying the SAME WAL file must reconstruct the terminal state.
        DurableJobQueue replayed = DurableJobQueue.openAt(wal, mapper, TEST_ENV);
        DurableJobQueue.Job r = replayed.get("job-1").orElseThrow();
        assertEquals(JobState.SUCCEEDED, r.state, "terminal state survives a restart");
        assertEquals("python", r.language);
        assertEquals("print('hi')", r.source, "source (not a secret) is WAL-persisted");
        assertNotNull(r.outputs);
        assertEquals("42", r.outputs.get("out"), "structured outputs survive the restart");
        assertEquals(0, replayed.queuedCount(), "terminal job is not re-queued");
        replayed.close();
    }

    @Test
    void finish_is_idempotent(@TempDir Path dir) throws IOException {
        DurableJobQueue q = DurableJobQueue.openAt(dir.resolve("jobs.wal"), mapper, TEST_ENV);
        q.submit("j", "python", "x");
        q.lease();
        q.finish("j", JobState.SUCCEEDED, 0, "a", "", null, Map.of());
        q.finish("j", JobState.FAILED, 1, "b", "boom", "late", Map.of()); // must be ignored
        DurableJobQueue.Job job = q.get("j").orElseThrow();
        assertEquals(JobState.SUCCEEDED, job.state, "first terminal finish wins");
        assertEquals(0, job.exitCode);
        q.close();
    }

    @Test
    void cancel_removes_from_ready_and_is_terminal(@TempDir Path dir) throws IOException {
        DurableJobQueue q = DurableJobQueue.openAt(dir.resolve("jobs.wal"), mapper, TEST_ENV);
        q.submit("j", "python", "x");
        q.cancel("j", "operator cancel");
        assertEquals(0, q.queuedCount());
        assertTrue(q.lease().isEmpty(), "cancelled job is not leasable");
        assertEquals(JobState.CANCELLED, q.get("j").orElseThrow().state);
        q.close();
    }

    @Test
    void running_job_is_requeued_on_replay_after_a_crash(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("jobs.wal");
        DurableJobQueue q = DurableJobQueue.openAt(wal, mapper, TEST_ENV);
        q.submit("j", "python", "x");
        q.lease(); // now RUNNING — simulate a crash before finish
        q.close();

        DurableJobQueue replayed = DurableJobQueue.openAt(wal, mapper, TEST_ENV);
        assertEquals(1, replayed.queuedCount(), "a RUNNING job left by a crash is re-queued");
        assertTrue(replayed.lease().isPresent(), "re-queued job is leasable again");
        replayed.close();
    }

    @Test
    void language_aware_lease_skips_other_languages(@TempDir Path dir) throws IOException {
        DurableJobQueue q = DurableJobQueue.openAt(dir.resolve("jobs.wal"), mapper, TEST_ENV);
        q.submit("py", "python", "a");
        q.submit("js", "javascript", "b");
        Optional<DurableJobQueue.Job> js = q.lease("javascript");
        assertTrue(js.isPresent());
        assertEquals("js", js.get().id, "language-aware lease picks the matching job");
        assertEquals(1, q.queuedCount(), "the python job stays queued");
        q.close();
    }

    @Test
    void lease_by_id_and_counts_track_the_exact_running_job(@TempDir Path dir) throws IOException {
        DurableJobQueue q = DurableJobQueue.openAt(dir.resolve("jobs.wal"), mapper, TEST_ENV);
        q.submit("first", "python", "a");
        q.submit("second", "python", "b");

        Optional<DurableJobQueue.Job> leased = q.leaseById("second");

        assertTrue(leased.isPresent());
        assertEquals("second", leased.orElseThrow().id);
        assertEquals(new DurableJobQueue.Counts(1, 1, 2), q.counts());
        assertEquals(JobState.QUEUED, q.get("first").orElseThrow().state);
        assertEquals(JobState.RUNNING, q.get("second").orElseThrow().state);
        q.close();
    }

    @Test
    void torn_tail_record_is_tolerated_on_replay(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("jobs.wal");
        DurableJobQueue q = DurableJobQueue.openAt(wal, mapper, TEST_ENV);
        q.submit("good", "python", "x");
        q.close();

        // Append 12 bytes of garbage to simulate a torn write mid-record.
        try (RandomAccessFile raf = new RandomAccessFile(wal.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
        }

        DurableJobQueue replayed = DurableJobQueue.openAt(wal, mapper, TEST_ENV);
        assertTrue(replayed.get("good").isPresent(), "records before the torn tail survive");
        assertFalse(replayed.snapshot().isEmpty());
        replayed.close();
    }
}
