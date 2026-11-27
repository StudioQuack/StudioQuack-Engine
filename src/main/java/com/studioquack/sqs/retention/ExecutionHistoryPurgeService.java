package com.studioquack.sqs.retention;

import java.time.Instant;

/**
 * Deletes execution history older than an operator-chosen instant. A run is removed only when
 * every instance for that business key has terminated before the cutoff.
 */
public interface ExecutionHistoryPurgeService {

    /**
     * Hard-deletes {@code execution_context} rows whose run terminated strictly before
     * {@code olderThan}, and ONLY for runs the engine confirms are fully terminated-and-aged (guard).
     * Never throws for an expected/validated condition — a {@code null} instant or a store hiccup
     * returns a zero result so the calling command handler can report cleanly.
     *
     * @param olderThan The exclusive upper bound; runs still live/parked or that ended at/after this
     * instant are left untouched
     * @return The number of rows deleted and the number of distinct runs affected
     */
    PurgeResult deleteBefore(Instant olderThan);

    /**
     * Hard-deletes history for runs terminated within {@code [from, to)} (open lower bound when
     * {@code from} is null). Implementations obey the exact same terminated-instance guard as
     * {@link #deleteBefore(Instant)} and commit in bounded batches.
     */
    default PurgeResult deleteInRange(Instant from, Instant to) {
        return deleteInRange(from, to, null);
    }

    /**
     * Same as {@link #deleteInRange(Instant, Instant)}, notifying {@code progress} after each
     * committed batch so long-running purges can report live counts.
     */
    default PurgeResult deleteInRange(Instant from, Instant to, ProgressListener progress) {
        if (from == null) {
            return deleteBefore(to);
        }
        return new PurgeResult(0, 0, 0, 0);
    }

    /** Callback notified after each committed batch with cumulative totals so far. */
    @FunctionalInterface
    interface ProgressListener {
        void onBatchCommitted(long rowsDeleted, long runsAffected, long sandboxLogRowsDeleted, long activitiRowsDeleted);
    }

    /**
     * Outcome of a purge pass — serialised verbatim into the command result.
     *
     * @param rowsDeleted          total {@code execution_context} rows hard-deleted
     * @param runsAffected         distinct terminated-and-aged runs whose rows were purged
     * @param sandboxLogRowsDeleted total {@code sandbox_log} rows hard-deleted for the same runs
     * @param activitiRowsDeleted  total {@code ACT_HI_PROCINST} rows hard-deleted (children cascade)
     */
    record PurgeResult(long rowsDeleted, long runsAffected, long sandboxLogRowsDeleted, long activitiRowsDeleted) {

        /** Backward-compatible 2-arg shape. */
        public PurgeResult(long rowsDeleted, long runsAffected) {
            this(rowsDeleted, runsAffected, 0L, 0L);
        }

        /** Backward-compatible 3-arg shape. */
        public PurgeResult(long rowsDeleted, long runsAffected, long sandboxLogRowsDeleted) {
            this(rowsDeleted, runsAffected, sandboxLogRowsDeleted, 0L);
        }
    }
}
