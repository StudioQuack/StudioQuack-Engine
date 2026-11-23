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
     * Outcome of a {@link #deleteBefore(Instant)} pass — serialised verbatim into the
     * {@code log-delete} command result. the control plane/FE glue reads
     * {@code {rowsDeleted,
runsAffected}}; {@code sandboxLogRowsDeleted} is additive and ignored by
     * older readers.
     *
     * @param rowsDeleted
     * @param runsAffected Distinct terminated-and-aged runs whose rows were purged
     * @param sandboxLogRowsDeleted Total {@code sandbox_log} rows hard-deleted for the same runs (:
     * the per-node stream purges with its run, under the same terminated-instance guard).
     */
    record PurgeResult(int rowsDeleted, int runsAffected, int sandboxLogRowsDeleted) {

        /** Backward-compatible shape: no sandbox-log count. */
        public PurgeResult(int rowsDeleted, int runsAffected) {
            this(rowsDeleted, runsAffected, 0);
        }
    }
}
