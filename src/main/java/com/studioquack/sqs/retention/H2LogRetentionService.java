package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Deletes the local database execution history older than an instant, and only for runs whose
 * instances have all terminated before that instant.
 */
@Slf4j
@Service
public class H2LogRetentionService implements ExecutionHistoryPurgeService {

    /** Bulk-delete IN-clause batch size (kept identical to the reaper for parity). */
    private static final int DELETE_BATCH = 500;

    private final ExecutionHistoryRetentionRepository retentionRepository;
    private final SqsEngine sqsEngine;

    // The per-node sandbox-log store purges WITH its run under the same guard. Optional field
    // injection keeps the existing 2-arg constructor (and its tests) untouched.
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.status.log.SandboxLogRetentionRepository sandboxLogRetentionRepository;

    // Sub-PI invocation index purges WITH its run under the same guard. Optional field
    // injection keeps the existing 2-arg constructor (and its tests) untouched.
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.execution.store.SubPiInvocationRepository subPiInvocationRepository;

    public H2LogRetentionService(ExecutionHistoryRetentionRepository retentionRepository, SqsEngine sqsEngine) {
        this.retentionRepository = retentionRepository;
        this.sqsEngine = sqsEngine;
    }

    @Override
    @Transactional
    public PurgeResult deleteBefore(Instant olderThan) {
        if (olderThan == null) {
            return new PurgeResult(0, 0);
        }
        OffsetDateTime cutoffAt = olderThan.atOffset(ZoneOffset.UTC);
        Date cutoff = Date.from(olderThan);

        List<String> candidateRuns = retentionRepository.findDistinctWorkflowExecutionIdsCreatedBefore(cutoffAt);
        if (candidateRuns.isEmpty()) {
            return new PurgeResult(0, 0);
        }
        // Keep ONLY runs the engine confirms are fully terminated and last ended before olderThan — a
        // running/parked (or recently-finished) run is never purged (safety).
        List<String> purgeableRuns = new ArrayList<>();
        for (String runId : candidateRuns) {
            if (sqsEngine.allInstancesTerminatedBefore(runId, cutoff)) {
                purgeableRuns.add(runId);
            }
        }
        if (purgeableRuns.isEmpty()) {
            log.info("log-delete[olderThan={}]: {} candidate run(s); none terminated+aged — nothing purged",
                    olderThan, candidateRuns.size());
            return new PurgeResult(0, 0);
        }
        int deleted = 0;
        for (int i = 0; i < purgeableRuns.size(); i += DELETE_BATCH) {
            List<String> batch = purgeableRuns.subList(i, Math.min(i + DELETE_BATCH, purgeableRuns.size()));
            deleted += retentionRepository.deleteByWorkflowExecutionIdIn(batch);
            purgeSubPiInvocations(batch);
        }
        int sandboxDeleted = purgeSandboxLogs(purgeableRuns);
        log.info("log-delete[olderThan={}]: purged {} execution_context row(s) + {} sandbox_log row(s) "
                + "across {} terminated run(s)", olderThan, deleted, sandboxDeleted, purgeableRuns.size());
        return new PurgeResult(deleted, purgeableRuns.size(), sandboxDeleted);
    }

    /**
     * Delete the {@code sandbox_log} rows of the same engine-confirmed terminated-and-aged runs. LOCAL
     * ONLY: the control-plane copy in is the continuation of the record — a local prune can never
     * delete it.
     */
    private int purgeSandboxLogs(List<String> purgeableRuns) {
        if (sandboxLogRetentionRepository == null || purgeableRuns.isEmpty()) {
            return 0;
        }
        int deleted = 0;
        for (int i = 0; i < purgeableRuns.size(); i += DELETE_BATCH) {
            List<String> batch = purgeableRuns.subList(i, Math.min(i + DELETE_BATCH, purgeableRuns.size()));
            deleted += sandboxLogRetentionRepository.deleteByWorkflowExecutionIdIn(batch);
        }
        return deleted;
    }

    /**
     * Delete {@code sub_pi_invocation} rows that reference this batch as parent or child so the
     * index never outlives the {@code execution_context} rows it points at. Best-effort: a
     * failure is logged (shape-only) and never fails the enclosing execution_context purge.
     */
    private void purgeSubPiInvocations(List<String> batch) {
        if (subPiInvocationRepository == null || batch == null || batch.isEmpty()) {
            return;
        }
        try {
            int deleted = subPiInvocationRepository.deleteByParentOrChildExecutionIdIn(batch);
            log.debug("log-delete: purged {} sub_pi_invocation row(s) for {} run(s)", deleted, batch.size());
        } catch (RuntimeException e) {
            log.warn("log-delete: sub_pi_invocation purge failed (runs={}): {}", batch.size(), e.toString());
        }
    }
}
