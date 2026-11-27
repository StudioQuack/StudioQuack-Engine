package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.store.OperatorInputOverrideRepository;
import com.studioquack.sqs.execution.store.SubPiInvocationRepository;
import com.studioquack.sqs.files.ClientFileRepository;
import com.studioquack.sqs.files.ClientFileStore;
import com.studioquack.sqs.status.log.SandboxLogRetentionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Deletes local database execution history older than an instant (or within a range) in bounded
 * per-batch transactions, purging only runs whose instances have all terminated before the cutoff.
 */
@Slf4j
@Service
public class H2LogRetentionService implements ExecutionHistoryPurgeService {

    /** Per-transaction batch size: bounds IN-clause parameters and lock duration. */
    static final int DELETE_BATCH = 200;

    private final ExecutionHistoryRetentionRepository retentionRepository;
    private final SqsEngine sqsEngine;

    @Autowired(required = false)
    private JdbcTemplate jdbcTemplate;

    @Autowired(required = false)
    private PlatformTransactionManager transactionManager;

    @Autowired(required = false)
    private SandboxLogRetentionRepository sandboxLogRetentionRepository;

    @Autowired(required = false)
    private SubPiInvocationRepository subPiInvocationRepository;

    @Autowired(required = false)
    private OperatorInputOverrideRepository operatorInputOverrideRepository;

    @Autowired(required = false)
    private ClientFileRepository clientFileRepository;

    @Autowired(required = false)
    private ActivitiHistoryRetentionRepository activitiHistoryRetentionRepository;

    public H2LogRetentionService(ExecutionHistoryRetentionRepository retentionRepository, SqsEngine sqsEngine) {
        this.retentionRepository = retentionRepository;
        this.sqsEngine = sqsEngine;
    }

    @Override
    public PurgeResult deleteBefore(Instant olderThan) {
        return deleteInRange(null, olderThan, null);
    }

    @Override
    public PurgeResult deleteInRange(Instant from, Instant to, ProgressListener progress) {
        if (to == null) {
            return new PurgeResult(0L, 0L, 0L, 0L);
        }
        if (from != null && !from.isBefore(to)) {
            return new PurgeResult(0L, 0L, 0L, 0L);
        }
        if (jdbcTemplate != null) {
            return deleteKeysetBatched(from, to, progress);
        }
        return deleteViaRepositoryFallback(to, progress);
    }

    private PurgeResult deleteKeysetBatched(Instant from, Instant to, ProgressListener progress) {
        long totalExecDeleted = 0L;
        long totalSandboxDeleted = 0L;
        long totalActivitiDeleted = 0L;
        long totalRunsAffected = 0L;

        String cursor = null;
        while (true) {
            List<String> batch = TerminatedRunQuerySupport.nextTerminatedRunBatch(
                    jdbcTemplate, from, to, cursor, DELETE_BATCH, true);
            if (batch.isEmpty()) {
                break;
            }
            cursor = batch.get(batch.size() - 1);

            BatchCounts counts = executeInBatchTransaction(() -> purgeRunBatch(batch));
            totalExecDeleted += counts.execRows();
            totalSandboxDeleted += counts.sandboxRows();
            totalActivitiDeleted += counts.activitiRows();
            totalRunsAffected += batch.size();

            if (progress != null) {
                progress.onBatchCommitted(totalExecDeleted, totalRunsAffected, totalSandboxDeleted, totalActivitiDeleted);
            }
        }

        if (from == null && activitiHistoryRetentionRepository != null) {
            totalActivitiDeleted += purgeOrphanedActivitiHistory(to);
        }

        log.info("log-delete[h2 range=[{},{}]]: purged {} execution_context + {} sandbox_log + {} ACT_HI_PROCINST "
                        + "row(s) across {} terminated run(s)",
                from, to, totalExecDeleted, totalSandboxDeleted, totalActivitiDeleted, totalRunsAffected);
        return new PurgeResult(totalExecDeleted, totalRunsAffected, totalSandboxDeleted, totalActivitiDeleted);
    }

    private PurgeResult deleteViaRepositoryFallback(Instant olderThan, ProgressListener progress) {
        OffsetDateTime cutoffAt = olderThan.atOffset(ZoneOffset.UTC);
        Date cutoff = Date.from(olderThan);

        Set<String> candidateRuns = new LinkedHashSet<>(
                retentionRepository.findDistinctWorkflowExecutionIdsCreatedBefore(cutoffAt));
        if (sandboxLogRetentionRepository != null) {
            candidateRuns.addAll(sandboxLogRetentionRepository.findDistinctWorkflowExecutionIdsCreatedBefore(cutoffAt));
        }
        if (candidateRuns.isEmpty()) {
            return new PurgeResult(0L, 0L, 0L, 0L);
        }
        List<String> purgeableRuns = new ArrayList<>();
        for (String runId : candidateRuns) {
            if (sqsEngine.allInstancesTerminatedBefore(runId, cutoff)) {
                purgeableRuns.add(runId);
            }
        }
        if (purgeableRuns.isEmpty()) {
            log.info("log-delete[olderThan={}]: {} candidate run(s); none terminated+aged — nothing purged",
                    olderThan, candidateRuns.size());
            return new PurgeResult(0L, 0L, 0L, 0L);
        }

        long totalExecDeleted = 0L;
        long totalSandboxDeleted = 0L;
        long totalActivitiDeleted = 0L;
        long totalRunsAffected = 0L;
        for (int i = 0; i < purgeableRuns.size(); i += DELETE_BATCH) {
            List<String> batch = purgeableRuns.subList(i, Math.min(i + DELETE_BATCH, purgeableRuns.size()));
            BatchCounts counts = executeInBatchTransaction(() -> purgeRunBatch(batch));
            totalExecDeleted += counts.execRows();
            totalSandboxDeleted += counts.sandboxRows();
            totalActivitiDeleted += counts.activitiRows();
            totalRunsAffected += batch.size();
            if (progress != null) {
                progress.onBatchCommitted(totalExecDeleted, totalRunsAffected, totalSandboxDeleted, totalActivitiDeleted);
            }
        }
        log.info("log-delete[olderThan={}]: purged {} execution_context row(s) + {} sandbox_log row(s) "
                        + "+ {} ACT_HI_PROCINST row(s) across {} terminated run(s)",
                olderThan, totalExecDeleted, totalSandboxDeleted, totalActivitiDeleted, totalRunsAffected);
        return new PurgeResult(totalExecDeleted, totalRunsAffected, totalSandboxDeleted, totalActivitiDeleted);
    }

    private BatchCounts purgeRunBatch(List<String> batch) {
        long execDeleted = retentionRepository.deleteByWorkflowExecutionIdIn(batch);
        long sandboxDeleted = sandboxLogRetentionRepository == null
                ? 0L
                : sandboxLogRetentionRepository.deleteByWorkflowExecutionIdIn(batch);
        purgeSubPiInvocations(batch);
        purgeOperatorInputOverrides(batch);
        purgeRunFiles(batch);
        long activitiDeleted = purgeActivitiHistory(batch);
        return new BatchCounts(execDeleted, sandboxDeleted, activitiDeleted);
    }

    private long purgeOrphanedActivitiHistory(Instant olderThan) {
        long totalDeleted = 0L;
        java.sql.Timestamp cutoffTs = java.sql.Timestamp.from(olderThan);
        for (int pass = 0; pass < 500; pass++) {
            List<String> orphanKeys;
            try {
                orphanKeys = activitiHistoryRetentionRepository.findOrphanedHistoryBusinessKeys(cutoffTs, DELETE_BATCH);
            } catch (RuntimeException ex) {
                log.debug("log-delete[h2]: orphaned ACT_HI lookup skipped: {}", ex.toString());
                break;
            }
            if (orphanKeys == null || orphanKeys.isEmpty()) {
                break;
            }
            List<String> batch = orphanKeys;
            long deleted = executeInBatchTransaction(() -> purgeActivitiHistory(batch));
            totalDeleted += deleted;
            if (deleted == 0L || orphanKeys.size() < DELETE_BATCH) {
                break;
            }
        }
        return totalDeleted;
    }

    private <T> T executeInBatchTransaction(java.util.function.Supplier<T> work) {
        if (transactionManager == null) {
            return work.get();
        }
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        return tx.execute(status -> work.get());
    }

    private void purgeSubPiInvocations(List<String> batch) {
        if (subPiInvocationRepository == null || batch.isEmpty()) {
            return;
        }
        try {
            subPiInvocationRepository.deleteByParentOrChildExecutionIdIn(batch);
        } catch (RuntimeException e) {
            log.warn("log-delete: sub_pi_invocation purge failed (runs={}): {}", batch.size(), e.toString());
        }
    }

    private void purgeOperatorInputOverrides(List<String> batch) {
        if (operatorInputOverrideRepository == null || batch.isEmpty()) {
            return;
        }
        try {
            operatorInputOverrideRepository.deleteByWorkflowExecutionIdIn(batch);
        } catch (RuntimeException e) {
            log.warn("log-delete: operator_input_override purge failed (runs={}): {}", batch.size(), e.toString());
        }
    }

    private void purgeRunFiles(List<String> batch) {
        if (clientFileRepository == null || batch.isEmpty()) {
            return;
        }
        try {
            clientFileRepository.deleteByScopeAndScopeKeyIn(ClientFileStore.SCOPE_RUN, batch);
        } catch (RuntimeException e) {
            log.warn("log-delete: client_file purge failed (runs={}): {}", batch.size(), e.toString());
        }
    }

    private long purgeActivitiHistory(List<String> batch) {
        if (activitiHistoryRetentionRepository == null || batch.isEmpty()) {
            return 0L;
        }
        try {
            List<String> instanceIds = activitiHistoryRetentionRepository.findProcessInstanceIdsByBusinessKeys(batch);
            if (instanceIds == null || instanceIds.isEmpty()) {
                return 0L;
            }
            long deleted = 0L;
            for (int i = 0; i < instanceIds.size(); i += DELETE_BATCH) {
                List<String> sub = instanceIds.subList(i, Math.min(i + DELETE_BATCH, instanceIds.size()));
                deleted += activitiHistoryRetentionRepository.deleteByProcessInstanceIds(sub);
            }
            return deleted;
        } catch (RuntimeException e) {
            log.warn("log-delete: ACT_HI_* purge failed (runs={}): {}", batch.size(), e.toString());
            return 0L;
        }
    }

    private record BatchCounts(long execRows, long sandboxRows, long activitiRows) {
    }
}
