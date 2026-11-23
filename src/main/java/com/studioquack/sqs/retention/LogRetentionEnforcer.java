package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.entitlement.EntitlementsHolder;
import com.studioquack.sqs.entitlement.EnvironmentEntitlements;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Deletes a finished run's local history after the logs are confirmed stored and the run has been
 * terminated longer than the retention window. A retention of {@code <= 0} skips deletion, and a
 * still-running run is never purged.
 */
@Slf4j
@Component
public class LogRetentionEnforcer {

    /**
     * Bulk-delete IN-clause batch size (bounds parameter count on Postgres; the local database is
     * unbounded).
     */
    private static final int DELETE_BATCH = 500;

    /**
     * Self-hosted keeps every run's local logs at least this long — a longer entitled window wins —
     * and deletes only runs whose logs BE confirmed it stores ({@link ReportedRunRegistry}).
     */
    public static final int SELF_HOSTED_MIN_RETENTION_DAYS = 2;

    /** Upper bound on acked runs examined per reported sweep (oldest first). */
    private static final int REPORTED_SWEEP_LIMIT = 2_000;

    private final ExecutionHistoryRetentionRepository retentionRepository;
    private final EntitlementsHolder entitlementsHolder;
    private final SqsEngine sqsEngine;
    private final DiskSpaceProbe diskProbe;
    private final boolean enabled;

    // Opt-in ("log-until-disk-full" safety valve): when disk usage crosses the high watermark, shed
    // finished-run history older than a safety floor even if the day-based window has not elapsed.
    private final boolean diskEnabled;
    private final int diskHighWatermarkPercent;
    private final long diskMinRetentionHours;

    // Optional. When absent, the reported-run purge is a no-op.
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private ReportedRunRegistry reportedRegistry;

    // The per-node sandbox-log store purges WITH its run, under the exact same terminated-instance
    // guard — never a second policy. Optional so slice tests wiring only the execution-context
    // repository keep working unchanged.
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.status.log.SandboxLogRetentionRepository sandboxLogRetentionRepository;

    // Sub-PI invocation index purges WITH its run under the same terminated-instance guard —
    // never a second policy. Optional so slice tests wiring only the execution-context
    // repository keep working unchanged.
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.execution.store.SubPiInvocationRepository subPiInvocationRepository;

    // Operator input overrides (apply-inputs) purge WITH their run under the same
    // terminated-instance guard — a run's sealed operator_input_override rows never outlive its
    // execution_context. Optional so slice tests keep wiring unchanged.
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.execution.store.OperatorInputOverrideRepository operatorInputOverrideRepository;

    // Activiti ACT_HI_* history purges WITH its run — the same guarded batch, so historyLevel=FULL
    // stops being an unbounded store (the multi-GB sqs-engine.mv.db on busy self-hosted engines).
    // Optional so slice tests keep wiring unchanged.
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private ActivitiHistoryRetentionRepository activitiHistoryRetentionRepository;

    public LogRetentionEnforcer(ExecutionHistoryRetentionRepository retentionRepository,
                                EntitlementsHolder entitlementsHolder,
                                SqsEngine sqsEngine,
                                DiskSpaceProbe diskProbe,
                                @Value("${sqs.log-retention.enabled:true}") boolean enabled,
                                @Value("${sqs.log-retention.disk.enabled:false}") boolean diskEnabled,
                                @Value("${sqs.log-retention.disk.high-watermark-percent:90}") int diskHighWatermarkPercent,
                                @Value("${sqs.log-retention.disk.min-retention-hours:24}") long diskMinRetentionHours) {
        this.retentionRepository = retentionRepository;
        this.entitlementsHolder = entitlementsHolder;
        this.sqsEngine = sqsEngine;
        this.diskProbe = diskProbe;
        this.enabled = enabled;
        this.diskEnabled = diskEnabled;
        this.diskHighWatermarkPercent = diskHighWatermarkPercent;
        this.diskMinRetentionHours = diskMinRetentionHours;
    }

    /**
     * Disk-watermark sweep, self-hosted only and opt-in via {@code sqs.log-retention.disk.enabled}
     * (off by default). Same rule as the acknowledged pass: only runs BE confirmed it stores, never
     * younger than {@link #SELF_HOSTED_MIN_RETENTION_DAYS}.
     */
    @Scheduled(fixedDelayString = "${sqs.log-retention.disk.interval-ms:300000}",
            initialDelayString = "${sqs.log-retention.disk.interval-ms:300000}")
    @Transactional
    public void enforceDiskWatermarkScheduled() {
        if (!enabled || !diskEnabled) {
            return;
        }
        enforceDiskWatermark();
    }

    /**
     * Sheds finished-run history when disk usage crosses the high watermark, regardless of the
     * day-based window. When usage is still over the watermark after shedding everything safely
     * purgeable, it logs an actionable ALERT rather than deleting live/recent data.
     */
    @Transactional
    public int enforceDiskWatermark() {
        // Disk-watermark shedding runs only when that sweep is enabled.
        if (!diskEnabled || !selfHosted()) {
            return 0;
        }
        double ratio = diskProbe.usageRatio();
        if (ratio < 0) {
            log.debug("log-retention.disk: usage unknown for {} — skipping watermark sweep", diskProbe.path());
            return 0;
        }
        int usagePct = (int) Math.round(ratio * 100.0);
        if (usagePct < diskHighWatermarkPercent) {
            return 0;
        }
        log.warn("log-retention.disk: usage {}% >= high watermark {}% on {} — shedding finished runs ended > {}h ago",
                usagePct, diskHighWatermarkPercent, diskProbe.path(), diskMinRetentionHours);

        OffsetDateTime floorAt = OffsetDateTime.now().minusHours(diskMinRetentionHours);
        if (selfHosted()) {
            OffsetDateTime selfHostedFloor = OffsetDateTime.now().minusDays(SELF_HOSTED_MIN_RETENTION_DAYS);
            if (floorAt.isAfter(selfHostedFloor)) {
                floorAt = selfHostedFloor;
            }
        }
        int deleted;
        try {
            deleted = purgeTerminatedBefore(floorAt, "disk-watermark");
        } catch (RuntimeException e) {
            log.warn("log-retention.disk: watermark purge failed: {}", e.toString());
            return 0;
        }

        int afterPct = diskProbe.usagePercent();
        if (afterPct >= diskHighWatermarkPercent) {
            // Everything safely reclaimable is gone but the disk is still hot — surface an operator
            // alert (health/liveness reads the same signal); we will NOT touch live or in-floor runs.
            log.error("log-retention.disk: usage still {}% after shedding {} row(s) — remaining history is "
                            + "live or within the {}h safety floor. Free disk manually or lower "
                            + "min-retention-hours. ALERT", afterPct, deleted, diskMinRetentionHours);
        } else {
            log.info("log-retention.disk: shed {} row(s); usage now {}% (< high watermark {}%)",
                    deleted, afterPct, diskHighWatermarkPercent);
        }
        return deleted;
    }

    /**
     * Purges local history for runs already marked reported, once the engine confirms they terminated.
     * No-op when {@code reportedRegistry} is absent.
     */
    @Scheduled(fixedDelayString = "${sqs.log-retention.reported.interval-ms:300000}",
            initialDelayString = "${sqs.log-retention.reported.interval-ms:300000}")
    @Transactional
    public void enforceReportedScheduled() {
        if (!enabled || reportedRegistry == null) {
            return;
        }
        enforceReportedPurge();
    }

    /**
     * Hard-deletes the {@code execution_context} of acknowledged, fully-terminated runs. Returns rows
     * purged (test/metric hook).
     */
    @Transactional
    public int enforceReportedPurge() {
        if (reportedRegistry == null) {
            return 0;
        }
        int retentionDays = entitlementsHolder.logRetentionDays();
        if (retentionDays <= EnvironmentEntitlements.UNLIMITED_RETENTION) {
            return 0;
        }
        // Self-hosted rule: BE confirmed it stores the run's logs AND the run terminated more than
        // max(2 days, entitled window) ago. Younger acked runs stay tracked for a later sweep.
        int keepDays = Math.max(SELF_HOSTED_MIN_RETENTION_DAYS, retentionDays);
        java.time.Instant cutoffInstant = java.time.Instant.now().minus(java.time.Duration.ofDays(keepDays));
        Date cutoff = Date.from(cutoffInstant);
        purgeOrphanedHistory(cutoffInstant);
        if (reportedRegistry.isEmpty()) {
            return 0;
        }
        List<String> reported = reportedRegistry.reportedBefore(cutoffInstant, REPORTED_SWEEP_LIMIT);
        List<String> purgeable = new ArrayList<>();
        for (String runId : reported) {
            // Only drop a reported run once the engine confirms every instance terminated before the cutoff.
            if (sqsEngine.allInstancesTerminatedBefore(runId, cutoff)) {
                purgeable.add(runId);
            }
        }
        if (purgeable.isEmpty()) {
            return 0;
        }
        int deleted = 0;
        try {
            for (int i = 0; i < purgeable.size(); i += DELETE_BATCH) {
                List<String> batch = purgeable.subList(i, Math.min(i + DELETE_BATCH, purgeable.size()));
                deleted += retentionRepository.deleteByWorkflowExecutionIdIn(batch);
                purgeSubPiInvocations(batch, "reported");
                purgeOperatorInputOverrides(batch, "reported");
            }
        } catch (RuntimeException e) {
            log.warn("log-retention[reported]: purge failed: {}", e.toString());
            return 0;
        }
        int sandboxDeleted = purgeSandboxLogs(purgeable, "reported");
        int historyDeleted = purgeActivitiHistory(purgeable, "reported");
        reportedRegistry.forget(purgeable);
        log.info("log-retention[reported]: purged {} execution_context row(s) + {} sandbox_log row(s) "
                + "+ {} ACT_HI_PROCINST row(s) across {} BE-acked terminated run(s)",
                deleted, sandboxDeleted, historyDeleted, purgeable.size());
        return deleted;
    }

    /**
     * Shared, guarded purge: delete the {@code execution_context} of every run whose business key
     * has NO row created on/after {@code cutoffAt} AND which the engine confirms is fully terminated
     * before the cutoff instant. Used by both the day-based window and the disk-watermark floor.
     */
    private int purgeTerminatedBefore(OffsetDateTime cutoffAt, String reason) {
        Date cutoff = Date.from(cutoffAt.toInstant());
        List<String> candidateRuns =
                retentionRepository.findDistinctWorkflowExecutionIdsCreatedBefore(cutoffAt);
        if (candidateRuns.isEmpty()) {
            return 0;
        }
        // Keep ONLY runs the engine confirms are fully terminated and last ended before the cutoff —
        // a running/parked (or recently-finished) run is left untouched. Self-hosted additionally
        // requires BE's confirmation that it stores the run's logs: never delete the only copy.
        List<String> purgeableRuns = new ArrayList<>();
        for (String runId : candidateRuns) {
            if (selfHosted() && !reportedRegistry.contains(runId)) {
                continue;
            }
            if (sqsEngine.allInstancesTerminatedBefore(runId, cutoff)) {
                purgeableRuns.add(runId);
            }
        }
        if (purgeableRuns.isEmpty()) {
            log.debug("log-retention[{}]: {} candidate run(s) before cutoff; none terminated+aged — nothing purged",
                    reason, candidateRuns.size());
            return 0;
        }
        int deleted = 0;
        for (int i = 0; i < purgeableRuns.size(); i += DELETE_BATCH) {
            List<String> batch = purgeableRuns.subList(i, Math.min(i + DELETE_BATCH, purgeableRuns.size()));
            deleted += retentionRepository.deleteByWorkflowExecutionIdIn(batch);
            purgeSubPiInvocations(batch, reason);
            purgeOperatorInputOverrides(batch, reason);
        }
        int sandboxDeleted = purgeSandboxLogs(purgeableRuns, reason);
        int historyDeleted = purgeActivitiHistory(purgeableRuns, reason);
        log.info("log-retention[{}]: purged {} execution_context row(s) + {} sandbox_log row(s) "
                        + "+ {} ACT_HI_PROCINST row(s) across {} terminated run(s)",
                reason, deleted, sandboxDeleted, historyDeleted, purgeableRuns.size());
        return deleted;
    }

    /**
     * Deletes the {@code ACT_HI_*} history of exactly the runs the caller already confirmed
     * terminated-and-aged and purged — the SAME guarded batch, never a second policy and never a
     * bare age filter. Best-effort like every satellite purge here: a failure is logged and the
     * next sweep retries idempotently.
     */
    private int purgeActivitiHistory(List<String> purgeableRuns, String reason) {
        // Skip when this sweep is disabled or there is nothing to delete.
        if (!selfHosted() || activitiHistoryRetentionRepository == null || purgeableRuns.isEmpty()) {
            return 0;
        }
        try {
            int deleted = 0;
            for (int i = 0; i < purgeableRuns.size(); i += DELETE_BATCH) {
                List<String> batch = purgeableRuns.subList(i, Math.min(i + DELETE_BATCH, purgeableRuns.size()));
                List<String> instanceIds =
                        activitiHistoryRetentionRepository.findProcessInstanceIdsByBusinessKeys(batch);
                for (int j = 0; j < instanceIds.size(); j += DELETE_BATCH) {
                    deleted += activitiHistoryRetentionRepository.deleteByProcessInstanceIds(
                            instanceIds.subList(j, Math.min(j + DELETE_BATCH, instanceIds.size())));
                }
            }
            return deleted;
        } catch (RuntimeException e) {
            log.warn("log-retention[{}]: ACT_HI_* purge failed: {}", reason, e.toString());
            return 0;
        }
    }

    /**
     * Engine history whose run has no local log row left and ended before the cutoff — the backlog of
     * runs finished under the earlier policy, whose logs were already deleted but whose
     * {@code ACT_HI_*} rows were never purged.
     */
    private void purgeOrphanedHistory(java.time.Instant cutoff) {
        if (activitiHistoryRetentionRepository == null) {
            return;
        }
        try {
            List<String> keys = activitiHistoryRetentionRepository.findOrphanedHistoryBusinessKeys(
                    java.sql.Timestamp.from(cutoff), REPORTED_SWEEP_LIMIT);
            if (keys.isEmpty()) {
                return;
            }
            int deleted = purgeActivitiHistory(keys, "orphaned-history");
            log.info("log-retention[orphaned-history]: purged {} ACT_HI_PROCINST row(s) across {} run(s) "
                    + "with no local log rows left", deleted, keys.size());
        } catch (RuntimeException e) {
            log.warn("log-retention[orphaned-history]: purge failed: {}", e.toString());
        }
    }


    private boolean selfHosted() {
        return reportedRegistry != null;
    }

    /**
     * Delete the {@code sandbox_log} rows of exactly the runs the caller already confirmed
     * terminated-and-aged, in the same batches. Best-effort: a sandbox-log purge failure is logged and
     * never fails the enclosing pass (the next sweep retries idempotently).
     */
    private int purgeSandboxLogs(List<String> purgeableRuns, String reason) {
        if (sandboxLogRetentionRepository == null || purgeableRuns.isEmpty()) {
            return 0;
        }
        try {
            int deleted = 0;
            for (int i = 0; i < purgeableRuns.size(); i += DELETE_BATCH) {
                List<String> batch = purgeableRuns.subList(i, Math.min(i + DELETE_BATCH, purgeableRuns.size()));
                deleted += sandboxLogRetentionRepository.deleteByWorkflowExecutionIdIn(batch);
            }
            return deleted;
        } catch (RuntimeException e) {
            log.warn("log-retention[{}]: sandbox_log purge failed: {}", reason, e.toString());
            return 0;
        }
    }

    /**
     * Delete {@code sub_pi_invocation} rows that reference this batch as parent or child so the index
     * never outlives the {@code execution_context} rows it points at. Best-effort: a failure is logged
     * (shape-only — run counts, never payloads) and never fails the enclosing execution_context purge.
     */
    private void purgeSubPiInvocations(List<String> batch, String reason) {
        if (subPiInvocationRepository == null || batch == null || batch.isEmpty()) {
            return;
        }
        try {
            int deleted = subPiInvocationRepository.deleteByParentOrChildExecutionIdIn(batch);
            log.debug("log-retention[{}]: purged {} sub_pi_invocation row(s) for {} run(s)",
                    reason, deleted, batch.size());
        } catch (RuntimeException e) {
            log.warn("log-retention[{}]: sub_pi_invocation purge failed (runs={}): {}",
                    reason, batch.size(), e.toString());
        }
    }

    /**
     * Delete {@code operator_input_override} rows keyed by this batch of runs so the sealed
     * apply-inputs overrides never outlive the {@code execution_context} they repaired. Same
     * guarded batch as every satellite purge here: best-effort, a failure is logged (counts
     * only) and the next sweep retries idempotently.
     */
    private void purgeOperatorInputOverrides(List<String> batch, String reason) {
        if (operatorInputOverrideRepository == null || batch == null || batch.isEmpty()) {
            return;
        }
        try {
            long deleted = operatorInputOverrideRepository.deleteByWorkflowExecutionIdIn(batch);
            log.debug("log-retention[{}]: purged {} operator_input_override row(s) for {} run(s)",
                    reason, deleted, batch.size());
        } catch (RuntimeException e) {
            log.warn("log-retention[{}]: operator_input_override purge failed (runs={}): {}",
                    reason, batch.size(), e.toString());
        }
    }
}
