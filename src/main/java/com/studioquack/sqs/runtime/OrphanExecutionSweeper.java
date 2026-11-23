package com.studioquack.sqs.runtime;

import com.studioquack.sqs.engine.SqsEngine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Reconciles the daemon's durable "run index" (the {@code dispatch_wait_state} + {@code sub_pi_pending}
 * tables of in-flight parked work) against the Activiti runtime/history, on boot and on a schedule
 * (item 4 — the client port of {@code activity_fix_plan.md} Layer 1). A PENDING row whose
 * Activiti execution is gone/terminal (or the ReceiveTask is no longer parked) is an orphan left by
 * a crash/restart; the sweeper drives it to a terminal state with an audited reason
 * ({@code ORPHANED_BOOT_SWEEP} / {@code ORPHANED_PERIODIC}) so the run index and the engine agree.
 *
 * <p>Distinct from {@link DurableRegistryBootstrap}: that recovers epoch-carry-over rows once at
 * startup; this catches <b>same-epoch stale</b> rows (a token that never parked, a PI that silently
 * died) and enforces 7-day retention on terminal {@code sub_pi_pending} rows.
 *
 * <p><b>Feature flag</b> {@code sqs.orphan-sweeper.enabled} (default {@code true}); metric
 * {@code sq.orphan_sweeper.reconciled}. Every mutation is idempotent and fully guarded — a sweep
 * failure never affects a live run.
 */
@Slf4j
@Component
public class OrphanExecutionSweeper {

    private static final String REASON_BOOT = "ORPHANED_BOOT_SWEEP";
    private static final String REASON_PERIODIC = "ORPHANED_PERIODIC";

    private final DispatchWaitStateRepository dispatchRepo;
    private final DispatchWaitStateTxService dispatchTx;
    private final DispatchExecutionRegistry dispatchRegistry;
    private final SubPiPendingRepository subPiRepo;
    private final SubPiPendingTxService subPiTx;
    private final EngineTriggerService engineTriggerService;
    private final SqsEngine sqsEngine;
    private final MeterRegistry meterRegistry;
    private final boolean enabled;
    private final long staleThresholdMs;
    private final long retentionMs;

    public OrphanExecutionSweeper(DispatchWaitStateRepository dispatchRepo,
                                  DispatchWaitStateTxService dispatchTx,
                                  DispatchExecutionRegistry dispatchRegistry,
                                  SubPiPendingRepository subPiRepo,
                                  SubPiPendingTxService subPiTx,
                                  EngineTriggerService engineTriggerService,
                                  SqsEngine sqsEngine,
                                  MeterRegistry meterRegistry,
                                  @Value("${sqs.orphan-sweeper.enabled:true}") boolean enabled,
                                  @Value("${sqs.orphan-sweeper.stale-threshold-ms:900000}") long staleThresholdMs,
                                  @Value("${sqs.orphan-sweeper.retention-ms:604800000}") long retentionMs) {
        this.dispatchRepo = dispatchRepo;
        this.dispatchTx = dispatchTx;
        this.dispatchRegistry = dispatchRegistry;
        this.subPiRepo = subPiRepo;
        this.subPiTx = subPiTx;
        this.engineTriggerService = engineTriggerService;
        this.sqsEngine = sqsEngine;
        this.meterRegistry = meterRegistry;
        this.enabled = enabled;
        this.staleThresholdMs = staleThresholdMs;
        this.retentionMs = retentionMs;
    }

    /** Boot reconciliation, after the registries have hydrated (see {@link DurableRegistryBootstrap}). */
    @EventListener(ApplicationReadyEvent.class)
    @Order(100)
    public void sweepOnBoot() {
        if (!enabled) {
            log.info("orphan-sweeper disabled (sqs.orphan-sweeper.enabled=false) — no boot sweep.");
            return;
        }
        sweep(REASON_BOOT);
    }

    /** Periodic reconciliation (default every 5 min). */
    @Scheduled(fixedDelayString = "${sqs.orphan-sweeper.interval-ms:300000}",
            initialDelayString = "${sqs.orphan-sweeper.interval-ms:300000}")
    public void sweepPeriodic() {
        if (!enabled) {
            return;
        }
        sweep(REASON_PERIODIC);
    }

    /**
     * One reconciliation pass. Returns the number of rows reconciled to a terminal state (test hook).
     */
    public int sweep(String reason) {
        int reconciled = 0;
        try {
            reconciled += reconcileDispatchWaitStates(reason);
            reconciled += reconcileSubPiPending(reason);
            purgeTerminalSubPi();
        } catch (RuntimeException e) {
            log.warn("orphan-sweeper pass failed (reason={}): {}", reason, e.toString());
        }
        increment(reconciled);
        if (reconciled > 0) {
            log.info("orphan-sweeper reconciled {} orphan(s) (reason={})", reconciled, reason);
        }
        return reconciled;
    }

    /** Stale PENDING dispatch rows whose ReceiveTask is no longer parked → DISCARDED. */
    private int reconcileDispatchWaitStates(String reason) {
        OffsetDateTime cutoff = OffsetDateTime.now().minusNanos(staleThresholdMs * 1_000_000L);
        List<DispatchWaitStateEntity> stale =
                dispatchRepo.findByStatusAndRegisteredAtBefore(DispatchStatus.PENDING.name(), cutoff);
        int n = 0;
        for (DispatchWaitStateEntity row : stale) {
            boolean stillParked = sqsEngine.findUnfinishedWaitExecution(
                    row.getProcessInstanceId(), row.getWaitActivityId()) != null;
            if (stillParked) {
                continue; // legitimately still waiting — leave it
            }
            // Clear any in-memory entry AND mark the durable row DISCARDED (audited orphan).
            dispatchRegistry.discard(row.getDispatchId(),
                    "reconciled by OrphanExecutionSweeper (" + reason + "): ReceiveTask not parked");
            n++;
            log.warn("orphan-sweeper.dispatch DISCARDED dispatchId={} pi={} ({})",
                    row.getDispatchId(), row.getProcessInstanceId(), reason);
        }
        return n;
    }

    /** Stale PENDING sub-PI rows whose parent execution is gone → recover-by-fail. */
    private int reconcileSubPiPending(String reason) {
        OffsetDateTime cutoff = OffsetDateTime.now().minusNanos(staleThresholdMs * 1_000_000L);
        List<SubPiPendingEntity> stale = subPiRepo.findByStatusInAndUpdatedAtBefore(
                List.of(SubPiStatus.PENDING.name()), cutoff);
        int n = 0;
        for (SubPiPendingEntity row : stale) {
            String parentExec = row.getParentExecutionId();
            boolean parentActive = parentExec != null && sqsEngine.isExecutionActive(parentExec);
            boolean childActive = row.getChildWorkflowExecutionId() != null
                    && sqsEngine.processInstances()
                    .processInstanceBusinessKey(row.getChildWorkflowExecutionId()).count() > 0;
            if (childActive) {
                continue; // child still running — not an orphan
            }
            subPiTx.markFailed(row.getChildWorkflowExecutionId(),
                    "reconciled by OrphanExecutionSweeper (" + reason + "): child gone");
            if (parentActive) {
                try {
                    engineTriggerService.fail(parentExec, SubProcessCompletionRegistry.ORPHAN_ERROR_CODE,
                            "function sub-process orphaned (" + reason + ")");
                } catch (RuntimeException e) {
                    log.warn("orphan-sweeper.subpi parent trigger failed parentExec={}: {}",
                            parentExec, e.toString());
                }
            }
            n++;
            log.warn("orphan-sweeper.subpi FAILED child={} parentExec={} ({})",
                    row.getChildWorkflowExecutionId(), parentExec, reason);
        }
        return n;
    }

    /** Deletes terminal {@code sub_pi_pending} rows past the 7-day retention window. */
    private void purgeTerminalSubPi() {
        OffsetDateTime cutoff = OffsetDateTime.now().minusNanos(retentionMs * 1_000_000L);
        List<SubPiPendingEntity> expired = subPiRepo.findByStatusInAndUpdatedAtBefore(
                List.of(SubPiStatus.COMPLETED.name(), SubPiStatus.FAILED.name(), SubPiStatus.CANCELED.name()),
                cutoff);
        if (!expired.isEmpty()) {
            subPiRepo.deleteAll(expired);
            log.info("orphan-sweeper retention: purged {} terminal sub_pi_pending row(s)", expired.size());
        }
    }

    private void increment(int amount) {
        if (meterRegistry != null && amount > 0) {
            Counter.builder("sq.orphan_sweeper.reconciled").register(meterRegistry).increment(amount);
        }
    }
}
