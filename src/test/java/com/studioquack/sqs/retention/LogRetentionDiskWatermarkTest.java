package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.entitlement.EntitlementsHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The disk-watermark reaper. Plain Mockito (no Spring, no real disk) so the watermark decision +
 * the safety-floor purge are proven deterministically
 */
class LogRetentionDiskWatermarkTest {

    private ExecutionHistoryRetentionRepository repo;
    private EntitlementsHolder entitlements;
    private SqsEngine engine;
    private DiskSpaceProbe probe;

    @BeforeEach
    void setUp() {
        repo = mock(ExecutionHistoryRetentionRepository.class);
        entitlements = mock(EntitlementsHolder.class);
        engine = mock(SqsEngine.class);
        probe = mock(DiskSpaceProbe.class);
    }

    private LogRetentionEnforcer enforcer(boolean diskEnabled, int highWatermarkPercent, long minRetentionHours) {
        return new LogRetentionEnforcer(repo, entitlements, engine, probe,
                true, diskEnabled, highWatermarkPercent, minRetentionHours);
    }

    /** The self-hosted shape: the server-acked registry is present. */
    private LogRetentionEnforcer selfHostedEnforcer(ReportedRunRegistry registry) {
        LogRetentionEnforcer enforcer = enforcer(true, 90, 24);
        ReflectionTestUtils.setField(enforcer, "reportedRegistry", registry);
        return enforcer;
    }

    @Test
    void pro_neverDeletesToMakeRoom_evenAboveTheWatermark() {
        when(probe.usageRatio()).thenReturn(0.99);

        assertEquals(0, enforcer(true, 90, 24).enforceDiskWatermark());
        verifyNoInteractions(repo);
    }

    @Test
    void disabled_isNoOp_andNeverProbes() {
        int deleted = enforcer(false, 90, 24).enforceDiskWatermark();
        assertEquals(0, deleted);
        verifyNoInteractions(probe);
        verifyNoInteractions(repo);
    }

    @Test
    void belowHighWatermark_doesNothing() {
        when(probe.usageRatio()).thenReturn(0.50); // 50%
        int deleted = selfHostedEnforcer(new ReportedRunRegistry()).enforceDiskWatermark();
        assertEquals(0, deleted);
        verify(repo, never()).findDistinctWorkflowExecutionIdsCreatedBefore(any());
    }

    @Test
    void usageUnknown_doesNothing() {
        when(probe.usageRatio()).thenReturn(-1.0);
        when(probe.path()).thenReturn("/data");
        int deleted = selfHostedEnforcer(new ReportedRunRegistry()).enforceDiskWatermark();
        assertEquals(0, deleted);
        verify(repo, never()).deleteByWorkflowExecutionIdIn(any());
    }

    @Test
    void aboveHighWatermark_shedsTerminatedRunsOlderThanFloor_keepsLiveRun() {
        when(probe.usageRatio()).thenReturn(0.95); // 95% ≥ 90% high watermark → shed
        when(probe.usagePercent()).thenReturn(70);  // after shedding, back below the watermark
        when(probe.path()).thenReturn("/data");
        ReportedRunRegistry registry = new ReportedRunRegistry();
        java.time.Instant longAgo = java.time.Instant.now().minus(java.time.Duration.ofDays(5));
        registry.markReported("run-old", longAgo);
        registry.markReported("run-live", longAgo);

        when(repo.findDistinctWorkflowExecutionIdsCreatedBefore(any(OffsetDateTime.class)))
                .thenReturn(List.of("run-old", "run-live"));
        // Only the terminated-aged run passes the engine guard; the live run is never purged.
        when(engine.allInstancesTerminatedBefore(eq("run-old"), any())).thenReturn(true);
        when(engine.allInstancesTerminatedBefore(eq("run-live"), any())).thenReturn(false);
        when(repo.deleteByWorkflowExecutionIdIn(any())).thenReturn(3);

        int deleted = selfHostedEnforcer(registry).enforceDiskWatermark();

        assertEquals(3, deleted);
        // Exactly the terminated run is deleted; the live run is excluded from the batch.
        verify(repo).deleteByWorkflowExecutionIdIn(argThat(list ->
                list.contains("run-old") && !list.contains("run-live")));
    }

    @Test
    void reportedPurge_dropsBeAckedRunsOlderThanTwoDays_keepsYoungAndLive() {
        // Self-hosted rule: the server confirmed it stores the logs AND the run ended more than 2 days ago.
        ReportedRunRegistry registry = new ReportedRunRegistry();
        java.time.Instant threeDaysAgo = java.time.Instant.now().minus(java.time.Duration.ofDays(3));
        registry.markReported("run-done", threeDaysAgo);
        registry.markReported("run-live", threeDaysAgo);
        registry.markReported("run-young", java.time.Instant.now().minus(java.time.Duration.ofDays(1)));
        LogRetentionEnforcer enforcer = enforcer(false, 90, 24);
        ReflectionTestUtils.setField(enforcer, "reportedRegistry", registry);

        when(entitlements.logRetentionDays()).thenReturn(1);
        when(engine.allInstancesTerminatedBefore(eq("run-done"), any())).thenReturn(true);
        when(engine.allInstancesTerminatedBefore(eq("run-live"), any())).thenReturn(false);
        when(repo.deleteByWorkflowExecutionIdIn(any())).thenReturn(1);

        int deleted = enforcer.enforceReportedPurge();

        assertEquals(1, deleted);
        verify(repo).deleteByWorkflowExecutionIdIn(argThat(list ->
                list.contains("run-done") && !list.contains("run-live") && !list.contains("run-young")));
        verify(engine, never()).allInstancesTerminatedBefore(eq("run-young"), any());
        assertFalse(registry.contains("run-done"), "purged run must be forgotten");
        assertTrue(registry.contains("run-live"), "still-live run stays tracked");
        assertTrue(registry.contains("run-young"), "a run acked within 2 days is not even a candidate");
    }

    @Test
    void selfHosted_diskWatermark_skipsRunsBeNeverConfirmed() {
        ReportedRunRegistry registry = new ReportedRunRegistry();
        registry.markReported("run-acked", java.time.Instant.now().minus(java.time.Duration.ofDays(5)));
        LogRetentionEnforcer enforcer = enforcer(true, 90, 24);
        ReflectionTestUtils.setField(enforcer, "reportedRegistry", registry);
        when(probe.usageRatio()).thenReturn(0.95);
        when(probe.usagePercent()).thenReturn(80);
        when(repo.findDistinctWorkflowExecutionIdsCreatedBefore(any()))
                .thenReturn(List.of("run-acked", "run-unacked"));
        when(engine.allInstancesTerminatedBefore(any(), any())).thenReturn(true);
        when(repo.deleteByWorkflowExecutionIdIn(any())).thenReturn(1);

        enforcer.enforceDiskWatermark();

        verify(repo).deleteByWorkflowExecutionIdIn(argThat(list ->
                list.contains("run-acked") && !list.contains("run-unacked")));
    }

    @Test
    void reportedPurge_noRegistry_isNoOp() {
        // With no ReportedRunRegistry bean the sweep must do nothing.
        int deleted = enforcer(false, 90, 24).enforceReportedPurge();
        assertEquals(0, deleted);
        verify(repo, never()).deleteByWorkflowExecutionIdIn(any());
    }
}
