package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.entitlement.EntitlementsHolder;
import com.studioquack.sqs.entitlement.EnvironmentEntitlements;
import com.studioquack.sqs.execution.store.SubPiInvocationRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Sub-PI invocation index rides the SAME terminated-instance guard as the
 * {@code execution_context} purge: a live/parked run is never touched, a
 * terminated-and-aged run loses index rows in the same pass, unlimited retention
 * skips both stores, and an index-delete failure is best-effort (context delete
 * still succeeds). Mirrors {@link SandboxLogRetentionPurgeTest}.
 */
class SubPiInvocationRetentionPurgeTest {

    private final ExecutionHistoryRetentionRepository contextRepo = mock(ExecutionHistoryRetentionRepository.class);
    private final SubPiInvocationRepository indexRepo = mock(SubPiInvocationRepository.class);
    private final SqsEngine engine = mock(SqsEngine.class);

    private EntitlementsHolder holder(int retentionDays) {
        EntitlementsHolder holder = new EntitlementsHolder(retentionDays, -1);
        holder.apply(new EnvironmentEntitlements(retentionDays, false, false, false, false, List.of()));
        return holder;
    }

    /** Self-hosted: only runs the server confirmed it stores are purge-eligible. */
    private final ReportedRunRegistry registry = new ReportedRunRegistry();

    private LogRetentionEnforcer enforcer(int retentionDays) {
        LogRetentionEnforcer enforcer = new LogRetentionEnforcer(
                contextRepo, holder(retentionDays), engine, new DiskSpaceProbe("./build"), true, false, 90, 24);
        ReflectionTestUtils.setField(enforcer, "subPiInvocationRepository", indexRepo);
        ReflectionTestUtils.setField(enforcer, "reportedRegistry", registry);
        return enforcer;
    }

    private void ackedLongAgo(String runId) {
        registry.markReported(runId, java.time.Instant.now().minus(java.time.Duration.ofDays(10)));
    }

    private H2LogRetentionService h2Service() {
        H2LogRetentionService service = new H2LogRetentionService(contextRepo, engine);
        ReflectionTestUtils.setField(service, "subPiInvocationRepository", indexRepo);
        return service;
    }

    @Test
    void terminatedAndAgedRunLosesSubPiIndexToo() {
        ackedLongAgo("run-old");
        when(engine.allInstancesTerminatedBefore(eq("run-old"), any(Date.class))).thenReturn(true);
        when(contextRepo.deleteByWorkflowExecutionIdIn(anyCollection())).thenReturn(3);

        enforcer(7).enforceReportedPurge();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<String>> batch = ArgumentCaptor.forClass(Collection.class);
        verify(indexRepo).deleteByParentOrChildExecutionIdIn(batch.capture());
        assertTrue(batch.getValue().contains("run-old"),
                "terminated-and-aged acked run: the purge deletes the index by that run id");
    }

    @Test
    void liveRunKeepsSubPiIndex() {
        ackedLongAgo("run-live");
        when(engine.allInstancesTerminatedBefore(eq("run-live"), any(Date.class))).thenReturn(false);

        enforcer(7).enforceReportedPurge();

        verify(contextRepo, never()).deleteByWorkflowExecutionIdIn(anyCollection());
        verify(indexRepo, never()).deleteByParentOrChildExecutionIdIn(anyCollection());
    }

    @Test
    void unlimitedRetentionSkipsContextAndIndex() {
        ackedLongAgo("run-old");
        enforcer(-1).enforceReportedPurge();

        verify(contextRepo, never()).deleteByWorkflowExecutionIdIn(anyCollection());
        verify(indexRepo, never()).deleteByParentOrChildExecutionIdIn(anyCollection());
    }

    @Test
    void logDeleteOlderThanPurgesIndexOfTerminatedRunNotLive() {
        when(contextRepo.findDistinctWorkflowExecutionIdsCreatedBefore(any(OffsetDateTime.class)))
                .thenReturn(List.of("run-old"));
        when(engine.allInstancesTerminatedBefore(eq("run-old"), any(Date.class))).thenReturn(true);
        when(contextRepo.deleteByWorkflowExecutionIdIn(anyCollection())).thenReturn(2);

        h2Service().deleteBefore(java.time.Instant.now().minusSeconds(86_400));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<String>> batch = ArgumentCaptor.forClass(Collection.class);
        verify(indexRepo).deleteByParentOrChildExecutionIdIn(batch.capture());
        assertTrue(batch.getValue().contains("run-old"),
                "H2LogRetentionService.deleteBefore: terminated run triggers index delete");

        when(contextRepo.findDistinctWorkflowExecutionIdsCreatedBefore(any(OffsetDateTime.class)))
                .thenReturn(List.of("run-live"));
        when(engine.allInstancesTerminatedBefore(eq("run-live"), any(Date.class))).thenReturn(false);

        h2Service().deleteBefore(java.time.Instant.now().minusSeconds(86_400));

        verify(indexRepo).deleteByParentOrChildExecutionIdIn(anyCollection());
    }

    @Test
    void indexDeleteFailureDoesNotBlockContextDelete() {
        ackedLongAgo("run-old");
        when(contextRepo.findDistinctWorkflowExecutionIdsCreatedBefore(any(OffsetDateTime.class)))
                .thenReturn(List.of("run-old"));
        when(engine.allInstancesTerminatedBefore(eq("run-old"), any(Date.class))).thenReturn(true);
        when(contextRepo.deleteByWorkflowExecutionIdIn(anyCollection())).thenReturn(3);
        when(indexRepo.deleteByParentOrChildExecutionIdIn(anyCollection()))
                .thenThrow(new RuntimeException("index unavailable"));

        assertEquals(3, enforcer(7).enforceReportedPurge(),
                "index delete throwing RuntimeException does NOT prevent execution_context delete");
        verify(contextRepo).deleteByWorkflowExecutionIdIn(anyCollection());

        ExecutionHistoryPurgeService.PurgeResult result =
                h2Service().deleteBefore(java.time.Instant.now().minusSeconds(86_400));
        assertEquals(3, result.rowsDeleted(),
                "H2LogRetentionService: index delete throwing does NOT prevent execution_context delete");
        assertEquals(1, result.runsAffected());
    }
}
