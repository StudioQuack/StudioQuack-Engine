package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Default date-bounded execution-history purge ({@link H2LogRetentionService}). The critical
 * invariant: it deletes ONLY runs the engine confirms are terminated-and-aged, reusing the {@code
 * SqsEngine.allInstancesTerminatedBefore} guard, so a live/parked (or recently-finished) run's
 * {@code execution_context} is NEVER purged no matter how old its rows.
 */
class H2LogRetentionServiceTest {

    private final ExecutionHistoryRetentionRepository repo = mock(ExecutionHistoryRetentionRepository.class);
    private final SqsEngine engine = mock(SqsEngine.class);
    private final H2LogRetentionService service = new H2LogRetentionService(repo, engine);

    @Test
    void deletesOnlyTerminatedAndAgedRuns_neverLiveOrRecent() {
        Instant olderThan = Instant.parse("2026-06-01T00:00:00Z");
        when(repo.findDistinctWorkflowExecutionIdsCreatedBefore(any()))
                .thenReturn(List.of("run-terminated", "run-live", "run-recent"));
        when(engine.allInstancesTerminatedBefore(eq("run-terminated"), any(Date.class))).thenReturn(true);
        when(engine.allInstancesTerminatedBefore(eq("run-live"), any(Date.class))).thenReturn(false);   // still running/parked
        when(engine.allInstancesTerminatedBefore(eq("run-recent"), any(Date.class))).thenReturn(false); // ended after cutoff
        when(repo.deleteByWorkflowExecutionIdIn(anyList())).thenReturn(5);

        ExecutionHistoryPurgeService.PurgeResult result = service.deleteBefore(olderThan);

        assertEquals(5, result.rowsDeleted());
        assertEquals(1, result.runsAffected());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<String>> batch = ArgumentCaptor.forClass(Collection.class);
        verify(repo).deleteByWorkflowExecutionIdIn(batch.capture());
        assertTrue(batch.getValue().contains("run-terminated"));
        assertFalse(batch.getValue().contains("run-live"), "a live/parked run is never purged");
        assertFalse(batch.getValue().contains("run-recent"), "a run ended within the window is never purged");
    }

    @Test
    void nullInstant_isNoOp() {
        ExecutionHistoryPurgeService.PurgeResult result = service.deleteBefore(null);
        assertEquals(0, result.rowsDeleted());
        assertEquals(0, result.runsAffected());
        verify(repo, never()).deleteByWorkflowExecutionIdIn(anyList());
    }

    @Test
    void noneTerminated_deletesNothing() {
        when(repo.findDistinctWorkflowExecutionIdsCreatedBefore(any())).thenReturn(List.of("run-a", "run-b"));
        when(engine.allInstancesTerminatedBefore(any(), any(Date.class))).thenReturn(false);

        ExecutionHistoryPurgeService.PurgeResult result = service.deleteBefore(Instant.parse("2026-06-01T00:00:00Z"));

        assertEquals(0, result.rowsDeleted());
        assertEquals(0, result.runsAffected());
        verify(repo, never()).deleteByWorkflowExecutionIdIn(anyList());
    }
}
