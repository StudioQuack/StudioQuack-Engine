package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.entitlement.EntitlementsHolder;
import com.studioquack.sqs.entitlement.EnvironmentEntitlements;
import com.studioquack.sqs.status.log.SandboxLogRetentionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The {@code sandbox_log} sweep rides the SAME terminated-instance guard as the {@code
 * execution_context} purge.
 */
class SandboxLogRetentionPurgeTest {

    private final ExecutionHistoryRetentionRepository contextRepo = mock(ExecutionHistoryRetentionRepository.class);
    private final SandboxLogRetentionRepository sandboxRepo = mock(SandboxLogRetentionRepository.class);
    private final SqsEngine engine = mock(SqsEngine.class);
    /** Self-hosted: only runs the server confirmed it stores are purge-eligible. */
    private final ReportedRunRegistry registry = new ReportedRunRegistry();

    private EntitlementsHolder holder(int retentionDays) {
        EntitlementsHolder holder = new EntitlementsHolder(retentionDays, -1);
        holder.apply(new EnvironmentEntitlements(retentionDays, false, false, false, false, List.of()));
        return holder;
    }

    private LogRetentionEnforcer enforcer(int retentionDays) {
        LogRetentionEnforcer enforcer = new LogRetentionEnforcer(
                contextRepo, holder(retentionDays), engine, new DiskSpaceProbe("./build"), true, false, 90, 24);
        ReflectionTestUtils.setField(enforcer, "sandboxLogRetentionRepository", sandboxRepo);
        ReflectionTestUtils.setField(enforcer, "reportedRegistry", registry);
        return enforcer;
    }

    private void ackedLongAgo(String runId) {
        registry.markReported(runId, java.time.Instant.now().minus(java.time.Duration.ofDays(10)));
    }

    @Test
    void terminatedAndAgedRunLosesSandboxLogsToo() {
        ackedLongAgo("run-old");
        when(engine.allInstancesTerminatedBefore(eq("run-old"), any(Date.class))).thenReturn(true);
        when(contextRepo.deleteByWorkflowExecutionIdIn(anyCollection())).thenReturn(3);
        when(sandboxRepo.deleteByWorkflowExecutionIdIn(anyCollection())).thenReturn(5);

        enforcer(7).enforceReportedPurge();

        verify(sandboxRepo).deleteByWorkflowExecutionIdIn(anyCollection());
    }

    @Test
    void liveRunKeepsSandboxLogs() {
        ackedLongAgo("run-live");
        when(engine.allInstancesTerminatedBefore(eq("run-live"), any(Date.class))).thenReturn(false);

        enforcer(7).enforceReportedPurge();

        verify(contextRepo, never()).deleteByWorkflowExecutionIdIn(anyCollection());
        verify(sandboxRepo, never()).deleteByWorkflowExecutionIdIn(anyCollection());
    }

    @Test
    void unlimitedRetentionSkipsBothStores() {
        ackedLongAgo("run-old");
        enforcer(-1).enforceReportedPurge();

        verify(contextRepo, never()).deleteByWorkflowExecutionIdIn(anyCollection());
        verify(sandboxRepo, never()).deleteByWorkflowExecutionIdIn(anyCollection());
    }

    @Test
    void logDeleteOlderThanPurgesSandboxLogsOfTheSameRuns() {
        H2LogRetentionService service = new H2LogRetentionService(contextRepo, engine);
        ReflectionTestUtils.setField(service, "sandboxLogRetentionRepository", sandboxRepo);
        when(contextRepo.findDistinctWorkflowExecutionIdsCreatedBefore(any(OffsetDateTime.class)))
                .thenReturn(List.of("run-old"));
        when(engine.allInstancesTerminatedBefore(eq("run-old"), any(Date.class))).thenReturn(true);
        when(contextRepo.deleteByWorkflowExecutionIdIn(anyCollection())).thenReturn(2);
        when(sandboxRepo.deleteByWorkflowExecutionIdIn(anyCollection())).thenReturn(4);

        ExecutionHistoryPurgeService.PurgeResult result =
                service.deleteBefore(java.time.Instant.now().minusSeconds(86_400));

        org.assertj.core.api.Assertions.assertThat(result.rowsDeleted()).isEqualTo(2);
        org.assertj.core.api.Assertions.assertThat(result.sandboxLogRowsDeleted()).isEqualTo(4);
        org.assertj.core.api.Assertions.assertThat(result.runsAffected()).isEqualTo(1);
    }
}
