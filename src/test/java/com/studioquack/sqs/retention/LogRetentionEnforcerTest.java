package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.entitlement.EntitlementsHolder;
import com.studioquack.sqs.entitlement.EnvironmentEntitlements;
import com.studioquack.sqs.execution.store.ExecutionContextEntity;
import com.studioquack.sqs.execution.store.ExecutionContextRepository;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link LogRetentionEnforcer} against the REAL embedded Activiti engine + embedded database store,
 * so the finished-run guard is exercised end-to-end (not mocked). The reaper's terminal signal is
 * Activiti history ({@code HistoricProcessInstance.getEndTime} via {@code SqsEngine}); the
 * DB-agnostic JPQL delete then removes only terminated-and-aged runs' {@code execution_context}
 * rows.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-retention-reaper-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-retention-jobs.wal",
        "sqs.orphan-sweeper.enabled=false",
        // Disable the SCHEDULED pass so only our explicit enforce() calls run; enforce() itself
        // ignores this flag (only the scheduled entry points honour it).
        "sqs.log-retention.enabled=false",
        // Lift the default retention's 30-day ceiling for THIS suite only. These cases exercise the
        // ENFORCER's window logic (including its unlimited branch); the ceiling itself is policy
        // and is covered by EntitlementsHolderTest.
        "sqs.log-retention-days-max=-1"
})
@ActiveProfiles("base")
class LogRetentionEnforcerTest {

    /** Minimal BPMN whose single receive task lets us park (running) or trigger (terminate) at will. */
    private static final String PROCESS_KEY = "RetentionReaperTestProc";
    private static final String RECEIVE_TASK_ID = "rt";
    private static final String BPMN = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         targetNamespace="http://studioquack/test/retention">
              <process id="RetentionReaperTestProc" isExecutable="true">
                <startEvent id="start"/>
                <sequenceFlow id="f1" sourceRef="start" targetRef="rt"/>
                <receiveTask id="rt" name="park"/>
                <sequenceFlow id="f2" sourceRef="rt" targetRef="end"/>
                <endEvent id="end"/>
              </process>
            </definitions>
            """;

    @Autowired private SqsEngine sqsEngine;
    @Autowired private ExecutionContextRepository contextRepository;
    @Autowired private LogRetentionEnforcer enforcer;
    @Autowired private EntitlementsHolder entitlementsHolder;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private ReportedRunRegistry reportedRegistry;

    @BeforeEach
    void deployProcess() {
        sqsEngine.deploy("retention-reaper-test", Map.of("retention-reaper-test.bpmn", BPMN));
    }

    @Test
    void runningKept_ackedAgedPurged_ackedRecentKept_unackedAgedKept() throws Exception {
        entitlementsHolder.apply(new EnvironmentEntitlements(1, false, false, false)); // 1-day window
        OffsetDateTime oldCdate = OffsetDateTime.now().minusDays(10); // every seeded row is old → all candidates
        Instant longAgo = Instant.now().minus(Duration.ofDays(10));

        // (1) RUNNING/parked instance — old rows, but MUST be kept (this is the corruption the fix prevents).
        String running = "wf-run-" + UUID.randomUUID();
        startAndPark(running);
        String runningRow = seedContextRow(running, oldCdate);
        reportedRegistry.markReported(running, longAgo);

        // (2) TERMINATED 10 days ago, the server confirmed its logs — MUST be purged (all its rows).
        String doneOld = "wf-done-old-" + UUID.randomUUID();
        completeInstance(startAndPark(doneOld));
        backdateEnd(doneOld, 10);
        String doneOldRow1 = seedContextRow(doneOld, oldCdate);
        String doneOldRow2 = seedContextRow(doneOld, oldCdate);
        reportedRegistry.markReported(doneOld, longAgo);

        // (3) TERMINATED just now — old rows, acked, but MUST be kept (younger than 2 days).
        String doneRecent = "wf-done-recent-" + UUID.randomUUID();
        completeInstance(startAndPark(doneRecent));
        String doneRecentRow = seedContextRow(doneRecent, oldCdate);
        reportedRegistry.markReported(doneRecent, longAgo);

        // (4) TERMINATED 10 days ago but the server never confirmed its logs — MUST be kept (only copy).
        String unacked = "wf-unacked-" + UUID.randomUUID();
        completeInstance(startAndPark(unacked));
        backdateEnd(unacked, 10);
        String unackedRow = seedContextRow(unacked, oldCdate);

        int deleted = enforcer.enforceReportedPurge();

        assertTrue(deleted >= 2, "the acked aged terminated run's rows are purged (deleted=" + deleted + ")");
        assertTrue(contextRepository.existsById(runningRow), "RUNNING instance context MUST NOT be purged");
        assertTrue(contextRepository.existsById(doneRecentRow), "a run younger than 2 days MUST be kept");
        assertTrue(contextRepository.existsById(unackedRow), "a run BE never confirmed MUST be kept");
        assertFalse(contextRepository.existsById(doneOldRow1), "acked aged terminated context purged");
        assertFalse(contextRepository.existsById(doneOldRow2), "acked aged terminated context purged");
    }

    @Test
    void unlimitedRetention_skipsEntirely() throws Exception {
        entitlementsHolder.apply(new EnvironmentEntitlements(-1, false, false, false)); // unlimited
        OffsetDateTime oldCdate = OffsetDateTime.now().minusDays(30);

        String doneOld = "wf-unlimited-" + UUID.randomUUID();
        completeInstance(startAndPark(doneOld));
        backdateEnd(doneOld, 30);
        String row = seedContextRow(doneOld, oldCdate);
        reportedRegistry.markReported(doneOld, Instant.now().minus(Duration.ofDays(30)));

        int deleted = enforcer.enforceReportedPurge();

        assertFalse(deleted > 0, "unlimited retention must never delete");
        assertTrue(contextRepository.existsById(row), "row kept under unlimited retention");
    }

    // ---- helpers -------------------------------------------------------------

    /** Start a fresh instance and wait until its token is genuinely parked at the receive task. */
    private ProcessInstance startAndPark(String businessKey) throws InterruptedException {
        ProcessInstance pi = sqsEngine.startLatest(PROCESS_KEY, businessKey, null);
        assertNotNull(pi);
        await(() -> sqsEngine.findUnfinishedWaitExecution(pi.getId(), RECEIVE_TASK_ID) != null);
        return pi;
    }

    /** Trigger the parked receive task so the instance runs to its end event and TERMINATES. */
    private void completeInstance(ProcessInstance pi) throws InterruptedException {
        String parked = sqsEngine.findUnfinishedWaitExecution(pi.getId(), RECEIVE_TASK_ID);
        assertNotNull(parked, "instance must be parked before completing");
        sqsEngine.trigger(parked, null);
        await(() -> sqsEngine.historicProcessInstances().processInstanceId(pi.getId()).finished().count() > 0);
    }

    /** Backdate a terminated instance's historic END time (test-only; embedded database in this suite). */
    private void backdateEnd(String businessKey, int daysAgo) {
        Timestamp end = Timestamp.from(OffsetDateTime.now().minusDays(daysAgo).toInstant());
        int updated = jdbcTemplate.update(
                "update ACT_HI_PROCINST set END_TIME_ = ? where BUSINESS_KEY_ = ?", end, businessKey);
        assertTrue(updated >= 1, "backdated historic end time for " + businessKey);
    }

    /** Seed one execution_context row for a run and return its id (nodeRefId) for assertions. */
    private String seedContextRow(String workflowExecutionId, OffsetDateTime cdate) {
        String nodeRefId = "node_" + UUID.randomUUID();
        contextRepository.saveAndFlush(ExecutionContextEntity.builder()
                .nodeRefId(nodeRefId)
                .executionId(workflowExecutionId)
                .workflowExecutionId(workflowExecutionId)
                .status("COMPLETED")
                .revision(0)
                .cdate(cdate)
                .udate(cdate)
                .build());
        return nodeRefId;
    }

    private void await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        assertTrue(cond.getAsBoolean(), "condition not met within timeout");
    }
}
