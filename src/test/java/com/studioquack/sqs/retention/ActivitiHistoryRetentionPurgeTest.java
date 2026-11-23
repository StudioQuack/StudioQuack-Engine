package com.studioquack.sqs.retention;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.entitlement.EntitlementsHolder;
import com.studioquack.sqs.entitlement.EnvironmentEntitlements;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code historyLevel=FULL} made {@code ACT_HI_*} an unbounded store — the multi-GB {@code
 * sqs-engine.mv.db} on busy self-hosted engines. The history purge rides the SAME guarded batch as
 * the {@code execution_context} purge.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-activiti-history-retention-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-activiti-history-retention-jobs.wal",
        "sqs.orphan-sweeper.enabled=false",
        "sqs.log-retention.enabled=false",
        "sqs.log-retention-days-max=-1"
    })
@ActiveProfiles("base")
class ActivitiHistoryRetentionPurgeTest {

    private static final String PROCESS_KEY = "ActivitiHistoryRetentionTestProc";
    private static final String RECEIVE_TASK_ID = "rt";
    private static final String BPMN = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         targetNamespace="http://studioquack/test/retention">
              <process id="ActivitiHistoryRetentionTestProc" isExecutable="true">
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
        sqsEngine.deploy("activiti-history-retention-test", Map.of("activiti-history-retention-test.bpmn", BPMN));
        entitlementsHolder.apply(new EnvironmentEntitlements(1, false, false, false)); // 1-day window
    }

    @Test
    void agedTerminatedRun_losesItsActivitiHistoryWithItsContext_runningKeepsBoth() throws Exception {
        OffsetDateTime oldCdate = OffsetDateTime.now().minusDays(10);

        Instant longAgo = Instant.now().minus(Duration.ofDays(10));
        String doneOld = "wf-hist-done-" + UUID.randomUUID();
        completeInstance(startAndPark(doneOld));
        backdateEnd(doneOld, 10);
        seedContextRow(doneOld, oldCdate);
        reportedRegistry.markReported(doneOld, longAgo);

        String running = "wf-hist-running-" + UUID.randomUUID();
        startAndPark(running);
        seedContextRow(running, oldCdate);
        reportedRegistry.markReported(running, longAgo);

        assertTrue(countHistory(doneOld) > 0, "fixture: the terminated run has ACT_HI_* rows");
        assertTrue(countHistory(running) > 0, "fixture: the running instance has ACT_HI_* rows");

        int deleted = enforcer.enforceReportedPurge();

        assertTrue(deleted >= 1, "the aged run's context is purged (deleted=" + deleted + ")");
        assertEquals(0, countHistory(doneOld),
                "the aged terminated run's ACT_HI_* rows purge WITH its context");
        assertTrue(countHistory(running) > 0,
                "a running instance keeps its history (the guard reads it)");
        assertEquals(0, countContextRows(doneOld));
        assertEquals(1, countContextRows(running));
    }

    @Test
    void legacyBacklog_historyWithNoLocalLogsLeft_isPurged_butAnUnackedRunWithLogsKeepsIt() throws Exception {
        // Finished under the earlier policy: its local logs are long gone, only ACT_HI_* is left.
        String orphan = "wf-hist-orphan-" + UUID.randomUUID();
        completeInstance(startAndPark(orphan));
        backdateEnd(orphan, 10);

        // Old and never confirmed by the server, with a local log row still here — the only copy.
        String unackedWithLogs = "wf-hist-unacked-" + UUID.randomUUID();
        completeInstance(startAndPark(unackedWithLogs));
        backdateEnd(unackedWithLogs, 10);
        seedContextRow(unackedWithLogs, OffsetDateTime.now().minusDays(10));

        assertTrue(countHistory(orphan) > 0, "fixture: the legacy run has ACT_HI_* rows");

        enforcer.enforceReportedPurge();

        assertEquals(0, countHistory(orphan), "history protecting no local log is purged");
        assertTrue(countHistory(unackedWithLogs) > 0, "a run whose logs are still the only copy keeps everything");
        assertEquals(1, countContextRows(unackedWithLogs));
    }

    @Test
    void unlimitedRetention_keepsHistoryToo() throws Exception {
        entitlementsHolder.apply(new EnvironmentEntitlements(-1, false, false, false));
        String doneOld = "wf-hist-unlimited-" + UUID.randomUUID();
        completeInstance(startAndPark(doneOld));
        backdateEnd(doneOld, 30);
        seedContextRow(doneOld, OffsetDateTime.now().minusDays(30));
        reportedRegistry.markReported(doneOld, Instant.now().minus(Duration.ofDays(30)));

        enforcer.enforceReportedPurge();

        assertTrue(countHistory(doneOld) > 0, "unlimited retention never purges history");
    }

    // ---- helpers -------------------------------------------------------------

    private long countHistory(String businessKey) {
        Long proc = jdbcTemplate.queryForObject(
                "select count(*) from ACT_HI_PROCINST where BUSINESS_KEY_ = ?", Long.class, businessKey);
        Long acts = jdbcTemplate.queryForObject(
                "select count(*) from ACT_HI_ACTINST where PROC_INST_ID_ in "
                        + "(select PROC_INST_ID_ from ACT_HI_PROCINST where BUSINESS_KEY_ = ?)",
                Long.class, businessKey);
        return (proc == null ? 0 : proc) + (acts == null ? 0 : acts);
    }

    private long countContextRows(String workflowExecutionId) {
        return contextRepository.findAll().stream()
                .filter(e -> workflowExecutionId.equals(e.getWorkflowExecutionId()))
                .count();
    }

    private ProcessInstance startAndPark(String businessKey) throws InterruptedException {
        ProcessInstance pi = sqsEngine.startLatest(PROCESS_KEY, businessKey, null);
        assertNotNull(pi);
        await(() -> sqsEngine.findUnfinishedWaitExecution(pi.getId(), RECEIVE_TASK_ID) != null);
        return pi;
    }

    private void completeInstance(ProcessInstance pi) throws InterruptedException {
        String parked = sqsEngine.findUnfinishedWaitExecution(pi.getId(), RECEIVE_TASK_ID);
        assertNotNull(parked, "instance must be parked before completing");
        sqsEngine.trigger(parked, null);
        await(() -> sqsEngine.historicProcessInstances().processInstanceId(pi.getId()).finished().count() > 0);
    }

    private void backdateEnd(String businessKey, int daysAgo) {
        Timestamp end = Timestamp.from(OffsetDateTime.now().minusDays(daysAgo).toInstant());
        int updated = jdbcTemplate.update(
                "update ACT_HI_PROCINST set END_TIME_ = ? where BUSINESS_KEY_ = ?", end, businessKey);
        assertTrue(updated >= 1, "backdated historic end time for " + businessKey);
    }

    private void seedContextRow(String workflowExecutionId, OffsetDateTime cdate) {
        contextRepository.saveAndFlush(com.studioquack.sqs.execution.store.ExecutionContextEntity.builder()
                .nodeRefId("node_" + UUID.randomUUID())
                .executionId(workflowExecutionId)
                .workflowExecutionId(workflowExecutionId)
                .status("COMPLETED")
                .revision(0)
                .cdate(cdate)
                .udate(cdate)
                .build());
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
