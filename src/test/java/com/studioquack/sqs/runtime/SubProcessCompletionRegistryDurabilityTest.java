package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Durable {@code sub_pi_pending} + daemon restart during a sub-process wait (recover-by-fail).
 * Covers register→complete/fail future resolution, the REF-ONLY whitelist on the persisted result
 * variables, and the boot recovery that (never resuming the orphaned child) fails the still-parked
 * ROOT parent — now held alive as a repairable dead-letter incident.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-subpi-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-subpi-jobs.wal",
        "sqs.orphan-sweeper.enabled=false"
})
@ActiveProfiles("base")
class SubProcessCompletionRegistryDurabilityTest {

    @Autowired private SubProcessCompletionRegistry registry;
    @Autowired private SubPiPendingRepository repository;
    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;

    @Test
    void register_complete_resolves_future_with_whitelisted_result_vars() throws Exception {
        String child = "child-ok-" + UUID.randomUUID();
        CompletableFuture<Map<String, Object>> future = registry.register(new SubProcessCompletionRegistry.SubPiLink(
                child, "parent-wfe", "parent-exec", "env", "Process_SubProc_root", null));
        assertEquals(SubPiStatus.PENDING.name(), repository.findById(child).orElseThrow().getStatus());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put(EngineVariableNames.STUDIOQUACK_REF, "ref-out");
        result.put("route__gw", "A");
        result.put("phiField", "patient PHI");   // must be filtered out of the store
        registry.complete(child, result);

        Map<String, Object> got = future.get();
        assertEquals("ref-out", got.get(EngineVariableNames.STUDIOQUACK_REF), "future carries the result");

        SubPiPendingEntity row = repository.findById(child).orElseThrow();
        assertEquals(SubPiStatus.COMPLETED.name(), row.getStatus());
        assertNotNull(row.getResultVariables());
        assertFalse(row.getResultVariables().contains("phiField"), "PHI is filtered from result_variables");
        assertTrue(row.getResultVariables().contains("route__gw"), "whitelisted routing token is kept");
    }

    /**
     * A provider node with no async sandbox dispatch runs its whole sub-PI inside the engine start
     * call, on the caller's own thread — so the completion lands BEFORE the caller gets to await.
     * The result must still reach it.
     */
    @Test
    void completion_before_the_caller_awaits_still_delivers_the_result() throws Exception {
        String child = "child-sync-" + UUID.randomUUID();
        CompletableFuture<Map<String, Object>> future = registry.register(new SubProcessCompletionRegistry.SubPiLink(
                child, "parent-wfe", "parent-exec", "env", "Process_SubProc_root", null));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put(EngineVariableNames.STUDIOQUACK_REF, "ref-sync");
        registry.complete(child, result);

        assertTrue(future.isDone(), "the held future resolves even though the registry entry is gone");
        assertEquals("ref-sync", future.get().get(EngineVariableNames.STUDIOQUACK_REF));
        assertEquals(0, registry.pending(), "the resolved child is no longer awaiting");
    }

    @Test
    void register_fail_completes_future_exceptionally() {
        String child = "child-fail-" + UUID.randomUUID();
        CompletableFuture<Map<String, Object>> future = registry.register(new SubProcessCompletionRegistry.SubPiLink(
                child, "parent-wfe", "parent-exec", "env", "Process_SubProc_root", null));

        registry.fail(child, "sandbox blew up");

        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertTrue(ex.getCause() instanceof SubProcessCompletionRegistry.SubProcessFailedException);
        assertEquals(SubPiStatus.FAILED.name(), repository.findById(child).orElseThrow().getStatus());
    }

    @Test
    void restart_during_sub_process_wait_recovers_parent_by_fail() throws Exception {
        // 1) Park a real PARENT at a ReceiveTask (bare ref → delegate parks without dispatching).
        String parentWfe = UUID.randomUUID().toString();
        WorkflowGraph graph = twoNode(parentWfe, "callerN");
        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessContextRef bareRef = ProcessContextRef.builder()
                .executionId(parentWfe).workflowExecutionId(parentWfe).envId("env-subpi").schemaVersion(1).build();
        var parentPi = deploymentService.startProcessInstance(graph, info, parentWfe,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), true);
        String parentExec = awaitParked(parentPi.getId(), "wait_callerN");
        assertNotNull(parentExec, "parent parked at the function-port wait");

        // 2) Simulate a PREVIOUS boot: a PENDING child link with an OLD epoch (no in-memory future).
        String child = "orphan-child-" + UUID.randomUUID();
        repository.save(SubPiPendingEntity.builder()
                .childWorkflowExecutionId(child).parentWorkflowExecutionId(parentWfe)
                .parentExecutionId(parentExec).environmentId("env-subpi").bpmnProcessId("Process_SubProc_root")
                .clientEpoch("PREVIOUS-BOOT-EPOCH").status(SubPiStatus.PENDING.name())
                .createdAt(java.time.OffsetDateTime.now().minusMinutes(2))
                .updatedAt(java.time.OffsetDateTime.now().minusMinutes(2)).build());

        // 3) Boot recover-by-fail: never resume the orphaned child; fail the parent to its error end.
        int recovered = registry.hydrateOnBoot();
        assertEquals(1, recovered, "the orphaned child link is recovered-by-fail");
        assertEquals(SubPiStatus.FAILED.name(), repository.findById(child).orElseThrow().getStatus());

        // 4) The parent run is a ROOT run: recovered-by-fail routes it through the error boundary
        // into the catch-all fail-route, which HOLDS it alive as a dead-letter incident (the
        // repair surface) instead of ending it at the shared error end — deterministic, not a hang.
        assertTrue(await(() -> sqsEngine.deadLetterJobsForProcessInstance(parentPi.getId()).count() > 0),
                "the parent's failure-hold job dead-lettered into a genuine Activiti incident");
        assertTrue(sqsEngine.processInstances().processInstanceBusinessKey(parentWfe).count() == 1,
                "the ROOT parent stays alive — held for repair, not left hanging and not ended");
        // Runtime execution query, not historic activity: a first-execution dead-letter rolls the
        // job's transaction back, so the hold task leaves no historic activity record.
        boolean parkedAtHold = sqsEngine.executions().processInstanceId(parentPi.getId())
                .activityId("hold_callerN").count() > 0;
        assertTrue(parkedAtHold, "the parent token parks at the failure-hold task");
        boolean tookErrorEnd = sqsEngine.historicActivityInstances(parentPi.getId()).list().stream()
                .anyMatch(a -> ActivitiBpmnElementHelper.SHARED_ERROR_END_ID.equals(a.getActivityId()));
        assertFalse(tookErrorEnd, "a held ROOT run never reaches the shared error end");

        // 5) The failure sentinel is what marks the held run FAILED (and what a repair clears
        // before re-queueing the hold job): assert it really is written and survives on the live
        // held instance — otherwise the run index can never show the run as failed/repairable.
        boolean sentinelRecorded = sqsEngine.historicVariables(parentPi.getId()).list().stream()
                .anyMatch(v -> EngineVariableNames.ERROR_CODE.equals(v.getVariableName())
                        && EngineVariableNames.isFailureSentinel(v.getValue()));
        assertTrue(sentinelRecorded, "the held run carries " + EngineVariableNames.ERROR_CODE
                + ", which is what makes the run index record FAILED rather than COMPLETED");
    }

    // ---- helpers -------------------------------------------------------------

    private WorkflowGraph twoNode(String wfId, String nodeId) {
        NodeDto start = simpleNode("startN", "start.event", null, "start");
        NodeDto work = simpleNode(nodeId, "custom.sandbox", "custom-script-executor", "serviceTask");
        PortDto out = new PortDto();
        out.setId("out"); out.setName("out"); out.setValue("out"); out.setType(PortType.OUTPUT);
        work.getData().setPorts(new ArrayList<>(List.of(out)));
        NodeDto end = simpleNode("endN", "end.event", null, "end");
        return new WorkflowGraph(wfId, "sub-pi durability", "1",
                List.of(start, work, end), List.of(edge("startN", nodeId), edge(nodeId, "endN")));
    }

    private NodeDto simpleNode(String id, String bpmnType, String jobType, String nodeType) {
        TemplateDto t = new TemplateDto();
        t.setBpmnType(bpmnType); t.setJobType(jobType); t.setNodeType(nodeType); t.setLabel(id);
        NodeDto n = new NodeDto();
        n.setId(id); n.setData(t);
        return n;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto e = new EdgeDto();
        e.setId("e_" + source + "_" + target); e.setSource(source); e.setTarget(target);
        return e;
    }

    private String awaitParked(String pi, String waitActivityId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            String exec = sqsEngine.findUnfinishedWaitExecution(pi, waitActivityId);
            if (exec != null) {
                return exec;
            }
            Thread.sleep(50);
        }
        return sqsEngine.findUnfinishedWaitExecution(pi, waitActivityId);
    }

    private boolean await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return cond.getAsBoolean();
    }
}
