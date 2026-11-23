package com.studioquack.sqs.gate;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.EngineTriggerService;
import com.studioquack.sqs.runtime.support.FakeSandboxConfig;
import com.studioquack.sqs.runtime.support.FakeSandboxExecutionPort;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.history.HistoricVariableInstance;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SECURITY — the REF-ONLY invariant holds end-to-end at {@code historyLevel=FULL}. A sandbox node
 * produces PHI-shaped output.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-gate-refonly-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-gate-refonly-jobs.wal",
        "sqs.orphan-sweeper.enabled=false"
})
@ActiveProfiles("base")
@Import(FakeSandboxConfig.class)
class RefOnlyHistoryAuditTest {

    /**
     * A distinctive PHI-shaped payload the node emits on its output port. Every token below is
     * asserted to be absent from every {@code ACT_*} variable — if any appears, PHI leaked onto
     * the engine.
     */
    private static final String PHI_VALUE =
            "PHI__patient=John_Q_Public__ssn=123-45-6789__dx=oncology__note=admitted_2026";

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private ExecutionContextStore contextStore;
    @Autowired private EngineTriggerService engineTriggerService;
    @Autowired private FakeSandboxExecutionPort fakeSandbox;
    @Autowired private RuntimeService runtimeService;

    @BeforeEach
    void resetFake() {
        fakeSandbox.reset();
    }

    // -----------------------------------------------------------------------------------------
    // #1 — PHI output is sealed in the context store and NEVER lands in ACT_HI_VARINST.
    // -----------------------------------------------------------------------------------------

    @Test
    void phi_node_output_is_sealed_in_context_store_and_absent_from_history() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String nodeId = "phiNode";
        WorkflowGraph graph = linearGraph("wfPhiHist-" + wfeId, nodeId);
        // The node returns PHI on its declared output port — this is the payload that must stay sealed.
        fakeSandbox.succeed(nodeId, Map.of("out", PHI_VALUE));

        ProcessInstance pi = seedAndStart(graph, nodeId, wfeId);
        assertTrue(awaitCompleted(wfeId), "PHI-producing flow completed");

        // (a) The PHI IS in the sealed execution_context store — it did carry through the pipeline,
        //     proving we are actually exercising a real payload (not a no-op).
        Map<String, List<InOutValueDto>> sealed = contextStore.collectOutputsByNode(wfeId);
        List<InOutValueDto> nodeOut = sealed.get(nodeId);
        assertTrue(nodeOut != null && nodeOut.stream()
                        .anyMatch(o -> PHI_VALUE.equals(String.valueOf(o.getValue()))),
                "the PHI payload is sealed in the encrypted context store (addressed by studioquackRef)");

        // (b) ACT_HI_VARINST holds ONLY whitelisted keys and NEVER the PHI value.
        List<HistoricVariableInstance> histVars = sqsEngine.historicVariables(pi.getId()).list();
        assertFalse(histVars.isEmpty(), "history recorded the REF-ONLY engine variables at FULL level");
        for (HistoricVariableInstance v : histVars) {
            assertTrue(EngineVariableNames.isPersistWhitelisted(v.getVariableName()),
                    () -> "non-whitelisted key leaked into ACT_HI_VARINST: " + v.getVariableName());
            assertNoPhi("ACT_HI_VARINST[" + v.getVariableName() + "]", v.getValue());
        }
        // The encrypted handle IS present (proves the engine carried the ref, not the payload).
        assertTrue(histVars.stream().anyMatch(v -> EngineVariableNames.STUDIOQUACK_REF.equals(v.getVariableName())),
                "the studioquackRef handle is present in history (REF-ONLY carry)");
        // No obvious payload-shaped variable names ever appear.
        assertTrue(histVars.stream().noneMatch(v -> "out".equals(v.getVariableName())
                        || "prompt".equals(v.getVariableName())
                        || v.getVariableName().startsWith("output_")),
                "no per-port / payload variable name reached history");
    }

    // -----------------------------------------------------------------------------------------
    // #2 — ACT_RU_VARIABLE (live runtime) is clean at the dispatch/parked moment.
    // -----------------------------------------------------------------------------------------

    @Test
    void runtime_variable_table_holds_only_ref_and_epoch_tokens_at_dispatch() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String nodeId = "phiRuntimeNode";
        WorkflowGraph graph = linearGraph("wfPhiRun-" + wfeId, nodeId);
        // Delay the sandbox return so the ReceiveTask is reliably observable in the parked state; the
        // delay runs on the dispatch pool (not the engine thread) — see the dispatch-pinning perf test.
        fakeSandbox.program(nodeId, req -> {
            sleepQuietly(500);
            return okWithPhiResponse();
        });

        ProcessInstance pi = seedAndStart(graph, nodeId, wfeId);

        // While parked at wait_<node>, read ACT_RU_VARIABLE as the auditor would: only the REF-ONLY
        // handle + the boot epoch may be present, and never any PHI.
        String parkedExec = awaitParked(pi.getId(), "wait_" + nodeId);
        assertNotNull(parkedExec, "the token parked at the sandbox ReceiveTask");
        Map<String, Object> runtimeVars = runtimeService.getVariables(pi.getId());
        assertFalse(runtimeVars.isEmpty(), "the running instance carries its REF-ONLY start variables");
        runtimeVars.forEach((name, value) -> {
            assertTrue(EngineVariableNames.isPersistWhitelisted(name),
                    () -> "non-whitelisted key in ACT_RU_VARIABLE: " + name);
            assertNoPhi("ACT_RU_VARIABLE[" + name + "]", value);
        });
        assertTrue(runtimeVars.containsKey(EngineVariableNames.STUDIOQUACK_REF),
                "the runtime instance carries the studioquackRef handle");
        assertTrue(runtimeVars.containsKey(EngineVariableNames.BE_EPOCH),
                "the runtime instance carries the boot-epoch token");

        // Let it finish so the context is torn down cleanly for the next test.
        assertTrue(awaitCompleted(wfeId), "the delayed PHI flow completed");
    }

    // -----------------------------------------------------------------------------------------
    // #3 — A whitelist violation is rejected on the trigger/complete persist path (nothing leaks).
    // -----------------------------------------------------------------------------------------

    @Test
    void whitelist_violation_is_rejected_on_the_trigger_complete_persist_path() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String nodeId = "parkNode";
        WorkflowGraph graph = linearGraph("wfPersistGuard-" + wfeId, nodeId);

        // A BARE ref (no currentNodeRefId) makes the delegate PARK the ReceiveTask without dispatching
        // or registering a wait-state — nothing will ever auto-resume it, so we get a stable parked
        // execution to probe the persist path against (mirrors CarryoverConformanceTest's cancel case).
        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessContextRef bareRef = ProcessContextRef.builder()
                .executionId(wfeId).workflowExecutionId(wfeId).envId("env-gate").schemaVersion(1).build();
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), true);
        String parkedExec = awaitParked(pi.getId(), "wait_" + nodeId);
        assertNotNull(parkedExec, "the token parked at the ReceiveTask (bare ref → no dispatch)");

        // Attempt to complete the parked task with a PHI-bearing variable map. The audit MUST
        // reject it at the boundary — this is exactly the whitelist gate asks for.
        Map<String, Object> phiVars = new java.util.LinkedHashMap<>();
        phiVars.put(EngineVariableNames.STUDIOQUACK_REF, bareRef);
        phiVars.put("diagnosis", PHI_VALUE);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> engineTriggerService.complete(parkedExec, phiVars, null),
                "a PHI-bearing variable map must be rejected on the trigger/complete persist path");
        assertTrue(ex.getMessage().contains("REF-ONLY"), "the rejection cites the REF-ONLY invariant");
        // Sanity: the guard itself agrees the offending key is not persist-whitelisted.
        assertFalse(RefOnlyVariableGuard.isAllowed("diagnosis"));

        // Nothing leaked: neither the offending key nor the PHI value reached ACT_HI_VARINST.
        for (HistoricVariableInstance v : sqsEngine.historicVariables(pi.getId()).list()) {
            assertTrue(EngineVariableNames.isPersistWhitelisted(v.getVariableName()),
                    () -> "rejected key still leaked to history: " + v.getVariableName());
            assertNoPhi("ACT_HI_VARINST[" + v.getVariableName() + "]", v.getValue());
        }

        sqsEngine.deleteProcessInstance(pi.getId(), "gate test cleanup");
    }

    // ---- fixtures ---------------------------------------------------------------------------

    /** Asserts a serialized variable value carries none of the PHI tokens. */
    private void assertNoPhi(String where, Object value) {
        String flat = String.valueOf(value);
        assertFalse(flat.contains(PHI_VALUE), () -> where + " leaked the PHI payload");
        assertFalse(flat.contains("123-45-6789"), () -> where + " leaked a PHI SSN token");
        assertFalse(flat.contains("John_Q_Public"), () -> where + " leaked a PHI name token");
    }

    /** Programs a success response carrying the PHI payload on the output port. */
    private com.studioquack.sqs.sandbox.SandboxJobResponse okWithPhiResponse() {
        return com.studioquack.sqs.sandbox.SandboxJobResponse.success(
                0, "ok", "", Map.of("out", PHI_VALUE), 1L, "fake");
    }

    private ProcessInstance seedAndStart(WorkflowGraph graph, String sandboxNodeId, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-gate");
        ctx.setCurrentNodeId(sandboxNodeId);
        ctx.setNodes(new ArrayList<>(graph.nodes()));
        ctx.setEdges(new ArrayList<>(graph.edges()));
        ProcessContextRef ref = contextStore.snapshot(ctx, null);

        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(pi, "process instance started");
        return pi;
    }

    private WorkflowGraph linearGraph(String workflowId, String nodeId) {
        return new WorkflowGraph(workflowId, "gate", "1",
                List.of(simpleNode("startN", "start.event", null, "start"),
                        sandboxNode(nodeId),
                        simpleNode("endN", "end.event", null, "end")),
                List.of(edge("startN", nodeId), edge(nodeId, "endN")));
    }

    private NodeDto sandboxNode(String id) {
        NodeDto n = simpleNode(id, "custom.sandbox", "custom-script-executor", "serviceTask");
        n.getData().setTemplateScript(new TemplateScriptDto(id + "-s", id + "-t", "python",
                "studioquack.return_result([{\"port\": \"out\", \"value\": \"x\"}])\n"));
        PortDto out = new PortDto();
        out.setId("out");
        out.setName("out");
        out.setValue("out");
        out.setType(PortType.OUTPUT);
        n.getData().setPorts(new ArrayList<>(List.of(out)));
        return n;
    }

    private NodeDto simpleNode(String id, String bpmnType, String jobType, String nodeType) {
        TemplateDto t = new TemplateDto();
        t.setBpmnType(bpmnType);
        t.setJobType(jobType);
        t.setNodeType(nodeType);
        t.setLabel(id);
        NodeDto n = new NodeDto();
        n.setId(id);
        n.setData(t);
        return n;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto e = new EdgeDto();
        e.setId("e_" + source + "_" + target);
        e.setSource(source);
        e.setTarget(target);
        return e;
    }

    private String awaitParked(String pi, String waitActivityId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            String exec = sqsEngine.findUnfinishedWaitExecution(pi, waitActivityId);
            if (exec != null) {
                return exec;
            }
            Thread.sleep(25);
        }
        return null;
    }

    private boolean awaitCompleted(String wfeId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            if (sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0
                    && sqsEngine.historicProcessInstances().processInstanceBusinessKey(wfeId)
                    .finished().count() > 0) {
                return true;
            }
            Thread.sleep(75);
        }
        return sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
