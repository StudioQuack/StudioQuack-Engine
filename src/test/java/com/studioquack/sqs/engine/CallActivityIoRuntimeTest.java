package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime proof for the {@code call.activity} port contract (audit C5) and the child-run
 * identity/context/failure contract (Sep 2026 incident fixes): the generated {@code CallActivity}
 * carries the {@code callActivityIoListener} on start+end and the {@code
 * callActivityFailurePropagationListener} on end, so
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-call-activity-io-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-call-activity-io-test-jobs.wal",
        "sqs.sandbox.resume-poll-ms=8000",
        "sqs.sandbox.warmpool.enabled=false",
        "sqs.sandbox.isolation-enforce=false"
})
@ActiveProfiles("base")
class CallActivityIoRuntimeTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private MessageCorrelationService correlationService;
    @Autowired
    private ExecutionContextStore contextStore;
    @Autowired
    private DeployedArtifactStore artifactStore;

    @Test
    void mapped_input_reaches_the_child_and_the_completion_envelope_reaches_the_parent() {
        String childWfId = "c5child-" + UUID.randomUUID();
        deployChild(childWfId);

        String parentWfId = "c5parent-" + UUID.randomUUID();
        WorkflowGraph parent = parentGraph(parentWfId, childWfId);
        String wfeId = UUID.randomUUID().toString();
        ProcessContextRef ref = sealContext(parent, wfeId);
        seedMappedInput(ref, Map.of(
                "variables", Map.of("mode", "strict"),
                "document", Map.of("orderId", "O-1"),
                "businessKey", "CHILD-BK-1"));

        DeploymentInfo info = deploymentService.deploy(parent, null, true);
        ProcessInstance parentPi = deploymentService.startProcessInstance(parent, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(parentPi, "parent started");
        assertTrue(parkedOn(parentPi.getId(), "node_callN"),
                "the parent token parks at the call activity while the child runs");

        ProcessInstance childPi = sqsEngine.processInstances()
                .processInstanceBusinessKey("CHILD-BK-1").singleResult();
        assertNotNull(childPi, "the child started under the mapped businessKey");

        List<InOutValueDto> childEntryInputs = contextStore.loadInputsTargetingNode(ref, "childTaskN");
        assertTrue(childEntryInputs.stream().anyMatch(r ->
                        "callN".equals(r.getNodeId())
                                && r.getValue() instanceof Map<?, ?> v
                                && v.get("variables") instanceof Map<?, ?> vars
                                && "strict".equals(vars.get("mode"))
                                && v.get("document") instanceof Map<?, ?> doc
                                && "O-1".equals(doc.get("orderId"))),
                "the mapped variables/document were delivered to the child start's downstream");

        var outcome = correlationService.decideUserTask(new MessageCorrelationService.UserTaskDecision(
                sqsEngine.findUserTask("CHILD-BK-1", "childTaskN").getId(), "childTaskN", childWfId,
                "CHILD-BK-1", "approved"));
        assertTrue(outcome.correlated(), "the child's parked task completed under its mapped key");
        assertTrue(parkedOn(parentPi.getId(), "node_nextN"),
                "the parent resumed past the call activity when the child completed");

        List<InOutValueDto> nextInputs = contextStore.loadInputsTargetingNode(ref, "nextN");
        InOutValueDto envelope = nextInputs.stream()
                .filter(r -> "callN".equals(r.getNodeId()) && "main_output".equals(r.getPortValue()))
                .findFirst().orElse(null);
        assertNotNull(envelope, "the completion envelope landed on the call activity's downstream");
        assertTrue(envelope.getValue() instanceof Map<?, ?> env
                        && childPi.getId().equals(env.get("childProcessInstanceId")),
                "the envelope carries the child process instance id");
        assertTrue(envelope.getValue() instanceof Map<?, ?> env2
                        && env2.get("result") instanceof Map<?, ?> result
                        && "approved".equals(result.get("decision")),
                "the envelope carries the child's terminal outputs (the task's decision) as result");
    }

    @Test
    void without_mapped_input_the_child_starts_with_a_minted_key_and_the_envelope_is_bare() {
        String childWfId = "c5child-" + UUID.randomUUID();
        deployChild(childWfId);

        String parentWfId = "c5parent-" + UUID.randomUUID();
        WorkflowGraph parent = parentGraph(parentWfId, childWfId);
        String wfeId = UUID.randomUUID().toString();
        ProcessContextRef ref = sealContext(parent, wfeId);

        DeploymentInfo info = deploymentService.deploy(parent, null, true);
        ProcessInstance parentPi = deploymentService.startProcessInstance(parent, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(parentPi, "parent started");
        assertTrue(parkedOn(parentPi.getId(), "node_callN"), "parked at the call activity");

        ProcessInstance childPi = sqsEngine.processInstances()
                .superProcessInstanceId(parentPi.getId()).singleResult();
        assertNotNull(childPi, "the child started");
        assertTrue(childPi.getBusinessKey() != null && childPi.getBusinessKey().startsWith("sub_"),
                "no mapped businessKey → the child starts with a minted sub_ key (run-index visible), "
                        + "not keyless — actual: " + childPi.getBusinessKey());
        assertTrue(contextStore.loadInputsTargetingNode(ref, "childTaskN").isEmpty(),
                "no mapped input → nothing is delivered to the child");

        var childTask = sqsEngine.findUserTaskByInstance(childPi.getId(), "childTaskN");
        assertNotNull(childTask, "the child parks on its user task");
        sqsEngine.completeUserTask(childTask.getId());
        assertTrue(parkedOn(parentPi.getId(), "node_nextN"), "the parent resumed");

        List<InOutValueDto> nextInputs = contextStore.loadInputsTargetingNode(ref, "nextN");
        InOutValueDto envelope = nextInputs.stream()
                .filter(r -> "callN".equals(r.getNodeId()) && "main_output".equals(r.getPortValue()))
                .findFirst().orElse(null);
        assertNotNull(envelope, "the completion envelope still lands");
        assertTrue(envelope.getValue() instanceof Map<?, ?> env
                        && childPi.getId().equals(env.get("childProcessInstanceId"))
                        && !env.containsKey("result"),
                "no terminal payload → the envelope carries only the child instance id");
    }

    @Test
    void child_start_reseeds_context_with_the_childs_own_graph() {
        String childWfId = "c5child-" + UUID.randomUUID();
        deployChild(childWfId);

        String parentWfId = "c5parent-" + UUID.randomUUID();
        WorkflowGraph parent = parentGraph(parentWfId, childWfId);
        String wfeId = UUID.randomUUID().toString();
        ProcessContextRef ref = sealContext(parent, wfeId);

        DeploymentInfo info = deploymentService.deploy(parent, null, true);
        ProcessInstance parentPi = deploymentService.startProcessInstance(parent, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(parentPi, "parent started");

        ProcessInstance childPi = sqsEngine.processInstances()
                .superProcessInstanceId(parentPi.getId()).singleResult();
        assertNotNull(childPi, "the child started");

        Object refVar = sqsEngine.getProcessVariable(childPi.getId(), EngineVariableNames.STUDIOQUACK_REF);
        assertTrue(refVar instanceof ProcessContextRef,
                "the child carries a studioquackRef — reseeded, not the inherited parent handle");
        ExecutionContext childCtx = contextStore.loadByRef((ProcessContextRef) refVar).orElseThrow();
        assertEquals(childWfId, childCtx.getWorkflowId(),
                "the reseeded context belongs to the CHILD's workflow");
        assertTrue(childCtx.getNodes() != null
                        && childCtx.getNodes().stream().anyMatch(n -> "childTaskN".equals(n.getId())),
                "the reseeded context carries the child's nodes");
        assertTrue(childCtx.getNodes().stream().noneMatch(n -> "callN".equals(n.getId())),
                "the reseeded context must not carry the parent's nodes");
    }

    @Test
    void child_error_end_propagates_to_the_parent_via_the_generated_catch_all() throws Exception {
        String childWfId = "c5errchild-" + UUID.randomUUID();
        deployErrorEndChild(childWfId);

        String parentWfId = "c5parent-" + UUID.randomUUID();
        WorkflowGraph parent = parentGraph(parentWfId, childWfId);
        String wfeId = UUID.randomUUID().toString();
        ProcessContextRef ref = sealContext(parent, wfeId);

        DeploymentInfo info = deploymentService.deploy(parent, null, true);
        ProcessInstance parentPi = deploymentService.startProcessInstance(parent, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(parentPi, "parent started");

        // The child's authored error end throws synchronously inside the start command; the
        // generated catch-all on the call activity routes the parent into its fail-route gateway.
        // The parent is a ROOT run, so it is HELD ALIVE at hold_callN as a dead-letter incident
        // (the repair surface) instead of ending at the shared error end.
        assertEquals(1, sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count(),
                "the ROOT parent stays alive — a failed root run is held for repair, not ended");
        assertTrue(await(Duration.ofSeconds(30), () ->
                        sqsEngine.deadLetterJobsForProcessInstance(parentPi.getId()).count() > 0),
                "the hold job dead-lettered into a genuine Activiti incident");
        assertTrue(parkedOn(parentPi.getId(), "hold_callN"),
                "the parent token parks at the failure-hold task");
        assertEquals(0, sqsEngine.processInstances()
                        .superProcessInstanceId(parentPi.getId()).count(),
                "no zombie child token — the child ended");
        assertTrue(sqsEngine.historicActivityInstances(parentPi.getId()).list().stream()
                        .noneMatch(a -> "Event_SandboxErrorEnd".equals(a.getActivityId())),
                "a held ROOT run never reaches the shared error end");
        var sentinel = sqsEngine.historicVariables(parentPi.getId())
                .variableName(EngineVariableNames.ERROR_CODE).singleResult();
        assertNotNull(sentinel, "the parent carries the failure sentinel (status maps to FAILED)");
    }

    @Test
    void child_dispatch_failure_ends_the_child_and_holds_the_root_parent_for_repair() throws Exception {
        String childWfId = "c5scriptchild-" + UUID.randomUUID();
        deployScriptChild(childWfId);

        String parentWfId = "c5parent-" + UUID.randomUUID();
        WorkflowGraph parent = parentGraph(parentWfId, childWfId);
        String wfeId = UUID.randomUUID().toString();
        ProcessContextRef ref = sealContext(parent, wfeId);

        DeploymentInfo info = deploymentService.deploy(parent, null, true);
        ProcessInstance parentPi = deploymentService.startProcessInstance(parent, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(parentPi, "parent started");

        // The child's script node dispatches async and fails deterministically (unknown language
        // "cobol" → DISPATCH_UNRESOLVED before any container work). The CHILD (super execution →
        // not a root run) ends at its plain shared error end as before; the propagation listener
        // re-raises on the parent, whose post-call error gate routes into the fail-route gateway.
        assertTrue(await(Duration.ofSeconds(30), () ->
                        sqsEngine.deadLetterJobsForProcessInstance(parentPi.getId()).count() > 0),
                "the parent's failure-hold job dead-lettered into a genuine Activiti incident");
        assertEquals(1, sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count(),
                "the ROOT parent stays alive — held for repair, not ended");
        assertTrue(parkedOn(parentPi.getId(), "hold_callN_gate"),
                "the parent token parks at the post-call failure-hold task");
        assertEquals(0, sqsEngine.processInstances()
                        .superProcessInstanceId(parentPi.getId()).count(),
                "no zombie child token — the child ended");
        assertTrue(sqsEngine.historicActivityInstances(parentPi.getId()).list().stream()
                        .noneMatch(a -> "Event_SandboxErrorEnd".equals(a.getActivityId())),
                "a held ROOT run never reaches the shared error end");
        var parentSentinel = sqsEngine.historicVariables(parentPi.getId())
                .variableName(EngineVariableNames.ERROR_CODE).singleResult();
        assertNotNull(parentSentinel, "the parent carries the failure sentinel");
        assertEquals("DISPATCH_UNRESOLVED", String.valueOf(parentSentinel.getValue()),
                "the propagation listener stamped the child's typed code on the parent");

        var child = sqsEngine.historicProcessInstances()
                .superProcessInstanceId(parentPi.getId()).singleResult();
        assertNotNull(child, "the child ran");
        assertTrue(sqsEngine.historicActivityInstances(child.getId()).list().stream()
                        .anyMatch(a -> "Event_SandboxErrorEnd".equals(a.getActivityId())),
                "the child (not a root run) still ended at its own shared error end");
    }

    // ---- fixtures --------------------------------------------------------------------------

    /** Deploys (without starting) the child: {@code start → childTaskN(user.task) → childEndN}. */
    private void deployChild(String childWfId) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto task = com.studioquack.sqs.usertask.UserTaskFixtures.withUserTaskPorts(
                node("childTaskN", "user.task", null));
        param(task, "assignees", "Jane Doe (jane@example.com)");
        NodeDto end = com.studioquack.sqs.usertask.UserTaskFixtures.withInputPort(
                node("childEndN", "end.event", "end"), "in");
        WorkflowGraph child = new WorkflowGraph(childWfId, "C5 child", "1",
                List.of(start, task, end),
                List.of(edge("startN", "childTaskN"),
                        com.studioquack.sqs.usertask.UserTaskFixtures.wire("childTaskN", "task_output", "childEndN", "in")));
        DeploymentInfo info = deploymentService.deploy(child, null, true);
        assertNotNull(info, "child deployed");
        // The child-context seeder hydrates from the deploy store — same rows the deploy command
        // writes in production.
        artifactStore.saveDefinition(childWfId, "1", info);
        artifactStore.saveDeployedGraph(childWfId, child.nodes(), child.edges());
    }

    /** Deploys the failing child: {@code start → errEndN(error.end.event code=CHILD_BOOM)}. */
    private void deployErrorEndChild(String childWfId) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto err = node("errEndN", "error.end.event", "end");
        param(err, "code", "CHILD_BOOM");
        WorkflowGraph child = new WorkflowGraph(childWfId, "C5 error-end child", "1",
                List.of(start, err), List.of(edge("startN", "errEndN")));
        assertNotNull(deploymentService.deploy(child, null, true), "error-end child deployed");
    }

    /**
     * Deploys the dispatch-failing child: {@code start → scriptN(sandbox, language "cobol") → end}.
     * The unknown wire language fails the dispatch deterministically before any container work.
     */
    private void deployScriptChild(String childWfId) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto script = node("scriptN", "custom.sandbox", "serviceTask");
        script.getData().setJobType("custom-script-executor");
        script.getData().setTemplateScript(
                new TemplateScriptDto("scriptN-s", "scriptN-t", "cobol", "DISPLAY \"x\"."));
        PortDto out = new PortDto();
        out.setId("out");
        out.setName("out");
        out.setValue("out");
        out.setType(PortType.OUTPUT);
        script.getData().setPorts(new ArrayList<>(List.of(out)));
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph child = new WorkflowGraph(childWfId, "C5 script child", "1",
                List.of(start, script, end),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN")));
        DeploymentInfo info = deploymentService.deploy(child, null, true);
        assertNotNull(info, "script child deployed");
        artifactStore.saveDefinition(childWfId, "1", info);
        artifactStore.saveDeployedGraph(childWfId, child.nodes(), child.edges());
    }

    /** {@code start → callN(call.activity → childWfId) → nextN(user.task) → endN}. */
    private WorkflowGraph parentGraph(String parentWfId, String childWfId) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto call = node("callN", "call.activity", null);
        param(call, "processId", childWfId);
        NodeDto next = node("nextN", "user.task", null);
        param(next, "assignee", "jdoe");
        NodeDto end = node("endN", "end.event", "end");
        return new WorkflowGraph(parentWfId, "C5 parent", "1",
                List.of(start, call, next, end),
                List.of(edge("startN", "callN"), edge("callN", "nextN"), edge("nextN", "endN")));
    }

    private ProcessContextRef sealContext(WorkflowGraph graph, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-c5");
        ctx.setCurrentNodeId("callN");
        ctx.setNodes(new ArrayList<>(graph.nodes()));
        ctx.setEdges(new ArrayList<>(graph.edges()));
        return contextStore.snapshot(ctx, null);
    }

    /** Seeds a DuckTape-mapped {@code main_input} row targeting the call-activity node. */
    private void seedMappedInput(ProcessContextRef ref, Map<String, Object> value) {
        InOutValueDto row = new InOutValueDto();
        row.setNodeId("startN");
        row.setPortId("start");
        row.setPortValue("start");
        row.setTargetNodeId("callN");
        row.setTargetPortId("main_input");
        row.setTargetPortValue("main_input");
        row.setValue(value);
        contextStore.saveOutputs(ref, List.of(row));
    }

    private boolean parkedOn(String processInstanceId, String activityId) {
        return sqsEngine.executions()
                .processInstanceId(processInstanceId)
                .activityId(activityId)
                .count() > 0;
    }

    private static boolean await(Duration budget, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + budget.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(100);
        }
        return condition.getAsBoolean();
    }

    private static void param(NodeDto node, String shortCode, String value) {
        ParameterDto param = new ParameterDto();
        param.setShortCode(shortCode);
        param.setValue(value);
        List<ParameterDto> existing = node.getData().getConfigurationParameters();
        List<ParameterDto> merged = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
        merged.add(param);
        node.getData().setConfigurationParameters(merged);
    }

    private NodeDto node(String id, String bpmnType, String nodeType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        return edge;
    }
}
