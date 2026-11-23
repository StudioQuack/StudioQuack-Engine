package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.command.MessageCorrelationService.MessageCorrelationOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.UserTaskDecision;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.usertask.UserTaskFixtures;
import com.studioquack.sqs.usertask.UserTaskIo;
import com.studioquack.sqs.status.CompositeSandboxLogCallback;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime proof for the user-task approval decision: a {@code user.task} parks the token as an
 * Activiti UserTask wait state, and only a decision addressed to EXACTLY that task — its task id,
 * node, workflow and instance — completes it
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-user-task-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class UserTaskCompletionRuntimeTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private MessageCorrelationService correlationService;
    @Autowired
    private ExecutionContextStore executionContextStore;
    @MockitoSpyBean
    private CompositeSandboxLogCallback sandboxLogCallback;

    @Test
    void decision_completes_the_parked_task_and_publishes_task_output_decision() {
        String workflowId = "utDone-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartTwoUserTasks(workflowId, businessKey);
        assertTrue(parkedOn(instance, "node_approveN"), "precondition: parked on the first user task");

        MessageCorrelationOutcome outcome = correlationService.decideUserTask(new UserTaskDecision(
                taskIdOn(businessKey, "approveN"), "approveN", workflowId, businessKey, "Approved"));

        assertTrue(outcome.correlated(), "the addressed task matched and completed");
        assertTrue(parkedOn(instance, "node_nextN"), "the token advanced to the next node");
        assertFalse(parkedOn(instance, "node_approveN"), "the completed task no longer holds the token");

        List<InOutValueDto> approveOutputs =
                executionContextStore.collectOutputsByNode(businessKey).get("approveN");
        assertNotNull(approveOutputs, "the decision was sealed as the task's outputs");
        assertTrue(approveOutputs.stream().anyMatch(o ->
                        "nextN".equals(o.getTargetNodeId())
                                && MessageCorrelationService.TASK_OUTPUT_PORT.equals(o.getPortValue())
                                && Map.of("decision", "approved").equals(o.getValue())),
                "task_output.decision reads downstream, normalized to lower case");
    }

    @Test
    void the_decision_leaves_an_execution_log_row_with_the_resolved_input_and_the_decision() {
        String workflowId = "utLog-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        deployAndStartTwoUserTasks(workflowId, businessKey);
        clearInvocations(sandboxLogCallback);

        correlationService.decideUserTask(new UserTaskDecision(
                taskIdOn(businessKey, "approveN"), "approveN", workflowId, businessKey, "rejected"));

        ArgumentCaptor<SandboxLogCallback.SandboxLogRecord> rows =
                ArgumentCaptor.forClass(SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, atLeastOnce()).report(rows.capture());
        SandboxLogCallback.SandboxLogRecord row = rows.getAllValues().stream()
                .filter(r -> "approveN".equals(r.nodeId()))
                .findFirst().orElseThrow(() -> new AssertionError("no log row for the user task"));
        assertEquals("COMPLETED", row.status());
        assertEquals(businessKey, row.workflowExecutionId());
        assertTrue(String.valueOf(row.output()).contains("rejected"), "the row's output carries the decision");
    }

    @Test
    void the_minted_element_id_form_addresses_the_node_too() {
        String workflowId = "utMint-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartTwoUserTasks(workflowId, businessKey);

        MessageCorrelationOutcome outcome = correlationService.decideUserTask(new UserTaskDecision(
                taskIdOn(businessKey, "approveN"), "node_approveN", workflowId, businessKey, "rejected"));

        assertTrue(outcome.correlated(), "the minted node_<id> form addresses the task");
        assertTrue(parkedOn(instance, "node_nextN"), "the token advanced");
    }

    @Test
    void a_decision_not_addressed_to_exactly_this_task_is_a_benign_no_match() {
        String workflowId = "utMiss-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartTwoUserTasks(workflowId, businessKey);
        String taskId = taskIdOn(businessKey, "approveN");

        List<UserTaskDecision> wrongAddresses = List.of(
                new UserTaskDecision("stale-" + UUID.randomUUID(), "approveN", workflowId, businessKey, "approved"),
                new UserTaskDecision(taskId, "approveN", "otherWorkflow", businessKey, "approved"),
                new UserTaskDecision(taskId, "approveN", workflowId, "other-run-" + UUID.randomUUID(), "approved"),
                new UserTaskDecision(taskId, "nextN", workflowId, businessKey, "approved"));
        for (UserTaskDecision wrong : wrongAddresses) {
            assertFalse(correlationService.decideUserTask(wrong).correlated(),
                    "a mismatched address is a no-match: " + wrong);
        }
        assertTrue(parkedOn(instance, "node_approveN"), "the parked task is untouched");
    }

    @Test
    void the_process_instance_id_addresses_the_instance_too() {
        String workflowId = "utPi-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartTwoUserTasks(workflowId, businessKey);

        MessageCorrelationOutcome outcome = correlationService.decideUserTask(new UserTaskDecision(
                taskIdOn(businessKey, "approveN"), "approveN", workflowId, instance.getId(), "approved"));

        assertTrue(outcome.correlated(), "a keyless child is addressed by its process instance id");
    }

    @Test
    void decision_values_and_address_fields_are_validated() {
        String workflowId = "utBad-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartTwoUserTasks(workflowId, businessKey);
        String taskId = taskIdOn(businessKey, "approveN");

        assertThrows(IllegalArgumentException.class, () -> correlationService.decideUserTask(
                new UserTaskDecision(taskId, "approveN", workflowId, businessKey, "maybe")),
                "only approved/rejected are decisions");
        assertThrows(IllegalArgumentException.class, () -> correlationService.decideUserTask(
                new UserTaskDecision(taskId, "approveN", workflowId, null, "approved")),
                "the instance is part of the address");
        assertThrows(IllegalArgumentException.class, () -> correlationService.decideUserTask(
                new UserTaskDecision(null, "approveN", workflowId, businessKey, "approved")),
                "the task id is part of the address");
        assertTrue(parkedOn(instance, "node_approveN"), "a refused decision leaves the task parked");
    }

    @Test
    void a_task_event_on_the_generic_event_path_is_refused() {
        String workflowId = "utGeneric-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartTwoUserTasks(workflowId, businessKey);

        assertThrows(IllegalArgumentException.class, () -> correlationService.correlateEvent(
                "TASK", null, businessKey, "approveN", Map.of("decision", "approved")),
                "TASK decisions enter only through the backend's decision service");
        assertTrue(parkedOn(instance, "node_approveN"), "the task stays parked");
    }

    // ---- fixtures --------------------------------------------------------------------------

    /** Deploys and starts {@code start → approveN(user.task) → nextN(user.task) → end}. */
    private ProcessInstance deployAndStartTwoUserTasks(String workflowId, String businessKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto approve = UserTaskFixtures.withUserTaskPorts(node("approveN", "user.task", null));
        param(approve, "assignees", "Jane Doe (jane@example.com)");
        param(approve, "subject", "Approve order");
        NodeDto next = UserTaskFixtures.withUserTaskPorts(node("nextN", "user.task", null));
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "User Task Completion", "1",
                List.of(start, approve, next, end),
                List.of(edge("startN", "approveN"),
                        UserTaskFixtures.wire("approveN", UserTaskIo.TASK_OUTPUT_PORT, "nextN", UserTaskIo.TASK_INPUT_PORT),
                        edge("nextN", "endN")));
        // Seeded like start-run: the run's context carries the graph the pipeline resolves wires from.
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowExecutionId(businessKey);
        seed.setNodes(graph.nodes());
        seed.setEdges(graph.edges());
        ProcessContextRef ref = executionContextStore.snapshot(seed, null);
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(instance, "process started");
        return instance;
    }

    private String taskIdOn(String businessKey, String nodeId) {
        return sqsEngine.findUserTask(businessKey, nodeId).getId();
    }

    private boolean parkedOn(ProcessInstance instance, String activityId) {
        return sqsEngine.executions()
                .processInstanceId(instance.getId())
                .activityId(activityId)
                .count() > 0;
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
