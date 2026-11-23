package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import org.activiti.engine.TaskService;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.task.Task;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime behaviour of the BPMN node types that need no sandbox, no boundary and no event bridge —
 * the plain token semantics the catalog promises
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-node-runtime-behaviour-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class BpmnNodeRuntimeBehaviourTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private TaskService taskService;

    @Test
    void user_task_parks_without_activiti_assignment_and_completes() {
        NodeDto task = node("taskN", "user.task", null);
        param(task, "assignees", "Jane Doe (jane@example.com)");
        param(task, "teams", "Finance");
        ProcessInstance instance = deployAndStart("ut-" + UUID.randomUUID(),
                List.of(node("startN", "start.event", "start"), task, node("endN", "end.event", "end")),
                List.of(edge("startN", "taskN"), edge("taskN", "endN")));

        Task parked = taskService.createTaskQuery().processInstanceId(instance.getId()).singleResult();
        assertNotNull(parked, "user.task parks as a real Activiti UserTask wait state");
        assertNull(parked.getAssignee(), "recipients are resolved by sq-backend, never mapped onto Activiti");
        assertTrue(taskService.getIdentityLinksForTask(parked.getId()).isEmpty(),
                "no Activiti candidate users/groups either");

        taskService.complete(parked.getId());

        assertFalse(sqsEngine.isProcessInstanceActive(instance.getId()),
                "completing the only task lets the token reach the end event");
    }

    @Test
    void terminate_end_event_cancels_every_sibling_token() {
        ProcessInstance instance = deployAndStart("term-" + UUID.randomUUID(),
                List.of(node("startN", "start.event", "start"),
                        node("forkN", "parallel.gateway", null),
                        node("taskA", "user.task", null),
                        node("taskB", "user.task", null),
                        node("termN", "terminate.end.event", "end")),
                List.of(edge("startN", "forkN"), edge("forkN", "taskA"),
                        edge("forkN", "taskB"), edge("taskB", "termN")));

        List<Task> parked = taskService.createTaskQuery().processInstanceId(instance.getId()).list();
        assertEquals(2, parked.size(), "both parallel branches park on their user task");
        Task branchB = parked.stream()
                .filter(t -> "node_taskB".equals(t.getTaskDefinitionKey()))
                .findFirst().orElseThrow();

        taskService.complete(branchB.getId());

        assertFalse(sqsEngine.isProcessInstanceActive(instance.getId()),
                "terminate.end.event ends the whole run, not just its own branch");
        assertTrue(taskService.createTaskQuery().processInstanceId(instance.getId()).list().isEmpty(),
                "the sibling branch's parked task is cancelled with the run");
    }

    @Test
    void link_throw_catch_moves_the_token_to_the_paired_catch() {
        // BPMN link events are an off-page connector: no sequence flow joins the pair — the
        // throw redirects the token to the catch carrying the same link name.
        NodeDto throwN = node("throwN", "link.intermediate.throw.event", null);
        param(throwN, "name", "L1");
        NodeDto catchN = node("catchN", "link.intermediate.catch.event", null);
        param(catchN, "name", "L1");
        ProcessInstance instance = deployAndStart("link-" + UUID.randomUUID(),
                List.of(node("startN", "start.event", "start"), throwN, catchN,
                        node("taskN", "user.task", null), node("endN", "end.event", "end")),
                List.of(edge("startN", "throwN"), edge("catchN", "taskN"), edge("taskN", "endN")));

        assertTrue(parkedOn(instance, "node_taskN"),
                "the token jumps from the link throw to the paired catch and parks on the task");
    }

    // ---- fixtures --------------------------------------------------------------------------

    private ProcessInstance deployAndStart(String workflowId, List<NodeDto> nodes, List<EdgeDto> edges) {
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Node Runtime Behaviour", "1", nodes, edges);
        ProcessInstance instance = deploymentService.deployAndStart(graph, null,
                UUID.randomUUID().toString(), null, true);
        assertNotNull(instance, "process started");
        return instance;
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
