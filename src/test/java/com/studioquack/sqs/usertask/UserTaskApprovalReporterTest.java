package com.studioquack.sqs.usertask;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.activiti.engine.task.Task;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The park report a {@code user.task} sends to the backend: addressed by task id, workflow id,
 * canvas node id and instance; carrying the node's subject/message/recipient fields as deployed
 * and the {@code values} wrapper mapped into Task Input — with oversized values withheld.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-user-task-report-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class UserTaskApprovalReporterTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore contextStore;
    @Autowired
    private UserTaskApprovalReporter reporter;

    @Test
    void report_carries_the_exact_address_the_node_fields_and_the_mapped_values() {
        String workflowId = "utReport-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        Task task = startAndPark(workflowId, businessKey, Map.of("customer_age", 18));

        Map<String, Object> report = reporter.buildReport(parked(task, businessKey));

        assertNotNull(report, "a designer user task of a deployed workflow is announced");
        assertEquals(task.getId(), report.get("taskId"));
        assertEquals(workflowId, report.get("workflowId"));
        assertEquals("approveN", report.get("nodeId"), "the canvas node id, never the template id");
        assertEquals(businessKey, report.get("workflowExecutionId"));
        assertEquals(task.getProcessInstanceId(), report.get("processInstanceId"));
        assertEquals("Approve order", report.get("subject"));
        assertEquals("Please check the age.", report.get("message"));
        assertEquals("Jane Doe (jane@example.com)", report.get("assignees"));
        assertEquals("Finance", report.get("teams"));
        assertEquals(Map.of("customer_age", 18), report.get("values"), "the Task Input values wrapper");
    }

    @Test
    void oversized_values_are_withheld_and_flagged() {
        String workflowId = "utReportBig-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        Task task = startAndPark(workflowId, businessKey,
                Map.of("blob", "x".repeat(UserTaskApprovalReporter.MAX_VALUES_BYTES + 1)));

        Map<String, Object> report = reporter.buildReport(parked(task, businessKey));

        assertNotNull(report);
        assertFalse(report.containsKey("values"), "values above the cap never leave the engine");
        assertTrue(Boolean.TRUE.equals(report.get("valuesOmitted")), "the backend learns they were withheld");
    }

    @Test
    void the_park_is_settled_through_the_channel_off_sink() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        Task task = startAndPark("utReportSettle-" + UUID.randomUUID(), businessKey, Map.of());

        for (int i = 0; i < 50 && !reporter.isSettled(task.getId()); i++) {
            Thread.sleep(100);
        }
        assertTrue(reporter.isSettled(task.getId()), "the create listener reported the park after commit");
    }

    // ---- fixtures --------------------------------------------------------------------------

    /** {@code start → approveN(user.task) → endN}, seeded like start-run, with Task Input values mapped. */
    private Task startAndPark(String workflowId, String businessKey, Map<String, Object> values) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto approve = UserTaskFixtures.withUserTaskPorts(node("approveN", "user.task", null));
        param(approve, "subject", "Approve order");
        param(approve, "message", "Please check the age.");
        param(approve, "assignees", "Jane Doe (jane@example.com)");
        param(approve, "teams", "Finance");
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "User Task Report", "1",
                List.of(start, approve, end),
                List.of(edge("startN", "approveN"), edge("approveN", "endN")));

        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowExecutionId(businessKey);
        seed.setNodes(graph.nodes());
        seed.setEdges(graph.edges());
        ProcessContextRef ref = contextStore.snapshot(seed, null);
        if (!values.isEmpty()) {
            InOutValueDto row = new InOutValueDto();
            row.setNodeId("startN");
            row.setPortId("out");
            row.setPortValue("out");
            row.setTargetNodeId("approveN");
            row.setTargetPortId("approveN-in");
            row.setTargetPortValue(UserTaskIo.TASK_INPUT_PORT);
            row.setValue(Map.of(UserTaskIo.VALUES_FIELD, values));
            contextStore.saveOutputs(ref, List.of(row));
        }
        deploymentService.deployAndStart(graph, null, businessKey,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        Task task = sqsEngine.findUserTask(businessKey, "approveN");
        assertNotNull(task, "precondition: parked on the user task");
        return task;
    }

    private static ParkedUserTask parked(Task task, String businessKey) {
        return new ParkedUserTask(task.getId(), task.getProcessInstanceId(), task.getProcessDefinitionId(),
                task.getTaskDefinitionKey(), task.getName(), businessKey);
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

    private static NodeDto node(String id, String bpmnType, String nodeType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private static EdgeDto edge(String source, String target) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        return edge;
    }
}
