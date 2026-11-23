package com.studioquack.sqs.usertask;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.SchemaDto;
import com.studioquack.common.dto.flow.SchemaItemDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.enums.flow.PortType;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The designer's own shape for a User Task right after a None Start Event: the DuckTape screen on
 * that edge maps {@code start_context.msg → task_input.values} and the author typed a value on the
 * start card's {@code msg}. The start event publishes {@code start_context} on that mapped
 * connection, so the resolved Task Input carries the typed value under Values; an unmapped
 * connection gets no start row.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-user-task-inbound-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class UserTaskInboundRuntimeTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore contextStore;
    @Autowired
    private UserTaskIo io;

    @Test
    void the_start_card_value_mapped_into_values_reaches_task_input() {
        String businessKey = UUID.randomUUID().toString();
        Task task = startDesignerShapedRun("utStartMap-" + UUID.randomUUID(), businessKey, true);

        UserTaskIo.Inbound inbound = io.resolveInbound(
                sqsEngine.getProcessVariable(task.getProcessInstanceId(), EngineVariableNames.STUDIOQUACK_REF),
                "taskN").orElseThrow();

        // A wrapper holds each mapped source under its own key.
        assertEquals(Map.of("msg", Map.of("key1", "value1")), UserTaskIo.values(inbound.inputVariables()));
    }

    @Test
    void an_unmapped_start_connection_gets_no_start_row() {
        String businessKey = UUID.randomUUID().toString();
        startDesignerShapedRun("utStartRaw-" + UUID.randomUUID(), businessKey, false);

        assertTrue(contextStore.loadInputsTargetingNode(seedRef, "taskN").isEmpty());
    }

    /** The run's seed context ref, kept by {@link #startDesignerShapedRun}. */
    private ProcessContextRef seedRef;

    // ---- fixtures: the shapes the designer saves ------------------------------------------------

    private Task startDesignerShapedRun(String workflowId, String businessKey, boolean mappedScreen) {
        NodeDto start = node("startN", "start.event", "start");
        addPort(start, port("start-out", "start_context", PortType.OUTPUT,
                schema("msg", "object"), schema("files", "list")));
        NodeDto task = UserTaskFixtures.withUserTaskPorts(node("taskN", "user.task", null));
        NodeDto end = node("endN", "end.event", "end");

        EdgeDto mapped = new EdgeDto();
        mapped.setId("e_start_task");
        mapped.setSource("startN");
        mapped.setTarget("taskN");
        mapped.setSourceHandle("OUTPUT-start_context");
        mapped.setTargetHandle("INPUT-task_input");
        EdgeTransformDto row = new EdgeTransformDto();
        row.setSourceNodeId("startN");
        row.setSourcePortId("start-out");
        row.setSourcePath("msg");
        row.setSourceSchemaType("schema");
        row.setSourceDataType("object");
        row.setTargetNodeId("taskN");
        row.setTargetPortId("taskN-in");
        row.setTargetPath("values");
        row.setTargetSchemaType("schema");
        row.setTargetDataType("wrapper");
        SchemaItemDto typed = new SchemaItemDto();
        typed.setType("source");
        typed.setNodeId("startN");
        typed.setPortId("start-out");
        typed.setShortCode("msg");
        typed.setSchemaName("schema");
        typed.setValue("{\"key1\":\"value1\"}");
        EdgeDataDto data = new EdgeDataDto();
        data.setTransformLanguage("jolt");
        data.setEdgeTransforms(mappedScreen ? new ArrayList<>(List.of(row)) : new ArrayList<>());
        data.setSchemaItems(mappedScreen ? new ArrayList<>(List.of(typed)) : new ArrayList<>());
        mapped.setData(data);

        WorkflowGraph graph = new WorkflowGraph(workflowId, "User Task Inbound", "1",
                List.of(start, task, end), List.of(mapped, UserTaskFixtures.wire("taskN", "task_output", "endN", "in")));
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowExecutionId(businessKey);
        seed.setNodes(graph.nodes());
        seed.setEdges(graph.edges());
        ProcessContextRef ref = contextStore.snapshot(seed, null);
        seedRef = ref;
        deploymentService.deployAndStart(graph, null, businessKey, Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        Task parked = sqsEngine.findUserTask(businessKey, "taskN");
        assertNotNull(parked, "precondition: parked on the user task");
        return parked;
    }

    private static NodeDto node(String id, String bpmnType, String nodeType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        template.setPorts(new ArrayList<>());
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private static void addPort(NodeDto node, PortDto port) {
        List<PortDto> ports = new ArrayList<>(node.getData().getPorts());
        ports.add(port);
        node.getData().setPorts(ports);
    }

    private static PortDto port(String id, String name, PortType type, SchemaDto... schema) {
        PortDto port = new PortDto();
        port.setId(id);
        port.setName(name);
        port.setValue(name);
        port.setType(type);
        port.setSchema(new ArrayList<>(List.of(schema)));
        port.setInputSchema(new ArrayList<>());
        port.setOutputSchema(new ArrayList<>());
        return port;
    }

    private static SchemaDto schema(String shortCode, String type) {
        SchemaDto schema = new SchemaDto();
        schema.setShortCode(shortCode);
        schema.setLabel(shortCode);
        schema.setType(type);
        schema.setChildren(new ArrayList<>());
        return schema;
    }
}
