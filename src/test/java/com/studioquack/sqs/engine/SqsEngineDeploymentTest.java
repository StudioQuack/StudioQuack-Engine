package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end deploy + start against the embedded engine on an in-memory database datasource. Proves
 * a 2-node-plus-function-port workflow
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-engine-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class SqsEngineDeploymentTest {

    @Autowired
    private WorkflowDeploymentService workflowDeploymentService;

    @Autowired
    private SqsEngine sqsEngine;

    private NodeDto node(String id, String bpmnType, String jobType, String nodeType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setJobType(jobType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private EdgeDto edge(String source, String target, String sourceHandle, String targetHandle) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        edge.setSourceHandle(sourceHandle);
        edge.setTargetHandle(targetHandle);
        return edge;
    }

    @Test
    void deploys_main_and_sub_bpmn_into_h2_and_starts_pi_with_business_key() {
        String workflowId = "wfEngineTest";
        // start -> agent(sandbox) -> end, with a function-port tool (its own sub-process).
        NodeDto start = node("startN", "start.event", null, "start");
        NodeDto agent = node("agentN", "custom.sandbox", "custom-script-executor", "serviceTask");
        NodeDto end = node("endN", "end.event", null, "end");
        NodeDto tool = node("toolFnRoot", "custom.sandbox", "function-script-executor", "serviceTask");

        List<NodeDto> nodes = List.of(start, agent, end, tool);
        List<EdgeDto> edges = List.of(
                edge("startN", "agentN", null, null),
                edge("agentN", "endN", null, null),
                edge("agentN", "toolFnRoot", "FUNCTION-model", "INPUT-main_input")); // special edge

        WorkflowGraph graph = new WorkflowGraph(workflowId, "Engine H2 Test", "1", nodes, edges);

        // 1) Deploy main + sub in ONE deployment (force = true to bypass the reuse cache).
        DeploymentInfo info = workflowDeploymentService.deploy(graph, null, true);
        assertNotNull(info, "deployment info");
        assertEquals("Process_" + workflowId, info.getBpmnProcessId(), "main process key");
        assertNotNull(info.getProcessDefinitionId(), "pinned process definition id");
        assertTrue(info.getVersion() >= 1, "definition version stamped");
        assertNotNull(info.getGraphSignature(), "v6 graph signature stamped");

        // The main process AND its function-port sub-process are both deployed in the engine.
        assertTrue(sqsEngine.latestDefinitionExists("Process_" + workflowId),
                "main process definition deployed in H2");
        assertTrue(sqsEngine.latestDefinitionExists("Process_SubProc_toolFnRoot"),
                "function-port sub-process definition deployed in the SAME deployment");

        // 2) Start a PI with businessKey = workflowExecutionId, REF-ONLY variables only.
        String workflowExecutionId = UUID.randomUUID().toString();
        ProcessContextRef ref = ProcessContextRef.builder()
                .executionId(workflowExecutionId)
                .workflowExecutionId(workflowExecutionId)
                .envId("env-under-test")
                .schemaVersion(1)
                .build();
        Map<String, Object> refOnlyVars = Map.of(EngineVariableNames.STUDIOQUACK_REF, ref);

        ProcessInstance instance = workflowDeploymentService.startProcessInstance(
                graph, info, workflowExecutionId, refOnlyVars, true);

        assertNotNull(instance, "process instance started");
        assertEquals(workflowExecutionId, instance.getBusinessKey(), "businessKey == workflowExecutionId");
        // StrongUuidGenerator → non-guessable UUID process-instance id.
        assertTrue(instance.getId() != null && instance.getId().contains("-"), "UUID process instance id");

        // The instance is resolvable in the runtime tables by businessKey via the facade (single-tenant).
        long running = sqsEngine.processInstances()
                .processInstanceBusinessKey(workflowExecutionId)
                .count();
        assertEquals(1L, running, "exactly one running PI with the businessKey");
    }
}
