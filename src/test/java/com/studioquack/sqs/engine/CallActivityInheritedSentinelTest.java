package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import org.activiti.engine.runtime.ProcessInstance;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime proof for the inherited-error-sentinel scrub (the call.activity inheritVariables path): a
 * parent that caught and handled a failure earlier in its run carries the stale {@code
 * __sq_error_code} family on its instance; with {@code inheritVariables=true} (the default)
 * Activiti copies every parent variable into the child — and without the scrub the child's first
 * sandbox error gate would read the inherited sentinel and raise a phantom {@code BpmnError}.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-inherit-sentinel-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class CallActivityInheritedSentinelTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;

    @Test
    void child_starts_without_the_parents_stale_error_sentinel_and_the_parent_keeps_it() {
        String childWf = "wfChild-" + UUID.randomUUID();
        deploymentService.deploy(childGraph(childWf), null, true);

        String businessKey = UUID.randomUUID().toString();
        ProcessInstance parent = deploymentService.deployAndStart(
                parentGraph("wfParent-" + UUID.randomUUID(), childWf), null, businessKey, null, false);
        assertNotNull(parent, "parent started");
        assertTrue(parkedOn(parent, "node_first"), "precondition: parent parked on its user task");

        // Simulate a caught-and-handled failure earlier in the parent's run: the stale sentinel
        // family sits on the parent instance when the token reaches the call activity.
        sqsEngine.setProcessVariables(parent.getId(), Map.of(
                EngineVariableNames.ERROR_CODE, "SQ_TEST_STALE",
                EngineVariableNames.ERROR_MESSAGE, "earlier failure, already handled",
                EngineVariableNames.ERROR_KIND, "SCRIPT"));

        sqsEngine.completeUserTask(sqsEngine.findUserTask(businessKey, "first").getId());

        ProcessInstance child = sqsEngine.processInstances()
                .processDefinitionKey("Process_" + childWf)
                .singleResult();
        assertNotNull(child, "the call activity started the child");
        assertTrue(parkedOn(child, "node_childWait"), "child parked on its user task, not error-ended");

        for (String name : EngineVariableNames.ERROR_SENTINELS) {
            assertNull(sqsEngine.getProcessVariable(child.getId(), name),
                    "child must not inherit " + name);
        }
        assertEquals("SQ_TEST_STALE",
                sqsEngine.getProcessVariable(parent.getId(), EngineVariableNames.ERROR_CODE),
                "the parent keeps its own sentinel — its end-of-run failure classification reads it");
    }

    @Test
    void start_variable_sentinel_injection_is_scrubbed_too() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deploymentService.deployAndStart(
                childGraph("wfInject-" + UUID.randomUUID()), null, businessKey,
                Map.of(EngineVariableNames.ERROR_CODE, "SQ_INJECTED"), true);
        assertNotNull(instance, "started");
        assertNull(sqsEngine.getProcessVariable(instance.getId(), EngineVariableNames.ERROR_CODE),
                "a fresh instance has no legitimate error state, however the sentinel arrived");
        assertTrue(parkedOn(instance, "node_childWait"), "the run parked normally");
    }

    /** Child/callee graph: {@code start → user.task(childWait) → end}. */
    private WorkflowGraph childGraph(String workflowId) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto wait = node("childWait", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        return new WorkflowGraph(workflowId, "Child", "1",
                List.of(start, wait, end),
                List.of(edge("startN", "childWait"), edge("childWait", "endN")));
    }

    /** Parent graph: {@code start → user.task(first) → call.activity(childWf) → end}. */
    private WorkflowGraph parentGraph(String workflowId, String childWf) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto first = node("first", "user.task", null);
        NodeDto call = node("callN", "call.activity", null);
        param(call, "processId", childWf);
        NodeDto end = node("endN", "end.event", "end");
        return new WorkflowGraph(workflowId, "Parent", "1",
                List.of(start, first, call, end),
                List.of(edge("startN", "first"), edge("first", "callN"), edge("callN", "endN")));
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
