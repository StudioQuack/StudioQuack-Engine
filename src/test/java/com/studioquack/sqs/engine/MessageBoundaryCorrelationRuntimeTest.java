package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.command.MessageCorrelationService.MessageCorrelationOutcome;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runtime proof for MESSAGE-boundary shared-key correlation (the reported "boundary never fires" scenario). */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-msg-boundary-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class MessageBoundaryCorrelationRuntimeTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private MessageCorrelationService correlationService;

    @Test
    void message_boundary_on_a_user_task_correlates_by_the_shared_authored_key() {
        String businessKey = UUID.randomUUID().toString();
        String messageName = "bm1." + UUID.randomUUID();
        String sharedKey = "shared-" + UUID.randomUUID();
        ProcessInstance instance = deployAndStartUserTaskWithMessageBoundary(
                "bndKey-" + UUID.randomUUID(), businessKey, messageName, sharedKey);

        assertTrue(parkedOn(instance, "node_userN"), "the run parks on the user task");
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, messageName),
                "the boundary arms a message subscription on the parked execution");
        assertNull(sqsEngine.findMessageEventExecution(sharedKey, messageName),
                "precondition: the shared key is nobody's businessKey");
        assertNotNull(sqsEngine.findMessageEventExecutionByCorrelationKey(messageName, sharedKey),
                "the host pinned the authored key at park time");

        MessageCorrelationOutcome outcome = correlationService.correlate(messageName, sharedKey, null);

        assertTrue(outcome.correlated(),
                "a sender knowing only the shared authored key correlates onto the boundary");
        assertTrue(parkedOn(instance, "node_handlerN"),
                "the interrupting boundary routed the token to its handler");
        assertFalse(parkedOn(instance, "node_userN"),
                "the interrupting boundary cancelled the user task");
    }

    @Test
    void message_boundary_without_a_key_stays_businesskey_only() {
        String businessKey = UUID.randomUUID().toString();
        String messageName = "bm1." + UUID.randomUUID();
        ProcessInstance instance = deployAndStartUserTaskWithMessageBoundary(
                "bndNoKey-" + UUID.randomUUID(), businessKey, messageName, null);

        assertTrue(parkedOn(instance, "node_userN"), "the run parks on the user task");
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, messageName),
                "the boundary arms a message subscription");

        MessageCorrelationOutcome miss = correlationService.correlate(messageName,
                "unrelated-" + UUID.randomUUID(), null);
        assertFalse(miss.correlated(),
                "no pin without an authored key — an unknown key is a benign no-match");
        assertTrue(parkedOn(instance, "node_userN"), "the user task is untouched");

        MessageCorrelationOutcome hit = correlationService.correlate(messageName, businessKey, null);
        assertTrue(hit.correlated(), "the run's own businessKey still correlates");
        assertTrue(parkedOn(instance, "node_handlerN"), "the boundary fired and routed");
    }

    // ---- fixtures --------------------------------------------------------------------------

    /**
     * Deploys and starts {@code start → userN (user.task, message boundary name/sharedKey →
     * handlerN) → endN}, with {@code handlerN} a second user task that parks for assertion.
     */
    private ProcessInstance deployAndStartUserTaskWithMessageBoundary(String workflowId,
                                                                      String businessKey,
                                                                      String messageName,
                                                                      String correlationKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto userTask = node("userN", "user.task", null);
        BoundaryEventConfig boundary = BoundaryEventConfig.message(messageName, correlationKey);
        boundary.setId("b1");
        boundary.setInterrupting(true);
        boundary.setTargetNodeId("handlerN");
        userTask.getData().setBoundaryEventConfigs(List.of(boundary));
        NodeDto handler = node("handlerN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Message Boundary Correlation", "1",
                List.of(start, userTask, handler, end),
                List.of(edge("startN", "userN"),
                        edge("userN", "endN"),
                        edge("handlerN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "process started");
        return instance;
    }

    private boolean parkedOn(ProcessInstance instance, String activityId) {
        return sqsEngine.executions()
                .processInstanceId(instance.getId())
                .activityId(activityId)
                .count() > 0;
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
