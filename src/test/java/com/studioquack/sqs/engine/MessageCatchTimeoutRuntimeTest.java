package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.command.MessageCorrelationService.MessageCorrelationOutcome;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime proof for the message-catch timeout synthesis (audit): a {@code
 * message.intermediate.catch.event} with {@code timeoutMs > 0} deploys as {@code EventGateway →
 * [message catch, timer catch(PTnS)]}. The XML shape is covered by {@code
 * BpmnGenerationServiceTest}; this suite proves the two arms behave as an event choice at runtime
 * on the real async executor.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-catch-timeout-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class MessageCatchTimeoutRuntimeTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private MessageCorrelationService correlationService;
    @Autowired
    private com.studioquack.sqs.execution.store.ExecutionContextStore executionContextStore;

    @Test
    void message_arm_wins_when_the_message_arrives_before_the_timeout() {
        String businessKey = UUID.randomUUID().toString();
        String messageName = "order.paid." + UUID.randomUUID();
        // PT1H: the timer arm is armed but can never fire inside the test budget.
        ProcessInstance instance = deployAndStartCatchWithTimeout("toMsg-" + UUID.randomUUID(),
                businessKey, messageName, "3600000");

        Execution subscription = sqsEngine.findMessageEventExecution(businessKey, messageName);
        assertNotNull(subscription, "the catch parks with a live message subscription");

        sqsEngine.messageEventReceived(messageName, subscription.getId(), Map.of());

        assertTrue(parkedOn(instance, "node_msgTaskN"),
                "a message arriving first routes down the message arm");
        assertFalse(parkedOn(instance, "node_lateTaskN"),
                "the timeout arm never runs once the message arm won");
        assertNull(sqsEngine.findMessageEventExecution(businessKey, messageName),
                "the event gateway consumes the subscription on delivery");
    }

    @Test
    void timer_arm_wins_when_no_message_arrives() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        String messageName = "order.paid." + UUID.randomUUID();
        // PT2S: the real async executor acquires and fires the timer job.
        ProcessInstance instance = deployAndStartCatchWithTimeout("toTmr-" + UUID.randomUUID(),
                businessKey, messageName, "2000");

        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, messageName),
                "the message arm is armed before the timer fires");

        assertTrue(awaitParked(instance, "node_lateTaskN", 30_000),
                "the timer arm fires within the budget and routes to the timeout handler");
        assertNull(sqsEngine.findMessageEventExecution(businessKey, messageName),
                "the event gateway deletes the losing message subscription");
        assertFalse(parkedOn(instance, "node_msgTaskN"),
                "the message arm never runs once the timer won");
    }

    // ---- fixtures --------------------------------------------------------------------------

    @Test
    void timer_arm_fans_out_the_template_promised_timeout_envelope() throws Exception {
        // The catch template's Timeout OUT port declares {timedOut, waitedMs, lastCheckedAt} —
        // the daemon writes that envelope to the timeout branch when the timer arm wins.
        String businessKey = UUID.randomUUID().toString();
        String messageName = "order.paid." + UUID.randomUUID();
        ProcessInstance instance = deployAndStartCatchWithTimeout("toEnv-" + UUID.randomUUID(),
                businessKey, messageName, "2000");

        assertTrue(awaitParked(instance, "node_lateTaskN", 30_000),
                "the timer arm fires within the budget");

        com.studioquack.common.dto.flow.ProcessContextRef readRef =
                new com.studioquack.common.dto.flow.ProcessContextRef();
        readRef.setExecutionId(businessKey);
        List<com.studioquack.common.dto.flow.InOutValueDto> rows =
                executionContextStore.loadInputsTargetingNode(readRef, "lateTaskN");
        com.studioquack.common.dto.flow.InOutValueDto row = rows.stream()
                .filter(r -> "catchN".equals(r.getNodeId()) && "timeout".equals(r.getPortId()))
                .findFirst().orElse(null);
        assertNotNull(row, "the catch node's `timeout` envelope row targets the timeout branch");
        assertTrue(row.getValue() instanceof Map, "the envelope is a single object value");
        Map<?, ?> envelope = (Map<?, ?>) row.getValue();
        org.junit.jupiter.api.Assertions.assertEquals(Boolean.TRUE, envelope.get("timedOut"));
        org.junit.jupiter.api.Assertions.assertEquals(2000,
                ((Number) envelope.get("waitedMs")).intValue(),
                "waitedMs is the effective armed wait");
        assertNotNull(envelope.get("lastCheckedAt"), "lastCheckedAt is stamped at fire time");
    }

    @Test
    void message_arm_writes_no_timeout_envelope() {
        String businessKey = UUID.randomUUID().toString();
        String messageName = "order.paid." + UUID.randomUUID();
        ProcessInstance instance = deployAndStartCatchWithTimeout("toEnvNeg-" + UUID.randomUUID(),
                businessKey, messageName, "3600000");

        Execution subscription = sqsEngine.findMessageEventExecution(businessKey, messageName);
        assertNotNull(subscription, "the catch parks with a live message subscription");
        sqsEngine.messageEventReceived(messageName, subscription.getId(), Map.of());
        assertTrue(parkedOn(instance, "node_msgTaskN"), "the message arm advanced");

        com.studioquack.common.dto.flow.ProcessContextRef readRef =
                new com.studioquack.common.dto.flow.ProcessContextRef();
        readRef.setExecutionId(businessKey);
        List<com.studioquack.common.dto.flow.InOutValueDto> rows =
                executionContextStore.loadInputsTargetingNode(readRef, "lateTaskN");
        assertTrue(rows.stream().noneMatch(r -> "timeout".equals(r.getPortId())),
                "a message win never writes the timeout envelope");
    }

    // ---- fixtures --------------------------------------------------------------------------

    @Test
    void authored_correlation_key_is_pinned_at_park_time_in_the_timeout_shape() {
        // Audit M1: with the event-gateway shape the token parks AT THE GATEWAY, so the catch's
        // start listener never fired at park time and the authored-key pin never existed — a
        // cross-flow sender knowing only the shared authored key could not correlate.
        String businessKey = UUID.randomUUID().toString();
        String messageName = "order.paid." + UUID.randomUUID();
        String sharedKey = "ORDER-" + UUID.randomUUID();
        ProcessInstance instance = deployAndStartCatchWithTimeoutAndKey(
                "toPin-" + UUID.randomUUID(), businessKey, messageName, "3600000", sharedKey);

        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, messageName),
                "the message arm is armed");
        assertNull(sqsEngine.findMessageEventExecution(sharedKey, messageName),
                "precondition: the shared key is nobody's businessKey");

        MessageCorrelationOutcome outcome = correlationService.correlate(messageName, sharedKey, null);
        assertTrue(outcome.correlated(),
                "the authored key correlates in the timeout shape — the pin rides the gateway execution");
        assertTrue(parkedOn(instance, "node_msgTaskN"), "the message arm advanced");
        assertFalse(parkedOn(instance, "node_lateTaskN"), "the timeout arm never runs");
    }

    /** Same graph as {@link #deployAndStartCatchWithTimeout} plus an authored correlation key. */
    private ProcessInstance deployAndStartCatchWithTimeoutAndKey(String workflowId, String businessKey,
                                                                 String messageName, String timeoutMs,
                                                                 String correlationKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto catchN = node("catchN", "message.intermediate.catch.event", null);
        param(catchN, "name", messageName);
        param(catchN, "timeoutMs", timeoutMs);
        param(catchN, "onTimeoutBehavior", "route");
        param(catchN, "correlationKey", correlationKey);
        NodeDto msgTask = node("msgTaskN", "user.task", null);
        NodeDto lateTask = node("lateTaskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Catch Timeout Authored Key", "1",
                List.of(start, catchN, msgTask, lateTask, end),
                List.of(edge("startN", "catchN"),
                        edge("catchN", "msgTaskN", "OUT-message", "INPUT-in"),
                        edge("catchN", "lateTaskN", "OUT-timeout", "INPUT-in"),
                        edge("msgTaskN", "endN"),
                        edge("lateTaskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "process started");
        return instance;
    }

    /**
     * Deploys and starts {@code start → message.intermediate.catch.event(name, timeoutMs, route)}
     * whose {@code message} port feeds {@code msgTaskN} and whose {@code timeout} port feeds
     * {@code lateTaskN} (both user tasks parking for assertion).
     */
    private ProcessInstance deployAndStartCatchWithTimeout(String workflowId, String businessKey,
                                                           String messageName, String timeoutMs) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto catchN = node("catchN", "message.intermediate.catch.event", null);
        param(catchN, "name", messageName);
        param(catchN, "timeoutMs", timeoutMs);
        param(catchN, "onTimeoutBehavior", "route");
        NodeDto msgTask = node("msgTaskN", "user.task", null);
        NodeDto lateTask = node("lateTaskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Catch Timeout Runtime", "1",
                List.of(start, catchN, msgTask, lateTask, end),
                List.of(edge("startN", "catchN"),
                        edge("catchN", "msgTaskN", "OUT-message", "INPUT-in"),
                        edge("catchN", "lateTaskN", "OUT-timeout", "INPUT-in"),
                        edge("msgTaskN", "endN"),
                        edge("lateTaskN", "endN")));
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

    private boolean awaitParked(ProcessInstance instance, String activityId, long budgetMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + budgetMs;
        while (System.currentTimeMillis() < deadline) {
            if (parkedOn(instance, activityId)) {
                return true;
            }
            Thread.sleep(250);
        }
        return parkedOn(instance, activityId);
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

    private EdgeDto edge(String source, String target, String sourceHandle, String targetHandle) {
        EdgeDto edge = edge(source, target);
        edge.setSourceHandle(sourceHandle);
        edge.setTargetHandle(targetHandle);
        return edge;
    }
}
