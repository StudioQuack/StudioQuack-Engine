package com.studioquack.sqs.engine.delegate;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Intra-engine signal payload bridge (audit) — proves that a {@code
 * signal.intermediate.throw.event} / {@code signal.end.event} fired from one process definition (a)
 * still wakes a parked {@code signal.intermediate.catch.event} via Activiti's NATIVE broadcast, and
 * (b) pre-seals the authored IN {@code payload} into the woken subscriber's execution-context store
 * under the subscriber's own businessKey, through the same REF-only {@code sealMsg} seam the
 * external webhook ingress uses.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-signal-sender-listener-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class SignalSenderExecutionListenerTest {

    private static final String SIGNAL_NAME = "shipment.ready";

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore executionContextStore;

    @Test
    void throw_event_seals_payload_into_waiting_catch_subscriber_store() {
        String receiverBusinessKey = UUID.randomUUID().toString();
        String senderBusinessKey = UUID.randomUUID().toString();

        ProcessInstance receiver = deployAndStartSignalCatchFlow(
                "sigRecv-" + UUID.randomUUID(), receiverBusinessKey);
        Execution parked = sqsEngine.findSignalEventExecution(receiverBusinessKey, SIGNAL_NAME);
        assertNotNull(parked, "receiver must park on the signal catch subscription before the sender fires");

        ProcessContextRef senderRef = seedSenderPayload(senderBusinessKey, "throwN",
                Map.of("sku", "A-9", "qty", 2));
        deployAndStartSignalThrowFlow("sigSend-" + UUID.randomUUID(), senderBusinessKey, senderRef);

        assertNull(sqsEngine.findSignalEventExecution(receiverBusinessKey, SIGNAL_NAME),
                "native broadcast must consume the catch subscription");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()), "receiver must continue past the catch");

        ProcessContextRef receiverRef = new ProcessContextRef();
        receiverRef.setExecutionId(receiverBusinessKey);
        List<InOutValueDto> sealed = executionContextStore.loadInputsTargetingNode(receiverRef, "catchN");
        assertTrue(sealed.stream().anyMatch(r -> "sku".equals(r.getTargetPortId()) && "A-9".equals(r.getValue())),
                "payload field 'sku' must be sealed under the receiver's businessKey targeting its catch node");
        assertTrue(sealed.stream().anyMatch(r -> "qty".equals(r.getTargetPortId()) && Integer.valueOf(2).equals(r.getValue())),
                "payload field 'qty' must be sealed under the receiver's businessKey targeting its catch node");

        // C1/F3: the template-promised `signal_payload` envelope is fanned out to the catch's
        // downstream (taskN) — {signalName, payload} in one object row (F3 dropped the
        // duplicate `name` spelling; the canonical key is `signalName`).
        List<InOutValueDto> downstream = executionContextStore.loadInputsTargetingNode(receiverRef, "taskN");
        InOutValueDto envelopeRow = downstream.stream()
                .filter(r -> "catchN".equals(r.getNodeId()) && "signal_payload".equals(r.getPortId()))
                .findFirst().orElse(null);
        assertNotNull(envelopeRow, "the catch node's `signal_payload` envelope row targets its downstream");
        assertTrue(envelopeRow.getValue() instanceof Map, "the envelope is a single object value");
        Map<?, ?> envelope = (Map<?, ?>) envelopeRow.getValue();
        assertEquals(SIGNAL_NAME, envelope.get("signalName"), "the canonical F3 spelling");
        assertFalse(envelope.containsKey("name"), "F3 unified on signalName — no duplicate name key");
        assertEquals(Map.of("sku", "A-9", "qty", 2), envelope.get("payload"), "payload verbatim");
    }

    @Test
    void throw_event_without_payload_still_broadcasts_natively() {
        String receiverBusinessKey = UUID.randomUUID().toString();
        String senderBusinessKey = UUID.randomUUID().toString();

        ProcessInstance receiver = deployAndStartSignalCatchFlow(
                "sigRecv-" + UUID.randomUUID(), receiverBusinessKey);
        assertNotNull(sqsEngine.findSignalEventExecution(receiverBusinessKey, SIGNAL_NAME),
                "receiver must park on the signal catch subscription before the sender fires");

        // No studioquackRef on the sender → no IN rows → payload-less broadcast.
        deployAndStartSignalThrowFlow("sigSend-" + UUID.randomUUID(), senderBusinessKey, null);

        assertNull(sqsEngine.findSignalEventExecution(receiverBusinessKey, SIGNAL_NAME),
                "native broadcast must fire even without a payload");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()), "receiver must continue past the catch");

        ProcessContextRef receiverRef = new ProcessContextRef();
        receiverRef.setExecutionId(receiverBusinessKey);
        List<InOutValueDto> sealed = executionContextStore.loadInputsTargetingNode(receiverRef, "catchN");
        assertTrue(sealed.isEmpty(), "no payload rows may be sealed when the sender authored none");
    }

    @Test
    void throw_event_writes_emit_result_rows_to_downstream() {
        String senderBusinessKey = UUID.randomUUID().toString();

        ProcessContextRef senderRef = seedSenderPayload(senderBusinessKey, "throwN",
                Map.of("note", "hi"));
        deployAndStartSignalThrowFlow("sigSend-" + UUID.randomUUID(), senderBusinessKey, senderRef);

        ProcessContextRef readRef = new ProcessContextRef();
        readRef.setExecutionId(senderBusinessKey);
        List<InOutValueDto> inputsToTask = executionContextStore.loadInputsTargetingNode(readRef, "taskN");
        assertTrue(inputsToTask.stream().anyMatch(r -> "throwN".equals(r.getNodeId())
                        && "success".equals(r.getTargetPortId()) && Boolean.TRUE.equals(r.getValue())),
                "emit_result.success=true must be persisted for the throw's downstream");
        assertTrue(inputsToTask.stream().anyMatch(r -> "throwN".equals(r.getNodeId())
                        && "timestamp".equals(r.getTargetPortId()) && r.getValue() instanceof String),
                "emit_result.timestamp must be persisted for the throw's downstream");
    }

    @Test
    void end_event_seals_payload_and_broadcasts_without_emit_result() {
        String receiverBusinessKey = UUID.randomUUID().toString();
        String senderBusinessKey = UUID.randomUUID().toString();

        deployAndStartSignalCatchFlow("sigRecv-" + UUID.randomUUID(), receiverBusinessKey);
        assertNotNull(sqsEngine.findSignalEventExecution(receiverBusinessKey, SIGNAL_NAME),
                "receiver must park on the signal catch subscription before the sender fires");

        ProcessContextRef senderRef = seedSenderPayload(senderBusinessKey, "endN",
                Map.of("lane", "west"));
        NodeDto start = node("startN", "start.event", "start");
        NodeDto endSig = signalNode("endN", "signal.end.event");
        WorkflowGraph graph = new WorkflowGraph("sigEnd-" + UUID.randomUUID(), "Signal End Sender", "1",
                List.of(start, endSig),
                List.of(edge("startN", "endN")));
        deploymentService.deployAndStart(graph, null, senderBusinessKey,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, senderRef), true);

        assertNull(sqsEngine.findSignalEventExecution(receiverBusinessKey, SIGNAL_NAME),
                "native broadcast from the end event must consume the catch subscription");

        ProcessContextRef receiverRef = new ProcessContextRef();
        receiverRef.setExecutionId(receiverBusinessKey);
        List<InOutValueDto> sealed = executionContextStore.loadInputsTargetingNode(receiverRef, "catchN");
        assertTrue(sealed.stream().anyMatch(r -> "lane".equals(r.getTargetPortId()) && "west".equals(r.getValue())),
                "payload field 'lane' must be sealed under the receiver's businessKey targeting its catch node");

        List<InOutValueDto> senderRows = executionContextStore.loadInputsTargetingNode(senderRef, "startN");
        boolean anyEmitRow = senderRows.stream()
                .anyMatch(r -> "endN".equals(r.getNodeId())
                        && ("success".equals(r.getTargetPortId()) || "timestamp".equals(r.getTargetPortId())));
        assertFalse(anyEmitRow, "signal.end.event has no OUTPUT port; no emit_result rows should be persisted");
    }

    @Test
    void ducktape_payload_is_read_even_without_a_pinned_ref() {
        // A run started without a seedContext carries no studioquackRef — the listener must fall
        // back to the businessKey-keyed lookup instead of dropping the mapped payload.
        String receiverBusinessKey = UUID.randomUUID().toString();
        String senderBusinessKey = UUID.randomUUID().toString();

        ProcessInstance receiver = deployAndStartSignalCatchFlow(
                "sigRecv-" + UUID.randomUUID(), receiverBusinessKey);
        assertNotNull(sqsEngine.findSignalEventExecution(receiverBusinessKey, SIGNAL_NAME));

        seedSenderPayload(senderBusinessKey, "throwN", Map.of("sku", "B-1"));
        deployAndStartSignalThrowFlow("sigSend-" + UUID.randomUUID(), senderBusinessKey,
                null /* no REF pinned */);

        assertNull(sqsEngine.findSignalEventExecution(receiverBusinessKey, SIGNAL_NAME),
                "native broadcast must consume the catch subscription");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()));

        ProcessContextRef receiverRef = new ProcessContextRef();
        receiverRef.setExecutionId(receiverBusinessKey);
        List<InOutValueDto> sealed = executionContextStore.loadInputsTargetingNode(receiverRef, "catchN");
        assertTrue(sealed.stream().anyMatch(r -> "sku".equals(r.getTargetPortId()) && "B-1".equals(r.getValue())),
                "the mapped payload reaches the subscriber even on a REF-less sender run");
    }

    // ---- fixtures --------------------------------------------------------------------------

    /**
     * Deploys and starts {@code start → signal.intermediate.catch.event(SIGNAL_NAME)
     * → user.task → end}; the instance parks on the signal subscription.
     */
    private ProcessInstance deployAndStartSignalCatchFlow(String workflowId, String businessKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto catchN = signalNode("catchN", "signal.intermediate.catch.event");
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Signal Receiver", "1",
                List.of(start, catchN, task, end),
                List.of(edge("startN", "catchN"), edge("catchN", "taskN"), edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "receiver started");
        return instance;
    }

    /**
     * Deploys and starts {@code start → signal.intermediate.throw.event(SIGNAL_NAME)
     * → user.task → end}; the throw fires during start, then the instance parks on the task.
     * When {@code senderRef} is non-null it is pinned as the instance's {@code studioquackRef}.
     */
    private ProcessInstance deployAndStartSignalThrowFlow(String workflowId, String senderBusinessKey,
                                                          ProcessContextRef senderRef) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto throwN = signalNode("throwN", "signal.intermediate.throw.event");
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Signal Sender", "1",
                List.of(start, throwN, task, end),
                List.of(edge("startN", "throwN"), edge("throwN", "taskN"), edge("taskN", "endN")));
        Map<String, Object> vars = senderRef == null
                ? null
                : Map.of(EngineVariableNames.STUDIOQUACK_REF, senderRef);
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, senderBusinessKey, vars, true);
        assertNotNull(instance, "sender started");
        return instance;
    }

    /**
     * Seeds one row targeting {@code targetNodeId}'s INPUT {@code signal} port under
     * {@code senderBusinessKey}, in the pipeline's real wire shape: {@code targetPortId} = the
     * target port's template UUID, {@code targetPortValue} = the port value ({@code signal}),
     * {@code value} = the mapped port object holding the {@code payload} field. Returns the
     * {@link ProcessContextRef} the rows hang off.
     */
    private ProcessContextRef seedSenderPayload(String senderBusinessKey, String targetNodeId,
                                                Map<String, Object> payload) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(senderBusinessKey);
        ctx.setCurrentNodeId("startN");
        ProcessContextRef ref = executionContextStore.snapshot(ctx, null);
        InOutValueDto row = new InOutValueDto();
        row.setNodeId("startN");
        row.setPortId("signal_payload");
        row.setPortValue("signal_payload");
        row.setTargetNodeId(targetNodeId);
        row.setTargetPortId("b8f2a1d0-0000-0000-0000-000000000000");
        row.setTargetPortValue("signal");
        row.setValue(Map.of("payload", payload));
        executionContextStore.saveOutputs(ref, List.of(row));
        return ref;
    }

    private NodeDto signalNode(String id, String bpmnType) {
        NodeDto node = node(id, bpmnType, null);
        ParameterDto nameParam = new ParameterDto();
        nameParam.setShortCode("name");
        nameParam.setValue(SIGNAL_NAME);
        node.getData().setConfigurationParameters(new ArrayList<>(List.of(nameParam)));
        return node;
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
