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
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The escalation bridge (audit: escalation throw/end support). Activiti 9 has no escalation model,
 * so an escalation boundary subscribes to the reserved signal {@code __sq_esc_<code>} and {@link
 * EscalationSenderExecutionListener} fires it instance-scoped — BPMN escalation is scope
 * propagation, not broadcast.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-escalation-sender-listener-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class EscalationSenderExecutionListenerTest {

    private static final String CODE_LATE = "LATE";
    private static final String RESERVED_LATE = "__sq_esc_" + CODE_LATE;
    private static final String RESERVED_URGENT = "__sq_esc_URGENT";

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore executionContextStore;

    @Test
    void throw_fires_the_matching_escalation_boundary_in_the_same_instance() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartXml("escSameThrow",
                sameInstanceXml("escSameThrow", CODE_LATE, RESERVED_LATE, false), businessKey, null);

        assertTrue(parkedOn(instance, "wait_host"), "the boundary's host parks before the throw runs");
        assertTrue(reservedSubscriptionHeldBy(instance, RESERVED_LATE), "the boundary is armed");

        triggerWaitState(instance, "preThrow");

        assertTrue(parkedOn(instance, "handlerN"),
                "the interrupting escalation boundary must route the token to its handler");
        assertTrue(parkedOn(instance, "afterThrow"),
                "the throwing branch continues — escalation is non-interrupting for the thrower");
        assertFalse(parkedOn(instance, "wait_host"),
                "the interrupting boundary abandons the host wait state");
        assertTrue(sqsEngine.findSignalEventExecutions(RESERVED_LATE).stream()
                        .noneMatch(e -> instance.getId().equals(e.getProcessInstanceId())),
                "the fired boundary's reserved subscription is consumed");

        ProcessContextRef readRef = new ProcessContextRef();
        readRef.setExecutionId(businessKey);
        List<InOutValueDto> rows = executionContextStore.loadInputsTargetingNode(readRef, "afterThrow");
        assertTrue(rows.stream().anyMatch(r -> "throwN".equals(r.getNodeId())
                        && "success".equals(r.getTargetPortId()) && Boolean.TRUE.equals(r.getValue())),
                "emit_result.success=true must be persisted for the throw's downstream");
        assertTrue(rows.stream().anyMatch(r -> "throwN".equals(r.getNodeId())
                        && "timestamp".equals(r.getTargetPortId()) && r.getValue() instanceof String),
                "emit_result.timestamp must be persisted for the throw's downstream");
    }

    @Test
    void throw_does_not_fire_a_boundary_of_another_instance() {
        // BPMN escalation propagates within its scope — with flat graphs the instance IS the
        // scope, so a different run's boundary on the same code must NOT fire.
        String catcherKey = "escCatcher-" + UUID.randomUUID();
        ProcessInstance catcher = deployAndStartXml(catcherKey, catcherOnlyXml(catcherKey),
                UUID.randomUUID().toString(), null);
        assertTrue(parkedOn(catcher, "wait_host"), "catcher parks on the boundary's host");
        assertTrue(reservedSubscriptionHeldBy(catcher, RESERVED_LATE),
                "the catcher holds a reserved subscription before the sender fires");

        String senderBusinessKey = UUID.randomUUID().toString();
        deployAndStartSenderFlow(senderBusinessKey, CODE_LATE, null);

        assertTrue(parkedOn(catcher, "wait_host"),
                "another instance's escalation must not fire this boundary");
        assertFalse(parkedOn(catcher, "handlerN"), "the catcher never reaches the handler");
        assertTrue(reservedSubscriptionHeldBy(catcher, RESERVED_LATE),
                "the other instance's subscription survives the sender's escalation");

        ProcessContextRef readRef = new ProcessContextRef();
        readRef.setExecutionId(senderBusinessKey);
        List<InOutValueDto> rows = executionContextStore.loadInputsTargetingNode(readRef, "taskN");
        assertTrue(rows.stream().anyMatch(r -> "throwN".equals(r.getNodeId())
                        && "success".equals(r.getTargetPortId()) && Boolean.TRUE.equals(r.getValue())),
                "the sender still reports a successful throw to its own downstream");
    }

    @Test
    void a_runtime_code_row_overrides_the_configured_code() {
        // Boundary subscribes to URGENT; the throw is configured LATE but its IN code row says
        // URGENT — the runtime row must steer the escalation (listener-driven delivery can honour
        // the override, unlike a native broadcast).
        String businessKey = UUID.randomUUID().toString();
        ProcessContextRef ref = seedCodeRow(businessKey, "throwN", "URGENT");
        ProcessInstance instance = deployAndStartXml("escSameOverride",
                sameInstanceXml("escSameOverride", CODE_LATE, RESERVED_URGENT, false), businessKey,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref));

        triggerWaitState(instance, "preThrow");

        assertTrue(parkedOn(instance, "handlerN"),
                "the runtime code row overrides the configured code and fires the URGENT boundary");
        assertTrue(parkedOn(instance, "afterThrow"), "the throwing branch continues");
    }

    @Test
    void a_runtime_code_row_overrides_the_configured_code_without_a_pinned_ref() {
        // Same override as above, but the run carries no studioquackRef — the listener falls
        // back to the businessKey-keyed lookup and the mapped code still steers the escalation.
        String businessKey = UUID.randomUUID().toString();
        seedCodeRow(businessKey, "throwN", "URGENT");
        ProcessInstance instance = deployAndStartXml("escSameOverrideRefless",
                sameInstanceXml("escSameOverrideRefless", CODE_LATE, RESERVED_URGENT, false),
                businessKey, null /* no REF pinned */);

        triggerWaitState(instance, "preThrow");

        assertTrue(parkedOn(instance, "handlerN"),
                "the mapped code overrides the configured code even on a REF-less run");
        assertTrue(parkedOn(instance, "afterThrow"), "the throwing branch continues");
    }

    @Test
    void end_event_raises_the_escalation_without_emit_result() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartXml("escSameEnd",
                sameInstanceXml("escSameEnd", CODE_LATE, RESERVED_LATE, true), businessKey, null);

        triggerWaitState(instance, "preThrow");

        assertTrue(parkedOn(instance, "handlerN"),
                "the escalation end event raises the escalation before ending the branch");
        assertFalse(parkedOn(instance, "wait_host"), "the interrupting boundary fired");

        ProcessContextRef readRef = new ProcessContextRef();
        readRef.setExecutionId(businessKey);
        List<InOutValueDto> rows = executionContextStore.loadInputsTargetingNode(readRef, "handlerN");
        assertTrue(rows.stream().noneMatch(r -> "endEscN".equals(r.getNodeId())),
                "an escalation end event has no OUTPUT port — no emit_result rows");
    }

    // ---- fixtures --------------------------------------------------------------------------

    /**
     * One process carrying both sides: {@code start → fork → [wait_host (boundary on the reserved
     * signal)] + [preThrow → throw/end with the bridge listener]}. The throw branch parks on {@code
     * preThrow} first; the test triggers it after the boundary is armed, so the assertion never
     * depends on parallel-gateway branch ordering.
     */
    private static String sameInstanceXml(String processKey, String configuredCode,
                                          String reservedSignal, boolean useEndEvent) {
        String signalDeclaration = "  <signal id=\"Signal_" + reservedSignal + "\" name=\""
                + reservedSignal + "\"/>\n";
        String thrower = useEndEvent
                ? "    <endEvent id=\"node_endEscN\">\n"
                + extensionXml(configuredCode)
                + "    </endEvent>\n"
                : "    <intermediateThrowEvent id=\"node_throwN\">\n"
                + extensionXml(configuredCode)
                + "    </intermediateThrowEvent>\n"
                + "    <userTask id=\"afterThrow\" name=\"after\"/>\n";
        String throwerFlows = useEndEvent
                ? "    <sequenceFlow id=\"f4\" sourceRef=\"preThrow\" targetRef=\"node_endEscN\"/>\n"
                : "    <sequenceFlow id=\"f4\" sourceRef=\"preThrow\" targetRef=\"node_throwN\"/>\n"
                + "    <sequenceFlow id=\"f5\" sourceRef=\"node_throwN\" targetRef=\"afterThrow\"/>\n"
                + "    <sequenceFlow id=\"f8\" sourceRef=\"afterThrow\" targetRef=\"endN\"/>\n";
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<definitions xmlns=\"http://www.omg.org/spec/BPMN/20100524/MODEL\"\n"
                + "             xmlns:activiti=\"http://activiti.org/bpmn\"\n"
                + "             xmlns:sq=\"http://studioquack.com/bpmn\"\n"
                + "             targetNamespace=\"http://studioquack.com/test\">\n"
                + signalDeclaration
                + "  <process id=\"" + processKey + "\" name=\"Escalation Test\" isExecutable=\"true\">\n"
                + "    <startEvent id=\"startN\"/>\n"
                + "    <parallelGateway id=\"fork\"/>\n"
                + "    <receiveTask id=\"wait_host\"/>\n"
                + "    <boundaryEvent id=\"besc_host_0\" attachedToRef=\"wait_host\" cancelActivity=\"true\">\n"
                + "      <signalEventDefinition signalRef=\"Signal_" + reservedSignal + "\"/>\n"
                + "    </boundaryEvent>\n"
                + "    <receiveTask id=\"preThrow\"/>\n"
                + thrower
                + "    <userTask id=\"handlerN\" name=\"handler\"/>\n"
                + "    <endEvent id=\"endN\"/>\n"
                + "    <sequenceFlow id=\"f1\" sourceRef=\"startN\" targetRef=\"fork\"/>\n"
                + "    <sequenceFlow id=\"f2\" sourceRef=\"fork\" targetRef=\"wait_host\"/>\n"
                + "    <sequenceFlow id=\"f3\" sourceRef=\"fork\" targetRef=\"preThrow\"/>\n"
                + throwerFlows
                + "    <sequenceFlow id=\"f6\" sourceRef=\"besc_host_0\" targetRef=\"handlerN\"/>\n"
                + "    <sequenceFlow id=\"f7\" sourceRef=\"handlerN\" targetRef=\"endN\"/>\n"
                + "  </process>\n"
                + "</definitions>\n";
    }

    private static String extensionXml(String configuredCode) {
        return "      <extensionElements>\n"
                + "        <sq:escalationConfig escalationCode=\"" + configuredCode + "\"/>\n"
                + "        <activiti:executionListener event=\"start\""
                + " delegateExpression=\"${escalationSenderExecutionListener}\"/>\n"
                + "      </extensionElements>\n";
    }

    /** A catcher-only process: parks on {@code wait_host} with a reserved-signal boundary. */
    private static String catcherOnlyXml(String processKey) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<definitions xmlns=\"http://www.omg.org/spec/BPMN/20100524/MODEL\"\n"
                + "             targetNamespace=\"http://studioquack.com/test\">\n"
                + "  <signal id=\"Signal_" + RESERVED_LATE + "\" name=\"" + RESERVED_LATE + "\"/>\n"
                + "  <process id=\"" + processKey + "\" name=\"Escalation Catcher\" isExecutable=\"true\">\n"
                + "    <startEvent id=\"startN\"/>\n"
                + "    <receiveTask id=\"wait_host\"/>\n"
                + "    <boundaryEvent id=\"besc_host_0\" attachedToRef=\"wait_host\" cancelActivity=\"true\">\n"
                + "      <signalEventDefinition signalRef=\"Signal_" + RESERVED_LATE + "\"/>\n"
                + "    </boundaryEvent>\n"
                + "    <userTask id=\"handlerN\" name=\"handler\"/>\n"
                + "    <endEvent id=\"endN\"/>\n"
                + "    <endEvent id=\"endHandled\"/>\n"
                + "    <sequenceFlow id=\"f1\" sourceRef=\"startN\" targetRef=\"wait_host\"/>\n"
                + "    <sequenceFlow id=\"f2\" sourceRef=\"wait_host\" targetRef=\"endN\"/>\n"
                + "    <sequenceFlow id=\"f3\" sourceRef=\"besc_host_0\" targetRef=\"handlerN\"/>\n"
                + "    <sequenceFlow id=\"f4\" sourceRef=\"handlerN\" targetRef=\"endHandled\"/>\n"
                + "  </process>\n"
                + "</definitions>\n";
    }

    private ProcessInstance deployAndStartXml(String deploymentName, String bpmnXml, String businessKey,
                                              Map<String, Object> variables) {
        sqsEngine.deploy(deploymentName, Map.of("escalation.bpmn20.xml", bpmnXml));
        String processKey = extractProcessKey(bpmnXml);
        ProcessInstance instance = sqsEngine.startLatest(processKey, businessKey, variables);
        assertNotNull(instance, "process started");
        return instance;
    }

    private static String extractProcessKey(String bpmnXml) {
        int idAttr = bpmnXml.indexOf("<process id=\"") + "<process id=\"".length();
        return bpmnXml.substring(idAttr, bpmnXml.indexOf('"', idAttr));
    }

    /**
     * Deploys and starts {@code start → escalation.intermediate.throw.event(code) → user.task →
     * end} through the REAL strategy pipeline; the throw fires during start, then the instance
     * parks on the task.
     */
    private ProcessInstance deployAndStartSenderFlow(String businessKey, String code,
                                                     ProcessContextRef senderRef) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto throwN = node("throwN", "escalation.intermediate.throw.event", null);
        ParameterDto codeParam = new ParameterDto();
        codeParam.setShortCode("code");
        codeParam.setValue(code);
        throwN.getData().setConfigurationParameters(List.of(codeParam));
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph("escSend-" + UUID.randomUUID(), "Escalation Sender", "1",
                List.of(start, throwN, task, end),
                List.of(edge("startN", "throwN"), edge("throwN", "taskN"), edge("taskN", "endN")));
        Map<String, Object> vars = senderRef == null
                ? null
                : Map.of(EngineVariableNames.STUDIOQUACK_REF, senderRef);
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, vars, true);
        assertNotNull(instance, "sender started");
        return instance;
    }

    /**
     * Seeds one row targeting {@code targetNodeId}'s INPUT {@code escalation} port in the
     * pipeline's real wire shape: {@code targetPortId} = the target port's template UUID,
     * {@code targetPortValue} = the port value ({@code escalation}), {@code value} = the mapped
     * port object holding the {@code code} field.
     */
    private ProcessContextRef seedCodeRow(String businessKey, String targetNodeId, String code) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(businessKey);
        ctx.setCurrentNodeId("startN");
        ProcessContextRef ref = executionContextStore.snapshot(ctx, null);
        InOutValueDto row = new InOutValueDto();
        row.setNodeId("startN");
        row.setPortId("emit_result");
        row.setPortValue("emit_result");
        row.setTargetNodeId(targetNodeId);
        row.setTargetPortId("c7a1b3d5-0000-0000-0000-000000000000");
        row.setTargetPortValue("escalation");
        row.setValue(Map.of("code", code));
        executionContextStore.saveOutputs(ref, List.of(row));
        return ref;
    }

    private boolean parkedOn(ProcessInstance instance, String activityId) {
        return sqsEngine.executions()
                .processInstanceId(instance.getId())
                .activityId(activityId)
                .count() > 0;
    }

    private boolean reservedSubscriptionHeldBy(ProcessInstance instance, String reservedSignal) {
        return sqsEngine.findSignalEventExecutions(reservedSignal).stream()
                .anyMatch(e -> instance.getId().equals(e.getProcessInstanceId()));
    }

    /** Completes a parked {@code ReceiveTask} — the same seam the sandbox result callback uses. */
    private void triggerWaitState(ProcessInstance instance, String activityId) {
        String executionId = sqsEngine.executions()
                .processInstanceId(instance.getId())
                .activityId(activityId)
                .singleResult()
                .getId();
        sqsEngine.trigger(executionId, null);
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
