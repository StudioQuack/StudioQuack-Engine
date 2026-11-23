package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.SchemaItemDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.bpmn.FeelToJuelTranslator;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageReceiverConfigurator;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageSenderConfigurator;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import com.studioquack.sqs.engine.strategy.helper.ActivitiTimerDefinitionFactory;
import com.studioquack.sqs.engine.strategy.impl.ActivitiCallActivityStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiEndEventStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiErrorEndStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiEscalationEndStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiEscalationThrowStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiEventBasedGatewayStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiExclusiveGatewayStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiInclusiveGatewayStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiLinkCatchStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiLinkThrowStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiMessageCatchStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiMessageEndStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiMessageStartStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiMessageThrowStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiNoneThrowStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiParallelGatewayStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiServiceTaskStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiSignalCatchStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiSignalEndStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiSignalStartStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiSignalThrowStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiStartEventStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiTerminateEndStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiTimerCatchStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiTimerStartStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiUserTaskStrategy;
import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BoundaryEvent;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.CallActivity;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.ErrorEventDefinition;
import org.activiti.bpmn.model.EventDefinition;
import org.activiti.bpmn.model.ExclusiveGateway;
import org.activiti.bpmn.model.ExtensionElement;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.LinkEventDefinition;
import org.activiti.bpmn.model.Message;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.ParallelGateway;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.Signal;
import org.activiti.bpmn.model.ThrowEvent;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.junit.jupiter.api.Test;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the daemon's {@link BpmnGenerationService}: the generator emits well-formed,
 * schema-valid Activiti BPMN with the sandbox async pattern (serviceTask async + receiveTask +
 * error gate + interrupting error boundary → shared error end), NCName-safe UUID ids, structural
 * start mapping, FEEL→JUEL gateway conditions, mixed node types, function-port sub-processes, and
 * main-process exclusion of function tools. Pure unit test — no Spring, no DB.
 */
class BpmnGenerationServiceTest {

    private BpmnGenerationService newService() {
        ActivitiSignalMessageRegistry reg = new ActivitiSignalMessageRegistry();
        ActivitiMessageSenderConfigurator senderCfg = new ActivitiMessageSenderConfigurator();
        ActivitiMessageReceiverConfigurator receiverCfg = new ActivitiMessageReceiverConfigurator();
        ActivitiBoundaryEventHelper boundaryHelper = new ActivitiBoundaryEventHelper(reg, receiverCfg);
        ActivitiBpmnElementHelper helper = new ActivitiBpmnElementHelper(boundaryHelper);
        // Fallback last, mirroring @Order(LOWEST_PRECEDENCE).
        List<ActivitiFlowNodeStrategy> strategies = List.of(
                new ActivitiStartEventStrategy(),
                new ActivitiTimerStartStrategy(),
                new ActivitiSignalStartStrategy(reg),
                new ActivitiMessageStartStrategy(reg, receiverCfg),
                new ActivitiEndEventStrategy(),
                new ActivitiTerminateEndStrategy(),
                new ActivitiErrorEndStrategy(),
                new ActivitiSignalEndStrategy(reg),
                new ActivitiMessageEndStrategy(reg, senderCfg),
                new ActivitiExclusiveGatewayStrategy(),
                new ActivitiParallelGatewayStrategy(),
                new ActivitiInclusiveGatewayStrategy(),
                new ActivitiEventBasedGatewayStrategy(),
                new ActivitiCallActivityStrategy(boundaryHelper,
                        new com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper(
                                boundaryHelper),
                        new com.studioquack.sqs.command.CallActivityShortCodeResolver(
                                org.mockito.Mockito.mock(
                                        com.studioquack.sqs.command.store.DeployedArtifactStore.class))),
                new ActivitiUserTaskStrategy(boundaryHelper),
                new ActivitiTimerCatchStrategy(),
                new ActivitiSignalCatchStrategy(reg),
                new ActivitiMessageCatchStrategy(reg, receiverCfg),
                new ActivitiLinkCatchStrategy(),
                new ActivitiSignalThrowStrategy(reg),
                new ActivitiMessageThrowStrategy(reg, senderCfg),
                new ActivitiLinkThrowStrategy(),
                new ActivitiNoneThrowStrategy(),
                new ActivitiEscalationThrowStrategy(),
                new ActivitiEscalationEndStrategy(),
                new ActivitiServiceTaskStrategy(helper));
        return new BpmnGenerationService(strategies, new FeelToJuelTranslator(), boundaryHelper);
    }

    private NodeDto node(String id, String bpmnType, String jobType) {
        return node(id, bpmnType, jobType, null);
    }

    /** A timer start/catch node with the authored {@code type} + {@code value} params. */
    private NodeDto timerNode(String id, String bpmnType, String type, String value) {
        NodeDto node = node(id, bpmnType, null);
        ParameterDto typeParam = new ParameterDto();
        typeParam.setShortCode("type");
        typeParam.setValue(type);
        ParameterDto valueParam = new ParameterDto();
        valueParam.setShortCode("value");
        valueParam.setValue(value);
        node.getData().setConfigurationParameters(List.of(typeParam, valueParam));
        return node;
    }

    private NodeDto linkNode(String id, String bpmnType, String linkName) {
        NodeDto node = node(id, bpmnType, null);
        ParameterDto nameParam = new ParameterDto();
        nameParam.setShortCode("name");
        nameParam.setValue(linkName);
        node.getData().setConfigurationParameters(List.of(nameParam));
        return node;
    }

    /** A call.activity node with the authored {@code processId} param. */
    private NodeDto callActivityNode(String id, String processId) {
        NodeDto node = node(id, "call.activity", null);
        ParameterDto processParam = new ParameterDto();
        processParam.setShortCode("processId");
        processParam.setValue(processId);
        node.getData().setConfigurationParameters(List.of(processParam));
        return node;
    }

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

    private NodeDto duckbotBoundary(String id, String nodeType) {
        NodeDto node = node(id, nodeType + ".event", "custom-script-executor", nodeType);
        node.getData().setCategory("conversion");
        return node;
    }

    /** Parses XML and returns true iff the main process has a top-level StartEvent with no event
     * definitions — the exact shape Activiti's parser needs to set the process initial element. */
    private boolean hasNoneStartEvent(String xml) {
        org.activiti.bpmn.model.BpmnModel model = new org.activiti.bpmn.converter.BpmnXMLConverter()
                .convertToBpmnModel(() -> new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                        false, false);
        org.activiti.bpmn.model.Process p = model.getMainProcess();
        return p != null && p.getFlowElements().stream()
                .filter(fe -> fe instanceof org.activiti.bpmn.model.StartEvent)
                .map(fe -> (org.activiti.bpmn.model.StartEvent) fe)
                .anyMatch(se -> se.getEventDefinitions() == null || se.getEventDefinitions().isEmpty());
    }

    /**
     * Deploy-parity validation: run the generated XML back through Activiti's
     * {@link BpmnXMLConverter} with schema validation ON — exactly what {@code RepositoryService.deploy}
     * does. Catches schema violations a plain DOM well-formedness parse would miss.
     */
    private void assertDeployableBpmn(String xml) {
        assertDoesNotThrow(() -> new BpmnXMLConverter().convertToBpmnModel(
                () -> new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                true,   // validateSchema — the check the engine runs on deploy
                false), // enableSafeBpmnXml
                "generated BPMN must pass Activiti schema validation; xml=" + xml);
    }

    private EdgeDto edge(String source, String target) {
        return edge(source, target, null, null);
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

    /** The designer edge that leaves a boundary event: carries {@code data.boundaryEventId}. */
    private EdgeDto boundaryEdge(String source, String target, String boundaryEventId) {
        EdgeDto edge = edge(source, target);
        EdgeDataDto data = new EdgeDataDto();
        data.setBoundaryEventId(boundaryEventId);
        edge.setData(data);
        return edge;
    }

    /** Sandbox node carrying {@code configs} as its boundary events. */
    private NodeDto sandboxWithBoundaries(String id, BoundaryEventConfig... configs) {
        NodeDto node = node(id, "custom.sandbox", "custom-script-executor", "serviceTask");
        node.getData().setBoundaryEventConfigs(List.of(configs));
        return node;
    }

    private BoundaryEventConfig cfg(BoundaryEventConfig config, String id, String targetNodeId) {
        config.setId(id);
        config.setTargetNodeId(targetNodeId);
        return config;
    }

    private BpmnModel parseModel(String xml) {
        return new BpmnXMLConverter().convertToBpmnModel(
                () -> new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), false, false);
    }

    private BoundaryEvent boundaryOf(String xml, String boundaryId) {
        FlowElement element = parseModel(xml).getMainProcess().getFlowElement(boundaryId);
        assertInstanceOf(BoundaryEvent.class, element,
                "boundary '" + boundaryId + "' must be emitted; xml=" + xml);
        return (BoundaryEvent) element;
    }

    /** @return the single event definition of {@code boundaryId}, cast to {@code type}. */
    private <T> T definitionOf(String xml, String boundaryId, Class<T> type) {
        BoundaryEvent boundary = boundaryOf(xml, boundaryId);
        assertEquals(1, boundary.getEventDefinitions().size(),
                "a boundary carries exactly one event definition; xml=" + xml);
        EventDefinition def = boundary.getEventDefinitions().get(0);
        assertInstanceOf(type, def, "unexpected event definition on " + boundaryId + "; xml=" + xml);
        return type.cast(def);
    }

    /** @return the {@code targetRef} of the single flow leaving {@code sourceElementId}. */
    private String routeTargetOf(String xml, String sourceElementId) {
        return parseModel(xml).getMainProcess().getFlowElements().stream()
                .filter(SequenceFlow.class::isInstance)
                .map(SequenceFlow.class::cast)
                .filter(f -> sourceElementId.equals(f.getSourceRef()))
                .map(SequenceFlow::getTargetRef)
                .findFirst()
                .orElse(null);
    }

    @Test
    void generates_wellformed_activiti_bpmn_with_sandbox_pattern() {
        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                node("script1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "script1"),
                edge("script1", "end1"));

        String xml = newService().generateBpmnXml("wf1", "My Flow", nodes, edges);

        assertTrue(xml.contains("Process_wf1"), "process id");
        assertTrue(xml.contains("scriptDispatchDelegate"), "dispatch delegate expression");
        assertTrue(xml.contains("receiveTask"), "receive task wait-state");
        assertTrue(xml.contains("sqErrorGateDelegate"), "error gate delegate");
        assertTrue(xml.contains("boundaryEvent"), "error boundary event");
        assertTrue(xml.contains("errorEventDefinition"), "error event definition");
        assertTrue(xml.contains("Event_SandboxErrorEnd"), "shared sandbox error end");
        assertTrue(xml.contains("async=\"true\"") || xml.contains("activiti:async=\"true\""),
                "service task is async");
        // Failure-hold wiring: catch-all boundary → failroute gateway → hold task (root) or the
        // shared error end (child default), with the repair-retry loop back onto the dispatch task.
        assertTrue(xml.contains("failroute_script1"), "fail-route gateway");
        assertTrue(xml.contains("hold_script1"), "hold task");
        assertTrue(xml.contains("sqFailureHoldPolicy.shouldHold(execution)"), "hold condition");
        assertTrue(xml.contains("sqFailureHoldDelegate"), "hold delegate expression");
        assertTrue(xml.contains("flow_script1_hold_to_retry"), "repair-retry loop-back flow");

        assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
        assertDeployableBpmn(xml);
    }

    @Test
    void catch_all_boundary_routes_through_failroute_gateway_to_hold_or_error_end() {
        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                node("script1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("start1", "script1"), edge("script1", "end1"));

        String xml = newService().generateBpmnXml("wfHold", "Hold Flow", nodes, edges);
        org.activiti.bpmn.model.Process process = parseModel(xml).getMainProcess();

        // The boundary no longer flows straight to the end: it feeds the fail-route gateway.
        assertEquals("failroute_script1", routeTargetOf(process, "err_script1"),
                "catch-all boundary feeds the fail-route gateway");
        FlowElement failrouteEl = process.getFlowElement("failroute_script1");
        ExclusiveGateway failroute = assertInstanceOf(ExclusiveGateway.class, failrouteEl,
                "failroute_<node> is an exclusive gateway");

        // HOLD leg: conditional on the root-run policy, into the async hold task.
        SequenceFlow toHold = outgoingOf(process, "failroute_script1", "hold_script1");
        assertEquals(ActivitiBpmnElementHelper.FAILURE_HOLD_CONDITION, toHold.getConditionExpression(),
                "the hold leg carries the shouldHold condition");
        org.activiti.bpmn.model.ServiceTask hold = assertInstanceOf(
                org.activiti.bpmn.model.ServiceTask.class, process.getFlowElement("hold_script1"),
                "hold_<node> is a service task");
        assertTrue(hold.isAsynchronous(), "the hold task is async so its throw dead-letters");
        assertEquals(ActivitiBpmnElementHelper.FAILURE_HOLD_DELEGATE, hold.getImplementation(),
                "the hold task invokes the failure-hold delegate");

        // END leg: the gateway's DEFAULT flow keeps the old flow id and ends at the shared end.
        SequenceFlow toEnd = outgoingOf(process, "failroute_script1",
                ActivitiBpmnElementHelper.SHARED_ERROR_END_ID);
        assertEquals("flow_script1_err_to_end", toEnd.getId(),
                "the default end leg keeps the pre-hold flow id");
        assertNull(toEnd.getConditionExpression(), "the default leg is unconditional");
        assertEquals(toEnd.getId(), failroute.getDefaultFlow(),
                "the error-end leg is the gateway default (child runs end FAILED as before)");

        // Repair-retry loop-back: hold → the node's dispatch task, so a repaired token re-dispatches.
        SequenceFlow retry = outgoingOf(process, "hold_script1", "node_script1");
        assertEquals("flow_script1_hold_to_retry", retry.getId(), "loop-back flow id");
        assertDeployableBpmn(xml);
    }

    /** @return the single flow from {@code sourceRef} to {@code targetRef} (fails when absent). */
    private SequenceFlow outgoingOf(org.activiti.bpmn.model.Process process,
                                    String sourceRef, String targetRef) {
        return process.getFlowElements().stream()
                .filter(SequenceFlow.class::isInstance)
                .map(SequenceFlow.class::cast)
                .filter(f -> sourceRef.equals(f.getSourceRef()) && targetRef.equals(f.getTargetRef()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no flow " + sourceRef + " -> " + targetRef));
    }

    /** routeTargetOf variant taking an already-parsed process. */
    private String routeTargetOf(org.activiti.bpmn.model.Process process, String sourceElementId) {
        return process.getFlowElements().stream()
                .filter(SequenceFlow.class::isInstance)
                .map(SequenceFlow.class::cast)
                .filter(f -> sourceElementId.equals(f.getSourceRef()))
                .map(SequenceFlow::getTargetRef)
                .findFirst()
                .orElse(null);
    }

    @Test
    void generates_schema_valid_bpmn_with_uuid_ids() {
        // Reproduces the real FE shape: node AND edge ids are UUIDs that START WITH A DIGIT, which
        // are illegal XML NCNames. The generator must sanitise every emitted id/ref so the BPMN
        // still passes Activiti schema validation on deploy.
        NodeDto start = node("9fb19140-e722-4d20-b9c7-6e36db65caea", "start.event", null);
        NodeDto sandbox = node("215f7e58-a9a9-4faa-8792-8797da19165c", "custom.sandbox", "custom-script-executor");
        NodeDto end = node("3ac6e446-e5c2-4efb-95ef-7d4fa3603191", "end.event", null);
        EdgeDto e1 = edge(start.getId(), sandbox.getId());
        e1.setId("9fb19140-e722-4d20-b9c7-6e36db65caea");
        EdgeDto e2 = edge(sandbox.getId(), end.getId());
        e2.setId("04d58a70-86eb-42a3-ae66-570242b4ea83");

        String xml = newService().generateBpmnXml(
                "215f7e58-a9a9-4faa-8792-8797da19165c", "test2",
                List.of(start, sandbox, end), List.of(e1, e2));

        assertDeployableBpmn(xml);
    }

    @Test
    void start_node_maps_to_none_start_event_by_nodeType_when_bpmnType_is_not_start_event() {
        // A start node whose bpmnType is NOT exactly "start.event" (here "start") but whose designer
        // role nodeType == "start" must still become a top-level none-StartEvent.
        NodeDto start = node("start1", "start", null, "start");
        NodeDto sandbox = node("script1", "custom.sandbox", "custom-script-executor", "serviceTask");
        NodeDto end = node("end1", "someCustomEnd", null, "end");
        List<EdgeDto> edges = List.of(edge("start1", "script1"), edge("script1", "end1"));

        String xml = newService().generateBpmnXml("wfStart", "Start By NodeType",
                List.of(start, sandbox, end), edges);

        assertTrue(xml.contains("<startEvent") || xml.contains(":startEvent"), "must emit a StartEvent; xml=" + xml);
        assertTrue(hasNoneStartEvent(xml),
                "main process must have a top-level none-StartEvent (Activiti initial element); xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void exclusive_gateway_condition_is_translated_to_juel() {
        NodeDto gateway = node("gw1", "exclusive.gateway", null);
        EdgeTransformDto transform = new EdgeTransformDto();
        transform.setTargetNodeId("a1");
        transform.setSourcePath("conditionExpression");
        transform.setSourcePathValue("= route__gw1 = 'approved'");
        gateway.setEdgeTransformListForCurrentNode(List.of(transform));

        // Two branches: Activiti forbids a condition on a SINGLE-outgoing gateway, so a legal
        // conditioned flow needs a sibling (the conditionless sibling becomes the default).
        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "gw1"),
                edge("gw1", "a1"),
                edge("gw1", "b1"),
                edge("a1", "end1"),
                edge("b1", "end1"));

        String xml = newService().generateBpmnXml("wf2", "Cond", nodes, edges);
        assertTrue(xml.contains("route__gw1 == &#39;approved&#39;") || xml.contains("route__gw1 == 'approved'"),
                "gateway condition translated to JUEL equality; xml=" + xml);
        assertNoProcessValidationErrors(xml);
    }

    // ---- Deploy-validation parity: the failing shapes from the daemon deploy log ----

    /**
     * Runs Activiti's own {@code ProcessValidatorImpl} — the exact check
     * {@code RepositoryService.deploy} fails on — over the round-tripped model, so a test fails
     * on ANY process-validation error (gateway rules, condition syntax), not just XML schema.
     */
    private void assertNoProcessValidationErrors(String xml) {
        BpmnModel model = parseModel(xml);
        List<org.activiti.validation.ValidationError> errors = new org.activiti.validation
                .ProcessValidatorFactory().createDefaultProcessValidator().validate(model);
        assertTrue(errors.isEmpty(),
                "generated BPMN must pass Activiti process validation; errors=" + errors + "; xml=" + xml);
    }

    @Test
    void single_outgoing_exclusive_gateway_flow_carries_no_condition() {
        // Mystery M1: a sole authored outgoing WITH a condition must KEEP that condition.
        // Activiti still forbids a condition on a single-outgoing XOR, so generation adds a
        // synthetic default to the shared error end (false → loud fail, not a silent pass).
        NodeDto gateway = node("node_61359ea8-4f2a-4c1b-9f00-abcdef012345", "exclusive.gateway", null);

        EdgeDto toA = edge(gateway.getId(), "a1");
        EdgeDataDto typed = new EdgeDataDto();
        typed.setCondition("$(@cart.total) > 100");
        toA.setData(typed);

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", gateway.getId()),
                toA,
                edge("a1", "end1"));

        String xml = newService().generateBpmnXml("wfSingle", "Single Outgoing", nodes, edges);

        FlowElement gwEl = parseModel(xml).getMainProcess()
                .getFlowElement("node_" + gateway.getId());
        assertInstanceOf(ExclusiveGateway.class, gwEl, "xml=" + xml);
        ExclusiveGateway xor = (ExclusiveGateway) gwEl;
        assertNotEquals("flow_" + toA.getId(), xor.getDefaultFlow(),
                "the conditioned sole exit must not be the gateway default; xml=" + xml);
        assertNotNull(xor.getDefaultFlow(),
                "a synthetic error default is required so Activiti validation still passes; xml=" + xml);
        SequenceFlow authored = (SequenceFlow) parseModel(xml).getMainProcess()
                .getFlowElement("flow_" + toA.getId());
        assertNotNull(authored.getConditionExpression(),
                "authored condition must be kept on the sole outgoing; xml=" + xml);
        SequenceFlow miss = (SequenceFlow) parseModel(xml).getMainProcess()
                .getFlowElement(xor.getDefaultFlow());
        assertNotNull(miss, "synthetic default flow must exist; xml=" + xml);
        assertEquals(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID, miss.getTargetRef(),
                "synthetic default must terminate at the shared error end; xml=" + xml);
        assertNull(miss.getConditionExpression(),
                "BPMN default flow must not carry a condition; xml=" + xml);
        assertTrue(xml.contains("conditionExpression"),
                "sole conditioned exit must emit conditionExpression; xml=" + xml);
        assertNoProcessValidationErrors(xml);
    }

    @Test
    void all_conditioned_exclusive_gateway_gets_synthetic_error_default() {
        // Two conditioned exits and no blank sibling: token __dead_end__ makes both JUEL
        // compares false; without a default Activiti throws no-outgoing-flow.
        NodeDto gateway = node("gwAllCond", "exclusive.gateway", null);
        EdgeDto toA = edge("gwAllCond", "a1");
        EdgeDataDto aData = new EdgeDataDto();
        aData.setCondition("@ai_agent.main_output.final_answer == \" \"");
        toA.setData(aData);
        EdgeDto toB = edge("gwAllCond", "b1");
        EdgeDataDto bData = new EdgeDataDto();
        bData.setCondition("@ai_agent.main_output.final_answer == \"hello\"");
        toB.setData(bData);

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "gwAllCond"), toA, toB, edge("a1", "end1"), edge("b1", "end1"));

        String xml = newService().generateBpmnXml("wfAllCond", "All Conditioned", nodes, edges);
        ExclusiveGateway xor = (ExclusiveGateway) parseModel(xml).getMainProcess()
                .getFlowElement("node_gwAllCond");
        assertNotNull(xor.getDefaultFlow(),
                "all-conditioned XOR must gain a synthetic error default; xml=" + xml);
        assertNotEquals("flow_" + toA.getId(), xor.getDefaultFlow());
        assertNotEquals("flow_" + toB.getId(), xor.getDefaultFlow());
        SequenceFlow miss = (SequenceFlow) parseModel(xml).getMainProcess()
                .getFlowElement(xor.getDefaultFlow());
        assertEquals(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID, miss.getTargetRef(),
                "synthetic default must terminate at the shared error end; xml=" + xml);
        assertNull(miss.getConditionExpression());
        assertNoProcessValidationErrors(xml);
    }

    @Test
    void typed_condition_with_hyphenated_uuid_gateway_id_is_valid_juel() {
        // Second reported error ("Conditional expression is not valid"): the route-token variable
        // embedded the RAW hyphenated FE node id — inside ${…} a hyphen parses as subtraction and
        // a digit-leading UUID segment is an outright JUEL parse error. The variable name must be
        // JUEL-sanitised (and match what SqsEngine.setRouteToken writes at runtime).
        String gwId = "node_61359ea8-4f2a-4c1b-9f00-abcdef012345";
        NodeDto gateway = node(gwId, "exclusive.gateway", null);

        EdgeDto toA = edge(gwId, "a1");
        EdgeDataDto typed = new EdgeDataDto();
        typed.setCondition("$(@cart.total) > 100");
        toA.setData(typed);
        EdgeDto toB = edge(gwId, "b1");
        EdgeDataDto defaultData = new EdgeDataDto();
        defaultData.setIsDefaultFlow(true);
        toB.setData(defaultData);

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", gwId), toA, toB, edge("a1", "end1"), edge("b1", "end1"));

        String xml = newService().generateBpmnXml("wfUuid", "UUID Cond", nodes, edges);

        assertTrue(xml.contains("route__node_61359ea8_4f2a_4c1b_9f00_abcdef012345 == &#39;a1&#39;")
                        || xml.contains("route__node_61359ea8_4f2a_4c1b_9f00_abcdef012345 == 'a1'"),
                "hyphenated gateway id must be JUEL-sanitised in the route token; xml=" + xml);
        assertFalse(xml.contains("route__node_61359ea8-"),
                "the raw hyphenated id must never appear inside a condition; xml=" + xml);
        // Runtime write/read agreement: the token the engine sets resolves this condition.
        assertEquals("route__node_61359ea8_4f2a_4c1b_9f00_abcdef012345",
                BpmnGenerationService.routeTokenName(gwId),
                "generator and runtime must agree on the sanitised token name");
        assertNoProcessValidationErrors(xml);
    }

    @Test
    void explicit_default_flow_never_carries_a_condition() {
        // Contradictory authoring: typed condition AND isDefaultFlow on the same edge. BPMN: a
        // default flow carries no condition — when BOTH are set, the CONDITION wins (the flow keeps
        // conditionExpression and is not the gateway default).
        NodeDto gateway = node("gw1", "exclusive.gateway", null);

        EdgeDto toA = edge("gw1", "a1");
        EdgeDataDto aData = new EdgeDataDto();
        aData.setCondition("$(x) > 1");
        aData.setIsDefaultFlow(true); // contradictory authoring: conditioned AND default
        toA.setData(aData);
        EdgeDto toB = edge("gw1", "b1");
        EdgeDataDto bData = new EdgeDataDto();
        bData.setCondition("$(x) > 5");
        toB.setData(bData);

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "gw1"), toA, toB, edge("a1", "end1"), edge("b1", "end1"));

        String xml = newService().generateBpmnXml("wfDefaultCond", "Default With Cond", nodes, edges);

        FlowElement gwEl = parseModel(xml).getMainProcess().getFlowElement("node_gw1");
        assertInstanceOf(ExclusiveGateway.class, gwEl, "xml=" + xml);
        assertNotEquals("flow_e_gw1_a1", ((ExclusiveGateway) gwEl).getDefaultFlow(),
                "conditioned+isDefaultFlow edge must not be the gateway default; xml=" + xml);
        SequenceFlow conditioned = (SequenceFlow) parseModel(xml).getMainProcess()
                .getFlowElement("flow_e_gw1_a1");
        assertNotNull(conditioned.getConditionExpression(),
                "CONDITION wins — the flow keeps conditionExpression; xml=" + xml);
        assertNoProcessValidationErrors(xml);
    }

    @Test
    void unparsable_or_plain_variable_condition_fails_deploy_instead_of_misrouting() {
        // Dropping a bad condition would leave flow_e_gw1_a1 unconditioned — an XOR takes an
        // unconditioned non-default flow unconditionally — so the deploy names the edge and fails.
        for (String bad : List.of("${unclosed == 'x'", "${approved}")) {
            NodeDto gateway = node("gw1", "exclusive.gateway", null);
            EdgeDto toA = edge("gw1", "a1");
            EdgeDataDto aData = new EdgeDataDto();
            aData.setCondition(bad);
            toA.setData(aData);
            EdgeDto toB = edge("gw1", "b1");
            EdgeDataDto bData = new EdgeDataDto();
            bData.setCondition("$(x) > 5");
            toB.setData(bData);

            List<NodeDto> nodes = List.of(
                    node("start1", "start.event", null),
                    gateway,
                    node("a1", "custom.sandbox", "custom-script-executor"),
                    node("b1", "custom.sandbox", "custom-script-executor"),
                    node("end1", "end.event", null));
            List<EdgeDto> edges = List.of(
                    edge("start1", "gw1"), toA, toB, edge("a1", "end1"), edge("b1", "end1"));

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> newService().generateBpmnXml("wfGarbage", "Garbage Cond", nodes, edges), bad);
            assertTrue(ex.getMessage().contains("route__"), ex.getMessage());
        }
    }

    @Test
    void bean_reaching_gateway_juel_fails_deploy() {
        NodeDto gateway = node("gw1", "exclusive.gateway", null);
        EdgeDto toA = edge("gw1", "a1");
        EdgeDataDto aData = new EdgeDataDto();
        aData.setCondition("${channelProperties.getBearer()}");
        toA.setData(aData);
        EdgeDto toB = edge("gw1", "b1");

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "gw1"), toA, toB, edge("a1", "end1"), edge("b1", "end1"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfInject", "Inject", nodes, edges));
        assertTrue(ex.getMessage().contains("route__"), ex.getMessage());
    }

    @Test
    void auto_inserts_parallel_fork_and_join_for_a_converging_branch() {
        // Duckbot-style graph: the start fans out to a Docling converter AND an agent in parallel,
        // then the agent also consumes the converter's output. Without an auto fork/join the agent
        // (2 incoming edges) fires on the FIRST token and the Docling branch never joins — the
        // exact defect in the report.
        NodeDto start = node("startN", "start.event", null);
        NodeDto docling = node("doclingN", "custom.sandbox", "custom-script-executor");
        NodeDto agent = node("agentN", "custom.sandbox", "custom-script-executor");
        NodeDto end = node("endN", "end.event", null);
        List<EdgeDto> edges = List.of(
                edge("startN", "doclingN"),
                edge("startN", "agentN"),
                edge("doclingN", "agentN"),
                edge("agentN", "endN"));

        String xml = newService().generateBpmnXml("wfPar", "Parallel Converge",
                List.of(start, docling, agent, end), edges);

        assertInstanceOf(ParallelGateway.class,
                parseModel(xml).getMainProcess().getFlowElement("gwfork_startN"),
                "start with >1 outgoing edge must fan out through a ParallelGateway fork; xml=" + xml);
        assertInstanceOf(ParallelGateway.class,
                parseModel(xml).getMainProcess().getFlowElement("gwjoin_agentN"),
                "agent with >1 incoming edge must merge through a ParallelGateway join; xml=" + xml);
        assertEquals(2, flowsFrom(xml, "gwfork_startN"),
                "fork must carry both outgoing branches; xml=" + xml);
        assertEquals(2, flowsInto(xml, "gwjoin_agentN"),
                "join must gather both incoming branches; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void authored_parallel_gateway_is_not_double_wrapped_but_parallel_branches_still_join() {
        // An explicit parallel.gateway already expresses the split, so it must NOT get a synthetic
        // fork. The two branches it opens still converge on the end event, which DOES get a join.
        NodeDto start = node("s1", "start.event", null);
        NodeDto par = node("p1", "parallel.gateway", null);
        NodeDto a = node("a1", "custom.sandbox", "custom-script-executor");
        NodeDto b = node("b1", "custom.sandbox", "custom-script-executor");
        NodeDto end = node("e1", "end.event", null);
        List<EdgeDto> edges = List.of(
                edge("s1", "p1"),
                edge("p1", "a1"),
                edge("p1", "b1"),
                edge("a1", "e1"),
                edge("b1", "e1"));

        String xml = newService().generateBpmnXml("wfAuthored", "Authored Parallel",
                List.of(start, par, a, b, end), edges);

        assertFalse(xml.contains("gwfork_p1"),
                "an authored parallel.gateway must not be wrapped in a synthetic fork; xml=" + xml);
        assertInstanceOf(ParallelGateway.class,
                parseModel(xml).getMainProcess().getFlowElement("gwjoin_e1"),
                "the two parallel branches must merge through a ParallelGateway join; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void exclusive_merge_is_left_as_is_no_parallel_join() {
        // Two exclusive branches converge on one node. Only ONE branch ever runs, so a parallel join
        // would deadlock — the generator must leave the merge as plain sequence flows.
        NodeDto start = node("s1", "start.event", null);
        NodeDto gw = node("gw1", "exclusive.gateway", null);
        NodeDto a = node("a1", "custom.sandbox", "custom-script-executor");
        NodeDto b = node("b1", "custom.sandbox", "custom-script-executor");
        NodeDto merge = node("m1", "custom.sandbox", "custom-script-executor");
        NodeDto end = node("e1", "end.event", null);
        List<EdgeDto> edges = List.of(
                edge("s1", "gw1"),
                edge("gw1", "a1"),
                edge("gw1", "b1"),
                edge("a1", "m1"),
                edge("b1", "m1"),
                edge("m1", "e1"));

        String xml = newService().generateBpmnXml("wfExcl", "Exclusive Merge",
                List.of(start, gw, a, b, merge, end), edges);

        assertFalse(xml.contains("gwjoin_m1"),
                "a merge fed by exclusive branches must not get a parallel join; xml=" + xml);
        assertFalse(xml.contains("gwfork_gw1"),
                "an authored exclusive.gateway must not be wrapped in a synthetic fork; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    /** Count of sequence flows leaving {@code sourceElementId} in the main process. */
    private long flowsFrom(String xml, String sourceElementId) {
        return parseModel(xml).getMainProcess().getFlowElements().stream()
                .filter(SequenceFlow.class::isInstance)
                .map(SequenceFlow.class::cast)
                .filter(f -> sourceElementId.equals(f.getSourceRef()))
                .count();
    }

    /** Count of sequence flows arriving at {@code targetElementId} in the main process. */
    private long flowsInto(String xml, String targetElementId) {
        return parseModel(xml).getMainProcess().getFlowElements().stream()
                .filter(SequenceFlow.class::isInstance)
                .map(SequenceFlow.class::cast)
                .filter(f -> targetElementId.equals(f.getTargetRef()))
                .count();
    }

    @Test
    void generates_mixed_as_is_node_types_wellformed() {
        List<NodeDto> nodes = List.of(
                timerNode("s1", "timer.start.event", "duration", "PT5M"),
                node("u1", "user.task", null),
                callActivityNode("c1", "wf-target"),
                node("g1", "inclusive.gateway", null),
                linkNode("se1", "signal.end.event", "order.closed"));
        List<EdgeDto> edges = List.of(
                edge("s1", "u1"), edge("u1", "c1"), edge("c1", "g1"), edge("g1", "se1"));

        String xml = newService().generateBpmnXml("wf3", "Mixed", nodes, edges);
        assertTrue(xml.contains("userTask"), "user task");
        assertTrue(xml.contains("callActivity"), "call activity");
        assertTrue(xml.contains("inclusiveGateway"), "inclusive gateway");
        assertTrue(xml.contains("signalEventDefinition"), "signal end definition");
        assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void call_activity_bare_process_id_is_minted_into_the_process_key_space() {
        // A bare authored id addresses another workflow's main process: Process_<id>.
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                callActivityNode("c1", "fulfillment"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "e1"));

        BpmnModel model = parseModel(newService().generateBpmnXml("wfCallBare", "Call Bare", nodes, edges));
        CallActivity ca = (CallActivity) model.getMainProcess().getFlowElement("node_c1");
        assertEquals("Process_fulfillment", ca.getCalledElement(),
                "bare id normalised into the daemon's process-key space");
    }

    @Test
    void call_activity_full_process_key_passes_through() {
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                callActivityNode("c1", "Process_SubProc_root9"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "e1"));

        BpmnModel model = parseModel(newService().generateBpmnXml("wfCallFull", "Call Full", nodes, edges));
        CallActivity ca = (CallActivity) model.getMainProcess().getFlowElement("node_c1");
        assertEquals("Process_SubProc_root9", ca.getCalledElement(),
                "a full Process_… key is already in the key space");
    }

    @Test
    void call_activity_draft_run_calls_the_same_draft_namespace() {
        // Draft/published isolation: a Designer Run must NOT reach the published target.
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                callActivityNode("c1", "fulfillment"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "e1"));

        BpmnModel model = parseModel(newService().generateBpmnXml(
                "wfCallDraft", "Call Draft", nodes, edges, "__draft_tok1"));
        CallActivity ca = (CallActivity) model.getMainProcess().getFlowElement("node_c1");
        assertEquals("Process_fulfillment__draft_tok1", ca.getCalledElement(),
                "the enclosing graph's draft namespace is appended to the target key");
    }

    @Test
    void call_activity_explicit_draft_marker_is_not_double_suffixed() {
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                callActivityNode("c1", "Process_other__draft_abc"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "e1"));

        BpmnModel model = parseModel(newService().generateBpmnXml(
                "wfCallExplicit", "Call Explicit", nodes, edges, "__draft_tok1"));
        CallActivity ca = (CallActivity) model.getMainProcess().getFlowElement("node_c1");
        assertEquals("Process_other__draft_abc", ca.getCalledElement(),
                "an authored draft marker is explicit — the enclosing suffix is not appended");
    }

    @Test
    void call_activity_expression_target_fails_deploy() {
        // Activiti evaluates a ${…} calledElement as JUEL in the daemon JVM — the same escape
        // the gateway grammar closes. A literal process key is the only legal target.
        for (String injected : List.of(
                "${targetProcess}",
                "${''.class.forName('java.lang.Runtime').getRuntime().exec('id')}",
                "#{scriptDispatchDelegate}",
                "Process_x}${y")) {
            List<NodeDto> nodes = List.of(
                    node("s1", "start.event", null),
                    callActivityNode("c1", injected),
                    node("e1", "end.event", null));
            List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "e1"));

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> newService().generateBpmnXml("wfCallExpr", "Call Expr", nodes, edges, "__draft_tok1"),
                    injected);
            assertTrue(ex.getMessage().contains("literal process key"), ex.getMessage());
        }
    }

    @Test
    void generates_intermediate_events_wellformed() {
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                timerNode("tc1", "timer.intermediate.catch.event", "duration", "PT5M"),
                linkNode("st1", "signal.intermediate.throw.event", "order.ready"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("s1", "tc1"), edge("tc1", "st1"), edge("st1", "e1"));

        String xml = newService().generateBpmnXml("wf4", "Events", nodes, edges);
        assertTrue(xml.contains("intermediateCatchEvent"), "timer catch");
        assertTrue(xml.contains("intermediateThrowEvent") || xml.contains("throwEvent"), "signal throw");
        assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void timer_catch_with_cycle_emits_a_repeating_time_cycle() {
        // Type=cycle used to fall silently into the duration branch.
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                timerNode("tc1", "timer.intermediate.catch.event", "cycle", "PT5M"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "tc1"), edge("tc1", "e1"));

        String xml = newService().generateBpmnXml("wfTimerCycle", "Timer Cycle", nodes, edges);
        BpmnModel model = parseModel(xml);
        FlowElement el = model.getMainProcess().getFlowElement("node_tc1");
        assertInstanceOf(org.activiti.bpmn.model.IntermediateCatchEvent.class, el, "timer catch; xml=" + xml);
        TimerEventDefinition def = (TimerEventDefinition)
                ((org.activiti.bpmn.model.IntermediateCatchEvent) el).getEventDefinitions().get(0);
        assertEquals("R" + ActivitiTimerDefinitionFactory.MAX_TIMER_CYCLE_REPEATS + "/PT5M",
                def.getTimeCycle(),
                "a bare ISO period is normalised to the repeating form and capped at the max repeat; xml=" + xml);
        assertNull(def.getTimeDuration(), "cycle must NOT degrade into a one-shot duration; xml=" + xml);
    }

    @Test
    void timer_node_cycle_is_bounded_and_rate_limited() {
        // Audit K1: a node-level cycle used to deploy verbatim — R/PT5S would re-fire every 5s
        // forever (and a timer START would spawn an instance every 5s, surviving restarts).
        List<NodeDto> nodes = List.of(
                timerNode("s1", "timer.start.event", "cycle", "R/PT5S"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "e1"));

        String xml = newService().generateBpmnXml("wfTimerStorm", "Timer Storm", nodes, edges);
        BpmnModel model = parseModel(xml);
        FlowElement el = model.getMainProcess().getFlowElement("node_s1");
        assertInstanceOf(org.activiti.bpmn.model.StartEvent.class, el, "timer start; xml=" + xml);
        TimerEventDefinition def = (TimerEventDefinition)
                ((org.activiti.bpmn.model.StartEvent) el).getEventDefinitions().get(0);
        assertEquals("R" + ActivitiTimerDefinitionFactory.MAX_TIMER_CYCLE_REPEATS + "/PT1M",
                def.getTimeCycle(),
                "an unbounded 5s cycle is capped to the max repeat and floored to 1 minute; xml=" + xml);
    }

    @Test
    void timer_node_cycle_cron_fails_deploy_loudly() {
        for (String bpmnType : List.of("timer.start.event", "timer.intermediate.catch.event")) {
            List<NodeDto> nodes = List.of(
                    node("s1", "start.event", null),
                    timerNode("t1", bpmnType, "cycle", "0 0/1 * * * ?"),
                    node("e1", "end.event", null));
            List<EdgeDto> edges = List.of(edge("s1", "t1"), edge("t1", "e1"));

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> newService().generateBpmnXml("wfCronCycle-" + bpmnType, "Cron Cycle", nodes, edges),
                    "a cron cycle cannot be bounded and must refuse the deploy for " + bpmnType);
            assertTrue(ex.getMessage().contains("cron"), "the message says cron is unsupported: "
                    + ex.getMessage());
        }
    }

    @Test
    void timer_node_with_a_garbage_date_or_duration_fails_deploy_loudly() {
        // Same late-failure class as K1: an unparseable value used to reach the engine verbatim
        // and fail at runtime (catch) or job-schedule time (start) instead of at generation.
        for (String bpmnType : List.of("timer.start.event", "timer.intermediate.catch.event")) {
            for (String[] bad : List.of(
                    new String[]{"date", "not-a-date"},
                    new String[]{"duration", "not-a-duration"},
                    new String[]{"duration", "PT0S"})) {
                List<NodeDto> nodes = List.of(
                        node("s1", "start.event", null),
                        timerNode("t1", bpmnType, bad[0], bad[1]),
                        node("e1", "end.event", null));
                List<EdgeDto> edges = List.of(edge("s1", "t1"), edge("t1", "e1"));

                assertThrows(IllegalArgumentException.class,
                        () -> newService().generateBpmnXml(
                                "wfBadValue-" + bpmnType + "-" + bad[0], "Bad Value", nodes, edges),
                        bpmnType + " type=" + bad[0] + " value=" + bad[1] + " must refuse the deploy");
            }
        }
    }

    @Test
    void timer_nodes_with_an_unknown_type_fail_deploy_loudly() {
        // An unrecognised type used to produce an empty TimerEventDefinition (start) or a silent
        // duration fallback (catch) — the timer never fired, no error.
        for (String bpmnType : List.of("timer.start.event", "timer.intermediate.catch.event")) {
            List<NodeDto> nodes = List.of(
                    node("s1", "start.event", null),
                    timerNode("t1", bpmnType, "cron", "0 0/1 * * * ?"),
                    node("e1", "end.event", null));
            List<EdgeDto> edges = List.of(edge("s1", "t1"), edge("t1", "e1"));

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> newService().generateBpmnXml("wfBadTimer-" + bpmnType, "Bad Timer", nodes, edges),
                    "unknown timer type must refuse the deploy for " + bpmnType);
            assertTrue(ex.getMessage().contains("cron"), "the message names the offending type: "
                    + ex.getMessage());
        }
    }

    @Test
    void timer_nodes_with_a_blank_value_fail_deploy_loudly() {
        for (String bpmnType : List.of("timer.start.event", "timer.intermediate.catch.event")) {
            List<NodeDto> nodes = List.of(
                    node("s1", "start.event", null),
                    timerNode("t1", bpmnType, "duration", " "),
                    node("e1", "end.event", null));
            List<EdgeDto> edges = List.of(edge("s1", "t1"), edge("t1", "e1"));

            assertThrows(IllegalArgumentException.class,
                    () -> newService().generateBpmnXml("wfBlankTimer-" + bpmnType, "Blank Timer", nodes, edges),
                    "a blank timer value must refuse the deploy for " + bpmnType);
        }
    }

    @Test
    void a_compensate_throw_or_end_fails_the_deploy_instead_of_silently_no_oping() {
        // Deep-audit M2: the engine has no compensation boundary events, handlers or
        // event-subprocesses, so a compensate throw/end was a no-op whose activityRef never
        // matched (the authored id lacks the node_ prefix). The dead strategies are gone and
        // the type now fails the deploy loudly, like complex.gateway / cancel.end.event.
        for (String[] tc : List.of(
                new String[]{"compensate.intermediate.throw.event", "intermediate"},
                new String[]{"compensate.end.event", "end"})) {
            assertUnsupportedTypeRefused(tc[0], tc[1]);
        }
    }

    @Test
    void conditional_and_multiple_event_types_fail_the_deploy_instead_of_degrading() {
        // Deep-audit M3: with no strategy the structural remap turned a conditional/multiple
        // START into a plain start.event — a conditional start would fire IMMEDIATELY without
        // its condition — and the intermediate catches fell into the sandbox service-task
        // fallback. All six are published:false; an API-crafted graph now fails loudly.
        for (String[] tc : List.of(
                new String[]{"conditional.start.event", "start"},
                new String[]{"multiple.start.event", "start"},
                new String[]{"parallel.multiple.start.event", "start"},
                new String[]{"conditional.intermediate.catch.event", "intermediate"},
                new String[]{"multiple.intermediate.catch.event", "intermediate"},
                new String[]{"parallel.multiple.intermediate.catch.event", "intermediate"})) {
            assertUnsupportedTypeRefused(tc[0], tc[1]);
        }
    }

    @Test
    void a_business_rule_task_fails_the_deploy_instead_of_failing_at_runtime() {
        // The daemon ships no DMN/rules engine, so an emitted BusinessRuleTask's ruleNames could
        // never resolve and the token failed at runtime. The dead strategy is gone and the type
        // fails the deploy loudly (template was already published:false).
        assertUnsupportedTypeRefused("business.rule.task", "serviceTask");
    }

    private void assertUnsupportedTypeRefused(String bpmnType, String nodeType) {
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                node("c1", bpmnType, null, nodeType),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "e1"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfUnsup-" + bpmnType, "Unsup", nodes, edges),
                bpmnType + " must refuse the deploy");
        assertTrue(ex.getMessage().contains(bpmnType),
                "the failure names the unsupported type; msg=" + ex.getMessage());
    }

    // =====================================================================
    // Link events — off-page connectors paired by link `name`, wired by
    // element id (Activiti validates target/source at deploy).
    // =====================================================================

    @Test
    void a_link_pair_is_wired_by_element_id() {
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                linkNode("lt1", "link.intermediate.throw.event", "L1"),
                linkNode("lc1", "link.intermediate.catch.event", "L1"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "lt1"), edge("lc1", "e1"));

        String xml = newService().generateBpmnXml("wfLinkPair", "Link Pair", nodes, edges);
        BpmnModel model = parseModel(xml);
        FlowElement throwEl = model.getMainProcess().getFlowElement("node_lt1");
        FlowElement catchEl = model.getMainProcess().getFlowElement("node_lc1");
        assertInstanceOf(ThrowEvent.class, throwEl, "link throw compiles to a throw event; xml=" + xml);
        assertInstanceOf(org.activiti.bpmn.model.IntermediateCatchEvent.class, catchEl,
                "link catch compiles to a catch event; xml=" + xml);
        assertTrue(((ThrowEvent) throwEl).getEventDefinitions().isEmpty(),
                "Activiti 9 parses LinkEventDefinition but never executes the jump — none throw; xml=" + xml);
        LinkEventDefinition catchDef = (LinkEventDefinition)
                ((org.activiti.bpmn.model.IntermediateCatchEvent) catchEl).getEventDefinitions().get(0);
        assertEquals(List.of("node_lt1"), catchDef.getSources(),
                "the catch's link definition names its throw source (Activiti validation); xml=" + xml);
        assertEquals("node_lc1", routeTargetOf(xml, "node_lt1"),
                "the off-page jump is materialised as a real sequence flow; xml=" + xml);
        assertDeployableBpmn(xml);
        assertNoProcessValidationErrors(xml);
    }

    @Test
    void an_unpaired_link_event_fails_the_deploy_loudly() {
        // A throw whose name no catch carries (and vice versa) used to deploy and then die in
        // Activiti's validator with an opaque parse error — pair (or refuse) at generation time.
        List<NodeDto> throwOnly = List.of(
                node("s1", "start.event", null),
                linkNode("lt1", "link.intermediate.throw.event", "L1"),
                node("e1", "end.event", null));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfLinkOrphanT", "Link Orphan", throwOnly,
                        List.of(edge("s1", "lt1"), edge("lt1", "e1"))),
                "a link throw with no paired catch must refuse the deploy");
        assertTrue(ex.getMessage().contains("L1"), "the message names the unpaired link: "
                + ex.getMessage());

        List<NodeDto> catchOnly = List.of(
                node("s1", "start.event", null),
                linkNode("lc1", "link.intermediate.catch.event", "L2"),
                node("e1", "end.event", null));
        ex = assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfLinkOrphanC", "Link Orphan", catchOnly,
                        List.of(edge("s1", "lc1"), edge("lc1", "e1"))),
                "a link catch with no paired throw must refuse the deploy");
        assertTrue(ex.getMessage().contains("L2"), "the message names the unpaired link: "
                + ex.getMessage());
    }

    @Test
    void two_catches_sharing_a_link_name_fail_the_deploy_loudly() {
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                linkNode("lt1", "link.intermediate.throw.event", "L1"),
                linkNode("lc1", "link.intermediate.catch.event", "L1"),
                linkNode("lc2", "link.intermediate.catch.event", "L1"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("s1", "lt1"), edge("lc1", "e1"), edge("lc2", "e1"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfLinkDup", "Link Duplicate", nodes, edges),
                "a link name identifies exactly one catch");
        assertTrue(ex.getMessage().contains("more than one catch"),
                "the message calls out the duplicate: " + ex.getMessage());
    }

    @Test
    void a_function_helper_returning_into_the_main_flow_does_not_steal_it() {
        // The reported defect: start fans out to a Docling converter and an agent, the converter feeds
        // the agent, and the agent also owns a FUNCTION port whose helper returns into it. The helper's
        // reachable set then touched the agent and swept the rest of the main flow into the sub-process,
        // so the main process lost nodes and edges, emitted zero gateways, and the Docling branch never
        // dispatched — with no error anywhere.
        NodeDto start = node("startN", "start.event", null);
        NodeDto docling = node("doclingN", "custom.sandbox", "custom-script-executor");
        NodeDto agent = node("agentN", "custom.sandbox", "custom-script-executor");
        NodeDto end = node("endN", "end.event", null);
        NodeDto helper = node("helperN", "custom.sandbox", "function-script-executor");
        List<EdgeDto> edges = List.of(
                edge("startN", "doclingN"),
                edge("startN", "agentN"),
                edge("doclingN", "agentN"),
                edge("agentN", "endN"),
                edge("agentN", "helperN", "FUNCTION-model", "INPUT-main_input"),
                edge("helperN", "agentN", "OUTPUT-main_output", "INPUT-main_input"));

        String xml = newService().generateBpmnXml("wfSteal", "Helper Returns",
                List.of(start, docling, agent, end, helper), edges);

        assertTrue(xml.contains("node_doclingN") || xml.contains("errgate_doclingN"),
                "the Docling branch must stay in the main process; xml=" + xml);
        assertTrue(xml.contains("node_agentN") || xml.contains("errgate_agentN"),
                "the agent must stay in the main process; xml=" + xml);
        assertInstanceOf(ParallelGateway.class,
                parseModel(xml).getMainProcess().getFlowElement("gwfork_startN"),
                "the fan-out must still fork; xml=" + xml);
        assertInstanceOf(ParallelGateway.class,
                parseModel(xml).getMainProcess().getFlowElement("gwjoin_agentN"),
                "the converging branches must still join; xml=" + xml);
        assertFalse(xml.contains("node_helperN"),
                "the function helper itself belongs to its sub-process, not the main flow; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void generates_function_port_subprocess() {
        // m1 --FUNCTION--> fr1 (function root, chained to step s1); nested set stops at the FUNCTION edge.
        NodeDto m1 = node("m1", "custom.sandbox", "custom-script-executor");
        NodeDto fr1 = node("fr1", "start.event", null); // root: start-like in parent → task in sub-process
        NodeDto s1 = node("s1", "custom.sandbox", "function-script-executor");
        EdgeDto funcEdge = edge("m1", "fr1", "FUNCTION-model", "INPUT-main_input");   // special (boundary)
        EdgeDto chainEdge = edge("fr1", "s1", "OUTPUT-main_output", "INPUT-main_input"); // non-special (in sub)

        Map<String, String> subs = newService()
                .generateSubProcessBpmnXmls("wf", List.of(m1, fr1, s1), List.of(funcEdge, chainEdge));

        assertTrue(subs.containsKey("fr1"), "function root fr1 gets a sub-process; keys=" + subs.keySet());
        String xml = subs.get("fr1");
        assertTrue(xml.contains("Process_SubProc_fr1"), "sub-process id");
        assertTrue(xml.contains("start_sub_fr1"), "synthetic start");
        assertTrue(xml.contains("node_s1") || xml.contains("errgate_s1"), "chained step s1 present");
        assertTrue(!xml.contains("node_m1"), "parent node m1 excluded from sub-process");
        assertTrue(xml.contains("subProcessCompletionListener"), "completion listener on sub-process end");
        assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
        assertDeployableBpmn(xml);
    }

    @Test
    void function_subprocess_synthesises_parallel_join_for_converging_branches() {
        // A node with >1 incoming edge inside a function sub-process must wait for every branch,
        // exactly like in the main process — without insertParallelGateways it fired on the FIRST
        // arriving token.
        NodeDto m1 = node("m1", "custom.sandbox", "custom-script-executor");
        NodeDto fr1 = node("fr1", "start.event", null); // function root (task-ified)
        NodeDto a1 = node("a1", "custom.sandbox", "function-script-executor");
        NodeDto b1 = node("b1", "custom.sandbox", "function-script-executor");
        NodeDto join1 = node("join1", "custom.sandbox", "function-script-executor");

        EdgeDto funcEdge = edge("m1", "fr1", "FUNCTION-model", "INPUT-main_input");
        List<NodeDto> nodes = List.of(m1, fr1, a1, b1, join1);
        List<EdgeDto> edges = List.of(
                funcEdge,
                edge("fr1", "a1", "OUTPUT-main_output", "INPUT-main_input"),
                edge("fr1", "b1", "OUTPUT-main_output", "INPUT-main_input"),
                edge("a1", "join1", "OUTPUT-main_output", "INPUT-main_input"),
                edge("b1", "join1", "OUTPUT-main_output", "INPUT-main_input"));

        Map<String, String> subs = newService().generateSubProcessBpmnXmls("wf", nodes, edges);

        String xml = subs.get("fr1");
        assertNotNull(xml, "sub-process generated");
        assertTrue(xml.contains("gwfork_fr1"), "root's two outgoing edges fan out through a fork; xml=" + xml);
        assertTrue(xml.contains("gwjoin_join1"), "the converging node joins through a parallel gateway; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void a_failing_sub_process_fails_the_whole_generation_loudly() {
        // Audit K2: a sub-process generation failure used to be logged and swallowed, deploying
        // the main flow WITHOUT Process_SubProc_<root> — the run only failed when
        // studioquack.execute() targeted the missing key.
        NodeDto m1 = node("m1", "custom.sandbox", "custom-script-executor");
        NodeDto fr1 = node("fr1", "start.event", null);
        NodeDto badTimer = timerNode("t1", "timer.intermediate.catch.event", "cycle", "0 0/1 * * * ?");
        EdgeDto funcEdge = edge("m1", "fr1", "FUNCTION-model", "INPUT-main_input");
        EdgeDto chainEdge = edge("fr1", "t1", "OUTPUT-main_output", "INPUT-main_input");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> newService().generateSubProcessBpmnXmls("wf", List.of(m1, fr1, badTimer),
                        List.of(funcEdge, chainEdge)),
                "a sub-process that cannot be generated must abort the deploy, not be omitted");
        assertTrue(ex.getMessage().contains("fr1"),
                "the failure names the offending function root: " + ex.getMessage());
    }

    @Test
    void main_process_excludes_function_tools_and_special_edges() {
        // Regression guard for the "all nodes ran at once with empty inputs" defect. The function
        // tool must compile ONLY into its own Process_SubProc_<root>; the MAIN process must not emit
        // the tool node nor wire the FUNCTION edge as a sequence flow.
        NodeDto start = node("startN", "start.event", null);
        NodeDto agent = node("agentN", "custom.sandbox", "custom-script-executor");
        NodeDto end = node("endN", "end.event", null);
        NodeDto tool = node("toolFnRoot", "custom.sandbox", "function-script-executor");

        EdgeDto e1 = edge("startN", "agentN");                                 // main flow
        EdgeDto e2 = edge("agentN", "endN");                                   // main flow
        EdgeDto fn = edge("agentN", "toolFnRoot", "FUNCTION-model", "INPUT-main_input"); // special

        List<NodeDto> nodes = List.of(start, agent, end, tool);
        List<EdgeDto> edges = List.of(e1, e2, fn);

        String xml = newService().generateBpmnXml("wfFn", "Agent With Tool", nodes, edges);

        assertTrue(xml.contains("node_agentN"), "agent node present in main process; xml=" + xml);
        assertTrue(xml.contains("flow_e_agentN_endN"), "main sequence flow agent -> end; xml=" + xml);
        assertTrue(!xml.contains("toolFnRoot"),
                "function tool must be excluded from the main process; xml=" + xml);
        assertTrue(hasNoneStartEvent(xml), "main process keeps its none-StartEvent; xml=" + xml);
        assertDeployableBpmn(xml);

        Map<String, String> subs = newService().generateSubProcessBpmnXmls("wfFn", nodes, edges);
        assertTrue(subs.containsKey("toolFnRoot"),
                "function tool compiles into its own Process_SubProc_<root>; keys=" + subs.keySet());
    }

    /**
     * A Designer Run must not become the newest version of the workflow the user deployed, so its
     * process keys carry a namespace — on the sub-processes too, which are keyed by node id and would
     * otherwise be reachable from a published run's {@code studioquack.execute()}.
     */
    @Test
    void draft_namespace_isolates_main_and_sub_process_keys() {
        NodeDto master = node("masterN", "custom.sandbox", "custom-script-executor");
        NodeDto fnRoot = node("toolFnRoot", "custom.sandbox", "custom-script-executor");
        List<NodeDto> nodes = List.of(master, fnRoot);
        List<EdgeDto> edges = List.of(edge("masterN", "toolFnRoot", "FUNCTION-model", "INPUT-main_input"));

        String published = newService().generateBpmnXml("wfFn", "Agent", nodes, edges);
        String drafted = newService().generateBpmnXml("wfFn", "Agent", nodes, edges, "__draft_abc123");
        Map<String, String> publishedSubs = newService().generateSubProcessBpmnXmls("Agent", nodes, edges);
        Map<String, String> draftedSubs =
                newService().generateSubProcessBpmnXmls("Agent", nodes, edges, "__draft_abc123");

        assertTrue(published.contains("Process_wfFn") && !published.contains("__draft_"),
                "a published deployment keeps the bare key; xml=" + published);
        assertTrue(drafted.contains("Process_wfFn__draft_abc123"),
                "a draft deployment is namespaced; xml=" + drafted);
        assertTrue(publishedSubs.get("toolFnRoot").contains("Process_SubProc_toolFnRoot")
                        && !publishedSubs.get("toolFnRoot").contains("__draft_"),
                "published sub-process keeps the bare key");
        assertTrue(draftedSubs.get("toolFnRoot").contains("Process_SubProc_toolFnRoot__draft_abc123"),
                "the sub-process must be namespaced too, or a published run reaches into the draft");
    }

    @Test
    void draft_namespace_does_not_change_the_graph_signature() {
        NodeDto n = node("n1", "custom.sandbox", "custom-script-executor");

        String bare = newService().generate("wf", "N", List.of(n), List.of(), "").graphSignature();
        String draft = newService().generate("wf", "N", List.of(n), List.of(), "__draft_x").graphSignature();

        assertEquals(bare, draft, "the signature identifies the graph, not the deployment it landed in");
    }

    @Test
    void duckbot_start_and_end_are_executable_boundary_tasks() {
        NodeDto start = duckbotBoundary("duckStart", "start");
        NodeDto agent = node("agentN", "custom.sandbox", "custom-script-executor");
        NodeDto end = duckbotBoundary("duckEnd", "end");

        String xml = newService().generateBpmnXml("wfDuck", "Duckbot Flow",
                List.of(start, agent, end),
                List.of(edge("duckStart", "agentN"), edge("agentN", "duckEnd")));

        assertTrue(xml.contains("start_duck_duckStart"),
                "duckbot start must be wrapped by a real BPMN start event; xml=" + xml);
        assertTrue(xml.contains("node_duckStart"),
                "duckbot start must execute as a service task so applyDuckbotStartBridge runs; xml=" + xml);
        assertTrue(xml.contains("node_duckEnd"),
                "duckbot end must execute as a service task so applyDuckbotEndBridge runs; xml=" + xml);
        assertTrue(xml.contains("end_duck_duckEnd"),
                "duckbot end task must flow into a real BPMN end event; xml=" + xml);
        assertTrue(xml.contains("scriptDispatchDelegate"),
                "duckbot boundary service tasks use the dispatch delegate bridge; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void duckbot_end_function_root_compiles_to_a_task_in_its_subprocess() {
        // Regression: an agent's thoughts FUNCTION edge into a Duckbot end makes the end a
        // function root, and the sub-process used to compile it as a plain EndEvent — the
        // dispatch delegate (and with it the Duckbot bridge) never ran, so every
        // execute("thoughts", …) delivery completed the sub-process in silence.
        NodeDto agent = node("agentN", "custom.sandbox", "custom-script-executor");
        NodeDto duckEnd = duckbotBoundary("duckEnd", "end");
        EdgeDto fn = edge("agentN", "duckEnd", "FUNCTION-thoughts", "INPUT-msg");

        Map<String, String> subs = newService()
                .generateSubProcessBpmnXmls("wf", List.of(agent, duckEnd), List.of(fn));

        String xml = subs.get("duckEnd");
        assertNotNull(xml, "duckbot end function root gets a sub-process; keys=" + subs.keySet());
        assertTrue(xml.contains("node_duckEnd"),
                "duckbot end must execute as a service task so the bridge runs; xml=" + xml);
        assertTrue(xml.contains("scriptDispatchDelegate"),
                "the boundary task routes through the dispatch delegate; xml=" + xml);
        assertTrue(xml.contains("end_sub_duckEnd"),
                "the sub-process synthetic end terminates the task; xml=" + xml);
        assertTrue(xml.contains("subProcessCompletionListener"),
                "completion listener still signals the parent; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    // Escalation degradation (Activiti 9 has no EscalationEventDefinition).

    @Test
    void escalation_end_and_throw_events_degrade_to_none_events_no_escalation_definition() {
        // Activiti 9.0.0's org.activiti.bpmn.model has no EscalationEventDefinition, so the as-is
        // escalation bpmnTypes are intentionally degraded to plain none end/throw events.
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                node("et1", "escalation.intermediate.throw.event", null),
                node("ee1", "escalation.end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "et1"), edge("et1", "ee1"));

        String xml = newService().generateBpmnXml("wfEsc", "Escalation Degraded", nodes, edges);

        assertFalse(xml.contains("escalationEventDefinition"),
                "escalation is degraded — NO escalationEventDefinition may be emitted on base; xml=" + xml);
        assertTrue(xml.contains("throwEvent") || xml.contains("intermediateThrowEvent"),
                "escalation throw degrades to a none throw event; xml=" + xml);
        assertTrue(xml.contains("endEvent") || xml.contains(":endEvent"),
                "escalation end degrades to a none end event; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    // =====================================================================
    // Docling v2 — a docling-converter node is ALWAYS deployable and dispatches to ${doclingDelegate}.
    // =====================================================================

    @Test
    void docling_node_is_deployable_and_emits_docling_delegate() {
        // Docling v2 (replaces the old fail-closed behaviour): a docling-converter node is now
        // always deployable. Generation must SUCCEED and emit the ${doclingDelegate} async-sandbox
        // expansion (serviceTask async + receiveTask wait-state + error gate + shared error end);
        // the single DoclingDelegate then forwards the conversion to the active DoclingBackend.
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                node("dc1", "docling.converter", "docling-converter", "serviceTask"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "dc1"), edge("dc1", "e1"));

        String xml = assertDoesNotThrow(
                () -> newService().generateBpmnXml("wfDocling", "Docling v2", nodes, edges),
                "a docling-converter node must be deployable in Docling v2 (no fail-closed throw)");

        assertTrue(xml.contains("doclingDelegate"),
                "the docling node dispatches to ${doclingDelegate}; xml=" + xml);
        assertTrue(xml.contains("receiveTask"),
                "docling is an async-sandbox node with a receiveTask wait-state; xml=" + xml);
        assertTrue(xml.contains(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID),
                "the shared sandbox error end is emitted; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    // BoundaryEventConfigs are consumed into per-code error boundaries.

    @Test
    void sandbox_node_emits_per_code_error_boundary_from_boundary_configs() {
        // The sandbox node declares a per-code ERROR boundary {VALIDATION_FAILED -> handlerN}; the
        // generator must emit that boundary (errorRef Error_VALIDATION_FAILED) routed to node_handlerN
        // IN ADDITION TO the catch-all boundary → shared error end.
        NodeDto sandbox = node("scriptN", "custom.sandbox", "custom-script-executor", "serviceTask");
        BoundaryEventConfig cfg = BoundaryEventConfig.error("VALIDATION_FAILED");
        cfg.setId("b1");
        cfg.setTargetNodeId("handlerN");
        sandbox.getData().setBoundaryEventConfigs(List.of(cfg));

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null),
                sandbox,
                node("handlerN", "end.event", null),   // per-code handler target
                node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(edge("startN", "scriptN"), edge("scriptN", "endN"));

        String xml = newService().generateBpmnXml("wfBoundary", "Per-Code Boundary", nodes, edges);

        assertTrue(xml.contains("errcode_scriptN_0"),
                "a per-code error boundary is emitted for the configured code; xml=" + xml);
        assertTrue(xml.contains("VALIDATION_FAILED"),
                "the per-code error declaration carries the configured errorCode; xml=" + xml);
        assertTrue(xml.contains("node_handlerN"),
                "the per-code boundary routes to its configured handler target; xml=" + xml);
        assertTrue(xml.contains(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID),
                "the catch-all boundary → shared error end is still emitted alongside; xml=" + xml);
        assertTrue(xml.contains("err_scriptN"),
                "typed-code user boundaries still coexist with the system catch-all; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void blank_error_code_emits_catchall_boundary_to_wired_handler_and_suppresses_system_err() {
        // A blank Error Code is a true Activiti catch-all on errgate_<id> (no errorRef), routed to
        // the wired handler. The system err_<node> → Event_SandboxErrorEnd must not also be emitted.
        String xml = generateWithBoundaries(
                sandboxWithBoundaries("scriptN", cfg(BoundaryEventConfig.error(""), "b1", "handlerN")),
                "handlerN");

        BoundaryEvent boundary = boundaryOf(xml, "errcode_scriptN_0");
        assertEquals("errgate_scriptN", boundary.getAttachedToRefId(),
                "a blank-code error boundary hosts on the error gate; xml=" + xml);
        // Error boundaries are always interrupting; Activiti omits cancelActivity="true" (the default).
        assertFalse(xml.contains("id=\"errcode_scriptN_0\"") && xml.contains("cancelActivity=\"false\""),
                "an error catch-all must not be non-interrupting; xml=" + xml);
        ErrorEventDefinition def = definitionOf(xml, "errcode_scriptN_0", ErrorEventDefinition.class);
        assertTrue(def.getErrorRef() == null || def.getErrorRef().isBlank(),
                "a blank Error Code emits no errorRef (Activiti catch-all); xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "errcode_scriptN_0"),
                "the user catch-all routes to its wired handler; xml=" + xml);
        assertFalse(xml.contains("err_scriptN"),
                "the system catch-all err_scriptN is suppressed when a user catch-all exists; xml=" + xml);
        assertNull(parseModel(xml).getMainProcess().getFlowElement("err_scriptN"),
                "the system catch-all element must be absent; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    // =====================================================================
    // Configured boundary events — TIMER / MESSAGE / SIGNAL / ERROR.
    // =====================================================================

    /** start → sandbox → endN, plus a structural handler node the boundaries can route to. */
    private String generateWithBoundaries(NodeDto sandbox, String... handlerNodeIds) {
        List<NodeDto> nodes = new java.util.ArrayList<>(List.of(
                node("startN", "start.event", null), sandbox, node("endN", "end.event", null)));
        for (String handler : handlerNodeIds) {
            nodes.add(node(handler, "end.event", null));
        }
        return newService().generateBpmnXml("wfB", "Boundaries", nodes,
                List.of(edge("startN", sandbox.getId()), edge(sandbox.getId(), "endN")));
    }

    @Test
    void user_task_and_sync_delegate_task_emit_their_authored_boundaries() {
        // Authored boundaries must not silently drop on node kinds without a synthesized wait
        // state: the user task IS the wait state; a sync delegate hosts them on itself.
        NodeDto userTask = node("userN", "user.task", null);
        BoundaryEventConfig timerCfg = BoundaryEventConfig.timer("PT30M");
        timerCfg.setId("b1");
        timerCfg.setTargetNodeId("handlerN");
        userTask.getData().setBoundaryEventConfigs(List.of(timerCfg));

        NodeDto syncTask = node("syncN", "custom.sync", "getConversationHistory", "serviceTask");
        BoundaryEventConfig errCfg = BoundaryEventConfig.error("SYNC_FAILED");
        errCfg.setId("b2");
        errCfg.setTargetNodeId("handlerN");
        syncTask.getData().setBoundaryEventConfigs(List.of(errCfg));

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null),
                userTask, syncTask,
                node("handlerN", "end.event", null),
                node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("startN", "userN"), edge("userN", "syncN"), edge("syncN", "endN"));

        String xml = newService().generateBpmnXml("wfUTB", "User/Sync Boundaries", nodes, edges);

        BoundaryEvent timer = boundaryOf(xml, "btimer_userN_0");
        assertEquals("node_userN", timer.getAttachedToRefId(),
                "a timer boundary hosts on the user task itself; xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "btimer_userN_0"),
                "the user-task boundary routes to its configured target; xml=" + xml);

        BoundaryEvent error = boundaryOf(xml, "errcode_syncN_0");
        assertEquals("node_syncN", error.getAttachedToRefId(),
                "an error boundary hosts on the sync delegate task itself; xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "errcode_syncN_0"),
                "the sync-task boundary routes to its configured target; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void call_activity_emits_its_authored_boundaries() {
        // The designer offers the Boundary Events submenu on call activities; the boundaries
        // must not silently drop at generation. The call activity IS the wait state (the
        // execution parks there for the whole child run), so every type hosts on it directly.
        NodeDto callActivity = callActivityNode("callN", "targetWf");
        BoundaryEventConfig timerCfg = BoundaryEventConfig.timer("PT30M");
        timerCfg.setId("b1");
        timerCfg.setTargetNodeId("handlerN");
        BoundaryEventConfig errCfg = BoundaryEventConfig.error("CHILD_FAILED");
        errCfg.setId("b2");
        errCfg.setTargetNodeId("handlerN");
        callActivity.getData().setBoundaryEventConfigs(List.of(timerCfg, errCfg));

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null),
                callActivity,
                node("handlerN", "end.event", null),
                node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("startN", "callN"), edge("callN", "endN"));

        String xml = newService().generateBpmnXml("wfCAB", "Call Activity Boundaries", nodes, edges);

        BoundaryEvent timer = boundaryOf(xml, "btimer_callN_0");
        assertEquals("node_callN", timer.getAttachedToRefId(),
                "a timer boundary hosts on the call activity itself; xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "btimer_callN_0"),
                "the call-activity timer routes to its configured target; xml=" + xml);

        BoundaryEvent error = boundaryOf(xml, "errcode_callN_0");
        assertEquals("node_callN", error.getAttachedToRefId(),
                "an error boundary hosts on the call activity itself; xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "errcode_callN_0"),
                "the call-activity error boundary routes to its configured target; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void event_based_gateway_may_only_feed_catch_events() {
        // A non-catch target used to slip through generation and fail late inside Activiti's
        // deploy validator. Generation now names the offending nodes itself.
        List<NodeDto> badNodes = List.of(
                node("s1", "start.event", null),
                node("gw", "event.based.gateway", null),
                node("e1", "end.event", null));
        List<EdgeDto> badEdges = List.of(edge("s1", "gw"), edge("gw", "e1"));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfEbgBad", "EBG Bad", badNodes, badEdges));
        assertTrue(ex.getMessage().contains("gw") && ex.getMessage().contains("e1"),
                "the violation names the gateway and the target; msg=" + ex.getMessage());

        List<NodeDto> okNodes = List.of(
                node("s1", "start.event", null),
                node("gw", "event.based.gateway", null),
                messageCatch("c1", "order.paid", null, null),
                node("e1", "end.event", null));
        List<EdgeDto> okEdges = List.of(
                edge("s1", "gw"), edge("gw", "c1"), edge("c1", "e1"));
        String xml = assertDoesNotThrow(
                () -> newService().generateBpmnXml("wfEbgOk", "EBG Ok", okNodes, okEdges));
        assertDeployableBpmn(xml);
    }

    @Test
    void blank_edge_ids_between_the_same_pair_get_unique_flow_ids() {
        // Two edges with blank ids between the same node pair used to BOTH become
        // flow_a_b → Activiti's duplicate-id error at deploy. The fallback now suffixes.
        EdgeDto e1 = edge("a", "b");
        e1.setId(null);
        EdgeDto e2 = edge("a", "b");
        e2.setId("  ");
        List<NodeDto> nodes = List.of(node("a", "start.event", null), node("b", "end.event", null));

        String xml = newService().generateBpmnXml("wfDupFlow", "Dup Flow Ids", nodes, List.of(e1, e2));

        assertTrue(xml.contains("flow_a_b\""), "first blank id takes the base fallback; xml=" + xml);
        assertTrue(xml.contains("flow_a_b_2\""), "the colliding second id is suffixed; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void timer_boundary_attaches_to_the_wait_state_with_the_configured_duration() {
        // A timer must fire WHILE the sandbox is running. The process is parked on wait_<id> for that
        // whole window (node_<id> is activiti:async and is left in milliseconds), so wait_<id> is the
        // only host on which a timeout can be observed.
        String xml = generateWithBoundaries(
                sandboxWithBoundaries("scriptN", cfg(BoundaryEventConfig.timer("PT30M"), "b1", "handlerN")),
                "handlerN");

        BoundaryEvent boundary = boundaryOf(xml, "btimer_scriptN_0");
        assertEquals("wait_scriptN", boundary.getAttachedToRefId(),
                "a timer boundary hosts on the sandbox wait state; xml=" + xml);
        assertTrue(boundary.isCancelActivity(), "the config is interrupting; xml=" + xml);
        assertEquals("PT30M", definitionOf(xml, "btimer_scriptN_0", TimerEventDefinition.class).getTimeDuration(),
                "the configured duration is emitted as timeDuration; xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "btimer_scriptN_0"),
                "the boundary routes to its configured targetNodeId; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void timer_boundary_emits_time_date_and_a_bounded_time_cycle() {
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.timerWithDate("2026-12-31T23:59:59Z"), "b1", "handlerN"),
                // R/… is "repeat forever": the generator must cap it, and PT5S must be rate-limited.
                cfg(BoundaryEventConfig.timerWithCycle("R/PT5S"), "b2", "handlerN")),
                "handlerN");

        assertEquals("2026-12-31T23:59:59Z",
                definitionOf(xml, "btimer_scriptN_0", TimerEventDefinition.class).getTimeDate(),
                "timerType=date is emitted as timeDate; xml=" + xml);
        assertEquals("R" + ActivitiTimerDefinitionFactory.MAX_TIMER_CYCLE_REPEATS + "/PT1M",
                definitionOf(xml, "btimer_scriptN_1", TimerEventDefinition.class).getTimeCycle(),
                "an unbounded cycle is capped and its interval raised to the floor; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void timer_cycle_that_cannot_be_bounded_is_refused() {
        // A cron cycle cannot be rewritten into a bounded repeat, so no timer job may be scheduled
        // from it at all.
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.timerWithCycle("0 0/1 * * * ?"), "b1", "handlerN"),
                cfg(BoundaryEventConfig.timer("not-a-duration"), "b2", "handlerN")),
                "handlerN");

        assertFalse(xml.contains("btimer_scriptN"),
                "an unboundable cycle and an unparseable duration emit no timer boundary; xml=" + xml);
        assertFalse(xml.contains("timerEventDefinition"), "no timer definition is emitted; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void non_interrupting_timer_boundary_does_not_cancel_the_sandbox_task() {
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.timerWithCycle("R3/PT10M"), "b1", "handlerN")), "handlerN");

        assertFalse(boundaryOf(xml, "btimer_scriptN_0").isCancelActivity(),
                "a non-interrupting config must not cancel its host; xml=" + xml);
        assertTrue(xml.contains("cancelActivity=\"false\""),
                "the emitted XML must say cancelActivity=false; xml=" + xml);
        assertTrue(xml.contains("flow_scriptN_wait_to_gate"),
                "the sandbox wait → error gate path survives a non-interrupting boundary; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void message_boundary_declares_the_message_and_references_it() {
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.message("CancelOrder", "=orderId"), "b1", "handlerN")), "handlerN");

        MessageEventDefinition def = definitionOf(xml, "bmsg_scriptN_0", MessageEventDefinition.class);
        assertEquals("Message_CancelOrder", def.getMessageRef(), "messageRef points at the declaration");
        Message declared = parseModel(xml).getMessage("Message_CancelOrder");
        assertNotNull(declared, "Activiti needs the <message> declaration, not just a ref; xml=" + xml);
        assertEquals("CancelOrder", declared.getName(), "the declaration carries the authored name");
        assertEquals("wait_scriptN", boundaryOf(xml, "bmsg_scriptN_0").getAttachedToRefId(),
                "a message boundary hosts on the sandbox wait state; xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "bmsg_scriptN_0"), "routed to its target");
        assertDeployableBpmn(xml);
    }

    @Test
    void message_boundary_with_a_correlation_key_emits_the_receiver_config_and_host_pin_listener() {
        // Cross-flow shared-key correlation: the authored key must ride the deployed model
        // (sq:receiverConfig on the boundary) and the host must carry the pin bridge — otherwise
        // a sender in another flow that knows only the shared key can never correlate.
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.message("CancelOrder", "=orderId"), "b1", "handlerN")), "handlerN");

        BoundaryEvent boundary = boundaryOf(xml, "bmsg_scriptN_0");
        List<ExtensionElement> receiverConfigs =
                boundary.getExtensionElements().get("receiverConfig");
        assertNotNull(receiverConfigs,
                "the authored correlationKey is emitted as sq:receiverConfig; xml=" + xml);
        assertEquals("=orderId", receiverConfigs.get(0).getAttributes().get("correlationKey")
                        .get(0).getValue(),
                "the receiver config carries the authored key verbatim; xml=" + xml);

        FlowElement host = parseModel(xml).getMainProcess().getFlowElement("wait_scriptN");
        assertNotNull(host, "the sandbox wait state exists; xml=" + xml);
        assertTrue(host.getExecutionListeners().stream().anyMatch(
                        l -> "${messageReceiverCorrelationListener}".equals(l.getImplementation())),
                "the host parks the token, so it carries the correlation-pin bridge; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void message_boundary_without_a_correlation_key_emits_no_receiver_config_or_pin_listener() {
        // Blank key = businessKey-only correlation: no receiverConfig bloat and no pin bridge
        // on the host (boundaries have no runtime override lane, so nothing could be pinned).
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.message("CancelOrder", null), "b1", "handlerN")), "handlerN");

        BoundaryEvent boundary = boundaryOf(xml, "bmsg_scriptN_0");
        assertTrue(boundary.getExtensionElements().get("receiverConfig") == null,
                "no sq:receiverConfig without an authored key; xml=" + xml);
        FlowElement host = parseModel(xml).getMainProcess().getFlowElement("wait_scriptN");
        assertNotNull(host, "the sandbox wait state exists; xml=" + xml);
        assertTrue(host.getExecutionListeners() == null || host.getExecutionListeners().stream()
                        .noneMatch(l -> "${messageReceiverCorrelationListener}".equals(l.getImplementation())),
                "no pin bridge without an authored key; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void user_task_message_boundary_hosts_the_pin_listener_on_the_user_task() {
        // The reported scenario: a message boundary on a user task must be correlatable by the
        // shared authored key — the pin bridge rides the user task itself (its execution holds
        // the boundary's message subscription).
        NodeDto userTask = node("userN", "user.task", null);
        userTask.getData().setBoundaryEventConfigs(List.of(
                cfg(BoundaryEventConfig.message("bm1", "1"), "b1", "handlerN")));
        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null), userTask,
                node("handlerN", "end.event", null), node("endN", "end.event", null));
        String xml = newService().generateBpmnXml("wfUMB", "User Task Message Boundary", nodes,
                List.of(edge("startN", "userN"), edge("userN", "endN")));

        BoundaryEvent boundary = boundaryOf(xml, "bmsg_userN_0");
        assertEquals("node_userN", boundary.getAttachedToRefId(),
                "the boundary hosts on the user task itself; xml=" + xml);
        assertEquals("1", boundary.getExtensionElements().get("receiverConfig")
                        .get(0).getAttributes().get("correlationKey").get(0).getValue(),
                "the authored key rides the boundary; xml=" + xml);
        FlowElement host = parseModel(xml).getMainProcess().getFlowElement("node_userN");
        assertNotNull(host, "the user task exists; xml=" + xml);
        assertTrue(host.getExecutionListeners().stream().anyMatch(
                        l -> "${messageReceiverCorrelationListener}".equals(l.getImplementation())),
                "the user task carries the correlation-pin bridge; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void signal_boundary_declares_the_signal_and_references_it() {
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.signal("AbortAll"), "b1", "handlerN")), "handlerN");

        assertEquals("Signal_AbortAll",
                definitionOf(xml, "bsig_scriptN_0", SignalEventDefinition.class).getSignalRef(),
                "signalRef points at the declaration; xml=" + xml);
        Signal declared = parseModel(xml).getSignal("Signal_AbortAll");
        assertNotNull(declared, "Activiti needs the <signal> declaration, not just a ref; xml=" + xml);
        assertEquals("AbortAll", declared.getName(), "the declaration carries the authored name");
        assertEquals("wait_scriptN", boundaryOf(xml, "bsig_scriptN_0").getAttachedToRefId(),
                "a signal boundary hosts on the sandbox wait state; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void signal_names_that_sanitise_alike_get_their_own_declaration() {
        // Both names sanitise to Signal_abort_all; sharing one declaration would subscribe the second
        // boundary under the first one's name.
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.signal("abort-all"), "b1", "handlerN"),
                cfg(BoundaryEventConfig.signal("abort_all"), "b2", "handlerN")), "handlerN");

        String first = definitionOf(xml, "bsig_scriptN_0", SignalEventDefinition.class).getSignalRef();
        String second = definitionOf(xml, "bsig_scriptN_1", SignalEventDefinition.class).getSignalRef();
        assertNotEquals(first, second, "two different names must not share one declaration; xml=" + xml);
        assertEquals("abort-all", parseModel(xml).getSignal(first).getName());
        assertEquals("abort_all", parseModel(xml).getSignal(second).getName());
        assertDeployableBpmn(xml);
    }

    // =====================================================================
    // ESCALATION boundaries — the reserved-signal bridge (no Activiti 9 escalation model).
    // =====================================================================

    private NodeDto escalationNode(String id, String bpmnType, String code) {
        NodeDto node = node(id, bpmnType, null);
        ParameterDto codeParam = new ParameterDto();
        codeParam.setShortCode("code");
        codeParam.setValue(code);
        node.getData().setConfigurationParameters(List.of(codeParam));
        return node;
    }

    @Test
    void escalation_boundary_is_emitted_as_a_reserved_signal_subscription() {
        // Activiti 9 has no escalation model, so the boundary subscribes to the reserved signal
        // name __sq_esc_<code>; the throw/end listener fires that name instance-scoped.
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.escalation("LATE_APPROVAL"), "b1", "handlerN")), "handlerN");

        String signalRef = definitionOf(xml, "besc_scriptN_0", SignalEventDefinition.class).getSignalRef();
        Signal declared = parseModel(xml).getSignal(signalRef);
        assertNotNull(declared, "the reserved signal must be declared, not just referenced; xml=" + xml);
        assertEquals("__sq_esc_LATE_APPROVAL", declared.getName(),
                "the subscription name is the reserved escalation lane for the configured code");
        assertEquals("wait_scriptN", boundaryOf(xml, "besc_scriptN_0").getAttachedToRefId(),
                "an escalation boundary hosts on the sandbox wait state like timer/message/signal; xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "besc_scriptN_0"),
                "the boundary routes to its configured handler; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void escalation_boundary_without_a_code_is_dropped() {
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.escalation(" "), "b1", "handlerN")), "handlerN");

        assertNull(parseModel(xml).getMainProcess().getFlowElement("besc_scriptN_0"),
                "a codeless escalation boundary matches nothing and must not be emitted; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void escalation_throw_and_end_carry_the_sender_bridge() {
        // The elements stay none events (Activiti 9 has no escalation definition); the configured
        // code travels as an sq:escalationConfig extension and the attached listener raises it.
        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null),
                escalationNode("throwN", "escalation.intermediate.throw.event", "LATE_APPROVAL"),
                escalationNode("endN", "escalation.end.event", "ESC_DONE"));
        String xml = newService().generateBpmnXml("wfEsc", "Escalation", nodes,
                List.of(edge("startN", "throwN"), edge("throwN", "endN")));

        ThrowEvent throwEvent = parseModel(xml).getMainProcess().getFlowElement("node_throwN") instanceof ThrowEvent t
                ? t : null;
        assertNotNull(throwEvent, "escalation throw stays a none ThrowEvent; xml=" + xml);
        assertTrue(throwEvent.getExtensionElements().containsKey("escalationConfig"),
                "the configured code travels as an sq:escalationConfig extension; xml=" + xml);
        assertTrue(xml.contains("escalationCode=\"LATE_APPROVAL\""),
                "the extension carries the configured code; xml=" + xml);
        assertTrue(throwEvent.getExecutionListeners().stream()
                        .anyMatch(l -> "${escalationSenderExecutionListener}".equals(l.getImplementation())),
                "the sender bridge listener is attached; xml=" + xml);
        assertTrue(xml.contains("escalationCode=\"ESC_DONE\""),
                "the end event carries its own configured code; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void an_authored_signal_name_with_the_reserved_prefix_fails_deploy() {
        // A user signal named __sq_esc_X would subscribe to — and be fired by — escalation
        // traffic for code X. Loud deploy failure, not a silent cross-wiring.
        NodeDto rogue = escalationNode("throwN", "signal.intermediate.throw.event", null);
        ParameterDto nameParam = new ParameterDto();
        nameParam.setShortCode("name");
        nameParam.setValue("__sq_esc_LATE_APPROVAL");
        rogue.getData().setConfigurationParameters(List.of(nameParam));

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null), rogue, node("endN", "end.event", null));
        assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfRogue", "Rogue Signal", nodes,
                        List.of(edge("startN", "throwN"), edge("throwN", "endN"))));
    }

    @Test
    void boundary_without_a_target_falls_back_to_the_shared_error_end() {
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.timer("PT10M"), "b1", null),
                cfg(BoundaryEventConfig.error("VALIDATION_FAILED"), "b2", null)));

        assertEquals(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID,
                routeTargetOf(xml, "btimer_scriptN_0"),
                "a targetless timer boundary terminates at the shared error end; xml=" + xml);
        assertEquals(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID,
                routeTargetOf(xml, "errcode_scriptN_0"),
                "a targetless error boundary keeps its existing fallback; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void a_target_that_was_never_emitted_falls_back_to_the_shared_error_end() {
        // An unresolved targetRef is an unresolved IDREF: it would fail schema validation on deploy
        // and make the whole workflow undeployable.
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.timer("PT10M"), "b1", "ghostN")));

        assertEquals(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID,
                routeTargetOf(xml, "btimer_scriptN_0"),
                "a boundary naming a dropped node is re-routed, not left dangling; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void several_boundaries_of_the_same_type_on_one_node_get_distinct_ids_and_routes() {
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.timer("PT5M"), "b1", "warnN"),
                cfg(BoundaryEventConfig.timer("PT30M"), "b2", "killN"),
                cfg(BoundaryEventConfig.error("VALIDATION_FAILED"), "b3", "warnN"),
                cfg(BoundaryEventConfig.error("RATE_LIMITED"), "b4", "killN")),
                "warnN", "killN");

        assertEquals("PT5M", definitionOf(xml, "btimer_scriptN_0", TimerEventDefinition.class).getTimeDuration());
        assertEquals("PT30M", definitionOf(xml, "btimer_scriptN_1", TimerEventDefinition.class).getTimeDuration());
        assertEquals("node_warnN", routeTargetOf(xml, "btimer_scriptN_0"));
        assertEquals("node_killN", routeTargetOf(xml, "btimer_scriptN_1"));
        assertEquals("node_warnN", routeTargetOf(xml, "errcode_scriptN_0"));
        assertEquals("node_killN", routeTargetOf(xml, "errcode_scriptN_1"));
        assertDeployableBpmn(xml);
    }

    @Test
    void a_boundary_edge_is_not_wired_as_a_main_sequence_flow() {
        // The handler hangs off the boundary. Wiring its designer edge as well would give the host a
        // second unconditional outgoing flow and reach the handler on every successful run.
        NodeDto sandbox = sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.timer("PT30M"), "b1", "handlerN"));
        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null), sandbox,
                node("handlerN", "custom.sandbox", "custom-script-executor", "serviceTask"),
                node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("startN", "scriptN"), edge("scriptN", "endN"),
                boundaryEdge("scriptN", "handlerN", "b1"));

        String xml = newService().generateBpmnXml("wfBE", "Boundary Edge", nodes, edges);

        assertTrue(xml.contains("node_handlerN"),
                "the handler is only reachable through the boundary, but must still be emitted; xml=" + xml);
        assertEquals("node_handlerN", routeTargetOf(xml, "btimer_scriptN_0"), "the boundary owns the route");
        assertFalse(xml.contains("flow_e_scriptN_handlerN"),
                "the boundary edge must NOT also become a sequence flow; xml=" + xml);
        assertEquals("node_endN", routeTargetOf(xml, "errgate_scriptN"),
                "the host keeps exactly its own success exit; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    // Target resolution from the boundary edge. This is the ONLY shape that reaches a real deploy:
    // BoundaryEventConfig.targetNodeId is transient, so persistence drops it when the workflow
    // document is stored and the daemon always reads it back null.

    @Test
    void boundary_with_no_targetNodeId_on_the_config_routes_via_its_edge() {
        BoundaryEventConfig timer = BoundaryEventConfig.timer("PT30M");
        timer.setId("b1");
        assertNull(timer.getTargetNodeId(), "the config arrives from the store without a target");
        NodeDto sandbox = node("scriptN", "custom.sandbox", "custom-script-executor", "serviceTask");
        sandbox.getData().setBoundaryEventConfigs(List.of(timer));

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null), sandbox,
                node("handlerN", "end.event", null), node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("startN", "scriptN"), edge("scriptN", "endN"),
                boundaryEdge("scriptN", "handlerN", "b1"));

        String xml = newService().generateBpmnXml("wfRes", "Resolved", nodes, edges);

        assertEquals("node_handlerN", routeTargetOf(xml, "btimer_scriptN_0"),
                "the handler is resolved from the edge carrying the boundary id; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void each_boundary_without_a_config_target_resolves_its_own_edge() {
        BoundaryEventConfig timer = BoundaryEventConfig.timer("PT5M");
        timer.setId("b_timer");
        BoundaryEventConfig message = BoundaryEventConfig.message("CancelOrder", "=orderId");
        message.setId("b_msg");
        BoundaryEventConfig error = BoundaryEventConfig.error("VALIDATION_FAILED");
        error.setId("b_err");
        NodeDto sandbox = node("scriptN", "custom.sandbox", "custom-script-executor", "serviceTask");
        sandbox.getData().setBoundaryEventConfigs(List.of(timer, message, error));

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null), sandbox,
                node("timeoutN", "end.event", null), node("cancelN", "end.event", null),
                node("invalidN", "end.event", null), node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("startN", "scriptN"), edge("scriptN", "endN"),
                boundaryEdge("scriptN", "timeoutN", "b_timer"),
                boundaryEdge("scriptN", "cancelN", "b_msg"),
                boundaryEdge("scriptN", "invalidN", "b_err"));

        String xml = newService().generateBpmnXml("wfResMulti", "Resolved Many", nodes, edges);

        assertEquals("node_timeoutN", routeTargetOf(xml, "btimer_scriptN_0"), "timer → its own handler");
        assertEquals("node_cancelN", routeTargetOf(xml, "bmsg_scriptN_0"), "message → its own handler");
        assertEquals("node_invalidN", routeTargetOf(xml, "errcode_scriptN_0"), "error → its own handler");
        assertDeployableBpmn(xml);
    }

    @Test
    void an_edge_only_matches_the_boundary_of_its_own_source_node() {
        // Boundary ids are unique per node, not per graph: a same-named boundary on another node must
        // not steal this one's edge.
        BoundaryEventConfig first = BoundaryEventConfig.timer("PT5M");
        first.setId("b1");
        BoundaryEventConfig second = BoundaryEventConfig.timer("PT5M");
        second.setId("b1");
        NodeDto scriptA = node("scriptA", "custom.sandbox", "custom-script-executor", "serviceTask");
        scriptA.getData().setBoundaryEventConfigs(List.of(first));
        NodeDto scriptB = node("scriptB", "custom.sandbox", "custom-script-executor", "serviceTask");
        scriptB.getData().setBoundaryEventConfigs(List.of(second));

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null), scriptA, scriptB,
                node("handlerA", "end.event", null), node("handlerB", "end.event", null),
                node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("startN", "scriptA"), edge("scriptA", "scriptB"), edge("scriptB", "endN"),
                boundaryEdge("scriptA", "handlerA", "b1"),
                boundaryEdge("scriptB", "handlerB", "b1"));

        String xml = newService().generateBpmnXml("wfResHost", "Per Host", nodes, edges);

        assertEquals("node_handlerA", routeTargetOf(xml, "btimer_scriptA_0"), "scriptA keeps its own edge");
        assertEquals("node_handlerB", routeTargetOf(xml, "btimer_scriptB_0"), "scriptB keeps its own edge");
        assertDeployableBpmn(xml);
    }

    @Test
    void a_config_target_is_still_honoured_when_no_boundary_edge_exists() {
        // A payload that never went through the store still carries targetNodeId; it stays a valid hint.
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN",
                cfg(BoundaryEventConfig.timer("PT30M"), "b1", "handlerN")), "handlerN");

        assertEquals("node_handlerN", routeTargetOf(xml, "btimer_scriptN_0"),
                "with no edge the config's own target is used; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void a_boundary_edge_wins_over_a_stale_config_target() {
        BoundaryEventConfig timer = cfg(BoundaryEventConfig.timer("PT30M"), "b1", "staleN");
        NodeDto sandbox = node("scriptN", "custom.sandbox", "custom-script-executor", "serviceTask");
        sandbox.getData().setBoundaryEventConfigs(List.of(timer));

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null), sandbox,
                node("staleN", "end.event", null), node("liveN", "end.event", null),
                node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("startN", "scriptN"), edge("scriptN", "endN"),
                boundaryEdge("scriptN", "liveN", "b1"));

        String xml = newService().generateBpmnXml("wfResWin", "Edge Wins", nodes, edges);

        assertEquals("node_liveN", routeTargetOf(xml, "btimer_scriptN_0"),
                "the graph's edge is authoritative over a hint on the config; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void boundary_with_neither_a_config_target_nor_an_edge_falls_back_to_the_shared_error_end() {
        BoundaryEventConfig timer = BoundaryEventConfig.timer("PT30M");
        timer.setId("b1");
        NodeDto sandbox = node("scriptN", "custom.sandbox", "custom-script-executor", "serviceTask");
        sandbox.getData().setBoundaryEventConfigs(List.of(timer));

        String xml = newService().generateBpmnXml("wfResNone", "No Handler",
                List.of(node("startN", "start.event", null), sandbox, node("endN", "end.event", null)),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN")));

        assertEquals(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID,
                routeTargetOf(xml, "btimer_scriptN_0"),
                "an unhandled boundary still terminates at the shared error end; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void boundary_targets_resolve_inside_a_function_port_sub_process() {
        // The sub-process path compiles its own Process_SubProc_<root>, so the resolution has to reach
        // it too or a boundary inside a function tool would silently lose its handler.
        NodeDto master = node("masterN", "custom.sandbox", "custom-script-executor", "serviceTask");
        BoundaryEventConfig timer = BoundaryEventConfig.timer("PT30M");
        timer.setId("b1");
        NodeDto toolRoot = node("toolFnRoot", "custom.sandbox", "function-script-executor", "serviceTask");
        toolRoot.getData().setBoundaryEventConfigs(List.of(timer));
        NodeDto handler = node("toolHandlerN", "custom.sandbox", "custom-script-executor", "serviceTask");

        List<NodeDto> nodes = List.of(master, toolRoot, handler);
        List<EdgeDto> edges = List.of(
                edge("masterN", "toolFnRoot", "FUNCTION-model", "INPUT-main_input"),
                boundaryEdge("toolFnRoot", "toolHandlerN", "b1"));

        Map<String, String> subs = newService().generateSubProcessBpmnXmls("wfSubB", nodes, edges);

        String xml = subs.get("toolFnRoot");
        assertNotNull(xml, "the function root compiles to its own sub-process; keys=" + subs.keySet());
        assertEquals("node_toolHandlerN", routeTargetOf(xml, "btimer_toolFnRoot_0"),
                "a boundary inside a sub-process resolves its handler too; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void boundary_count_per_node_is_bounded() {
        BoundaryEventConfig[] configs = new BoundaryEventConfig[
                ActivitiBoundaryEventHelper.MAX_BOUNDARIES_PER_NODE + 5];
        for (int i = 0; i < configs.length; i++) {
            configs[i] = cfg(BoundaryEventConfig.timer("PT" + (i + 1) + "M"), "b" + i, null);
        }
        String xml = generateWithBoundaries(sandboxWithBoundaries("scriptN", configs));

        assertTrue(xml.contains("btimer_scriptN_" + (ActivitiBoundaryEventHelper.MAX_BOUNDARIES_PER_NODE - 1)),
                "boundaries up to the cap are emitted; xml=" + xml);
        assertFalse(xml.contains("btimer_scriptN_" + ActivitiBoundaryEventHelper.MAX_BOUNDARIES_PER_NODE),
                "a config list may not schedule unbounded jobs per node; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    /**
     * Pins the exact bytes a graph with NO boundary configs compiles to. Boundary support must be
     * additive: an existing workflow's XML — and therefore its deployed definition — must not move.
     */
    @Test
    void a_graph_without_boundary_configs_is_byte_identical_to_the_committed_golden() throws Exception {
        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null),
                node("scriptN", "custom.sandbox", "custom-script-executor"),
                node("gwN", "exclusive.gateway", null),
                node("script2N", "custom.sandbox", "function-script-executor"),
                node("endN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("startN", "scriptN"), edge("scriptN", "gwN"),
                edge("gwN", "script2N"), edge("script2N", "endN"));

        String xml = newService().generateBpmnXml("wfGolden", "Golden Flow", nodes, edges);

        try (java.io.InputStream golden =
                     getClass().getResourceAsStream("/bpmn/no-boundary-golden.xml")) {
            assertNotNull(golden, "golden BPMN resource missing");
            // The generator always emits LF; a Windows checkout (core.autocrlf=true) materialises
            // the golden with CRLF. Normalise so the byte-parity check compares content, not the
            // checkout's line-ending policy.
            String goldenXml = new String(golden.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("\r\n", "\n");
            assertEquals(goldenXml, xml,
                    "a boundary-free graph must still compile byte-for-byte to the golden XML");
        }
    }

    // ASSOCIATION exclusion, typed edge conditions, explicit default

    @Test
    void association_edge_is_excluded_from_sequence_flows_and_fork_degree() {
        // start --SEQUENCE--> task1 --> end, plus start --ASSOCIATION--> task2 (visual only).
        // The association must not emit a sequenceFlow and must not raise start's out-degree to 2
        // (no synthetic ParallelGateway fork).
        NodeDto start = node("startN", "start.event", null);
        NodeDto task1 = node("task1", "custom.sandbox", "custom-script-executor");
        NodeDto task2 = node("task2", "custom.sandbox", "custom-script-executor");
        NodeDto end = node("endN", "end.event", null);

        EdgeDto assoc = edge("startN", "task2");
        EdgeDataDto assocData = new EdgeDataDto();
        assocData.setFlowType("ASSOCIATION");
        assoc.setData(assocData);

        String xml = newService().generateBpmnXml("wfAssoc", "Assoc",
                List.of(start, task1, task2, end),
                List.of(edge("startN", "task1"), assoc, edge("task1", "endN")));

        assertNull(parseModel(xml).getMainProcess().getFlowElement("gwfork_startN"),
                "association-only second outgoing must not create a parallel fork; xml=" + xml);
        assertEquals(1, flowsFrom(xml, "node_startN"),
                "start must keep a single main-flow outgoing sequenceFlow; xml=" + xml);
        boolean touchesTask2 = parseModel(xml).getMainProcess().getFlowElements().stream()
                .filter(SequenceFlow.class::isInstance)
                .map(SequenceFlow.class::cast)
                .anyMatch(f -> "node_task2".equals(f.getSourceRef()) || "node_task2".equals(f.getTargetRef()));
        assertFalse(touchesTask2,
                "ASSOCIATION must not produce any sequenceFlow involving task2; xml=" + xml);
    }

    @Test
    void typed_edge_condition_emits_branch_identity_juel() {
        // Typed EdgeDataDto.condition wins over a legacy FEEL row; the BPMN condition is the
        // branch-identity token, never FeelToJuelTranslator on the typed expression.
        NodeDto gateway = node("gw1", "exclusive.gateway", null);
        EdgeTransformDto legacy = new EdgeTransformDto();
        legacy.setTargetNodeId("a1");
        legacy.setSourcePath("conditionExpression");
        legacy.setSourcePathValue("= route__gw1 = 'legacy'");
        gateway.setEdgeTransformListForCurrentNode(List.of(legacy));

        EdgeDto toA = edge("gw1", "a1");
        EdgeDataDto typed = new EdgeDataDto();
        typed.setCondition("$(@cart.total) > 100");
        toA.setData(typed);
        // A second (conditionless → default) branch: Activiti forbids a condition on a
        // single-outgoing gateway, so the conditioned branch needs a sibling.
        EdgeDto toB = edge("gw1", "b1");

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "gw1"),
                toA,
                toB,
                edge("a1", "end1"),
                edge("b1", "end1"));

        String xml = newService().generateBpmnXml("wfTypedCond", "Typed Cond", nodes, edges);

        assertTrue(xml.contains("route__gw1 == &#39;a1&#39;") || xml.contains("route__gw1 == 'a1'"),
                "typed condition must emit branch-identity JUEL; xml=" + xml);
        assertFalse(xml.contains("cart.total") || xml.contains("legacy"),
                "authored typed/legacy expression text must not appear in BPMN; xml=" + xml);
    }

    @Test
    void explicit_isDefaultFlow_marks_gateway_default() {
        NodeDto gateway = node("gw1", "exclusive.gateway", null);

        EdgeDto toA = edge("gw1", "a1");
        EdgeDataDto aData = new EdgeDataDto();
        aData.setCondition("$(x) > 1");
        toA.setData(aData);

        EdgeDto toB = edge("gw1", "b1");
        EdgeDataDto bData = new EdgeDataDto();
        bData.setIsDefaultFlow(true);
        toB.setData(bData);

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "gw1"),
                toA,
                toB,
                edge("a1", "end1"),
                edge("b1", "end1"));

        String xml = newService().generateBpmnXml("wfDefault", "Default Flow", nodes, edges);

        FlowElement gwEl = parseModel(xml).getMainProcess().getFlowElement("node_gw1");
        assertInstanceOf(ExclusiveGateway.class, gwEl, "xml=" + xml);
        assertEquals("flow_e_gw1_b1", ((ExclusiveGateway) gwEl).getDefaultFlow(),
                "explicit isDefaultFlow must set the gateway default attribute; xml=" + xml);
    }

    @Test
    void schemaItems_condition_emits_route_token_juel_not_default() {
        // Exclusive-gateway OUTPUT schemaItems `condition` is a filled FE carrier; generation
        // used to miss it and mark DEFAULT (always-pass). Must emit route__gw1 == 'a1'.
        NodeDto gateway = node("gw1", "exclusive.gateway", null);

        EdgeDto toA = edge("gw1", "a1");
        EdgeDataDto aData = new EdgeDataDto();
        SchemaItemDto conditionRow = new SchemaItemDto();
        conditionRow.setShortCode("condition");
        conditionRow.setValue("$(@cart.total) > 100");
        aData.setSchemaItems(List.of(conditionRow));
        toA.setData(aData);

        EdgeDto toB = edge("gw1", "b1");

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "gw1"),
                toA,
                toB,
                edge("a1", "end1"),
                edge("b1", "end1"));

        String xml = newService().generateBpmnXml("wfSchemaCond", "Schema Cond", nodes, edges);

        assertTrue(xml.contains("route__gw1 == &#39;a1&#39;") || xml.contains("route__gw1 == 'a1'"),
                "schemaItems condition must emit branch-identity JUEL; xml=" + xml);
        FlowElement gwEl = parseModel(xml).getMainProcess().getFlowElement("node_gw1");
        assertInstanceOf(ExclusiveGateway.class, gwEl, "xml=" + xml);
        assertEquals("flow_e_gw1_b1", ((ExclusiveGateway) gwEl).getDefaultFlow(),
                "blank sibling may be default; authored schemaItems branch must not; xml=" + xml);
        assertFalse(xml.contains("cart.total"),
                "authored expression text must not appear in BPMN; xml=" + xml);
    }

    @Test
    void node_condition_param_emits_route_token_juel_not_default() {
        NodeDto gateway = node("gw1", "exclusive.gateway", null);
        ParameterDto conditionParam = new ParameterDto();
        conditionParam.setShortCode("condition");
        conditionParam.setValue("$(wf.score) > 5");
        gateway.getData().setConfigurationParameters(List.of(conditionParam));

        EdgeDto toA = edge("gw1", "a1");
        EdgeDto toB = edge("gw1", "b1");

        List<NodeDto> nodes = List.of(
                node("start1", "start.event", null),
                gateway,
                node("a1", "custom.sandbox", "custom-script-executor"),
                node("b1", "custom.sandbox", "custom-script-executor"),
                node("end1", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("start1", "gw1"),
                toA,
                toB,
                edge("a1", "end1"),
                edge("b1", "end1"));

        String xml = newService().generateBpmnXml("wfNodeCond", "Node Cond", nodes, edges);

        assertTrue(xml.contains("route__gw1 == &#39;a1&#39;") || xml.contains("route__gw1 == 'a1'"),
                "node condition parameter must emit branch-identity JUEL; xml=" + xml);
        FlowElement gwEl = parseModel(xml).getMainProcess().getFlowElement("node_gw1");
        assertInstanceOf(ExclusiveGateway.class, gwEl, "xml=" + xml);
        assertNotEquals("flow_e_gw1_a1", ((ExclusiveGateway) gwEl).getDefaultFlow(),
                "authored node-param branch must not be the gateway default; xml=" + xml);
        assertFalse(xml.contains("wf.score") || xml.contains("score) > 5"),
                "authored expression text must not appear in BPMN; xml=" + xml);
    }

    // =====================================================================
    // Message catch timeout — event-based-gateway synthesis (timeoutMs / onTimeoutBehavior).
    // =====================================================================

    private NodeDto messageCatch(String id, String name, String timeoutMs, String onTimeoutBehavior) {
        NodeDto node = node(id, "message.intermediate.catch.event", null);
        List<ParameterDto> params = new java.util.ArrayList<>();
        ParameterDto nameParam = new ParameterDto();
        nameParam.setShortCode("name");
        nameParam.setValue(name);
        params.add(nameParam);
        if (timeoutMs != null) {
            ParameterDto timeoutParam = new ParameterDto();
            timeoutParam.setShortCode("timeoutMs");
            timeoutParam.setValue(timeoutMs);
            params.add(timeoutParam);
        }
        if (onTimeoutBehavior != null) {
            ParameterDto behaviorParam = new ParameterDto();
            behaviorParam.setShortCode("onTimeoutBehavior");
            behaviorParam.setValue(onTimeoutBehavior);
            params.add(behaviorParam);
        }
        node.getData().setConfigurationParameters(params);
        return node;
    }

    @Test
    void message_catch_with_timeout_synthesises_event_based_gateway_and_timer_branch() {
        // timeoutMs=5000 + onTimeoutBehavior=route: the catch must expand to
        // EventBasedGateway -> [message catch, timer catch(PT5S)], the message edge must leave
        // from the message catch and the Timeout-port edge from the timer catch — and the node
        // must NOT be wrapped in a parallel fork (event choice, not fan-out).
        NodeDto catchNode = messageCatch("c1", "order.paid", "5000", "route");
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                catchNode,
                node("okN", "end.event", null),
                node("lateN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("s1", "c1"),
                edge("c1", "okN", "OUT-message", "INPUT-in"),
                edge("c1", "lateN", "OUT-timeout", "INPUT-in"));

        String xml = newService().generateBpmnXml("wfTO", "Catch Timeout", nodes, edges);
        BpmnModel model = parseModel(xml);

        assertInstanceOf(org.activiti.bpmn.model.EventGateway.class,
                model.getMainProcess().getFlowElement("node_c1_ebg"),
                "timeout>0 must synthesise an event-based gateway; xml=" + xml);
        FlowElement timerEl = model.getMainProcess().getFlowElement("node_c1_timeout");
        assertInstanceOf(org.activiti.bpmn.model.IntermediateCatchEvent.class, timerEl,
                "the timeout branch is a timer catch; xml=" + xml);
        TimerEventDefinition timerDef = (TimerEventDefinition)
                ((org.activiti.bpmn.model.IntermediateCatchEvent) timerEl).getEventDefinitions().get(0);
        assertEquals("PT5S", timerDef.getTimeDuration(), "timeoutMs=5000 → PT5S; xml=" + xml);
        assertEquals("node_c1_ebg", routeTargetOf(xml, "node_s1"),
                "inbound edges land on the gateway, not the catch; xml=" + xml);
        assertEquals("node_okN", routeTargetOf(xml, "node_c1"),
                "the message port exits from the message catch; xml=" + xml);
        assertEquals("node_lateN", routeTargetOf(xml, "node_c1_timeout"),
                "the timeout port exits from the timer catch; xml=" + xml);
        assertNull(model.getMainProcess().getFlowElement("gwfork_c1"),
                "an event-choice node must never get a parallel fork; xml=" + xml);
        assertDeployableBpmn(xml);
        assertNoProcessValidationErrors(xml);
    }

    @Test
    void message_catch_without_timeout_stays_a_single_catch() {
        NodeDto catchNode = messageCatch("c1", "order.paid", null, null);
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null), catchNode, node("okN", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "okN"));

        String xml = newService().generateBpmnXml("wfNoTO", "No Timeout", nodes, edges);

        assertFalse(xml.contains("eventBasedGateway"),
                "timeoutMs absent → no event-based gateway synthesis; xml=" + xml);
        assertNull(parseModel(xml).getMainProcess().getFlowElement("node_c1_timeout"),
                "no timer branch without a timeout; xml=" + xml);
        assertEquals("node_c1", routeTargetOf(xml, "node_s1"),
                "inbound edges land on the catch directly; xml=" + xml);
        assertDeployableBpmn(xml);
    }

    @Test
    void message_catch_with_unparseable_timeout_fails_the_deploy_loudly() {
        // A non-numeric/negative timeoutMs must not degrade to "no timeout": the deployed
        // graph would silently lose the authored Timeout branch.
        for (String bad : new String[] {"soon", "-5"}) {
            NodeDto catchNode = messageCatch("c1", "order.paid", bad, null);
            List<NodeDto> nodes = List.of(
                    node("s1", "start.event", null), catchNode, node("okN", "end.event", null));
            List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "okN"));
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> newService().generateBpmnXml("wfBadTO", "Bad Timeout", nodes, edges),
                    "timeoutMs=" + bad);
            assertTrue(ex.getMessage().contains("timeoutMs"), ex.getMessage());
        }
    }

    @Test
    void message_catch_timeout_cancel_absorbs_the_token_and_swallows_the_timeout_port() {
        // onTimeoutBehavior=cancel: the timer branch ends in a plain (none) end event and an
        // authored Timeout-port edge is moot — it must be skipped, not wired into a fork.
        NodeDto catchNode = messageCatch("c1", "order.paid", "250", "cancel");
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                catchNode,
                node("okN", "end.event", null),
                node("lateN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("s1", "c1"),
                edge("c1", "okN", "OUT-message", "INPUT-in"),
                edge("c1", "lateN", "OUT-timeout", "INPUT-in"));

        String xml = newService().generateBpmnXml("wfCancel", "Timeout Cancel", nodes, edges);
        BpmnModel model = parseModel(xml);

        assertEquals("PT0.250S",
                ((TimerEventDefinition) ((org.activiti.bpmn.model.IntermediateCatchEvent)
                        model.getMainProcess().getFlowElement("node_c1_timeout"))
                        .getEventDefinitions().get(0)).getTimeDuration(),
                "sub-second timeout keeps millisecond precision; xml=" + xml);
        FlowElement cancelEnd = model.getMainProcess().getFlowElement("node_c1_timeout_end");
        assertInstanceOf(EndEvent.class, cancelEnd, "cancel branch ends in an end event; xml=" + xml);
        assertTrue(((EndEvent) cancelEnd).getEventDefinitions().isEmpty(),
                "cancel is a plain none end (token absorbed), not terminate/error; xml=" + xml);
        assertEquals(1, flowsFrom(xml, "node_c1_timeout"),
                "the authored Timeout-port edge is swallowed — only the terminal flow remains; xml=" + xml);
        assertNull(model.getMainProcess().getFlowElement("flow_e_c1_lateN"),
                "the moot timeout edge must not be wired; xml=" + xml);
        assertDeployableBpmn(xml);
        assertNoProcessValidationErrors(xml);
    }

    @Test
    void message_catch_timeout_error_raises_timeout_expired_bpmn_error() {
        NodeDto catchNode = messageCatch("c1", "order.paid", "60000", "error");
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null), catchNode, node("okN", "end.event", null));
        List<EdgeDto> edges = List.of(
                edge("s1", "c1"), edge("c1", "okN", "OUT-message", "INPUT-in"));

        String xml = newService().generateBpmnXml("wfErr", "Timeout Error", nodes, edges);
        BpmnModel model = parseModel(xml);

        FlowElement errorEnd = model.getMainProcess().getFlowElement("node_c1_timeout_err");
        assertInstanceOf(EndEvent.class, errorEnd, "error branch ends in an end event; xml=" + xml);
        EventDefinition def = ((EndEvent) errorEnd).getEventDefinitions().get(0);
        assertInstanceOf(org.activiti.bpmn.model.ErrorEventDefinition.class, def,
                "error branch raises a BPMN error; xml=" + xml);
        assertEquals("Error_timeout_expired",
                ((org.activiti.bpmn.model.ErrorEventDefinition) def).getErrorRef(),
                "the error end references the model-level error declaration; xml=" + xml);
        assertTrue(xml.contains("timeout.expired"),
                "the declaration carries the template contract's error code; xml=" + xml);
        assertDeployableBpmn(xml);
        assertNoProcessValidationErrors(xml);
    }

    // Deploy-time required-parameter validation (audit).

    @Test
    void a_blank_required_parameter_fails_the_deploy_loudly() {
        // The message catch template marks `name` required. Blank used to slip through and the
        // registry minted a UUID message name — the deployed flow could never be correlated.
        NodeDto catchNode = messageCatch("c1", "", null, null);
        ParameterDto nameParam = catchNode.getData().getConfigurationParameters().get(0);
        nameParam.setRequired(true);
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null), catchNode, node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "e1"));

        IllegalArgumentException ex = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfReq", "Required", nodes, edges));
        assertTrue(ex.getMessage().contains("c1") && ex.getMessage().contains("name"),
                "the violation names the node and the parameter; msg=" + ex.getMessage());
    }

    @Test
    void a_filled_required_parameter_deploys() {
        NodeDto catchNode = messageCatch("c1", "order.paid", null, null);
        catchNode.getData().getConfigurationParameters().get(0).setRequired(true);
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null), catchNode, node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "c1"), edge("c1", "e1"));

        String xml = assertDoesNotThrow(
                () -> newService().generateBpmnXml("wfReqOk", "Required Ok", nodes, edges));
        assertDeployableBpmn(xml);
    }

    @Test
    void a_blank_required_parameter_on_a_dropped_unreachable_node_does_not_block_deploy() {
        // The node never reaches the main flow (no main edge touches it), so it is dropped with a
        // warn — failing the deploy for a node that will not run would be noise.
        NodeDto orphan = messageCatch("orphanN", "", null, null);
        orphan.getData().getConfigurationParameters().get(0).setRequired(true);
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                node("t1", "custom.sandbox", "custom-script-executor"),
                node("e1", "end.event", null),
                orphan);
        List<EdgeDto> edges = List.of(edge("s1", "t1"), edge("t1", "e1"));

        String xml = assertDoesNotThrow(
                () -> newService().generateBpmnXml("wfReqDrop", "Required Dropped", nodes, edges));
        assertFalse(xml.contains("node_orphanN"), "the orphan is not emitted; xml=" + xml);
    }

    // =====================================================================
    // Unsupported standard types fail loudly (audit M2) — no silent remap.
    // =====================================================================

    @Test
    void a_complex_gateway_fails_the_deploy_instead_of_silently_becoming_xor() {
        // complex.gateway is published:false and has no strategy; the generic nodeType=gateway
        // remap would have made it an exclusive gateway, dropping the activationCondition.
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                node("gw1", "complex.gateway", null, "gateway"),
                node("e1", "end.event", null));
        List<EdgeDto> edges = List.of(edge("s1", "gw1"), edge("gw1", "e1"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfComplex", "Complex", nodes, edges));
        assertTrue(ex.getMessage().contains("complex.gateway"),
                "the failure names the unsupported type; msg=" + ex.getMessage());
    }

    @Test
    void a_cancel_end_event_fails_the_deploy_instead_of_silently_becoming_a_none_end() {
        // cancel.end.event is published:false and has no strategy; the generic nodeType=end remap
        // would have made it a none end — there is no transaction subprocess to cancel.
        List<NodeDto> nodes = List.of(
                node("s1", "start.event", null),
                node("end1", "cancel.end.event", null, "end"));
        List<EdgeDto> edges = List.of(edge("s1", "end1"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> newService().generateBpmnXml("wfCancel", "Cancel", nodes, edges));
        assertTrue(ex.getMessage().contains("cancel.end.event"),
                "the failure names the unsupported type; msg=" + ex.getMessage());
    }
}
