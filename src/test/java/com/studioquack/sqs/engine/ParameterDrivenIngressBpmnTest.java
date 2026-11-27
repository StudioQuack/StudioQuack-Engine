package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.webhook.WebhookKeyRefHelper;
import com.studioquack.sqs.engine.bpmn.FeelToJuelTranslator;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageReceiverConfigurator;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import com.studioquack.sqs.engine.strategy.impl.ActivitiEndEventStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiMessageCatchStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiMessageStartStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiServiceTaskStrategy;
import com.studioquack.sqs.engine.strategy.impl.ActivitiStartEventStrategy;
import com.studioquack.sqs.webhook.WebhookMessageNames;
import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BoundaryEvent;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EventGateway;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ReceiveTask;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParameterDrivenIngressBpmnTest {

    private BpmnGenerationService generator;

    @BeforeEach
    void setUp() {
        ActivitiSignalMessageRegistry reg = new ActivitiSignalMessageRegistry();
        ActivitiMessageReceiverConfigurator receiverCfg = new ActivitiMessageReceiverConfigurator();
        ActivitiBoundaryEventHelper boundaryHelper = new ActivitiBoundaryEventHelper(reg, receiverCfg);
        ActivitiBpmnElementHelper elementHelper = new ActivitiBpmnElementHelper(boundaryHelper);

        List<ActivitiFlowNodeStrategy> strategies = List.of(
                new ActivitiStartEventStrategy(),
                new ActivitiEndEventStrategy(),
                new ActivitiMessageStartStrategy(reg, receiverCfg),
                new ActivitiMessageCatchStrategy(reg, receiverCfg),
                new ActivitiServiceTaskStrategy(elementHelper));
        generator = new BpmnGenerationService(strategies, new FeelToJuelTranslator(), boundaryHelper);
    }

    @Test
    void listenRole_compilesToMessageStartEvent_andRejectsIncomingEdge() throws Exception {
        NodeDto listen = nodeWithSelectRole("n_listen", "operation", "listen:Listen:listen", "listen",
                "whk_111111111111111111111111");
        NodeDto task = plainServiceNode("n_task");
        NodeDto end = bpmnNode("n_end", "end.event");

        String xml = generator.generateBpmnXml("wf1", "Listen WF",
                List.of(listen, task, end),
                List.of(edge("e1", "n_listen", "n_task"), edge("e2", "n_task", "n_end")));

        BpmnModel model = parseXml(xml);
        Process process = model.getMainProcess();
        StartEvent start = assertInstanceOf(StartEvent.class, process.getFlowElement("node_n_listen"));
        MessageEventDefinition med = assertInstanceOf(MessageEventDefinition.class,
                start.getEventDefinitions().get(0));
        String expectedName = WebhookMessageNames.forNode("wf1", "n_listen");
        assertEquals(expectedName, model.getMessage(med.getMessageRef()).getName());

        // Illegal incoming sequence flow to a LISTEN node must fail deploy loudly.
        NodeDto rogueStart = bpmnNode("n_start", "start.event");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                generator.generateBpmnXml("wf1", "Bad Listen WF",
                        List.of(rogueStart, listen, end),
                        List.of(edge("e0", "n_start", "n_listen"), edge("e1", "n_listen", "n_end"))));
        assertTrue(ex.getMessage().contains("LISTEN"));
    }

    @Test
    void messageCatchRole_withTimeout_expandsToEventGatewayAndTimer() throws Exception {
        NodeDto start = bpmnNode("n_start", "start.event");
        NodeDto catchNode = nodeWithSelectRole("n_catch", "operation",
                "wait_reply:Wait for Reply:message_catch", "wait_reply", "whk_222222222222222222222222");
        addParam(catchNode, "timeoutMs", "number", "30000");
        addParam(catchNode, "onTimeoutBehavior", "string", "route");
        NodeDto onReply = plainServiceNode("n_reply");
        NodeDto onTimeout = plainServiceNode("n_timeout");
        NodeDto end = bpmnNode("n_end", "end.event");

        String xml = generator.generateBpmnXml("wf2", "Catch WF",
                List.of(start, catchNode, onReply, onTimeout, end),
                List.of(
                        edge("e1", "n_start", "n_catch"),
                        edgeWithHandles("e2", "n_catch", "OUTPUT-message", "n_reply", "INPUT-in"),
                        edgeWithHandles("e3", "n_catch", "OUTPUT-timeout", "n_timeout", "INPUT-in"),
                        edge("e4", "n_reply", "n_end"),
                        edge("e5", "n_timeout", "n_end")));

        BpmnModel model = parseXml(xml);
        Process p = model.getMainProcess();
        assertInstanceOf(EventGateway.class, p.getFlowElement("node_n_catch_ebg"));
        IntermediateCatchEvent msgCatch = assertInstanceOf(IntermediateCatchEvent.class,
                p.getFlowElement("node_n_catch"));
        MessageEventDefinition med = assertInstanceOf(MessageEventDefinition.class,
                msgCatch.getEventDefinitions().get(0));
        assertEquals(WebhookMessageNames.forNode("wf2", "n_catch"),
                model.getMessage(med.getMessageRef()).getName());
        IntermediateCatchEvent timerCatch = assertInstanceOf(IntermediateCatchEvent.class,
                p.getFlowElement("node_n_catch_timeout"));
        assertInstanceOf(TimerEventDefinition.class, timerCatch.getEventDefinitions().get(0));
    }

    @Test
    void sendAndWaitRole_compilesCompoundExpansionWithPinAndTimeout() throws Exception {
        NodeDto start = bpmnNode("n_start", "start.event");
        NodeDto sw = nodeWithSelectRole("n_sw", "operation",
                "send_wait:Send & Wait:send_and_wait", "send_wait", "whk_333333333333333333333333");
        addParam(sw, "timeoutMs", "number", "60000");
        NodeDto next = plainServiceNode("n_next");
        NodeDto timedOut = plainServiceNode("n_to");
        NodeDto end = bpmnNode("n_end", "end.event");

        String xml = generator.generateBpmnXml("wf3", "SendAndWait WF",
                List.of(start, sw, next, timedOut, end),
                List.of(
                        edge("e1", "n_start", "n_sw"),
                        edgeWithHandles("e2", "n_sw", "OUTPUT-reply", "n_next", "INPUT-in"),
                        edgeWithHandles("e3", "n_sw", "OUTPUT-timeout", "n_to", "INPUT-in"),
                        edge("e4", "n_next", "n_end"),
                        edge("e5", "n_to", "n_end")));

        BpmnModel model = parseXml(xml);
        Process p = model.getMainProcess();
        ServiceTask dispatch = assertInstanceOf(ServiceTask.class, p.getFlowElement("node_n_sw"));
        assertTrue(dispatch.isAsynchronous());
        assertInstanceOf(ReceiveTask.class, p.getFlowElement("wait_n_sw"));
        assertInstanceOf(ServiceTask.class, p.getFlowElement("errgate_n_sw"));
        ServiceTask pin = assertInstanceOf(ServiceTask.class, p.getFlowElement("pin_n_sw"));
        assertEquals("${sendAndWaitCorrelationPinDelegate}", pin.getImplementation());
        assertInstanceOf(EventGateway.class, p.getFlowElement("node_n_sw_ebg"));
        IntermediateCatchEvent replyCatch = assertInstanceOf(IntermediateCatchEvent.class,
                p.getFlowElement("catch_n_sw"));
        MessageEventDefinition med = assertInstanceOf(MessageEventDefinition.class,
                replyCatch.getEventDefinitions().get(0));
        assertEquals(WebhookMessageNames.forNode("wf3", "n_sw"),
                model.getMessage(med.getMessageRef()).getName());
        assertInstanceOf(IntermediateCatchEvent.class, p.getFlowElement("node_n_sw_timeout"));
    }

    @Test
    void messageThrowRole_compilesToAsyncServiceTaskWithErrorGate() throws Exception {
        NodeDto start = bpmnNode("n_start", "start.event");
        NodeDto thr = nodeWithSelectRole("n_thr", "operation",
                "notify:Notify:message_throw", "notify", null);
        NodeDto end = bpmnNode("n_end", "end.event");

        String xml = generator.generateBpmnXml("wf4", "Throw WF",
                List.of(start, thr, end),
                List.of(edge("e1", "n_start", "n_thr"), edge("e2", "n_thr", "n_end")));

        BpmnModel model = parseXml(xml);
        Process p = model.getMainProcess();
        ServiceTask dispatch = assertInstanceOf(ServiceTask.class, p.getFlowElement("node_n_thr"));
        assertTrue(dispatch.isAsynchronous());
        assertInstanceOf(ReceiveTask.class, p.getFlowElement("wait_n_thr"));
        assertInstanceOf(ServiceTask.class, p.getFlowElement("errgate_n_thr"));
    }

    @Test
    void ingressBoundaryEvent_emitsDeterministicMessageBoundaryWithCorrelationPin() throws Exception {
        NodeDto start = bpmnNode("n_start", "start.event");
        NodeDto host = plainServiceNode("n_host");
        BoundaryEventConfig b = new BoundaryEventConfig();
        b.setId("b_cancel");
        b.setType(BoundaryEventConfig.BoundaryEventType.MESSAGE);
        b.setInterrupting(true);
        b.putConfig(BoundaryEventConfig.CONFIG_MESSAGE_SOURCE, BoundaryEventConfig.MESSAGE_SOURCE_INGRESS_NODE);
        b.putConfig("correlationKey", "${customerWaId}");
        Map<String, Object> ingressBinding = new LinkedHashMap<>();
        ingressBinding.put("templateId", "tpl-wa");
        ingressBinding.put("configurationParameters", List.of(
                Map.of("shortCode", "webhook_key", "value", WebhookKeyRefHelper.placeholder("whk_444444444444444444444444"))));
        b.putConfig(BoundaryEventConfig.CONFIG_INGRESS_NODE_BINDING, ingressBinding);
        host.getData().setBoundaryEventConfigs(List.of(b));

        NodeDto handler = plainServiceNode("n_handler");
        NodeDto end = bpmnNode("n_end", "end.event");

        EdgeDto bEdge = edge("e_b", "n_host", "n_handler");
        EdgeDataDto bEdgeData = new EdgeDataDto();
        bEdgeData.setBoundaryEventId("b_cancel");
        bEdge.setData(bEdgeData);

        String xml = generator.generateBpmnXml("wf5", "Boundary Ingress WF",
                List.of(start, host, handler, end),
                List.of(edge("e1", "n_start", "n_host"), edge("e2", "n_host", "n_end"),
                        bEdge, edge("e3", "n_handler", "n_end")));

        BpmnModel model = parseXml(xml);
        Process p = model.getMainProcess();
        BoundaryEvent be = assertInstanceOf(BoundaryEvent.class, p.getFlowElement("bmsg_n_host_0"));
        assertEquals("wait_n_host", be.getAttachedToRefId());
        MessageEventDefinition med = assertInstanceOf(MessageEventDefinition.class,
                be.getEventDefinitions().get(0));
        assertEquals(WebhookMessageNames.forBoundary("wf5", "n_host", b, ""),
                model.getMessage(med.getMessageRef()).getName());
        assertNotNull(p.getFlowElement("wait_n_host").getExecutionListeners());
        assertTrue(p.getFlowElement("wait_n_host").getExecutionListeners().stream()
                .anyMatch(l -> "${messageReceiverCorrelationListener}".equals(l.getImplementation())));
    }

    @Test
    void classicMessageEventsWithoutWebhookKey_remainUnchanged() throws Exception {
        NodeDto msgStart = bpmnNode("n_ms", "message.start.event");
        addParam(msgStart, "name", "string", "ClassicOrderPlaced");
        NodeDto msgCatch = bpmnNode("n_mc", "message.intermediate.catch.event");
        addParam(msgCatch, "name", "string", "ClassicPaymentDone");
        NodeDto end = bpmnNode("n_end", "end.event");

        String xml = generator.generateBpmnXml("wf6", "Classic WF",
                List.of(msgStart, msgCatch, end),
                List.of(edge("e1", "n_ms", "n_mc"), edge("e2", "n_mc", "n_end")));

        BpmnModel model = parseXml(xml);
        Process p = model.getMainProcess();
        StartEvent se = assertInstanceOf(StartEvent.class, p.getFlowElement("node_n_ms"));
        MessageEventDefinition seDef = assertInstanceOf(MessageEventDefinition.class, se.getEventDefinitions().get(0));
        assertEquals("ClassicOrderPlaced", model.getMessage(seDef.getMessageRef()).getName());

        IntermediateCatchEvent ce = assertInstanceOf(IntermediateCatchEvent.class, p.getFlowElement("node_n_mc"));
        MessageEventDefinition ceDef = assertInstanceOf(MessageEventDefinition.class, ce.getEventDefinitions().get(0));
        assertEquals("ClassicPaymentDone", model.getMessage(ceDef.getMessageRef()).getName());
    }

    @Test
    void graphSignature_changesWhenSelectOptionRoleChanges() {
        NodeDto normalNode = nodeWithSelectRole("n1", "operation", "op:Op", "op", null);
        NodeDto listenNode = nodeWithSelectRole("n1", "operation", "op:Op:listen", "op", null);
        String sigA = generator.computeGraphSignature("wf", List.of(normalNode), List.of());
        String sigB = generator.computeGraphSignature("wf", List.of(listenNode), List.of());
        assertNotEquals(sigA, sigB);
    }

    private static BpmnModel parseXml(String xml) throws Exception {
        XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(new StringReader(xml));
        return new BpmnXMLConverter().convertToBpmnModel(reader);
    }

    private static NodeDto nodeWithSelectRole(String id, String paramShortCode, String optionSpec,
                                              String value, String webhookKeyRef) {
        TemplateDto tpl = new TemplateDto();
        tpl.setId("tpl_" + id);
        tpl.setLabel(id);
        tpl.setJobType("custom-script-executor");
        ParameterDto sel = new ParameterDto();
        sel.setShortCode(paramShortCode);
        sel.setType("select");
        sel.setOptions(List.of(optionSpec));
        sel.setValue(value);

        List<ParameterDto> params = new ArrayList<>();
        params.add(sel);
        if (webhookKeyRef != null) {
            ParameterDto wh = new ParameterDto();
            wh.setShortCode("webhook_key");
            wh.setType("webhook_key");
            wh.setValue(WebhookKeyRefHelper.placeholder(webhookKeyRef));
            params.add(wh);
        }
        tpl.setConfigurationParameters(params);

        NodeDto n = new NodeDto();
        n.setId(id);
        n.setData(tpl);
        return n;
    }

    private static NodeDto plainServiceNode(String id) {
        TemplateDto tpl = new TemplateDto();
        tpl.setId("tpl_" + id);
        tpl.setLabel(id);
        tpl.setJobType("custom-script-executor");
        NodeDto n = new NodeDto();
        n.setId(id);
        n.setData(tpl);
        return n;
    }

    private static NodeDto bpmnNode(String id, String bpmnType) {
        TemplateDto tpl = new TemplateDto();
        tpl.setBpmnType(bpmnType);
        tpl.setLabel(id);
        NodeDto n = new NodeDto();
        n.setId(id);
        n.setType(bpmnType);
        n.setData(tpl);
        return n;
    }

    private static void addParam(NodeDto node, String shortCode, String type, Object value) {
        ParameterDto p = new ParameterDto();
        p.setShortCode(shortCode);
        p.setType(type);
        p.setValue(value);
        List<ParameterDto> params = node.getData().getConfigurationParameters() == null
                ? new ArrayList<>() : new ArrayList<>(node.getData().getConfigurationParameters());
        params.add(p);
        node.getData().setConfigurationParameters(params);
    }

    private static EdgeDto edge(String id, String src, String tgt) {
        EdgeDto e = new EdgeDto();
        e.setId(id);
        e.setSource(src);
        e.setTarget(tgt);
        return e;
    }

    private static EdgeDto edgeWithHandles(String id, String src, String srcHandle, String tgt, String tgtHandle) {
        EdgeDto e = edge(id, src, tgt);
        e.setSourceHandle(srcHandle);
        e.setTargetHandle(tgtHandle);
        return e;
    }
}
