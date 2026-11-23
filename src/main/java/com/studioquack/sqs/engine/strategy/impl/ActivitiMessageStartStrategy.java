package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageReceiverConfigurator;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.StartEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code message.start.event} → Activiti {@link StartEvent} + {@link MessageEventDefinition} (as-is).
 * Message registered once on the {@link BpmnModel}; started via {@code startProcessInstanceByMessage(name)}.
 */
@Order(1)
@Component("activitiMessageStartStrategy")
@RequiredArgsConstructor
public class ActivitiMessageStartStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiSignalMessageRegistry registry;
    private final ActivitiMessageReceiverConfigurator receiverConfigurator;

    @Override
    public boolean supports(String bpmnType) {
        return "message.start.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String messageName = node.getData().findParameterValueByShortCode("name");
        String messageId = registry.getOrCreateMessage(model, messageName);

        StartEvent startEvent = new StartEvent();
        startEvent.setId("node_" + node.getId());
        startEvent.setName(nodeLabel);

        MessageEventDefinition def = new MessageEventDefinition();
        def.setMessageRef(messageId);
        startEvent.addEventDefinition(def);

        // Emit the receiver config (correlationKey / businessKeyExpression) as an
        // sq:receiverConfig extension element so the correlation service can read it
        // without re-parsing the graph.
        receiverConfigurator.configureReceiver(startEvent, node);

        process.addFlowElement(startEvent);
        return GeneratedNode.single(startEvent.getId());
    }
}
