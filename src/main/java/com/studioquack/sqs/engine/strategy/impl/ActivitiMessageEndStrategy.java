package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageSenderConfigurator;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code message.end.event} → Activiti {@link EndEvent} + throw {@link MessageEventDefinition},
 * plus the {@code ${messageSenderExecutionListener}} runtime bridge that delivers the thrown
 * message to a waiting intermediate catch / message-start on THIS engine (intra-engine
 * collaboration, "C" scenario: shared correlation key across two process definitions).
 * External webhook ingress is unaffected — both callers converge on the same
 * {@link com.studioquack.sqs.command.MessageCorrelationService#correlateWebhook} seam.
 */
@Order(1)
@Component("activitiMessageEndStrategy")
@RequiredArgsConstructor
public class ActivitiMessageEndStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiSignalMessageRegistry registry;
    private final ActivitiMessageSenderConfigurator senderConfigurator;

    @Override
    public boolean supports(String bpmnType) {
        return "message.end.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String messageId = registry.getOrCreateMessage(model, node.getData().findParameterValueByShortCode("name"));
        EndEvent endEvent = new EndEvent();
        endEvent.setId("node_" + node.getId());
        endEvent.setName(nodeLabel);
        MessageEventDefinition def = new MessageEventDefinition();
        def.setMessageRef(messageId);
        endEvent.addEventDefinition(def);

        // Attach the intra-engine sender bridge (listener + optional per-node config).
        senderConfigurator.configureSender(endEvent, node);

        process.addFlowElement(endEvent);
        return GeneratedNode.single(endEvent.getId());
    }
}
