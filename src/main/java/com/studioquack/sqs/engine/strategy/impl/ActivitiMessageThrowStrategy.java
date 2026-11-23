package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageSenderConfigurator;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ThrowEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code message.intermediate.throw.event} → Activiti {@link ThrowEvent} +
 * {@link MessageEventDefinition}, plus the {@code ${messageSenderExecutionListener}} runtime
 * bridge (see {@link com.studioquack.sqs.engine.delegate.MessageSenderExecutionListener}).
 * At runtime the listener enters the same
 * {@link com.studioquack.sqs.command.MessageCorrelationService#correlateWebhook} seam the
 * external webhook ingress uses, so intra-engine and external message delivery share one
 * correlation code path.
 */
@Order(1)
@Component("activitiMessageThrowStrategy")
@RequiredArgsConstructor
public class ActivitiMessageThrowStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiSignalMessageRegistry registry;
    private final ActivitiMessageSenderConfigurator senderConfigurator;

    @Override
    public boolean supports(String bpmnType) {
        return "message.intermediate.throw.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String messageId = registry.getOrCreateMessage(model, node.getData().findParameterValueByShortCode("name"));
        ThrowEvent throwEvent = new ThrowEvent();
        throwEvent.setId("node_" + node.getId());
        throwEvent.setName(nodeLabel);
        MessageEventDefinition def = new MessageEventDefinition();
        def.setMessageRef(messageId);
        throwEvent.addEventDefinition(def);

        // Attach the intra-engine sender bridge (listener + optional per-node config).
        senderConfigurator.configureSender(throwEvent, node);

        process.addFlowElement(throwEvent);
        return GeneratedNode.single(throwEvent.getId());
    }
}
