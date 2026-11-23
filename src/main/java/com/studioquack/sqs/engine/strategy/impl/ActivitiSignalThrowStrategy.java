package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.delegate.SignalSenderExecutionListener;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.activiti.bpmn.model.ThrowEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code signal.intermediate.throw.event} → Activiti {@link ThrowEvent} +
 * {@link SignalEventDefinition} (as-is). The broadcast itself stays native; the attached
 * {@link SignalSenderExecutionListener} (event=start) pre-seals the authored IN payload into
 * every parked signal-catch subscriber's context store before the broadcast wakes them.
 */
@Order(1)
@Component("activitiSignalThrowStrategy")
@RequiredArgsConstructor
public class ActivitiSignalThrowStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiSignalMessageRegistry registry;

    @Override
    public boolean supports(String bpmnType) {
        return "signal.intermediate.throw.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String signalId = registry.getOrCreateSignal(model, node.getData().findParameterValueByShortCode("name"));
        ThrowEvent throwEvent = new ThrowEvent();
        throwEvent.setId("node_" + node.getId());
        throwEvent.setName(nodeLabel);
        SignalEventDefinition def = new SignalEventDefinition();
        def.setSignalRef(signalId);
        throwEvent.addEventDefinition(def);
        SignalSenderExecutionListener.attachTo(throwEvent);
        process.addFlowElement(throwEvent);
        return GeneratedNode.single(throwEvent.getId());
    }
}
