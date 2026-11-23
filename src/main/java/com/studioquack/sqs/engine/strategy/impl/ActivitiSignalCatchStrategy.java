package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code signal.intermediate.catch.event} → Activiti {@link IntermediateCatchEvent} +
 * {@link SignalEventDefinition} (as-is). Delivered via {@code signalEventReceived(name[, executionId])}.
 */
@Order(1)
@Component("activitiSignalCatchStrategy")
@RequiredArgsConstructor
public class ActivitiSignalCatchStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiSignalMessageRegistry registry;

    @Override
    public boolean supports(String bpmnType) {
        return "signal.intermediate.catch.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String signalId = registry.getOrCreateSignal(model, node.getData().findParameterValueByShortCode("name"));
        IntermediateCatchEvent catchEvent = new IntermediateCatchEvent();
        catchEvent.setId("node_" + node.getId());
        catchEvent.setName(nodeLabel);
        SignalEventDefinition def = new SignalEventDefinition();
        def.setSignalRef(signalId);
        catchEvent.addEventDefinition(def);
        process.addFlowElement(catchEvent);
        return GeneratedNode.single(catchEvent.getId());
    }
}
