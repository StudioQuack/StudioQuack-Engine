package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.activiti.bpmn.model.StartEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code signal.start.event} → Activiti {@link StartEvent} + {@link SignalEventDefinition} (as-is).
 * The signal is registered once on the {@link BpmnModel}; started via {@code signalEventReceived}.
 */
@Order(1)
@Component("activitiSignalStartStrategy")
@RequiredArgsConstructor
public class ActivitiSignalStartStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiSignalMessageRegistry registry;

    @Override
    public boolean supports(String bpmnType) {
        return "signal.start.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String signalName = node.getData().findParameterValueByShortCode("name");
        String signalId = registry.getOrCreateSignal(model, signalName);

        StartEvent startEvent = new StartEvent();
        startEvent.setId("node_" + node.getId());
        startEvent.setName(nodeLabel);

        SignalEventDefinition def = new SignalEventDefinition();
        def.setSignalRef(signalId);
        startEvent.addEventDefinition(def);
        process.addFlowElement(startEvent);
        return GeneratedNode.single(startEvent.getId());
    }
}
