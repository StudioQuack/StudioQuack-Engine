package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.delegate.SignalSenderExecutionListener;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code signal.end.event} → Activiti {@link EndEvent} + throw {@link SignalEventDefinition}.
 * Activiti 9 has NO signal end-event behavior (the parser maps it to
 * {@code NoneEndEventActivityBehavior}, leaving the definition decorative), so the attached
 * {@link SignalSenderExecutionListener} (event=start) both pre-seals the authored IN payload
 * into every parked signal-catch subscriber's context store AND performs the broadcast itself.
 */
@Order(1)
@Component("activitiSignalEndStrategy")
@RequiredArgsConstructor
public class ActivitiSignalEndStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiSignalMessageRegistry registry;

    @Override
    public boolean supports(String bpmnType) {
        return "signal.end.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String signalId = registry.getOrCreateSignal(model, node.getData().findParameterValueByShortCode("name"));
        EndEvent endEvent = new EndEvent();
        endEvent.setId("node_" + node.getId());
        endEvent.setName(nodeLabel);
        SignalEventDefinition def = new SignalEventDefinition();
        def.setSignalRef(signalId);
        endEvent.addEventDefinition(def);
        SignalSenderExecutionListener.attachTo(endEvent);
        process.addFlowElement(endEvent);
        return GeneratedNode.single(endEvent.getId());
    }
}
