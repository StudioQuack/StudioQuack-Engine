package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.TerminateEventDefinition;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** {@code terminate.end.event} → Activiti {@link EndEvent} + {@link TerminateEventDefinition} (as-is). */
@Order(1)
@Component("activitiTerminateEndStrategy")
public class ActivitiTerminateEndStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "terminate.end.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        EndEvent endEvent = new EndEvent();
        endEvent.setId("node_" + node.getId());
        endEvent.setName(nodeLabel);
        endEvent.addEventDefinition(new TerminateEventDefinition());
        process.addFlowElement(endEvent);
        return GeneratedNode.single(endEvent.getId());
    }
}
