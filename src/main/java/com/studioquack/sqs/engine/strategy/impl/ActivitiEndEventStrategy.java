package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Process;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** {@code end.event} becomes an Activiti {@link EndEvent}. */
@Order(1)
@Component("activitiEndEventStrategy")
public class ActivitiEndEventStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "end.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        EndEvent endEvent = new EndEvent();
        endEvent.setId("node_" + node.getId());
        endEvent.setName(nodeLabel);
        process.addFlowElement(endEvent);
        return GeneratedNode.single(endEvent.getId());
    }
}
