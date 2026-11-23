package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ThrowEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code none.intermediate.throw.event} → Activiti {@link ThrowEvent} with no event definition
 * (BPMN none intermediate throw).
 */
@Order(1)
@Component("activitiNoneThrowStrategy")
public class ActivitiNoneThrowStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "none.intermediate.throw.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        ThrowEvent throwEvent = new ThrowEvent();
        throwEvent.setId("node_" + node.getId());
        throwEvent.setName(nodeLabel);
        process.addFlowElement(throwEvent);
        return GeneratedNode.single(throwEvent.getId());
    }
}
