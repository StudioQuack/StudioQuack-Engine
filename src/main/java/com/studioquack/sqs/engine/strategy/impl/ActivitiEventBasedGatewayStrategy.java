package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EventGateway;
import org.activiti.bpmn.model.Process;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** {@code event.based.gateway} → Activiti {@link EventGateway} (as-is), followed by catch events. */
@Order(1)
@Component("activitiEventBasedGatewayStrategy")
public class ActivitiEventBasedGatewayStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "event.based.gateway".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        EventGateway gateway = new EventGateway();
        gateway.setId("node_" + node.getId());
        gateway.setName(nodeLabel);
        process.addFlowElement(gateway);
        return GeneratedNode.single(gateway.getId());
    }
}
