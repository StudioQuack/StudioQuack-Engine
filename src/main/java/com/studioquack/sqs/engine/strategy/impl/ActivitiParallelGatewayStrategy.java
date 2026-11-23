package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.ParallelGateway;
import org.activiti.bpmn.model.Process;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** {@code parallel.gateway} → Activiti {@link ParallelGateway} (as-is). */
@Order(1)
@Component("activitiParallelGatewayStrategy")
public class ActivitiParallelGatewayStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "parallel.gateway".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        ParallelGateway gateway = new ParallelGateway();
        gateway.setId("node_" + node.getId());
        gateway.setName(nodeLabel);
        process.addFlowElement(gateway);
        return GeneratedNode.single(gateway.getId());
    }
}
