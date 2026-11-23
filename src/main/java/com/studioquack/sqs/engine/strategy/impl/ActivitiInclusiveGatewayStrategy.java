package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.InclusiveGateway;
import org.activiti.bpmn.model.Process;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** {@code inclusive.gateway} → Activiti {@link InclusiveGateway} (as-is). JUEL conditions on outgoing flows. */
@Order(1)
@Component("activitiInclusiveGatewayStrategy")
public class ActivitiInclusiveGatewayStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "inclusive.gateway".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        InclusiveGateway gateway = new InclusiveGateway();
        gateway.setId("node_" + node.getId());
        gateway.setName(nodeLabel);
        process.addFlowElement(gateway);
        return GeneratedNode.single(gateway.getId());
    }
}
