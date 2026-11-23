package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.ExclusiveGateway;
import org.activiti.bpmn.model.Process;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code exclusive.gateway} → Activiti {@link ExclusiveGateway} (as-is). Outgoing sequence-flow
 * conditions are set by the generator as JUEL reading the execution-local namespaced routing
 * token ({@code ${route__<nodeId> == '…'}}) — never a shared global bag, never PHI.
 */
@Order(1)
@Component("activitiExclusiveGatewayStrategy")
public class ActivitiExclusiveGatewayStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "exclusive.gateway".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        ExclusiveGateway gateway = new ExclusiveGateway();
        gateway.setId("node_" + node.getId());
        gateway.setName(nodeLabel);
        process.addFlowElement(gateway);
        return GeneratedNode.single(gateway.getId());
    }
}
