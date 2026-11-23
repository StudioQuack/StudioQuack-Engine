package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.LinkEventDefinition;
import org.activiti.bpmn.model.Process;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code link.intermediate.catch.event} → Activiti {@link IntermediateCatchEvent} +
 * {@link LinkEventDefinition} (a bare none catch is rejected by Activiti's validator).
 * See {@link ActivitiLinkThrowStrategy} — the pair is validated and wired with a real sequence
 * flow at generation time; the {@code name} param identifies the pair, and the pairing pass sets
 * the definition's {@code source} (Activiti validates its presence).
 */
@Order(1)
@Component("activitiLinkCatchStrategy")
public class ActivitiLinkCatchStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "link.intermediate.catch.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        IntermediateCatchEvent catchEvent = new IntermediateCatchEvent();
        catchEvent.setId("node_" + node.getId());
        catchEvent.setName(nodeLabel);
        LinkEventDefinition def = new LinkEventDefinition();
        def.setId("linkdef_" + node.getId());
        def.setName(node.getData().findParameterValueByShortCode("name"));
        catchEvent.addEventDefinition(def);
        process.addFlowElement(catchEvent);
        return GeneratedNode.single(catchEvent.getId());
    }
}
