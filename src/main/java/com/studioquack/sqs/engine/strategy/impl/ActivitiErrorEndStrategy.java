package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.ErrorEventDefinition;
import org.activiti.bpmn.model.Process;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code error.end.event} → Activiti {@link EndEvent} + {@link ErrorEventDefinition} (as-is).
 * Params {@code name}, {@code code}. The error is registered on the {@link BpmnModel} so a
 * matching error boundary can catch it by code.
 */
@Order(1)
@Component("activitiErrorEndStrategy")
public class ActivitiErrorEndStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "error.end.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String code = node.getData().findParameterValueByShortCode("code");
        String name = node.getData().findParameterValueByShortCode("name");
        String errorCode = StringUtils.defaultIfBlank(code, StringUtils.defaultIfBlank(name, "ERROR"));
        String errorRef = "Error_" + errorCode.replaceAll("[^A-Za-z0-9_]", "_");
        // addError(errorRef, errorName, errorCode) — errorRef is what the definition points at.
        model.addError(errorRef, StringUtils.defaultIfBlank(name, errorCode), errorCode);

        EndEvent endEvent = new EndEvent();
        endEvent.setId("node_" + node.getId());
        endEvent.setName(nodeLabel);
        ErrorEventDefinition def = new ErrorEventDefinition();
        def.setErrorRef(errorRef);
        endEvent.addEventDefinition(def);
        process.addFlowElement(endEvent);
        return GeneratedNode.single(endEvent.getId());
    }
}
