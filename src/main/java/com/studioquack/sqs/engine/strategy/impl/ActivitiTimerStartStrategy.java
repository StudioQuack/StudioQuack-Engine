package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiTimerDefinitionFactory;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code timer.start.event} → Activiti {@link StartEvent} + {@link TimerEventDefinition} (as-is).
 * Params {@code type} ∈ {cycle,date,duration}, {@code value} — mapped and validated by
 * {@link ActivitiTimerDefinitionFactory}. Runs on the async job executor; every fire starts its own
 * run ({@code TimerStartRunLauncher}). The {@code end} listener {@code ${timerInfoEmissionListener}}
 * fans the template-promised {@code timer_info} envelope out to the downstream nodes of that run.
 */
@Order(1)
@Component("activitiTimerStartStrategy")
public class ActivitiTimerStartStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "timer.start.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String type = node.getData().findParameterValueByShortCode("type");
        String value = node.getData().findParameterValueByShortCode("value");

        StartEvent startEvent = new StartEvent();
        startEvent.setId("node_" + node.getId());
        startEvent.setName(nodeLabel);

        TimerEventDefinition timer = ActivitiTimerDefinitionFactory.build(node.getId(), type, value);
        startEvent.addEventDefinition(timer);
        org.activiti.bpmn.model.ActivitiListener timerInfo = new org.activiti.bpmn.model.ActivitiListener();
        timerInfo.setEvent("end");
        timerInfo.setImplementationType(
                org.activiti.bpmn.model.ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        timerInfo.setImplementation("${timerInfoEmissionListener}");
        java.util.List<org.activiti.bpmn.model.ActivitiListener> listeners = startEvent.getExecutionListeners() == null
                ? new java.util.ArrayList<>() : new java.util.ArrayList<>(startEvent.getExecutionListeners());
        listeners.add(timerInfo);
        startEvent.setExecutionListeners(listeners);
        process.addFlowElement(startEvent);
        return GeneratedNode.single(startEvent.getId());
    }
}
