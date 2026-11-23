package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiTimerDefinitionFactory;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code timer.intermediate.catch.event} → Activiti {@link IntermediateCatchEvent} +
 * {@link TimerEventDefinition} (as-is). Wait-state on the async executor.
 * Params {@code type} ∈ {duration,date,cycle}, {@code value} — mapped and validated by
 * {@link ActivitiTimerDefinitionFactory}; a {@code cycle} catch re-arms after each fire
 * until the token leaves the wait state. The {@code end} execution listener
 * ({@code ${timerInfoEmissionListener}}) fans the template-promised {@code timer_info}
 * envelope ({@code firedAt}/{@code kind}/{@code due}) out to the node's downstream when the
 * timer fires.
 */
@Order(1)
@Component("activitiTimerCatchStrategy")
public class ActivitiTimerCatchStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "timer.intermediate.catch.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String type = node.getData().findParameterValueByShortCode("type");
        String value = node.getData().findParameterValueByShortCode("value");

        IntermediateCatchEvent catchEvent = new IntermediateCatchEvent();
        catchEvent.setId("node_" + node.getId());
        catchEvent.setName(nodeLabel);

        TimerEventDefinition timer = ActivitiTimerDefinitionFactory.build(node.getId(), type, value);
        catchEvent.addEventDefinition(timer);
        attachTimerInfoListener(catchEvent);
        process.addFlowElement(catchEvent);
        return GeneratedNode.single(catchEvent.getId());
    }

    /**
     * Attaches the {@code timer_info} emitter on the {@code end} event: it fires exactly when the
     * token leaves the catch (the timer job has fired), never at park time.
     */
    private static void attachTimerInfoListener(IntermediateCatchEvent catchEvent) {
        org.activiti.bpmn.model.ActivitiListener listener = new org.activiti.bpmn.model.ActivitiListener();
        listener.setEvent("end");
        listener.setImplementationType(
                org.activiti.bpmn.model.ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation("${timerInfoEmissionListener}");
        List<org.activiti.bpmn.model.ActivitiListener> listeners =
                catchEvent.getExecutionListeners() == null
                        ? new ArrayList<>()
                        : new ArrayList<>(catchEvent.getExecutionListeners());
        listeners.add(listener);
        catchEvent.setExecutionListeners(listeners);
    }
}
