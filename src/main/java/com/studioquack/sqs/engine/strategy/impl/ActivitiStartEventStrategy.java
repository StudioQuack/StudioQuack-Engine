package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.StartEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code start.event} becomes an Activiti {@link StartEvent}.
 *
 * <p>Every none start also carries the {@code ${callActivityChildContextListener}} on
 * {@code start}: when the process instance was created by a {@code call.activity} (it has a super
 * execution), the listener reseeds the child PI's {@code studioquackRef} with the CHILD's own
 * deployed graph — the inherited handle points at the parent's graph, which cannot resolve the
 * child's node definitions. For any other start the listener no-ops.
 *
 * <p>Then {@code ${startContextPublishListener}} publishes the start event's {@code start_context}
 * output onto every mapped connection, so DuckTape mappings from the start card reach the next node.
 */
@Order(1)
@Component("activitiStartEventStrategy")
public class ActivitiStartEventStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "start.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        StartEvent startEvent = new StartEvent();
        startEvent.setId("node_" + node.getId());
        startEvent.setName(nodeLabel);
        ActivitiListener childContext = new ActivitiListener();
        childContext.setEvent("start");
        childContext.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        childContext.setImplementation("${callActivityChildContextListener}");
        ActivitiListener startContext = new ActivitiListener();
        startContext.setEvent("start");
        startContext.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        startContext.setImplementation("${startContextPublishListener}");
        List<ActivitiListener> listeners = new ArrayList<>();
        listeners.add(childContext);
        listeners.add(startContext);
        startEvent.setExecutionListeners(listeners);
        process.addFlowElement(startEvent);
        return GeneratedNode.single(startEvent.getId());
    }
}
