package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.LinkEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ThrowEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code link.intermediate.throw.event} → Activiti {@link ThrowEvent} (none).
 *
 * <p>Activiti 9 parses {@code LinkEventDefinition} but never executes the jump — the throw
 * behavior is the default pass-through and the definition is decorative (and breaks the runtime
 * token). The off-page jump is therefore materialised at generation time: the pair is validated
 * and wired with a real sequence flow by {@code BpmnGenerationService.pairLinkEvents}. The
 * {@code name} param still identifies the pair.
 *
 * <p>An {@code end} execution listener ({@code linkJumpExecutionListener}) carries the throw's
 * mapped {@code link} IN {@code payload} across the jump, re-publishing it as the paired catch's
 * {@code link_payload} OUT to the catch's downstream.
 */
@Order(1)
@Component("activitiLinkThrowStrategy")
public class ActivitiLinkThrowStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "link.intermediate.throw.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        ThrowEvent throwEvent = new ThrowEvent();
        throwEvent.setId("node_" + node.getId());
        throwEvent.setName(nodeLabel);
        attachLinkJumpListener(throwEvent);
        process.addFlowElement(throwEvent);
        return GeneratedNode.single(throwEvent.getId());
    }

    /**
     * Attaches the link-payload carrier on the {@code end} event: it fires exactly when the token
     * leaves the throw toward the materialised link flow, never at entry.
     */
    private static void attachLinkJumpListener(ThrowEvent throwEvent) {
        org.activiti.bpmn.model.ActivitiListener listener = new org.activiti.bpmn.model.ActivitiListener();
        listener.setEvent("end");
        listener.setImplementationType(
                org.activiti.bpmn.model.ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation("${linkJumpExecutionListener}");
        java.util.List<org.activiti.bpmn.model.ActivitiListener> listeners =
                throwEvent.getExecutionListeners() == null
                        ? new java.util.ArrayList<>()
                        : new java.util.ArrayList<>(throwEvent.getExecutionListeners());
        listeners.add(listener);
        throwEvent.setExecutionListeners(listeners);
    }
}
