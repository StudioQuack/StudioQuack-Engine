package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.delegate.EscalationSenderExecutionListener;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ThrowEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code escalation.intermediate.throw.event} (as-is).
 *
 * <p>Activiti 9.0.0 has no {@code EscalationEventDefinition}, so the element stays a plain
 * (none) {@link ThrowEvent} — the token passes through — and the escalation itself is raised by
 * the attached {@link EscalationSenderExecutionListener}: it fires the reserved
 * {@code __sq_esc_<code>} signal at the matching escalation boundary subscriptions of this
 * process instance. The configured {@code code} travels on the
 * element as an {@code sq:escalationConfig} extension; a runtime IN {@code code} row overrides
 * it at fire time.
 */
@Slf4j
@Order(1)
@Component("activitiEscalationThrowStrategy")
public class ActivitiEscalationThrowStrategy implements ActivitiFlowNodeStrategy {

    @Override
    public boolean supports(String bpmnType) {
        return "escalation.intermediate.throw.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        ThrowEvent throwEvent = new ThrowEvent();
        throwEvent.setId("node_" + node.getId());
        throwEvent.setName(nodeLabel);
        EscalationSenderExecutionListener.writeConfig(throwEvent, node);
        EscalationSenderExecutionListener.attachTo(throwEvent);
        process.addFlowElement(throwEvent);
        return GeneratedNode.single(throwEvent.getId());
    }
}
