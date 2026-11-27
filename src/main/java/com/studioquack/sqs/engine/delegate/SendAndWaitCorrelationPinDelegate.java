package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Event;
import org.activiti.bpmn.model.EventGateway;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.Message;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Post-send correlation-key pinning delegate for {@code send_and_wait} compound BPMN nodes:
 * {@code node_X → wait_X → errgate_X → pin_X → [EventGateway →] catch_X}.
 *
 * <p>Reads the REF-only correlation key emitted during Phase 1 ({@code studioquack.setCorrelationKey(key)},
 * stored on {@code __sq_swcorr_<nodeId>}) and pins it onto {@code __sq_msgcorr_<messageName>} so the
 * immediately following {@code catch_X} (or {@code EventGateway → catch_X}) wakes deterministically
 * when the correlated inbound webhook reply arrives.
 */
@Slf4j
@Component("sendAndWaitCorrelationPinDelegate")
public class SendAndWaitCorrelationPinDelegate implements JavaDelegate {

    public static final String PIN_ID_PREFIX = "pin_";
    public static final String DELEGATE_EXPRESSION = "${sendAndWaitCorrelationPinDelegate}";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;

    public SendAndWaitCorrelationPinDelegate(ObjectProvider<SqsEngine> sqsEngineProvider) {
        this.sqsEngineProvider = sqsEngineProvider;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String activityId = execution.getCurrentActivityId();
        String nodeId = StringUtils.removeStart(activityId, PIN_ID_PREFIX);
        String swVarName = EngineVariableNames.sendWaitCorrelationVariableName(nodeId);
        Object rawKey = execution.getVariable(swVarName);
        if (rawKey == null || StringUtils.isBlank(rawKey.toString())) {
            log.debug("sendAndWaitCorrelationPinDelegate: no Phase-1 correlationKey emitted for node={}", nodeId);
            return;
        }
        String correlationKey = rawKey.toString().trim();
        SqsEngine engine = sqsEngineProvider.getIfAvailable();
        if (engine == null) {
            return;
        }
        try {
            BpmnModel model = engine.readDeployedModel(execution.getProcessDefinitionId());
            if (model == null) {
                return;
            }
            String messageName = findDownstreamMessageName(model, activityId);
            if (StringUtils.isBlank(messageName)) {
                log.warn("sendAndWaitCorrelationPinDelegate: could not locate downstream catch messageName for {}",
                        activityId);
                return;
            }
            String msgCorrVar = EngineVariableNames.messageCorrelationVariableName(messageName);
            // Set on execution scope chain (visible both to direct catch_X and to child executions
            // created by an EventGateway timeout wrapper) and remove the temporary handoff variable.
            execution.setVariable(msgCorrVar, correlationKey);
            execution.removeVariable(swVarName);
            log.info("sendAndWaitCorrelationPinDelegate.pinned node={} messageName={} var={}",
                    nodeId, messageName, msgCorrVar);
        } catch (Exception ex) {
            log.warn("sendAndWaitCorrelationPinDelegate: failed pinning correlation key for {}: {}",
                    activityId, ex.getMessage());
        }
    }

    private static String findDownstreamMessageName(BpmnModel model, String pinActivityId) {
        FlowElement fe = model.getFlowElement(pinActivityId);
        if (!(fe instanceof FlowNode pinNode) || pinNode.getOutgoingFlows() == null) {
            return null;
        }
        for (SequenceFlow flow : pinNode.getOutgoingFlows()) {
            FlowElement target = model.getFlowElement(flow.getTargetRef());
            if (target instanceof IntermediateCatchEvent catchEvent) {
                String name = resolveMessageName(model, catchEvent);
                if (StringUtils.isNotBlank(name)) {
                    return name;
                }
            } else if (target instanceof EventGateway gateway && gateway.getOutgoingFlows() != null) {
                for (SequenceFlow gwFlow : gateway.getOutgoingFlows()) {
                    FlowElement arm = model.getFlowElement(gwFlow.getTargetRef());
                    if (arm instanceof IntermediateCatchEvent catchEvent) {
                        String name = resolveMessageName(model, catchEvent);
                        if (StringUtils.isNotBlank(name)) {
                            return name;
                        }
                    }
                }
            }
        }
        return null;
    }

    private static String resolveMessageName(BpmnModel model, Event event) {
        if (event.getEventDefinitions() == null) {
            return null;
        }
        return event.getEventDefinitions().stream()
                .filter(ed -> ed instanceof MessageEventDefinition)
                .map(ed -> {
                    String ref = ((MessageEventDefinition) ed).getMessageRef();
                    if (StringUtils.isBlank(ref)) {
                        return null;
                    }
                    Message msg = model.getMessage(ref);
                    return msg != null && StringUtils.isNotBlank(msg.getName()) ? msg.getName() : ref;
                })
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
    }
}
