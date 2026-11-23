package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageReceiverConfigurator;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.Activity;
import org.activiti.bpmn.model.BoundaryEvent;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Event;
import org.activiti.bpmn.model.EventGateway;
import org.activiti.bpmn.model.ExtensionAttribute;
import org.activiti.bpmn.model.ExtensionElement;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.Message;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Runtime bridge for the message RECEIVER side — {@code message.intermediate.catch.event}
 * (attached by
 * {@link com.studioquack.sqs.engine.strategy.impl.ActivitiMessageCatchStrategy}) and MESSAGE
 * boundary hosts (attached by
 * {@link com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper} to the user
 * task / call activity / sandbox wait state the boundary rides on). Fires (event=
 * {@code start}) as the token ARRIVES at the park point, resolves the authored correlation
 * key, and pins it as an execution-local, REF-only variable
 * ({@code __sq_msgcorr_<messageName>} = key, see
 * {@link EngineVariableNames#messageCorrelationVariableName}) on the very execution that holds
 * the message subscription.
 *
 * <p><b>Why this exists.</b> The engine's primary correlation seam matches a waiting catch by
 * {@code (receiver process-instance businessKey, messageName)} — but a sender in ANOTHER flow
 * cannot know the receiver run's businessKey at authoring time. The authored
 * {@code correlationKey} (the shared domain key both sides configure, e.g. {@code "order-42"})
 * was previously emitted as {@code sq:receiverConfig} yet never consumed at runtime, so
 * shared-key correlation never matched. The pin makes the authored key queryable:
 * {@link com.studioquack.sqs.command.MessageCorrelationService} falls back to
 * {@link SqsEngine#findMessageEventExecutionByCorrelationKey(String, String)} when the plain
 * businessKey lookup misses.
 *
 * <p><b>Resolution precedence</b> (mirrors the sender side): runtime INPUT-port override (a
 * DuckTape-mapped row on this node's {@code subscription} port carrying a {@code correlationKey}
 * field) → literal {@code correlationKey} config → {@code businessKeyExpression} minimal
 * {@code ${varName}} lookup against the receiver execution. All blank → no pin; the default
 * businessKey correlation then applies unchanged. The mapped value wins over the panel: a mapping
 * is a deliberate per-wire authoring act, the panel value is the default.
 *
 * <p>The message {@code name} is NOT runtime-overridable: it is baked into the deployed BPMN
 * {@code messageEventDefinition messageRef} and the subscription is armed under that static name
 * before any mapped data could arrive.
 *
 * <p><b>REF-only.</b> The pinned value is a non-PHI routing token (same class as a businessKey);
 * the variable name is whitelisted via {@link EngineVariableNames#MSG_CORR_PREFIX}, so the pin is
 * legal in {@code ACT_RU_VARIABLE}/{@code ACT_HI_VARINST} at {@code historyLevel=FULL} and is
 * durable by construction: it rides the same runtime tables as the parked subscription, so it
 * survives a daemon restart exactly like the subscription does.
 *
 * <p><b>Failure isolation.</b> Any failure (model unreadable, config unreadable, store error) is
 * caught and logged — the token's arrival at the catch is never rolled back; without a pin the
 * catch simply correlates by businessKey only, exactly as before.
 */
@Slf4j
@Component("messageReceiverCorrelationListener")
public class MessageReceiverCorrelationListener implements ExecutionListener {

    /** Activity id prefix the strategies mint: designer id {@code X} → activity id {@code node_X}. */
    private static final String NODE_ID_PREFIX = "node_";
    /** Activity id suffix of the synthesized event-based gateway in the timeout shape. */
    private static final String GATEWAY_ID_SUFFIX = "_ebg";
    /** Receiver INPUT port value whose mapped rows may carry a runtime correlation-key override. */
    private static final String INPUT_SUBSCRIPTION_PORT = "subscription";
    /** Field inside the mapped {@code subscription} payload carrying the correlation key. */
    private static final String FIELD_CORRELATION_KEY = "correlationKey";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<ExecutionContextStore> executionContextStoreProvider;

    public MessageReceiverCorrelationListener(ObjectProvider<SqsEngine> sqsEngineProvider,
                                              ObjectProvider<ExecutionContextStore> executionContextStoreProvider) {
        this.sqsEngineProvider = sqsEngineProvider;
        this.executionContextStoreProvider = executionContextStoreProvider;
    }

    @Override
    public void notify(DelegateExecution execution) {
        SqsEngine engine = sqsEngineProvider.getIfAvailable();
        if (engine == null) {
            return;
        }
        String activityId = execution.getCurrentActivityId();
        try {
            BpmnModel bpmnModel = engine.readDeployedModel(execution.getProcessDefinitionId());
            if (bpmnModel == null) {
                return;
            }
            FlowElement fe = bpmnModel.getFlowElement(activityId);
            Event configSource;
            String receiverNodeId;
            if (fe instanceof Event event) {
                configSource = event;
                receiverNodeId = stripNodePrefix(activityId);
            } else if (fe instanceof EventGateway gateway) {
                // Timeout shape (audit M1): the token parks AT the event-based gateway and the
                // message subscription rides the gateway execution, so the pin must be written
                // here — the downstream catch's own start listener only fires after the message
                // has already arrived. The message name and receiver config live on the
                // gateway's downstream message catch.
                configSource = findMessageCatchDownstream(bpmnModel, gateway);
                receiverNodeId = StringUtils.removeEnd(stripNodePrefix(activityId), GATEWAY_ID_SUFFIX);
                if (configSource == null) {
                    return;
                }
            } else if (fe instanceof Activity host) {
                // MESSAGE-boundary host (user task / call activity / sandbox wait state): the
                // token parks at the HOST and every boundary message subscription rides this
                // execution, so each keyed message boundary's pin is written here. A listener
                // on the boundary element itself would only fire when the boundary triggers —
                // Activiti activates boundaries through their event handler, not the ordinary
                // activity-start operation.
                pinBoundaryCorrelationKeys(bpmnModel, execution, host);
                return;
            } else {
                return;
            }
            String messageName = resolveMessageName(bpmnModel, configSource);
            if (StringUtils.isBlank(messageName)) {
                return;
            }

            ReceiverConfig cfg = readConfig(configSource);
            String runtimeOverride = readRuntimeCorrelationKey(execution, receiverNodeId);
            String effectiveKey = resolveCorrelationKey(cfg, execution, runtimeOverride);
            if (StringUtils.isBlank(effectiveKey)) {
                // Nothing authored/overridden → no pin; businessKey correlation applies as before.
                return;
            }
            String varName = EngineVariableNames.messageCorrelationVariableName(messageName);
            execution.setVariableLocal(varName, effectiveKey);
            log.info("messageReceiverCorrelationListener.pinned activity={} messageName={} var={}",
                    activityId, messageName, varName);
        } catch (Exception ex) {
            log.warn("messageReceiverCorrelationListener: unexpected failure for activity={}: {}",
                    activityId, ex.getMessage());
        }
    }

    // ---------------------------------------------------------------------
    // Deploy-time defaults (BPMN model).
    // ---------------------------------------------------------------------

    private static String resolveMessageName(BpmnModel model, Event event) {
        return event.getEventDefinitions().stream()
                .filter(ed -> ed instanceof MessageEventDefinition)
                .map(ed -> {
                    MessageEventDefinition med = (MessageEventDefinition) ed;
                    String ref = med.getMessageRef();
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

    /**
     * The message-bearing catch directly downstream of a timeout-shape event gateway — the
     * element carrying the {@code messageEventDefinition} and the {@code sq:receiverConfig}
     * the gateway itself does not have.
     */
    private static IntermediateCatchEvent findMessageCatchDownstream(BpmnModel model,
                                                                     EventGateway gateway) {
        if (gateway.getOutgoingFlows() == null) {
            return null;
        }
        for (SequenceFlow flow : gateway.getOutgoingFlows()) {
            FlowElement target = model.getFlowElement(flow.getTargetRef());
            if (target instanceof IntermediateCatchEvent catchEvent
                    && catchEvent.getEventDefinitions().stream()
                            .anyMatch(ed -> ed instanceof MessageEventDefinition)) {
                return catchEvent;
            }
        }
        return null;
    }

    /**
     * Boundary-host branch: pins every MESSAGE boundary of {@code host} that carries an
     * authored {@code correlationKey} ({@code sq:receiverConfig}, emitted by
     * {@code ActivitiBoundaryEventHelper.attachMessage}). The pin lands execution-locally on
     * the host execution; Activiti parks the boundary's message subscription on a CHILD
     * execution of the host, and the correlation fallback re-checks candidates through the
     * execution scope chain, so the host-scope pin is visible to it (see
     * {@link SqsEngine#findMessageEventExecutionByCorrelationKey(String, String)}).
     *
     * <p>Unlike the catch-node branch there is no runtime INPUT-port override lane: the
     * {@code subscription} port convention belongs to the catch node's template, a boundary
     * host (user task / call activity) has no such port. The authored key (literal or
     * {@code ${varName}}) is the only source.
     */
    private void pinBoundaryCorrelationKeys(BpmnModel model, DelegateExecution execution,
                                            Activity host) {
        Process process = findProcess(model, host.getId());
        if (process == null) {
            return;
        }
        for (BoundaryEvent boundary : process.findFlowElementsOfType(BoundaryEvent.class, true)) {
            if (!host.getId().equals(boundary.getAttachedToRefId())) {
                continue;
            }
            String messageName = resolveMessageName(model, boundary);
            if (StringUtils.isBlank(messageName)) {
                continue;
            }
            ReceiverConfig cfg = readConfig(boundary);
            String effectiveKey = resolveCorrelationKey(cfg, execution, null);
            if (StringUtils.isBlank(effectiveKey)) {
                continue;
            }
            String varName = EngineVariableNames.messageCorrelationVariableName(messageName);
            execution.setVariableLocal(varName, effectiveKey);
            log.info("messageReceiverCorrelationListener.pinned activity={} boundary={} messageName={} var={}",
                    host.getId(), boundary.getId(), messageName, varName);
        }
    }

    /** The process whose (recursive) flow-element tree contains {@code activityId}. */
    private static Process findProcess(BpmnModel model, String activityId) {
        for (Process process : model.getProcesses()) {
            if (process.getFlowElement(activityId, true) != null) {
                return process;
            }
        }
        return null;
    }

    private static ReceiverConfig readConfig(FlowElement fe) {
        ReceiverConfig cfg = new ReceiverConfig();
        Map<String, List<ExtensionElement>> extensions = fe.getExtensionElements();
        if (extensions == null) {
            return cfg;
        }
        List<ExtensionElement> elements =
                extensions.get(ActivitiMessageReceiverConfigurator.RECEIVER_CONFIG_ELEMENT);
        if (elements == null || elements.isEmpty()) {
            return cfg;
        }
        ExtensionElement config = elements.get(0);
        cfg.correlationKey = getAttr(config, ActivitiMessageReceiverConfigurator.ATTR_CORRELATION_KEY);
        cfg.businessKeyExpression = getAttr(config,
                ActivitiMessageReceiverConfigurator.ATTR_BUSINESS_KEY_EXPRESSION);
        return cfg;
    }

    private static String getAttr(ExtensionElement el, String name) {
        Map<String, List<ExtensionAttribute>> attrs = el.getAttributes();
        if (attrs == null) {
            return null;
        }
        List<ExtensionAttribute> vals = attrs.get(name);
        if (vals == null || vals.isEmpty()) {
            return null;
        }
        return StringUtils.trimToNull(vals.get(0).getValue());
    }

    /**
     * Resolves the effective correlation key. Precedence mirrors the sender side: runtime
     * INPUT-port override → literal {@code correlationKey} config → {@code businessKeyExpression}
     * minimal {@code ${varName}} lookup. Blank overall ⇒ {@code null} (no pin).
     */
    private static String resolveCorrelationKey(ReceiverConfig cfg, DelegateExecution execution,
                                                 String runtimeOverride) {
        if (StringUtils.isNotBlank(runtimeOverride)) {
            return runtimeOverride;
        }
        if (StringUtils.isNotBlank(cfg.correlationKey)) {
            return evaluateJuel(cfg.correlationKey, execution);
        }
        if (StringUtils.isNotBlank(cfg.businessKeyExpression)) {
            return evaluateJuel(cfg.businessKeyExpression, execution);
        }
        return null;
    }

    /**
     * Minimal {@code ${varName}} JUEL evaluation — reads a single variable from the receiver
     * execution scope. Anything else is returned verbatim (treated as a literal). Blank/null
     * variables collapse to {@code null} so the caller can fall through.
     */
    private static String evaluateJuel(String expr, DelegateExecution execution) {
        String trimmed = expr.trim();
        if (trimmed.startsWith("${") && trimmed.endsWith("}") && trimmed.length() > 3) {
            String varName = trimmed.substring(2, trimmed.length() - 1).trim();
            Object value = execution.getVariable(varName);
            return value == null ? null : value.toString();
        }
        return trimmed;
    }

    // ---------------------------------------------------------------------
    // Runtime override — a DuckTape-mapped row on this node's INPUT `subscription` port whose
    // payload carries a `correlationKey` field. Rows arrive in the pipeline's wire shape:
    // targetPortValue = the port's value ("subscription"), value = the mapped payload object
    // (same loadInputsTargetingNode seam the sender listener uses).
    // ---------------------------------------------------------------------

    private String readRuntimeCorrelationKey(DelegateExecution execution, String receiverNodeId) {
        ExecutionContextStore contextStore = executionContextStoreProvider.getIfAvailable();
        if (contextStore == null) {
            return null;
        }
        // The store is keyed by workflowExecutionId == businessKey (StartRunCommandHandler), so a
        // run started without a seedContext (no pinned studioquackRef) can still look its mapped
        // inputs up by businessKey — the override lane must not silently die on REF-less runs.
        ProcessContextRef lookup = new ProcessContextRef();
        Object refVar = execution.getVariable(EngineVariableNames.STUDIOQUACK_REF);
        if (refVar instanceof ProcessContextRef ref && StringUtils.isNotBlank(ref.getExecutionId())) {
            lookup.setExecutionId(ref.getExecutionId());
        } else {
            lookup.setExecutionId(execution.getProcessInstanceBusinessKey());
        }
        List<InOutValueDto> inputs;
        try {
            inputs = contextStore.loadInputsTargetingNode(lookup, receiverNodeId);
        } catch (Exception ex) {
            log.warn("messageReceiverCorrelationListener: failed to read runtime inputs for {}: {}",
                    receiverNodeId, ex.getMessage());
            return null;
        }
        if (inputs == null || inputs.isEmpty()) {
            return null;
        }
        for (InOutValueDto in : inputs) {
            if (!INPUT_SUBSCRIPTION_PORT.equalsIgnoreCase(StringUtils.trimToEmpty(in.getTargetPortValue()))) {
                continue;
            }
            if (in.getValue() instanceof Map<?, ?> map) {
                Object v = map.get(FIELD_CORRELATION_KEY);
                if (v != null && StringUtils.isNotBlank(v.toString())) {
                    return v.toString();
                }
            }
        }
        return null;
    }

    // ---------------------------------------------------------------------
    // Helpers.
    // ---------------------------------------------------------------------

    /** Reverses the {@code node_<id>} activity-id convention the strategies mint at deploy time. */
    private static String stripNodePrefix(String activityId) {
        if (activityId != null && activityId.startsWith(NODE_ID_PREFIX)) {
            return activityId.substring(NODE_ID_PREFIX.length());
        }
        return activityId;
    }

    /** Mutable holder for the extension-element-read receiver config. */
    private static final class ReceiverConfig {
        String correlationKey;
        String businessKeyExpression;
    }
}
