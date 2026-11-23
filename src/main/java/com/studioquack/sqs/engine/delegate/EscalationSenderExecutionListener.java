package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageSenderConfigurator;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Event;
import org.activiti.bpmn.model.ExtensionAttribute;
import org.activiti.bpmn.model.ExtensionElement;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.activiti.engine.runtime.Execution;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runtime bridge for {@code escalation.intermediate.throw.event} and
 * {@code escalation.end.event}. Activiti 9.0.0 has NO escalation support — no
 * {@code EscalationEventDefinition} model class, no engine behavior — so this listener IS the
 * escalation semantics, attached (event={@code start}) by the escalation strategies.
 *
 * <p><b>The bridge.</b> An escalation boundary event is emitted as a signal boundary subscribed
 * to the reserved name {@code __sq_esc_<code>} (see
 * {@link EngineVariableNames#ESCALATION_SIGNAL_PREFIX}). On throw/end this listener resolves the
 * escalation code — a runtime IN {@code code} row wins over the configured
 * {@code code} — and fires that reserved signal at every matching subscription
 * <b>of the same process instance</b>, one {@code signalEventReceived(name, executionId)} per
 * parked execution. BPMN escalation is scope-propagation, not broadcast; with the designer's
 * flat (subprocess-free) graphs the instance IS the scope, so instance-scoped delivery is the
 * faithful mapping. Interrupting vs non-interrupting is the boundary's own
 * {@code cancelActivity} flag — native.
 *
 * <p><b>No business payload crosses.</b> The fired boundary's handler receives the boundary
 * metadata payload published by {@link SqBoundaryEventDelegate} (eventId / firedAt /
 * escalationCode), exactly like a signal boundary. The throw template therefore declares no IN
 * {@code payload} field — a field the runtime cannot deliver is not promised.
 *
 * <p><b>Failure isolation.</b> Any failure (model unreadable, store error) is caught and
 * logged — the throwing branch is never rolled back by an escalation mishap (BPMN throw
 * semantics: the caller's flow continues regardless).
 */
@Slf4j
@Component("escalationSenderExecutionListener")
public class EscalationSenderExecutionListener implements ExecutionListener {

    /** Activity id prefix the strategies mint: designer id {@code X} → activity id {@code node_X}. */
    private static final String NODE_ID_PREFIX = "node_";
    /** Sender INPUT port value whose mapped rows may carry a runtime code override. */
    private static final String INPUT_ESCALATION_PORT = "escalation";
    /** Field inside the mapped {@code escalation} payload object carrying the code override. */
    private static final String FIELD_CODE = "code";
    /** Sender OUTPUT port field: escalation acknowledgement flag. */
    private static final String OUTPUT_SUCCESS_PORT = "success";
    /** Sender OUTPUT port field: ISO-8601 UTC instant when the throw completed. */
    private static final String OUTPUT_TIMESTAMP_PORT = "timestamp";

    /** Extension element carrying the deploy-time escalation config on the event. */
    public static final String ESCALATION_CONFIG_ELEMENT = "escalationConfig";
    /** Attribute of {@link #ESCALATION_CONFIG_ELEMENT}: the configured escalation code. */
    public static final String ATTR_ESCALATION_CODE = "escalationCode";

    private static final String DELEGATE_EXPRESSION = "${escalationSenderExecutionListener}";
    private static final String LISTENER_EVENT_START = "start";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<ExecutionContextStore> executionContextStoreProvider;

    public EscalationSenderExecutionListener(
            ObjectProvider<SqsEngine> sqsEngineProvider,
            ObjectProvider<ExecutionContextStore> executionContextStoreProvider) {
        this.sqsEngineProvider = sqsEngineProvider;
        this.executionContextStoreProvider = executionContextStoreProvider;
    }

    /** Attaches this bridge (event={@code start}) to an escalation throw/end event at deploy time. */
    public static void attachTo(Event event) {
        ActivitiListener listener = new ActivitiListener();
        listener.setEvent(LISTENER_EVENT_START);
        listener.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation(DELEGATE_EXPRESSION);
        List<ActivitiListener> listeners = new ArrayList<>();
        if (event.getExecutionListeners() != null) {
            listeners.addAll(event.getExecutionListeners());
        }
        listeners.add(listener);
        event.setExecutionListeners(listeners);
    }

    /**
     * Writes the deploy-time escalation config onto the event: the configured {@code code}
     * (required by the templates, so a blank value fails the deploy through the
     * required-parameter gate before this is ever read).
     */
    public static void writeConfig(Event event, NodeDto node) {
        String code = readParam(node, "code");
        if (StringUtils.isBlank(code)) {
            return;
        }
        ExtensionElement config = new ExtensionElement();
        config.setName(ESCALATION_CONFIG_ELEMENT);
        config.setNamespace(ActivitiMessageSenderConfigurator.NAMESPACE);
        config.setNamespacePrefix(ActivitiMessageSenderConfigurator.NAMESPACE_PREFIX);
        ExtensionAttribute attr = new ExtensionAttribute(ATTR_ESCALATION_CODE);
        attr.setValue(code);
        config.addAttribute(attr);
        event.addExtensionElement(config);
    }

    private static String readParam(NodeDto node, String shortCode) {
        return node == null || node.getData() == null ? null
                : StringUtils.trimToNull(node.getData().findParameterValueByShortCode(shortCode));
    }

    @Override
    public void notify(DelegateExecution execution) {
        SqsEngine engine = sqsEngineProvider.getIfAvailable();
        ExecutionContextStore contextStore = executionContextStoreProvider.getIfAvailable();
        if (engine == null || contextStore == null) {
            log.warn("escalationSenderExecutionListener: services not available, escalation not raised");
            return;
        }

        String activityId = execution.getCurrentActivityId();
        String senderNodeId = stripNodePrefix(activityId);
        String businessKey = execution.getProcessInstanceBusinessKey();
        try {
            BpmnModel bpmnModel = engine.readDeployedModel(execution.getProcessDefinitionId());
            if (bpmnModel == null) {
                return;
            }
            FlowElement fe = bpmnModel.getFlowElement(activityId);
            if (!(fe instanceof Event event)) {
                return;
            }
            String code = resolveCode(execution, fe, senderNodeId, contextStore);
            if (StringUtils.isBlank(code)) {
                log.info("escalationSenderExecutionListener: no escalation code on activity={}", activityId);
                writeEmitResult(event, senderNodeId, businessKey, false, contextStore);
                return;
            }

            String reservedSignal = EngineVariableNames.ESCALATION_SIGNAL_PREFIX + code;
            List<Execution> subscribers = engine.findSignalEventExecutions(reservedSignal).stream()
                    .filter(candidate -> execution.getProcessInstanceId().equals(candidate.getProcessInstanceId()))
                    .collect(Collectors.toList());
            int fired = 0;
            for (Execution subscriber : subscribers) {
                engine.signalEventReceived(reservedSignal, subscriber.getId());
                fired++;
            }
            log.info("escalationSenderExecutionListener.raised activity={} code={} boundariesFired={}",
                    activityId, code, fired);
            writeEmitResult(event, senderNodeId, businessKey, true, contextStore);
        } catch (Exception ex) {
            log.warn("escalationSenderExecutionListener: unexpected failure for activity={}: {}",
                    activityId, ex.getMessage());
        }
    }

    // ---------------------------------------------------------------------
    // Code resolution: runtime IN.code row wins, then the configured code.
    // ---------------------------------------------------------------------

    private static String resolveCode(DelegateExecution execution, FlowElement fe, String senderNodeId,
                                      ExecutionContextStore contextStore) {
        String runtimeCode = readRuntimeCode(execution, senderNodeId, contextStore);
        return StringUtils.isNotBlank(runtimeCode) ? runtimeCode : readConfiguredCode(fe);
    }

    private static String readRuntimeCode(DelegateExecution execution, String senderNodeId,
                                          ExecutionContextStore contextStore) {
        // The store is keyed by workflowExecutionId == businessKey (StartRunCommandHandler), so a
        // run started without a seedContext (no pinned studioquackRef) still finds its mapped
        // inputs by businessKey — the DuckTape lane must not silently die on REF-less runs.
        ProcessContextRef lookup = new ProcessContextRef();
        Object refVar = execution.getVariable(EngineVariableNames.STUDIOQUACK_REF);
        if (refVar instanceof ProcessContextRef ref && StringUtils.isNotBlank(ref.getExecutionId())) {
            lookup.setExecutionId(ref.getExecutionId());
        } else {
            lookup.setExecutionId(execution.getProcessInstanceBusinessKey());
        }
        if (StringUtils.isBlank(lookup.getExecutionId())) {
            return null;
        }
        List<InOutValueDto> inputs;
        try {
            inputs = contextStore.loadInputsTargetingNode(lookup, senderNodeId);
        } catch (Exception ex) {
            log.warn("escalationSenderExecutionListener: failed to read runtime inputs for {}: {}",
                    senderNodeId, ex.getMessage());
            return null;
        }
        if (inputs == null) {
            return null;
        }
        for (InOutValueDto in : inputs) {
            if (!INPUT_ESCALATION_PORT.equalsIgnoreCase(StringUtils.trimToEmpty(in.getTargetPortValue()))) {
                continue;
            }
            if (in.getValue() instanceof Map<?, ?> asMap) {
                Object code = asMap.get(FIELD_CODE);
                String value = code == null ? null : StringUtils.trimToNull(String.valueOf(code));
                if (value != null) {
                    return value;
                }
            }
        }
        return null;
    }

    private static String readConfiguredCode(FlowElement fe) {
        Map<String, List<ExtensionElement>> extensions = fe.getExtensionElements();
        if (extensions == null) {
            return null;
        }
        List<ExtensionElement> elements = extensions.get(ESCALATION_CONFIG_ELEMENT);
        if (elements == null || elements.isEmpty()) {
            return null;
        }
        Map<String, List<ExtensionAttribute>> attrs = elements.get(0).getAttributes();
        if (attrs == null) {
            return null;
        }
        List<ExtensionAttribute> values = attrs.get(ATTR_ESCALATION_CODE);
        return values == null || values.isEmpty() ? null
                : StringUtils.trimToNull(values.get(0).getValue());
    }

    // ---------------------------------------------------------------------
    // Emit result — write (success, timestamp) rows for every downstream node.
    // ---------------------------------------------------------------------

    private static void writeEmitResult(Event event, String senderNodeId, String businessKey,
                                        boolean success, ExecutionContextStore contextStore) {
        if (!(event instanceof FlowNode flowNode)) {
            return;
        }
        List<SequenceFlow> outgoing = flowNode.getOutgoingFlows();
        if (outgoing == null || outgoing.isEmpty()) {
            // Escalation end event: no downstream to inform; the template declares no OUTPUT port.
            return;
        }
        List<String> downstreamIds = outgoing.stream()
                .map(SequenceFlow::getTargetRef)
                .filter(StringUtils::isNotBlank)
                .map(EscalationSenderExecutionListener::stripNodePrefix)
                .distinct()
                .collect(Collectors.toList());
        if (downstreamIds.isEmpty() || StringUtils.isBlank(businessKey)) {
            return;
        }

        ProcessContextRef writeRef;
        try {
            ExecutionContext senderCtx = new ExecutionContext();
            senderCtx.setWorkflowExecutionId(businessKey);
            senderCtx.setCurrentNodeId(senderNodeId);
            writeRef = contextStore.snapshot(senderCtx, null);
        } catch (Exception ex) {
            log.warn("escalationSenderExecutionListener: failed to snapshot emit_result context for {}: {}",
                    senderNodeId, ex.getMessage());
            return;
        }

        String isoTimestamp = OffsetDateTime.now(ZoneOffset.UTC).toString();
        List<InOutValueDto> outputs = new ArrayList<>(downstreamIds.size() * 2);
        for (String downstreamId : downstreamIds) {
            outputs.add(makeOutputRow(senderNodeId, downstreamId, OUTPUT_SUCCESS_PORT, success));
            outputs.add(makeOutputRow(senderNodeId, downstreamId, OUTPUT_TIMESTAMP_PORT, isoTimestamp));
        }
        try {
            contextStore.saveOutputs(writeRef, outputs);
        } catch (Exception ex) {
            log.warn("escalationSenderExecutionListener: failed to save emit_result outputs for {}: {}",
                    senderNodeId, ex.getMessage());
        }
    }

    private static InOutValueDto makeOutputRow(String fromNodeId, String toNodeId,
                                               String portShortCode, Object value) {
        InOutValueDto dto = new InOutValueDto();
        dto.setNodeId(fromNodeId);
        dto.setPortId(portShortCode);
        dto.setPortValue(portShortCode);
        dto.setTargetNodeId(toNodeId);
        dto.setTargetPortId(portShortCode);
        dto.setTargetPortValue(portShortCode);
        dto.setValue(value);
        return dto;
    }

    /** Reverses the {@code node_<id>} activity-id convention the strategies mint at deploy time. */
    private static String stripNodePrefix(String activityId) {
        if (activityId != null && activityId.startsWith(NODE_ID_PREFIX)) {
            return activityId.substring(NODE_ID_PREFIX.length());
        }
        return activityId;
    }
}
