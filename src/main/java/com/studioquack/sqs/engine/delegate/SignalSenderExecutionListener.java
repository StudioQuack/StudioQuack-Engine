package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Event;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.Signal;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.activiti.engine.runtime.Execution;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runtime bridge for {@code signal.intermediate.throw.event} and {@code signal.end.event}
 * (audit item 8). Attached (event={@code start}) by the signal throw/end strategies; fires as
 * the token reaches the event, BEFORE Activiti's native signal broadcast. Reads the authored
 * IN.{@code signal} {@code payload} rows targeting this node and pre-seals them into every
 * parked signal-catch subscriber's execution-context store — REF-only, through the same
 * {@code sealMsg} mechanism the external webhook ingress uses — so when the native broadcast
 * wakes a catcher, its payload rows are already in place under its own businessKey.
 *
 * <p><b>The wake is native for throws, listener-driven for ends.</b> An intermediate throw's
 * {@link SignalEventDefinition} broadcast reaches catch subscribers, boundary subscribers and
 * signal-start events. Activiti 9 has NO signal end-event behavior — the parser maps a signal
 * end event to {@code NoneEndEventActivityBehavior}, leaving the definition decorative — so
 * for {@code signal.end.event} this listener performs the broadcast itself after sealing.
 * Two honest limits, by construction:
 * <ul>
 *   <li><b>Signal-start subscribers receive no payload</b> — the instance does not exist at
 *       throw time, so there is no businessKey to seal under.</li>
 *   <li><b>The signal name is always the configured one</b> — a runtime name override cannot
 *       steer the native broadcast, so the templates declare no such input field.</li>
 * </ul>
 *
 * <p><b>Failure isolation.</b> Any failure (model unreadable, store error) is caught and
 * logged — the sender's transaction is never rolled back by a sealing mishap; the native
 * broadcast still fires (BPMN throw semantics: the caller's flow continues regardless).
 */
@Slf4j
@Component("signalSenderExecutionListener")
public class SignalSenderExecutionListener implements ExecutionListener {

    /** Activity id prefix the strategies mint: designer id {@code X} → activity id {@code node_X}. */
    private static final String NODE_ID_PREFIX = "node_";
    /** Sender INPUT port value whose mapped rows carry the runtime payload. */
    private static final String INPUT_SIGNAL_PORT = "signal";
    /** Field inside the mapped {@code signal} payload object carrying the broadcast payload. */
    private static final String FIELD_PAYLOAD = "payload";
    /** Sender OUTPUT port field: broadcast acknowledgement flag. */
    private static final String OUTPUT_SUCCESS_PORT = "success";
    /** Sender OUTPUT port field: ISO-8601 UTC instant when the throw completed. */
    private static final String OUTPUT_TIMESTAMP_PORT = "timestamp";

    private static final String DELEGATE_EXPRESSION = "${signalSenderExecutionListener}";
    private static final String LISTENER_EVENT_START = "start";

    private final ObjectProvider<MessageCorrelationService> correlationServiceProvider;
    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<ExecutionContextStore> executionContextStoreProvider;

    public SignalSenderExecutionListener(
            ObjectProvider<MessageCorrelationService> correlationServiceProvider,
            ObjectProvider<SqsEngine> sqsEngineProvider,
            ObjectProvider<ExecutionContextStore> executionContextStoreProvider) {
        this.correlationServiceProvider = correlationServiceProvider;
        this.sqsEngineProvider = sqsEngineProvider;
        this.executionContextStoreProvider = executionContextStoreProvider;
    }

    /** Attaches this bridge (event={@code start}) to a signal throw/end event at deploy time. */
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

    @Override
    public void notify(DelegateExecution execution) {
        MessageCorrelationService correlationService = correlationServiceProvider.getIfAvailable();
        SqsEngine engine = sqsEngineProvider.getIfAvailable();
        ExecutionContextStore contextStore = executionContextStoreProvider.getIfAvailable();
        if (correlationService == null || engine == null || contextStore == null) {
            log.warn("signalSenderExecutionListener: services not available, skipping payload seal");
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
            String signalName = resolveSignalName(bpmnModel, event);
            if (StringUtils.isBlank(signalName)) {
                log.info("signalSenderExecutionListener: no signal name on activity={}", activityId);
                writeEmitResult(event, senderNodeId, businessKey, false, contextStore);
                return;
            }

            Map<String, Object> payload = readRuntimePayload(execution, senderNodeId, contextStore);
            if (!payload.isEmpty()) {
                List<Execution> subscribers = engine.findSignalEventExecutions(signalName);
                int sealed = 0;
                for (Execution subscriber : subscribers) {
                    String subscriberBusinessKey = engine.findBusinessKey(subscriber.getProcessInstanceId());
                    String subscriberNodeId = stripNodePrefix(subscriber.getActivityId());
                    if (StringUtils.isBlank(subscriberBusinessKey) || StringUtils.isBlank(subscriberNodeId)) {
                        continue;
                    }
                    correlationService.sealBroadcastPayload(subscriberBusinessKey, subscriberNodeId,
                            subscriber.getProcessInstanceId(), signalName, payload);
                    sealed++;
                }
                log.info("signalSenderExecutionListener.sealed activity={} signalName={} subscribers={} fields={}",
                        activityId, signalName, sealed, payload.size());
            }
            if (event instanceof EndEvent) {
                // Activiti 9 maps a signal end event to NoneEndEventActivityBehavior (the
                // SignalEventDefinition is decorative there), so the broadcast is this
                // listener's job. Intermediate throws broadcast natively — do NOT double-fire.
                engine.signalEventReceived(signalName, (Map<String, Object>) null);
            }
            writeEmitResult(event, senderNodeId, businessKey, true, contextStore);
        } catch (Exception ex) {
            log.warn("signalSenderExecutionListener: unexpected failure for activity={}: {}",
                    activityId, ex.getMessage());
        }
    }

    // ---------------------------------------------------------------------
    // Deploy-time default (BPMN model) + runtime payload (IN.signal rows).
    // ---------------------------------------------------------------------

    private static String resolveSignalName(BpmnModel model, Event event) {
        return event.getEventDefinitions().stream()
                .filter(ed -> ed instanceof SignalEventDefinition)
                .map(ed -> {
                    SignalEventDefinition sed = (SignalEventDefinition) ed;
                    String ref = sed.getSignalRef();
                    if (StringUtils.isBlank(ref)) {
                        return null;
                    }
                    Signal signal = model.getSignal(ref);
                    return signal != null && StringUtils.isNotBlank(signal.getName())
                            ? signal.getName() : ref;
                })
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
    }

    private static Map<String, Object> readRuntimePayload(DelegateExecution execution,
                                                          String senderNodeId,
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
            return Map.of();
        }
        List<InOutValueDto> inputs;
        try {
            inputs = contextStore.loadInputsTargetingNode(lookup, senderNodeId);
        } catch (Exception ex) {
            log.warn("signalSenderExecutionListener: failed to read runtime inputs for {}: {}",
                    senderNodeId, ex.getMessage());
            return Map.of();
        }
        if (inputs == null || inputs.isEmpty()) {
            return Map.of();
        }
        for (InOutValueDto in : inputs) {
            if (!INPUT_SIGNAL_PORT.equalsIgnoreCase(StringUtils.trimToEmpty(in.getTargetPortValue()))) {
                continue;
            }
            if (in.getValue() instanceof Map<?, ?> asMap
                    && asMap.get(FIELD_PAYLOAD) instanceof Map<?, ?> payloadMap && !payloadMap.isEmpty()) {
                Map<String, Object> coerced = new LinkedHashMap<>(payloadMap.size());
                for (Map.Entry<?, ?> entry : payloadMap.entrySet()) {
                    if (entry.getKey() != null) {
                        coerced.put(entry.getKey().toString(), entry.getValue());
                    }
                }
                return coerced;
            }
        }
        return Map.of();
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
            // Signal end event: no downstream to inform; the template declares no OUTPUT port.
            return;
        }
        List<String> downstreamIds = outgoing.stream()
                .map(SequenceFlow::getTargetRef)
                .filter(StringUtils::isNotBlank)
                .map(SignalSenderExecutionListener::stripNodePrefix)
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
            log.warn("signalSenderExecutionListener: failed to snapshot emit_result context for {}: {}",
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
            log.warn("signalSenderExecutionListener: failed to save emit_result outputs for {}: {}",
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
