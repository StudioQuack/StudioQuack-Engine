package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageReceiverConfigurator;
import com.studioquack.sqs.engine.strategy.helper.ActivitiSignalMessageRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.ErrorEventDefinition;
import org.activiti.bpmn.model.EventGateway;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code message.intermediate.catch.event} → Activiti {@link IntermediateCatchEvent} +
 * {@link MessageEventDefinition}, plus the {@code ${messageReceiverCorrelationListener}} runtime
 * bridge that pins the authored/runtime {@code correlationKey} as an execution-local REF-only
 * variable at park time — making the catch correlatable by the SHARED authored key (a cross-flow
 * sender cannot know the receiver run's businessKey). Correlation itself is resolved in Java
 * ({@code MessageCorrelationService}: query the waiting execution by businessKey, falling back to
 * the pinned correlation-key variable, then {@code messageEventReceived}).
 *
 * <p><b>Timeout synthesis ({@code timeoutMs} &gt; 0).</b> A BPMN intermediate catch cannot carry a
 * boundary timer, so the node expands to the BPMN-native event-choice shape:
 * {@code EventGateway → [message catch, timer catch]}. Inbound edges land on the gateway;
 * the engine parks a token on BOTH arms and the first event to fire wins (the loser's
 * subscription is deleted by the engine). Edges leaving the node's {@code message} port are wired
 * from the message catch (the default exit); edges leaving the {@code timeout} port are wired
 * from the timer catch via {@link GeneratedNode#exitElementIdByPortValue()}.
 * {@code onTimeoutBehavior}:
 * <ul>
 *   <li>{@code route} (default) — the token continues down the authored Timeout-port edge.</li>
 *   <li>{@code cancel} — the timer catch flows into a plain (none) end event: the token is
 *       absorbed without failing the run. Timeout-port edges are swallowed (the template only
 *       emits that port for {@code route}).</li>
 *   <li>{@code error} — the timer catch flows into an error end event with
 *       {@code errorCode="timeout.expired"}, so an error boundary / event sub-process can catch
 *       it; uncaught it fails the run loudly.</li>
 * </ul>
 */
@Slf4j
@Order(1)
@Component("activitiMessageCatchStrategy")
@RequiredArgsConstructor
public class ActivitiMessageCatchStrategy implements ActivitiFlowNodeStrategy {

    /** Source-port value carrying the timeout branch (FE template OUT port {@code Timeout}). */
    static final String TIMEOUT_PORT_VALUE = "timeout";
    /** BPMN error code raised on the timeout branch when {@code onTimeoutBehavior=error}. */
    static final String TIMEOUT_ERROR_CODE = "timeout.expired";

    private final ActivitiSignalMessageRegistry registry;
    private final ActivitiMessageReceiverConfigurator receiverConfigurator;

    @Override
    public boolean supports(String bpmnType) {
        return "message.intermediate.catch.event".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String messageName = node.getData().findParameterValueByShortCode("name");
        return createWithMessageName(model, process, node, nodeLabel, "node_" + node.getId(), messageName);
    }

    /**
     * Emits an {@link IntermediateCatchEvent} (plus optional {@code EventGateway → [catch, timer]}
     * timeout synthesis when {@code timeoutMs > 0}) bound to {@code messageName} under
     * {@code catchElementId}. Shared by standard {@code message.intermediate.catch.event} and
     * parameter-driven {@code MESSAGE_CATCH} / {@code SEND_AND_WAIT} nodes.
     */
    public GeneratedNode createWithMessageName(BpmnModel model, Process process, NodeDto node,
                                               String nodeLabel, String catchElementId, String messageName) {
        String messageId = registry.getOrCreateMessage(model, messageName);
        IntermediateCatchEvent catchEvent = new IntermediateCatchEvent();
        catchEvent.setId(catchElementId);
        catchEvent.setName(nodeLabel);
        MessageEventDefinition def = new MessageEventDefinition();
        def.setMessageRef(messageId);
        catchEvent.addEventDefinition(def);

        // Emit the receiver config (correlationKey / businessKeyExpression / timeoutMs /
        // onTimeoutBehavior) as an sq:receiverConfig extension element — the runtime-readable
        // record of the authored config on the deployed model.
        receiverConfigurator.configureReceiver(catchEvent, node);
        // Attach the correlation-pin bridge: at park time it pins the resolved correlation key
        // execution-locally so senders can correlate by the shared authored key.
        receiverConfigurator.attachCorrelationPinListener(catchEvent);

        process.addFlowElement(catchEvent);

        long timeoutMs = parseTimeoutMs(node.getData().findParameterValueByShortCode("timeoutMs"),
                node.getId());
        if (timeoutMs <= 0) {
            return GeneratedNode.single(catchEvent.getId());
        }
        return withTimeoutBranch(model, process, node, nodeLabel, catchEvent, timeoutMs);
    }

    /**
     * Expands the catch into {@code EventGateway → [message catch, timer catch]} and returns
     * wiring handles whose entry is the gateway, whose default exit is the message catch, and
     * whose {@code timeout} port exit follows {@code onTimeoutBehavior}.
     */
    private GeneratedNode withTimeoutBranch(BpmnModel model, Process process, NodeDto node,
                                            String nodeLabel, IntermediateCatchEvent catchEvent,
                                            long timeoutMs) {
        String nodeId = node.getId();
        String behavior = StringUtils.trimToEmpty(
                node.getData().findParameterValueByShortCode("onTimeoutBehavior"))
                .toLowerCase(Locale.ROOT);
        if (behavior.isEmpty()) {
            behavior = "route";
        }

        EventGateway gateway = new EventGateway();
        gateway.setId("node_" + nodeId + "_ebg");
        gateway.setName(nodeLabel + " (message or timeout)");
        // The token parks AT THE GATEWAY, so the correlation pin must be written by the
        // gateway's start listener — the catch's own start listener only fires after the
        // message has already arrived, which is too late for a sender that correlates by the
        // shared authored key (audit M1).
        receiverConfigurator.attachCorrelationPinListener(gateway);
        process.addFlowElement(gateway);
        process.addFlowElement(newFlow("flow_" + nodeId + "_ebg_msg", gateway.getId(), catchEvent.getId()));

        IntermediateCatchEvent timerCatch = new IntermediateCatchEvent();
        timerCatch.setId("node_" + nodeId + "_timeout");
        timerCatch.setName(nodeLabel + " (timeout)");
        TimerEventDefinition timer = new TimerEventDefinition();
        timer.setTimeDuration(isoDuration(timeoutMs));
        timerCatch.addEventDefinition(timer);
        process.addFlowElement(timerCatch);
        process.addFlowElement(newFlow("flow_" + nodeId + "_ebg_timeout", gateway.getId(), timerCatch.getId()));

        return switch (behavior) {
            case "cancel" -> {
                // Absorb the token: a plain none end event ends THIS branch without failing the
                // run (a terminate end would kill sibling tokens too — heavier than "cancel").
                EndEvent cancelEnd = new EndEvent();
                cancelEnd.setId("node_" + nodeId + "_timeout_end");
                cancelEnd.setName(nodeLabel + " (timeout: cancel)");
                process.addFlowElement(cancelEnd);
                process.addFlowElement(newFlow("flow_" + nodeId + "_timeout_end",
                        timerCatch.getId(), cancelEnd.getId()));
                yield new GeneratedNode(gateway.getId(), catchEvent.getId(),
                        Map.of(TIMEOUT_PORT_VALUE, ""));
            }
            case "error" -> {
                // Activiti 9: an error end references a model-level <error> declaration by
                // errorRef; the declaration carries the catchable errorCode.
                String errorRef = "Error_" + TIMEOUT_ERROR_CODE.replaceAll("[^A-Za-z0-9_]", "_");
                model.addError(errorRef, TIMEOUT_ERROR_CODE, TIMEOUT_ERROR_CODE);
                EndEvent errorEnd = new EndEvent();
                errorEnd.setId("node_" + nodeId + "_timeout_err");
                errorEnd.setName(nodeLabel + " (timeout: error)");
                ErrorEventDefinition errorDef = new ErrorEventDefinition();
                errorDef.setErrorRef(errorRef);
                errorEnd.addEventDefinition(errorDef);
                process.addFlowElement(errorEnd);
                process.addFlowElement(newFlow("flow_" + nodeId + "_timeout_err",
                        timerCatch.getId(), errorEnd.getId()));
                yield new GeneratedNode(gateway.getId(), catchEvent.getId(),
                        Map.of(TIMEOUT_PORT_VALUE, ""));
            }
            default -> {
                if (!"route".equals(behavior)) {
                    log.warn("message.intermediate.catch.event node={} has unknown onTimeoutBehavior='{}'"
                            + " — falling back to 'route'", nodeId, behavior);
                }
                attachTimeoutInfoListener(timerCatch);
                yield new GeneratedNode(gateway.getId(), catchEvent.getId(),
                        Map.of(TIMEOUT_PORT_VALUE, timerCatch.getId()));
            }
        };
    }

    private static SequenceFlow newFlow(String id, String sourceRef, String targetRef) {
        SequenceFlow flow = new SequenceFlow(sourceRef, targetRef);
        flow.setId(id);
        return flow;
    }

    /**
     * Attaches the {@code timeout} envelope emitter on the timer catch's {@code end} event: it
     * fires exactly when the timer arm wins the event choice, fanning the template-promised
     * {@code timedOut}/{@code waitedMs}/{@code lastCheckedAt} object to the Timeout-port
     * downstream. Only attached in the {@code route} shape — {@code cancel}/{@code error} absorb
     * the token into an end event and the template emits the port only for {@code route}.
     */
    private static void attachTimeoutInfoListener(IntermediateCatchEvent timerCatch) {
        org.activiti.bpmn.model.ActivitiListener listener = new org.activiti.bpmn.model.ActivitiListener();
        listener.setEvent("end");
        listener.setImplementationType(
                org.activiti.bpmn.model.ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation("${timeoutInfoEmissionListener}");
        List<org.activiti.bpmn.model.ActivitiListener> listeners =
                timerCatch.getExecutionListeners() == null
                        ? new ArrayList<>()
                        : new ArrayList<>(timerCatch.getExecutionListeners());
        listeners.add(listener);
        timerCatch.setExecutionListeners(listeners);
    }

    /**
     * Parses {@code timeoutMs}; blank means "no timeout" ({@code 0}). A non-numeric or
     * negative value fails the deploy loudly: degrading it to 0 would deploy a graph with
     * no timeout branch while the authored one has Timeout-port edges — those edges would
     * then be skipped as un-wirable, and the deployed graph would silently differ from
     * what the author sees on the canvas.
     */
    private static long parseTimeoutMs(String raw, String nodeId) {
        if (StringUtils.isBlank(raw)) {
            return 0L;
        }
        try {
            long parsed = Long.parseLong(raw.trim());
            if (parsed < 0) {
                throw new IllegalArgumentException(
                        "message.intermediate.catch.event node=" + nodeId
                                + " has negative timeoutMs=" + raw);
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    "message.intermediate.catch.event node=" + nodeId
                            + " has non-numeric timeoutMs='" + raw + "'", ex);
        }
    }

    /** ISO-8601 duration for a millisecond count ({@code PT5S}, {@code PT0.250S}). */
    static String isoDuration(long timeoutMs) {
        long seconds = timeoutMs / 1000;
        long millis = timeoutMs % 1000;
        return millis == 0
                ? "PT" + seconds + "S"
                : String.format(Locale.ROOT, "PT%d.%03dS", seconds, millis);
    }
}
