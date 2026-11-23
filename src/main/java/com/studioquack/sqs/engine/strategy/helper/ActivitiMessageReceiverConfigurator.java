package com.studioquack.sqs.engine.strategy.helper;

import com.studioquack.common.dto.flow.NodeDto;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.Event;
import org.activiti.bpmn.model.ExtensionAttribute;
import org.activiti.bpmn.model.ExtensionElement;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.ImplementationType;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared deploy-time configurator for {@code message.start.event} /
 * {@code message.intermediate.catch.event} — emits the node's authored v1.1.0 receiver
 * parameters onto the BPMN element as an {@code sq:receiverConfig} extension element so
 * runtime bridges (the correlation-pin listener) read them without re-parsing the authoring
 * graph. The catch-side {@code timeoutMs}/{@code onTimeoutBehavior} are consumed at DEPLOY time
 * by the catch strategy's event-based-gateway synthesis; they ride the same element as the
 * authored-config record.
 *
 * <p>Mirrors {@link ActivitiMessageSenderConfigurator}: same namespace, same
 * skip-when-nothing-authored rule — an element with no authored parameters would only bloat
 * the BPMN, and runtime defaults (correlationKey = processInstanceBusinessKey) take over.
 * Unlike the sender side there is no runtime listener to attach: the receiver is parked by
 * the engine and woken through {@code MessageCorrelationService}; this class only carries
 * the authored config.
 */
@Component
public class ActivitiMessageReceiverConfigurator {

    /** BPMN extension namespace under which the receiver's runtime config is emitted. */
    public static final String NAMESPACE = ActivitiMessageSenderConfigurator.NAMESPACE;
    /** Namespace prefix used in the serialized BPMN XML. */
    public static final String NAMESPACE_PREFIX = ActivitiMessageSenderConfigurator.NAMESPACE_PREFIX;
    /** Element local name carrying the receiver's runtime config. */
    public static final String RECEIVER_CONFIG_ELEMENT = "receiverConfig";
    /** Attribute: optional per-node override for the correlation key (literal or {@code ${var}}). */
    public static final String ATTR_CORRELATION_KEY = "correlationKey";
    /**
     * Attribute: JUEL expression evaluated when the literal {@link #ATTR_CORRELATION_KEY}
     * is blank. Aligned with the v1.1.0 FE template {@code businessKeyExpression} parameter.
     */
    public static final String ATTR_BUSINESS_KEY_EXPRESSION = "businessKeyExpression";
    /** Attribute: catch-side timeout in milliseconds (consumed at deploy time by the catch strategy). */
    public static final String ATTR_TIMEOUT_MS = "timeoutMs";
    /** Attribute: behaviour when the catch-side timeout fires ({@code route} / {@code cancel} / {@code error}). */
    public static final String ATTR_ON_TIMEOUT_BEHAVIOR = "onTimeoutBehavior";
    // subscriptionMode / webhookBindingId / apiKeyRef deliberately absent: all three were
    // emitted here but had zero consumers — Activiti subscriptions are always persisted (no
    // volatile mode exists), and the binding/API-key ingress gate was never built. Removed
    // with the FE template parameters (audit item 6).

    /** Node parameter short-code for the optional per-node correlation key override. */
    private static final String PARAM_CORRELATION_KEY = "correlationKey";
    /** Node parameter short-code for the optional JUEL business-key expression fallback. */
    private static final String PARAM_BUSINESS_KEY_EXPRESSION = "businessKeyExpression";
    /** Node parameter short-code for the optional catch-side timeout. */
    private static final String PARAM_TIMEOUT_MS = "timeoutMs";
    /** Node parameter short-code for the optional timeout behaviour. */
    private static final String PARAM_ON_TIMEOUT_BEHAVIOR = "onTimeoutBehavior";

    private static final String CORRELATION_PIN_DELEGATE = "${messageReceiverCorrelationListener}";
    private static final String LISTENER_EVENT_START = "start";

    /**
     * Attaches the {@code ${messageReceiverCorrelationListener}} runtime bridge (event=
     * {@code start}) to a {@code message.intermediate.catch.event} — or, for the timeout
     * shape, to the synthesized {@code EventGateway} the token actually parks at (the catch's
     * own start listener would only fire AFTER the message arrived, so the pin must be written
     * on the gateway execution; the listener resolves the message name/config from the
     * gateway's downstream message catch). At park time the listener resolves the
     * authored/runtime correlation key and pins it as an execution-local REF-only variable so a
     * sender can correlate by the shared authored key (see
     * {@link com.studioquack.sqs.engine.delegate.MessageReceiverCorrelationListener}). Attached
     * unconditionally — a runtime INPUT-port override on the {@code subscription} port wins over
     * the authored key when one is mapped; the listener no-ops when neither exists. NOT attached to
     * {@code message.start.event}: a start event launches a new instance by message name alone,
     * there is no parked wait to narrow by a key.
     *
     * <p>For a MESSAGE <b>boundary</b> the same bridge is attached to the boundary's HOST
     * activity (the user task / call activity / sandbox wait state the subscription rides on):
     * Activiti activates a boundary through its event handler rather than the ordinary
     * activity-start operation, so a listener on the boundary element itself would only fire
     * when the boundary triggers — too late for the pin, exactly like the catch's own listener
     * in the timeout shape.
     */
    public void attachCorrelationPinListener(FlowElement element) {
        // Idempotent: a host activity with several keyed message boundaries is offered the
        // listener once per boundary but must carry exactly one.
        List<ActivitiListener> listeners = new ArrayList<>();
        if (element.getExecutionListeners() != null) {
            for (ActivitiListener existing : element.getExecutionListeners()) {
                if (CORRELATION_PIN_DELEGATE.equals(existing.getImplementation())) {
                    return;
                }
            }
            listeners.addAll(element.getExecutionListeners());
        }
        ActivitiListener listener = new ActivitiListener();
        listener.setEvent(LISTENER_EVENT_START);
        listener.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation(CORRELATION_PIN_DELEGATE);
        listeners.add(listener);
        element.setExecutionListeners(listeners);
    }

    /**
     * Emits an {@code sq:receiverConfig} carrying only the authored {@code correlationKey} —
     * the MESSAGE-boundary variant of {@link #configureReceiver(Event, NodeDto)}. A boundary
     * config has no {@code businessKeyExpression}/{@code timeoutMs} fields and its key comes
     * from the {@link com.studioquack.common.dto.flow.BoundaryEventConfig}, not from node
     * parameters. Emitted on the boundary element so the correlation-pin listener can read it
     * back from the deployed model at park time.
     */
    public void emitCorrelationKeyConfig(FlowElement element, String correlationKey) {
        if (StringUtils.isBlank(correlationKey)) {
            return;
        }
        ExtensionElement config = new ExtensionElement();
        config.setName(RECEIVER_CONFIG_ELEMENT);
        config.setNamespace(NAMESPACE);
        config.setNamespacePrefix(NAMESPACE_PREFIX);
        addAttr(config, ATTR_CORRELATION_KEY, correlationKey);
        element.addExtensionElement(config);
    }

    /**
     * Emits the receiver config extension element.
     *
     * @param event the BPMN message-bearing event just built by the strategy — a
     *              {@link org.activiti.bpmn.model.StartEvent} for {@code message.start.event}
     *              or an {@link org.activiti.bpmn.model.IntermediateCatchEvent} for
     *              {@code message.intermediate.catch.event}. Both share {@link Event} as their
     *              common supertype and carry {@code extensionElements}.
     * @param node  the authoring NodeDto whose parameters seed the config
     */
    public void configureReceiver(Event event, NodeDto node) {
        String correlationKey = readParam(node, PARAM_CORRELATION_KEY);
        String businessKeyExpression = readParam(node, PARAM_BUSINESS_KEY_EXPRESSION);
        String timeoutMs = readParam(node, PARAM_TIMEOUT_MS);
        String onTimeoutBehavior = readParam(node, PARAM_ON_TIMEOUT_BEHAVIOR);
        if (StringUtils.isBlank(correlationKey) && StringUtils.isBlank(businessKeyExpression)
                && StringUtils.isBlank(timeoutMs) && StringUtils.isBlank(onTimeoutBehavior)) {
            // Nothing authored → runtime defaults take over; no need to bloat the BPMN with
            // an empty element.
            return;
        }
        ExtensionElement config = new ExtensionElement();
        config.setName(RECEIVER_CONFIG_ELEMENT);
        config.setNamespace(NAMESPACE);
        config.setNamespacePrefix(NAMESPACE_PREFIX);
        addAttr(config, ATTR_CORRELATION_KEY, correlationKey);
        addAttr(config, ATTR_BUSINESS_KEY_EXPRESSION, businessKeyExpression);
        addAttr(config, ATTR_TIMEOUT_MS, timeoutMs);
        addAttr(config, ATTR_ON_TIMEOUT_BEHAVIOR, onTimeoutBehavior);
        event.addExtensionElement(config);
    }

    private static String readParam(NodeDto node, String shortCode) {
        return node == null || node.getData() == null ? null
                : StringUtils.trimToNull(node.getData().findParameterValueByShortCode(shortCode));
    }

    private static void addAttr(ExtensionElement el, String name, String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        ExtensionAttribute attr = new ExtensionAttribute(name);
        attr.setValue(value);
        el.addAttribute(attr);
    }
}
