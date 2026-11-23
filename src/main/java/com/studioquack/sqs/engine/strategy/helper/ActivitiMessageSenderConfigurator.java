package com.studioquack.sqs.engine.strategy.helper;

import com.studioquack.common.dto.flow.NodeDto;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.Event;
import org.activiti.bpmn.model.ExtensionAttribute;
import org.activiti.bpmn.model.ExtensionElement;
import org.activiti.bpmn.model.ImplementationType;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared deploy-time configurator for {@code message.end.event} /
 * {@code message.intermediate.throw.event} — attaches the runtime
 * {@code ${messageSenderExecutionListener}} bridge and emits the node's authored
 * sender parameters ({@code correlationKey}, {@code targetKind}) onto the BPMN element
 * as an {@code sq:senderConfig} extension element so the listener can read them at runtime
 * without re-reading the deploy-time graph.
 *
 * <p>Rationale: SQS's embedded Activiti engine does NOT natively deliver a thrown BPMN
 * message (from an end event or an intermediate throw event) to a waiting message catch
 * / message-start event in this engine. To keep intra-engine collaboration ("C" scenario:
 * process definition A throws → process definition B was parked on the same message name
 * with a shared correlation key) working through the same code path that external webhooks
 * use, the listener enters the shared {@link com.studioquack.sqs.command.MessageCorrelationService}
 * at its {@code correlateWebhook(...)} seam — same REF-only guard, same address routing,
 * same engine seams as the external webhook ingress.
 *
 * <p>External-facing HTTP gates (API key / entitlement / RBAC / quota) live upstream of
 * this service on the ingress controllers, so an internal caller entering here bypasses
 * none of the runtime safety (REF-only guard still applies) but pays none of the network
 * / commercial gate cost either — this is intentional and analogous to any other Activiti
 * ExecutionListener touching engine seams directly.
 */
@Component
public class ActivitiMessageSenderConfigurator {

    /** BPMN extension namespace under which the sender's runtime config is emitted. */
    public static final String NAMESPACE = "http://studioquack.com/bpmn";
    /** Namespace prefix used in the serialized BPMN XML. */
    public static final String NAMESPACE_PREFIX = "sq";
    /** Element local name carrying the sender's runtime config. */
    public static final String SENDER_CONFIG_ELEMENT = "senderConfig";
    /** Attribute: optional per-node override for the correlation key (literal or {@code ${var}}). */
    public static final String ATTR_CORRELATION_KEY = "correlationKey";
    /** Attribute: optional per-node override for the target kind ({@code MESSAGE_CATCH} / {@code MESSAGE_START}). */
    public static final String ATTR_TARGET_KIND = "targetKind";
    /**
     * Attribute: JUEL expression evaluated against the sender execution when the literal
     * {@link #ATTR_CORRELATION_KEY} is blank. Aligned with the v1.1.0 FE template
     * {@code businessKeyExpression} configuration parameter.
     */
    public static final String ATTR_BUSINESS_KEY_EXPRESSION = "businessKeyExpression";
    /**
     * Attribute: delivery mode enum — {@code sync} | {@code fire_and_forget}
     * ({@code sync} = in-thread delivery with the configured retry policy;
     * {@code fire_and_forget} = exactly one attempt). Aligned with the FE template
     * {@code deliveryMode} configuration parameter. Legacy {@code async} values in
     * already-deployed models are normalized to {@code sync} by the listener — true async
     * delivery never existed (no outbox), so the template no longer offers it.
     */
    public static final String ATTR_DELIVERY_MODE = "deliveryMode";
    /**
     * Attribute: JSON-encoded retry policy carried verbatim from the FE template
     * ({@code {maxAttempts, backoffMs, backoffMultiplier, maxBackoffMs}}). Consumed by
     * {@link com.studioquack.sqs.engine.delegate.MessageSenderExecutionListener}.
     */
    public static final String ATTR_RETRY_POLICY = "retryPolicy";
    // sealPayload deliberately absent: sealing is unconditional in the correlation path
    // (REF-only invariant), so an opt-out flag was never legally consumable. Removed with the
    // FE template parameter (audit item 4).

    private static final String DELEGATE_EXPRESSION = "${messageSenderExecutionListener}";
    private static final String LISTENER_EVENT_START = "start";

    /** Node parameter short-code for the optional per-node correlation key override. */
    private static final String PARAM_CORRELATION_KEY = "correlationKey";
    /** Node parameter short-code for the optional per-node target kind override. */
    private static final String PARAM_TARGET_KIND = "targetKind";
    /** Node parameter short-code for the optional JUEL business-key expression fallback. */
    private static final String PARAM_BUSINESS_KEY_EXPRESSION = "businessKeyExpression";
    /** Node parameter short-code for the optional delivery mode. */
    private static final String PARAM_DELIVERY_MODE = "deliveryMode";
    /** Node parameter short-code for the optional retry policy (JSON object). */
    private static final String PARAM_RETRY_POLICY = "retryPolicy";

    /**
     * Attaches the runtime listener and emits the sender config extension element.
     *
     * @param event the BPMN message-bearing event just built by the strategy — an
     *              {@link org.activiti.bpmn.model.EndEvent} for {@code message.end.event}
     *              or a {@link org.activiti.bpmn.model.ThrowEvent} for
     *              {@code message.intermediate.throw.event}. Both share {@link Event} as their
     *              common supertype and carry {@code executionListeners} + {@code extensionElements}.
     * @param node  the authoring NodeDto whose parameters seed the config
     */
    public void configureSender(Event event, NodeDto node) {
        attachListener(event);
        emitConfig(event, node);
    }

    private void attachListener(Event event) {
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

    private void emitConfig(Event event, NodeDto node) {
        String correlationKey = readParam(node, PARAM_CORRELATION_KEY);
        String targetKind = readParam(node, PARAM_TARGET_KIND);
        String businessKeyExpression = readParam(node, PARAM_BUSINESS_KEY_EXPRESSION);
        String deliveryMode = readParam(node, PARAM_DELIVERY_MODE);
        String retryPolicy = readParam(node, PARAM_RETRY_POLICY);
        if (StringUtils.isBlank(correlationKey) && StringUtils.isBlank(targetKind)
                && StringUtils.isBlank(businessKeyExpression) && StringUtils.isBlank(deliveryMode)
                && StringUtils.isBlank(retryPolicy)) {
            // Nothing authored → runtime defaults (correlationKey = processInstanceBusinessKey,
            // targetKind = MESSAGE_CATCH, deliveryMode = async) take over;
            // no need to bloat the BPMN with an empty element.
            return;
        }
        ExtensionElement config = new ExtensionElement();
        config.setName(SENDER_CONFIG_ELEMENT);
        config.setNamespace(NAMESPACE);
        config.setNamespacePrefix(NAMESPACE_PREFIX);
        addAttr(config, ATTR_CORRELATION_KEY, correlationKey);
        addAttr(config, ATTR_TARGET_KIND, targetKind);
        addAttr(config, ATTR_BUSINESS_KEY_EXPRESSION, businessKeyExpression);
        addAttr(config, ATTR_DELIVERY_MODE, deliveryMode);
        addAttr(config, ATTR_RETRY_POLICY, retryPolicy);
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
