package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.command.MessageCorrelationService.SqEventType;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventRequest;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageSenderConfigurator;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Event;
import org.activiti.bpmn.model.ExtensionAttribute;
import org.activiti.bpmn.model.ExtensionElement;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.Message;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Runtime bridge for {@code message.end.event} and {@code message.intermediate.throw.event}.
 * Attached (event={@code start}) by {@link ActivitiMessageSenderConfigurator}; fires as the
 * token reaches the throw/end event, resolves the BPMN message name and the optional per-node
 * config, and enters the shared {@link MessageCorrelationService#correlateWebhook(WebhookEventRequest)}
 * — the SAME service method the external webhook ingress calls — so intra-engine sender delivery
 * shares one code path (address routing, REF-only guard, engine seams) with external ingress.
 *
 * <p><b>Aligned with the FE node template contract</b>
 * ({@code bpmn-node-templates/message.intermediate.throw.event_definition.json},
 * {@code bpmn-node-templates/message.end.event_definition.json}):
 * <ul>
 *   <li><b>INPUT.message.name</b> — runtime override for the configured message name;
 *       a DuckTape-mapped row on the sender's {@code message} INPUT port whose payload carries
 *       a {@code name} (or {@code messageName}) field wins over the deploy-time
 *       {@code messageRef}. Absent → the BPMN {@code messageRef} default. The
 *       {@code messageName} alias keeps a direct receiver-OUTPUT → sender-INPUT wire working
 *       without a rename transform.</li>
 *   <li><b>INPUT.message.payload</b> — a Map value sealed as the correlation {@code msg} via
 *       the shared {@code correlateWebhook} → {@code sealMsg} path (REF-only seal, one row per
 *       top-level field, targeting the receiver's own downstream). Absent → empty msg (wake-up
 *       only, no data transfer).</li>
 *   <li><b>OUTPUT.emit_result.success</b> / <b>OUTPUT.emit_result.timestamp</b> — written to
 *       every outgoing edge's target node (only for the throw event; end events have no
 *       outgoing sequence flows) so a downstream node can read the delivery ACK. {@code
 *       success} is {@code true} when the correlation was accepted by a waiting receiver /
 *       started a new instance; {@code false} on benign no-match or any correlation failure.</li>
 * </ul>
 *
 * <p><b>Why "start" and not "end".</b> On a message end event the "end" listener fires after
 * the end event completes the token; the correlation must happen while the process instance
 * is still alive — the {@code start} listener event fires BEFORE the token leaves the
 * throw/end event, matching that requirement.
 *
 * <p><b>Correlation semantics.</b>
 * <ul>
 *   <li>{@code targetKind} — when explicitly authored on the node it is honoured as-is
 *       ({@code MESSAGE_CATCH} = deliver only to a waiting catch; {@code MESSAGE_START} =
 *       always launch a new instance keyed on the message name). When NOT authored the
 *       listener FANS OUT per BPMN throw semantics: try {@code MESSAGE_CATCH} first (fast
 *       in-instance / same-businessKey path), and on a benign no-match fall through to
 *       {@code MESSAGE_START} so a deployed message-start event in another flow gets
 *       triggered too. {@code emit_result.success} is {@code true} when EITHER seam
 *       accepted the message.</li>
 *   <li>{@code correlationKey} defaults to the sender instance's
 *       {@code processInstanceBusinessKey}. Authors may override with a literal string or a
 *       minimal {@code ${varName}} lookup against the sender execution. On the
 *       {@code MESSAGE_START} leg an AUTHORED key also travels as the addressed key, so a
 *       message-start node carrying a static correlationKey filter fires when both sides
 *       authored the same key (the started run then takes that key as its businessKey). The
 *       businessKey fallback is never forwarded there — an unkeyed throw cannot trip a
 *       filtered start.</li>
 * </ul>
 *
 * <p><b>Delivery semantics ({@code deliveryMode}).</b> Delivery is always in-thread on the
 * sender's command — there is no outbox, so a true {@code async} mode does not exist.
 * {@code sync} (default) runs the configured {@code retryPolicy} and the token waits for the
 * outcome; {@code fire_and_forget} makes exactly one attempt and never retries. Legacy
 * {@code async} values in already-deployed models are normalized to {@code sync} at read
 * time so {@code emit_result.deliveryMode} always echoes a truthful mode.
 *
 * <p><b>Failure isolation.</b> Any correlation failure (no waiting execution, invalid config,
 * downstream engine error) is caught and reported through {@code emit_result.success=false}
 * — the sender's own transaction is <b>never</b> rolled back by a delivery mishap on the
 * receiver side (BPMN throw semantics: caller's flow continues regardless).
 */
@Slf4j
@Component("messageSenderExecutionListener")
public class MessageSenderExecutionListener implements ExecutionListener {

    /** Activity id prefix the strategies mint: designer id {@code X} → activity id {@code node_X}. */
    private static final String NODE_ID_PREFIX = "node_";
    /**
     * Sender INPUT port value whose mapped rows carry the runtime overrides. DuckTape-authored
     * edges arrive in the pipeline's wire shape: {@code targetPortValue} = the port's value
     * ("message"), {@code value} = the mapped payload object holding the fields below.
     */
    private static final String INPUT_MESSAGE_PORT = "message";
    /** Field inside the mapped {@code message} payload: runtime message-name override. The FE
     *  template's schema shortCode is {@code messageName} (canonical since F3) — the same key a
     *  direct receiver-OUTPUT → sender-INPUT wire carries, so that wire needs no rename
     *  transform. {@code name} is the pre-F3 spelling, still accepted as a fallback so older
     *  mappings keep working. */
    private static final String FIELD_NAME = "name"; // legacy spelling, still accepted (F3)
    private static final String FIELD_NAME_ALIAS = "messageName"; // canonical since F3
    /** Field inside the mapped {@code message} payload: runtime correlation-key override. */
    private static final String FIELD_CORRELATION_KEY = "correlationKey";
    /** Field inside the mapped {@code message} payload: runtime message payload (an object). */
    private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_PAYLOAD_REF = "payloadRef";
    private static final String FIELD_ENVELOPE_META = "envelopeMeta";
    /** Sender OUTPUT port field: correlation acknowledgement flag. */
    private static final String OUTPUT_SUCCESS_PORT = "success";
    /** Sender OUTPUT port field: ISO-8601 UTC instant when the throw completed. */
    private static final String OUTPUT_TIMESTAMP_PORT = "timestamp";
    /** Sender OUTPUT port field: engine-assigned unique id for the emitted message (v1.1.0). */
    private static final String OUTPUT_MESSAGE_ID_PORT = "messageId";
    /** Sender OUTPUT port field: pointer into the append-only audit chain (F-02, v1.1.0). */
    private static final String OUTPUT_AUDIT_REF_PORT = "auditRef";
    /** Sender OUTPUT port field: number of delivery attempts actually made (v1.1.0). */
    private static final String OUTPUT_ATTEMPTS_PORT = "attempts";
    /** Sender OUTPUT port field: effective delivery mode used (v1.1.0). */
    private static final String OUTPUT_DELIVERY_MODE_PORT = "deliveryMode";

    /** Delivery mode: in-thread delivery with the configured retry policy; the token waits for the outcome. */
    private static final String DELIVERY_MODE_SYNC = "sync";
    /** Delivery mode: exactly one delivery attempt, no retry — the retryPolicy does not apply. */
    private static final String DELIVERY_MODE_FIRE_AND_FORGET = "fire_and_forget";
    /** Default delivery mode when the author leaves the config blank. */
    private static final String DEFAULT_DELIVERY_MODE = DELIVERY_MODE_SYNC;
    /** Default retry policy applied when the FE template omits {@code retryPolicy}. */
    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final long DEFAULT_BACKOFF_MS = 500L;
    private static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0d;
    private static final long DEFAULT_MAX_BACKOFF_MS = 30_000L;
    /**
     * Parses {@code {"maxAttempts":..., "backoffMs":..., "backoffMultiplier":..., "maxBackoffMs":...}}
     * without pulling a JSON library — the FE writes these as flat numeric fields.
     */
    private static final Pattern RETRY_FIELD = Pattern.compile(
            "\"(maxAttempts|backoffMs|backoffMultiplier|maxBackoffMs)\"\\s*:\\s*(-?\\d+(?:\\.\\d+)?)");

    private final ObjectProvider<MessageCorrelationService> correlationServiceProvider;
    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<ExecutionContextStore> executionContextStoreProvider;

    public MessageSenderExecutionListener(
            ObjectProvider<MessageCorrelationService> correlationServiceProvider,
            ObjectProvider<SqsEngine> sqsEngineProvider,
            ObjectProvider<ExecutionContextStore> executionContextStoreProvider) {
        this.correlationServiceProvider = correlationServiceProvider;
        this.sqsEngineProvider = sqsEngineProvider;
        this.executionContextStoreProvider = executionContextStoreProvider;
    }

    @Override
    public void notify(DelegateExecution execution) {
        MessageCorrelationService correlationService = correlationServiceProvider.getIfAvailable();
        SqsEngine engine = sqsEngineProvider.getIfAvailable();
        ExecutionContextStore contextStore = executionContextStoreProvider.getIfAvailable();
        if (correlationService == null || engine == null || contextStore == null) {
            log.warn("messageSenderExecutionListener: services not available, skipping delivery");
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

            // Deploy-time defaults from the BPMN model.
            String configuredMessageName = resolveMessageName(bpmnModel, event);
            SenderConfig cfg = readConfig(fe);

            // Runtime overrides authored by upstream nodes' output rows.
            RuntimeInputs runtimeInputs = readRuntimeInputs(execution, senderNodeId, contextStore);

            // Effective values: runtime input > deploy-time default > null.
            String effectiveMessageName =
                    firstNonBlank(runtimeInputs.nameOverride, configuredMessageName);
            if (StringUtils.isBlank(effectiveMessageName)) {
                log.info("messageSenderExecutionListener: no message name on activity={}", activityId);
                writeEmitResult(event, senderNodeId, businessKey, false, 0, cfg.deliveryMode,
                        contextStore);
                return;
            }
            boolean explicitKind = cfg.targetKind != null;
            SqEventType targetKind = explicitKind ? cfg.targetKind : SqEventType.MESSAGE_CATCH;
            String correlationKey = resolveCorrelationKey(cfg, execution, runtimeInputs);
            // The START leg's addressed key: only an AUTHORED key travels — never the
            // businessKey fallback, or every started run would inherit the sender's key and an
            // unkeyed throw could never satisfy a filtered start anyway.
            String addressedKey = authoredCorrelationKey(cfg, execution, runtimeInputs);
            if (explicitKind && targetKind == SqEventType.MESSAGE_CATCH
                    && StringUtils.isBlank(correlationKey)) {
                log.warn("messageSenderExecutionListener: no correlationKey for MESSAGE_CATCH on activity={}",
                        activityId);
                writeEmitResult(event, senderNodeId, businessKey, false, 0, cfg.deliveryMode,
                        contextStore);
                return;
            }

            Map<String, Object> effectiveMsg = runtimeInputs.payload != null
                    ? runtimeInputs.payload : Map.of();
            if (runtimeInputs.payloadRefUnresolved) {
                // C6: a mapped payloadRef that does not resolve must FAIL the send — silently
                // publishing an empty message would masquerade data loss as delivery.
                log.warn("messageSenderExecutionListener: mapped payloadRef did not resolve on activity={}",
                        activityId);
                writeEmitResult(event, senderNodeId, businessKey, false, 0, cfg.deliveryMode,
                        contextStore);
                return;
            }

            // BPMN throw/end semantics: a thrown message reaches EITHER a waiting intermediate/boundary
            // message-catch OR a deployed message-start event with the same message name — the sender
            // does not care which. When targetKind is EXPLICITLY authored on the node we honour it
            // (back-compat); otherwise we fan out: try CATCH first (fast in-instance path), and on a
            // benign no-match fall through to START (which triggers Activiti's deploy-time
            // message-start handler → a NEW process instance). This makes cross-flow collaboration
            // work by default (senaryo 2: flow A's message end event → flow B's message start event).
            RetryPolicy retryPolicy = parseRetryPolicy(cfg.retryPolicy);
            if (DELIVERY_MODE_FIRE_AND_FORGET.equals(cfg.deliveryMode)) {
                retryPolicy.maxAttempts = 1;
            }
            DeliveryOutcome delivery = deliverWithRetry(correlationService, senderNodeId,
                    correlationKey, addressedKey, effectiveMessageName, effectiveMsg, targetKind,
                    explicitKind, activityId, retryPolicy);
            log.info("messageSenderExecutionListener.{} activity={} messageName={} kind={} deliveredVia={} correlationKey={} attempts={} payloadRef={} envelopeId={}",
                    delivery.delivered ? "delivered" : "no-match",
                    activityId, effectiveMessageName, targetKind, delivery.deliveredVia,
                    correlationKey, delivery.attempts, runtimeInputs.payloadRef,
                    runtimeInputs.envelopeMeta == null ? null : runtimeInputs.envelopeMeta.get("envelopeId"));

            // Always write emit_result — downstream nodes should be able to read a deterministic
            // (success, timestamp, messageId, auditRef, attempts, deliveryMode) tuple regardless
            // of whether delivery succeeded.
            writeEmitResult(event, senderNodeId, businessKey, delivery.delivered, delivery.attempts,
                    cfg.deliveryMode, contextStore);
        } catch (Exception ex) {
            log.warn("messageSenderExecutionListener: unexpected failure for activity={}: {}",
                    activityId, ex.getMessage());
        }
    }

    // ---------------------------------------------------------------------
    // Delivery with retry (v1.1.0 — retryPolicy configuration parameter).
    // ---------------------------------------------------------------------

    private static DeliveryOutcome deliverWithRetry(MessageCorrelationService correlationService,
                                                     String senderNodeId, String correlationKey,
                                                     String addressedKey,
                                                     String messageName, Map<String, Object> msg,
                                                     SqEventType targetKind, boolean explicitKind,
                                                     String activityId, RetryPolicy retryPolicy) {
        DeliveryOutcome outcome = new DeliveryOutcome();
        long backoff = retryPolicy.backoffMs;
        for (int attempt = 1; attempt <= retryPolicy.maxAttempts; attempt++) {
            outcome.attempts = attempt;
            if (targetKind == SqEventType.MESSAGE_CATCH && StringUtils.isNotBlank(correlationKey)) {
                try {
                    WebhookEventRequest req = new WebhookEventRequest(
                            SqEventType.MESSAGE_CATCH, null, senderNodeId, correlationKey,
                            messageName, msg, WebhookEventRequest.SOURCE_BRIDGE);
                    WebhookEventOutcome result = correlationService.correlateWebhook(req);
                    if (result != null && result.correlated()) {
                        outcome.delivered = true;
                        outcome.deliveredVia = SqEventType.MESSAGE_CATCH;
                        return outcome;
                    }
                } catch (Exception ex) {
                    log.warn("messageSenderExecutionListener: message-catch correlation failed attempt={} activity={}: {}",
                            attempt, activityId, ex.getMessage());
                }
            }
            if (targetKind == SqEventType.MESSAGE_START || !explicitKind) {
                // Fanout to message-start (or explicit MESSAGE_START). The correlation service
                // pre-checks the start subscription (hasMessageStartSubscription) and answers a
                // benign not-correlated when nothing subscribes — an ActivitiObjectNotFoundException
                // escaping here would poison the SHARED command context and roll the sender's own
                // transaction back at close, even though it is caught below. The addressed key is
                // the sender's AUTHORED correlation key (null when unkeyed): a message-start node
                // with a static correlationKey filter matches it, an unfiltered start takes it as
                // the new run's businessKey.
                try {
                    WebhookEventRequest req = new WebhookEventRequest(
                            SqEventType.MESSAGE_START, null, senderNodeId, addressedKey,
                            messageName, msg, WebhookEventRequest.SOURCE_BRIDGE);
                    WebhookEventOutcome result = correlationService.correlateWebhook(req);
                    if (result != null && result.correlated()) {
                        outcome.delivered = true;
                        outcome.deliveredVia = SqEventType.MESSAGE_START;
                        return outcome;
                    }
                } catch (Exception ex) {
                    log.info("messageSenderExecutionListener.message-start.miss attempt={} activity={} messageName={}: {}",
                            attempt, activityId, messageName, ex.getMessage());
                }
            }
            if (attempt < retryPolicy.maxAttempts) {
                try {
                    Thread.sleep(Math.min(backoff, retryPolicy.maxBackoffMs));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return outcome;
                }
                backoff = Math.min((long) (backoff * retryPolicy.backoffMultiplier),
                        retryPolicy.maxBackoffMs);
            }
        }
        return outcome;
    }

    private static RetryPolicy parseRetryPolicy(String raw) {
        RetryPolicy p = new RetryPolicy();
        p.maxAttempts = DEFAULT_MAX_ATTEMPTS;
        p.backoffMs = DEFAULT_BACKOFF_MS;
        p.backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
        p.maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
        if (StringUtils.isBlank(raw)) {
            return p;
        }
        Matcher m = RETRY_FIELD.matcher(raw);
        while (m.find()) {
            String field = m.group(1);
            String value = m.group(2);
            try {
                switch (field) {
                    case "maxAttempts" -> p.maxAttempts = Math.max(1, (int) Double.parseDouble(value));
                    case "backoffMs" -> p.backoffMs = Math.max(0L, (long) Double.parseDouble(value));
                    case "backoffMultiplier" -> p.backoffMultiplier =
                            Math.max(1.0d, Double.parseDouble(value));
                    case "maxBackoffMs" -> p.maxBackoffMs = Math.max(0L, (long) Double.parseDouble(value));
                    default -> {
                        // unknown — ignore
                    }
                }
            } catch (NumberFormatException ignored) {
                // leave default
            }
        }
        return p;
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

    private static SenderConfig readConfig(FlowElement fe) {
        SenderConfig cfg = new SenderConfig();
        Map<String, List<ExtensionElement>> extensions = fe.getExtensionElements();
        if (extensions == null) {
            return cfg;
        }
        List<ExtensionElement> elements = extensions.get(ActivitiMessageSenderConfigurator.SENDER_CONFIG_ELEMENT);
        if (elements == null || elements.isEmpty()) {
            return cfg;
        }
        ExtensionElement config = elements.get(0);
        cfg.correlationKey = getAttr(config, ActivitiMessageSenderConfigurator.ATTR_CORRELATION_KEY);
        String kindRaw = getAttr(config, ActivitiMessageSenderConfigurator.ATTR_TARGET_KIND);
        if (StringUtils.isNotBlank(kindRaw)) {
            try {
                SqEventType parsed = SqEventType.valueOf(kindRaw.trim().toUpperCase(Locale.ROOT));
                if (parsed == SqEventType.MESSAGE_CATCH || parsed == SqEventType.MESSAGE_START) {
                    cfg.targetKind = parsed;
                }
            } catch (IllegalArgumentException ignored) {
                // Unknown value → fall back to default at call site.
            }
        }
        cfg.businessKeyExpression = getAttr(config,
                ActivitiMessageSenderConfigurator.ATTR_BUSINESS_KEY_EXPRESSION);
        String deliveryMode = getAttr(config, ActivitiMessageSenderConfigurator.ATTR_DELIVERY_MODE);
        String normalizedMode = StringUtils.isNotBlank(deliveryMode)
                ? deliveryMode.trim().toLowerCase(Locale.ROOT) : DEFAULT_DELIVERY_MODE;
        // Legacy "async" (pre-1.1.1 templates) never had distinct runtime semantics — delivery
        // was always in-thread — so it maps onto sync and emit_result echoes a truthful mode.
        cfg.deliveryMode = "async".equals(normalizedMode) ? DELIVERY_MODE_SYNC : normalizedMode;
        cfg.retryPolicy = getAttr(config, ActivitiMessageSenderConfigurator.ATTR_RETRY_POLICY);
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
     * Resolves the correlation key. Precedence: runtime input override (a DuckTape-mapped
     * {@code correlationKey} field on the INPUT {@code message} port) → literal
     * {@code correlationKey} config → {@code businessKeyExpression} JUEL fallback → sender
     * instance's businessKey. The mapped value wins over the panel: a mapping is a deliberate
     * per-wire authoring act, the panel value is the default. Supports a minimal
     * {@code ${varName}} lookup to read a routing token from the sender execution scope;
     * anything else is treated as a literal string.
     */
    private static String resolveCorrelationKey(SenderConfig cfg, DelegateExecution execution,
                                                 RuntimeInputs runtimeInputs) {
        if (runtimeInputs != null && StringUtils.isNotBlank(runtimeInputs.correlationKeyOverride)) {
            return runtimeInputs.correlationKeyOverride;
        }
        String configured = cfg.correlationKey;
        if (StringUtils.isBlank(configured) && StringUtils.isNotBlank(cfg.businessKeyExpression)) {
            String resolved = evaluateJuel(cfg.businessKeyExpression, execution);
            if (StringUtils.isNotBlank(resolved)) {
                return resolved;
            }
        }
        if (StringUtils.isBlank(configured)) {
            return execution.getProcessInstanceBusinessKey();
        }
        return evaluateJuel(configured, execution);
    }

    /**
     * The correlation key only when it was AUTHORED — a runtime input override, a configured
     * literal, or a {@code businessKeyExpression} that resolves non-blank. {@code null} when the
     * effective key is just the sender's businessKey fallback: the START fan-out leg forwards
     * this as the addressed key so a message-start node's static correlationKey filter can match
     * it (and an unfiltered start takes it as the new run's businessKey), while the fallback must
     * never leak there — it would key every started run after the sender's own run.
     */
    private static String authoredCorrelationKey(SenderConfig cfg, DelegateExecution execution,
                                                 RuntimeInputs runtimeInputs) {
        if (runtimeInputs != null && StringUtils.isNotBlank(runtimeInputs.correlationKeyOverride)) {
            return runtimeInputs.correlationKeyOverride;
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
     * Minimal {@code ${varName}} JUEL evaluation — reads a single variable from the sender
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
    // Runtime inputs — read user-authored INPUT.message rows targeting this node.
    // ---------------------------------------------------------------------

    private static RuntimeInputs readRuntimeInputs(DelegateExecution execution, String senderNodeId,
                                                    ExecutionContextStore contextStore) {
        RuntimeInputs result = new RuntimeInputs();
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
        if (StringUtils.isBlank(lookup.getExecutionId())) {
            return result;
        }
        List<InOutValueDto> inputs;
        try {
            inputs = contextStore.loadInputsTargetingNode(lookup, senderNodeId);
        } catch (Exception ex) {
            log.warn("messageSenderExecutionListener: failed to read runtime inputs for {}: {}",
                    senderNodeId, ex.getMessage());
            return result;
        }
        if (inputs == null || inputs.isEmpty()) {
            return result;
        }
        for (InOutValueDto in : inputs) {
            if (!INPUT_MESSAGE_PORT.equalsIgnoreCase(StringUtils.trimToEmpty(in.getTargetPortValue()))) {
                continue;
            }
            if (!(in.getValue() instanceof Map<?, ?> asMap)) {
                continue;
            }
            Object name = asMap.get(FIELD_NAME_ALIAS);
            if (name == null) {
                name = asMap.get(FIELD_NAME);
            }
            if (name != null && StringUtils.isNotBlank(name.toString())) {
                result.nameOverride = name.toString();
            }
            Object corr = asMap.get(FIELD_CORRELATION_KEY);
            if (corr != null && StringUtils.isNotBlank(corr.toString())) {
                result.correlationKeyOverride = corr.toString();
            }
            Object payload = asMap.get(FIELD_PAYLOAD);
            if (payload instanceof Map<?, ?> payloadMap && !payloadMap.isEmpty()) {
                Map<String, Object> coerced = new LinkedHashMap<>(payloadMap.size());
                for (Map.Entry<?, ?> entry : payloadMap.entrySet()) {
                    if (entry.getKey() != null) {
                        coerced.put(entry.getKey().toString(), entry.getValue());
                    }
                }
                result.payload = coerced;
            }
            Object payloadRef = asMap.get(FIELD_PAYLOAD_REF);
            if (payloadRef != null && StringUtils.isNotBlank(payloadRef.toString())) {
                result.payloadRef = payloadRef.toString().trim();
                Object meta = asMap.get(FIELD_ENVELOPE_META);
                if (meta instanceof Map<?, ?> metaMap && !metaMap.isEmpty()) {
                    Map<String, Object> coercedMeta = new LinkedHashMap<>(metaMap.size());
                    for (Map.Entry<?, ?> entry : metaMap.entrySet()) {
                        if (entry.getKey() != null) {
                            coercedMeta.put(entry.getKey().toString(), entry.getValue());
                        }
                    }
                    result.envelopeMeta = coercedMeta;
                }
                // Template contract: payloadRef forwards a pre-sealed envelope VERBATIM and
                // ignores the literal payload field.
                Map<String, Object> forwarded = resolveEnvelopePayload(contextStore, result.payloadRef);
                if (forwarded == null) {
                    result.payloadRefUnresolved = true;
                } else {
                    result.payload = forwarded;
                }
            }
        }
        return result;
    }

    /**
     * Resolves a {@code payloadRef} (a sealed snapshot's {@code currentNodeRefId}, as cited by the
     * catch/start OUT envelope) back into its payload map: the snapshot's producer node id +
     * execution id are recovered via {@code loadByRef}, then the producer's sealed rows rebuild
     * {@code {field: value}}. The ref id is an unguessable capability (same strength as the
     * {@code studioquackRef} handle), so a cross-run forward is legitimate when the author holds
     * the ref. Returns {@code null} when the reference does not resolve (unknown id, no rows).
     *
     * <p>Cost note: the producer-filtered collect still opens every row of the execution — a
     * producer's outputs sit on the row its SUCCESSOR minted, so the row label cannot pre-filter
     * (see {@code H2ExecutionContextStore.collectByProducer}). The filter keeps the grouping and
     * dedup work to the one producer we need. If this ever shows up hot, the real fix is a
     * producer column on the row (schema change on both stores), not a cache here.
     */
    private static Map<String, Object> resolveEnvelopePayload(ExecutionContextStore contextStore,
                                                              String payloadRef) {
        try {
            ProcessContextRef probe = new ProcessContextRef();
            probe.setCurrentNodeRefId(payloadRef);
            var env = contextStore.loadByRef(probe);
            if (env.isEmpty()) {
                return null;
            }
            String producerNodeId = env.get().getCurrentNodeId();
            String executionId = env.get().getWorkflowExecutionId();
            if (StringUtils.isBlank(producerNodeId) || StringUtils.isBlank(executionId)) {
                return null;
            }
            List<InOutValueDto> rows = contextStore
                    .collectOutputsByNode(executionId, Set.of(producerNodeId)).get(producerNodeId);
            if (rows == null || rows.isEmpty()) {
                return null;
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            for (InOutValueDto row : rows) {
                String field = StringUtils.defaultIfBlank(row.getPortId(), row.getPortValue());
                if (StringUtils.isNotBlank(field) && row.getValue() != null) {
                    payload.put(field, row.getValue());
                }
            }
            return payload.isEmpty() ? null : payload;
        } catch (Exception ex) {
            log.warn("messageSenderExecutionListener: payloadRef {} resolution failed: {}",
                    payloadRef, ex.getMessage());
            return null;
        }
    }

    // ---------------------------------------------------------------------
    // Emit result — write (success, timestamp, messageId, auditRef, attempts, deliveryMode)
    // rows for every downstream node.
    // ---------------------------------------------------------------------

    private static void writeEmitResult(Event event, String senderNodeId, String businessKey,
                                         boolean success, int attempts, String deliveryMode,
                                         ExecutionContextStore contextStore) {
        if (!(event instanceof FlowNode flowNode)) {
            return;
        }
        List<SequenceFlow> outgoing = flowNode.getOutgoingFlows();
        if (outgoing == null || outgoing.isEmpty()) {
            // Message end event: no downstream to inform. FE template correctly declares no
            // OUTPUT port on message.end.event; the throw event is the only variant that gets
            // an emit_result.
            return;
        }
        List<String> downstreamIds = outgoing.stream()
                .map(SequenceFlow::getTargetRef)
                .filter(StringUtils::isNotBlank)
                .map(MessageSenderExecutionListener::stripNodePrefix)
                .distinct()
                .collect(Collectors.toList());
        if (downstreamIds.isEmpty()) {
            return;
        }
        if (StringUtils.isBlank(businessKey)) {
            log.warn("messageSenderExecutionListener: no businessKey; cannot snapshot emit_result for {}",
                    senderNodeId);
            return;
        }

        ProcessContextRef writeRef;
        try {
            ExecutionContext senderCtx = new ExecutionContext();
            senderCtx.setWorkflowExecutionId(businessKey);
            senderCtx.setCurrentNodeId(senderNodeId);
            writeRef = contextStore.snapshot(senderCtx, null);
        } catch (Exception ex) {
            log.warn("messageSenderExecutionListener: failed to snapshot emit_result context for {}: {}",
                    senderNodeId, ex.getMessage());
            return;
        }

        String isoTimestamp = OffsetDateTime.now(ZoneOffset.UTC).toString();
        // v1.1.0: locally minted messageId + auditRef marker. The auditRef prefix "local-"
        // signals the append to the F-02 audit chain has not been performed at this seam —
        // MessageCorrelationService is expected to overwrite it with a real audit_event id in
        // a follow-up change (phase D of the control plane impact report).
        String messageId = "msg_" + UUID.randomUUID();
        String auditRef = "local-" + messageId;
        String effectiveDeliveryMode = StringUtils.isNotBlank(deliveryMode)
                ? deliveryMode : DEFAULT_DELIVERY_MODE;

        List<InOutValueDto> outputs = new ArrayList<>(downstreamIds.size() * 6);
        for (String downstreamId : downstreamIds) {
            outputs.add(makeOutputRow(senderNodeId, downstreamId, OUTPUT_SUCCESS_PORT, success));
            outputs.add(makeOutputRow(senderNodeId, downstreamId, OUTPUT_TIMESTAMP_PORT, isoTimestamp));
            outputs.add(makeOutputRow(senderNodeId, downstreamId, OUTPUT_MESSAGE_ID_PORT, messageId));
            outputs.add(makeOutputRow(senderNodeId, downstreamId, OUTPUT_AUDIT_REF_PORT, auditRef));
            outputs.add(makeOutputRow(senderNodeId, downstreamId, OUTPUT_ATTEMPTS_PORT, attempts));
            outputs.add(makeOutputRow(senderNodeId, downstreamId, OUTPUT_DELIVERY_MODE_PORT,
                    effectiveDeliveryMode));
        }
        try {
            contextStore.saveOutputs(writeRef, outputs);
        } catch (Exception ex) {
            log.warn("messageSenderExecutionListener: failed to save emit_result outputs for {}: {}",
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

    private static String firstNonBlank(String a, String b) {
        if (StringUtils.isNotBlank(a)) {
            return a;
        }
        return b;
    }

    /**
     * Mutable holder for the extension-element-read sender config. No {@code sealPayload}: the
     * flag was parsed but never consumable — sealing is unconditional on the
     * {@code correlateWebhook → sealMsg} path because the REF-only invariant forbids persisting
     * raw payloads as engine variables. Removed with the FE template parameter (audit item 4).
     */
    private static final class SenderConfig {
        String correlationKey;
        SqEventType targetKind;
        String businessKeyExpression;
        String deliveryMode = DEFAULT_DELIVERY_MODE;
        String retryPolicy;
    }

    /** Mutable holder for runtime inputs read from the sender node's INPUT.message port. */
    private static final class RuntimeInputs {
        String nameOverride;
        Map<String, Object> payload;
        String correlationKeyOverride;
        /** Mapped {@code payloadRef} — a pre-sealed envelope forwarded verbatim (C6). */
        String payloadRef;
        /** Mapped {@code envelopeMeta} — sealing metadata carried for audit/key rotation (C6). */
        Map<String, Object> envelopeMeta;
        /** A mapped {@code payloadRef} that did not resolve — the send must fail, not send empty. */
        boolean payloadRefUnresolved;
    }

    /** Retry policy parsed from the {@code retryPolicy} extension attribute. */
    private static final class RetryPolicy {
        int maxAttempts;
        long backoffMs;
        double backoffMultiplier;
        long maxBackoffMs;
    }

    /** Result of a delivery attempt (including retry accounting). */
    private static final class DeliveryOutcome {
        boolean delivered;
        SqEventType deliveredVia;
        int attempts;
    }
}
