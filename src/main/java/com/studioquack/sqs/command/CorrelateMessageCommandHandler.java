package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.webhook.EngineApiKeyScope;
import com.studioquack.common.dto.webhook.WebhookKeyFormat;
import com.studioquack.common.security.envelope.SecureEnvelope;
import com.studioquack.sqs.channel.EnvelopeClient;
import com.studioquack.sqs.channel.SecurityTelemetryAccumulator;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.command.MessageCorrelationService.MessageCorrelationOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.SqEventType;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventRequest;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.webhook.DaemonWebhookKeyStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * {@code correlate-message}: the daemon side of message-event correlation. An external caller
 * delivers a BPMN message / intermediate message event through the control plane over the reverse
 * channel to a running workflow instance on this environment's embedded Activiti engine.
 *
 * <p>When the payload carries an inline FR-200 {@code apiKeyVerification} envelope (AAD
 * {@code "engine-api-key-verify"}), this handler verifies the presented secret in constant time
 * against {@link DaemonWebhookKeyStore} under {@link EngineApiKeyScope#ENGINE_WIDE} before
 * touching the workflow engine. When absent (pre-migration key validated by the control plane),
 * correlation proceeds directly.
 */
@Slf4j
@Component
public class CorrelateMessageCommandHandler implements DaemonCommandHandler {

    /** Wire command type matching the control plane. */
    public static final String TYPE = "correlate-message";

    /** FR-200 envelope AAD tag for inline zero-knowledge engine API key verification. */
    public static final String AAD_API_KEY_VERIFY = "engine-api-key-verify";

    /** Wire error returned when inline API key verification fails (invalid, revoked, or wrong scope). */
    public static final String ERROR_INVALID_API_KEY = "Error_InvalidApiKey";

    /** Security telemetry counter class incremented on failed API key verification. */
    public static final String TELEMETRY_WEBHOOK_KEY_AUTH = "webhook_key_auth";

    private final ObjectMapper objectMapper;
    private final MessageCorrelationService correlationService;
    @Nullable
    private final SqsChannelProperties channel;
    @Nullable
    private final OsSecretManager osSecrets;
    @Nullable
    private final SecretStoreNamespace namespace;
    @Nullable
    private final DaemonWebhookKeyStore webhookKeyStore;

    private SecurityTelemetryAccumulator securityTelemetry;

    /** Null in slice tests that construct this handler by hand. */
    private com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;

    @Autowired
    public CorrelateMessageCommandHandler(ObjectMapper objectMapper,
                                          MessageCorrelationService correlationService,
                                          @Nullable SqsChannelProperties channel,
                                          @Nullable OsSecretManager osSecrets,
                                          @Nullable SecretStoreNamespace namespace,
                                          @Nullable DaemonWebhookKeyStore webhookKeyStore) {
        this.objectMapper = objectMapper;
        this.correlationService = correlationService;
        this.channel = channel;
        this.osSecrets = osSecrets;
        this.namespace = namespace;
        this.webhookKeyStore = webhookKeyStore;
    }

    /** Convenience constructor for unit tests without inline key custody dependencies. */
    public CorrelateMessageCommandHandler(ObjectMapper objectMapper,
                                          MessageCorrelationService correlationService) {
        this(objectMapper, correlationService, null, null, null, null);
    }

    @Autowired(required = false)
    public void setSecurityTelemetry(SecurityTelemetryAccumulator securityTelemetry) {
        this.securityTelemetry = securityTelemetry;
    }

    @Autowired(required = false)
    public void setExecutionHaltGuard(com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard) {
        this.executionHaltGuard = executionHaltGuard;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        String rawPayload = command == null ? null : command.payload();
        CorrelateMessagePayload payload;
        try {
            payload = objectMapper.readValue(rawPayload, CorrelateMessagePayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("correlate-message: malformed payload: " + message(parse));
        }
        if (payload == null) {
            return DaemonCommandOutcome.failed("correlate-message: payload requires messageName");
        }

        // Inline zero-knowledge verification for daemon-custodied ENGINE_WIDE API keys.
        // Must run before touching Activiti; absent envelope falls through to the legacy path.
        if (payload.hasApiKeyVerification()) {
            if (!verifyInlineApiKey(payload.apiKeyVerification())) {
                if (securityTelemetry != null) {
                    securityTelemetry.record(TELEMETRY_WEBHOOK_KEY_AUTH);
                }
                return DaemonCommandOutcome.failed(ERROR_INVALID_API_KEY);
            }
        }

        // A halted engine takes no delivery at all — a start, a catch into a parked run, or a user-task
        // decision — so the caller sees the refusal and can retry, instead of a message consumed by a
        // run that cannot move.
        String haltRefusal = executionHaltGuard == null ? null : executionHaltGuard.refusal();
        if (haltRefusal != null) {
            return DaemonCommandOutcome.failed("correlate-message: " + haltRefusal);
        }

        // New-style payload carries sqEventType, resolved by the control plane before forwarding:
        // elementNodeId/name/businessKey are already resolved, and msg is the raw payload to seal
        // REF-only. A payload without sqEventType uses the legacy messageName/eventKind contract.
        if (StringUtils.isNotBlank(payload.sqEventType())) {
            return handleWebhook(payload);
        }
        if ("TASK".equalsIgnoreCase(StringUtils.trimToEmpty(payload.eventKind()))) {
            return handleUserTaskDecision(payload);
        }

        try {
            MessageCorrelationOutcome outcome = correlationService.correlateEvent(
                    payload.eventKind(), payload.messageName(), payload.correlationBusinessKey(),
                    payload.activityId(), payload.variables());
            log.info("correlate-message.{} envId={} eventKind={} name={} correlationKey={} executionId={}",
                    outcome.correlated() ? "done" : "no-match", payload.envId(), payload.eventKind(),
                    payload.messageName(), payload.correlationBusinessKey(), outcome.executionId());
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    new CorrelateMessageResult(outcome.correlated(), outcome.executionId(), outcome.messageName(),
                            null)));
        } catch (Exception ex) {
            log.warn("correlate-message.failed envId={} messageName={} correlationKey={}: {}",
                    payload.envId(), payload.messageName(), payload.correlationBusinessKey(), ex.toString());
            return DaemonCommandOutcome.failed("correlate-message: " + message(ex));
        }
    }

    /**
     * Opens the FR-200 {@code apiKeyVerification} envelope ({@code Direction.BE_TO_AGENT},
     * {@code AAD = "engine-api-key-verify"}) and verifies the presented secret in constant time
     * against {@link DaemonWebhookKeyStore} under {@link EngineApiKeyScope#ENGINE_WIDE}.
     * Fails closed on any missing dependency, decryption failure, invalid/revoked key, or wrong scope.
     */
    private boolean verifyInlineApiKey(JsonNode rawEnvelope) {
        if (webhookKeyStore == null || channel == null || osSecrets == null || namespace == null) {
            log.warn("correlate-message.api-key-verify: key custody/channel beans unavailable — failing closed");
            return false;
        }
        try {
            SecureEnvelope envelope;
            if (rawEnvelope.isObject()) {
                envelope = objectMapper.treeToValue(rawEnvelope, SecureEnvelope.class);
            } else if (rawEnvelope.isTextual()) {
                envelope = objectMapper.readValue(rawEnvelope.asText(), SecureEnvelope.class);
            } else {
                log.warn("correlate-message.api-key-verify: unexpected envelope JSON type");
                return false;
            }
            if (envelope == null) {
                return false;
            }
            EnvelopeClient envelopeClient = EngineApiKeyCommandHandlers.buildEnvelopeClient(
                    channel, osSecrets, namespace, objectMapper);
            ApiKeyVerifyPayload verifyPayload = envelopeClient.openJson(
                    envelope, AAD_API_KEY_VERIFY, ApiKeyVerifyPayload.class);
            if (verifyPayload == null) {
                return false;
            }
            DaemonWebhookKeyStore.VerificationResult verdict = webhookKeyStore.verifySecret(
                    verifyPayload.refId(),
                    verifyPayload.keyId(),
                    verifyPayload.presentedSecret(),
                    EngineApiKeyScope.ENGINE_WIDE);
            if (!verdict.valid()) {
                log.warn("correlate-message.api-key-rejected refId={} keyPrefix={} reason={}",
                        verifyPayload.refId(),
                        WebhookKeyFormat.safeKeyIdPrefix(verifyPayload.keyId()),
                        verdict.failureReason());
                return false;
            }
            return true;
        } catch (Exception ex) {
            log.warn("correlate-message.api-key-verify failed: {}", message(ex));
            return false;
        }
    }

    /** New-style webhook dispatch (fully-resolved address; {@code msg} sealed REF-only). */
    private DaemonCommandOutcome handleWebhook(CorrelateMessagePayload payload) {
        try {
            SqEventType eventType = SqEventType.from(payload.sqEventType());
            WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                    eventType, payload.workflowId(), payload.nodeId(), payload.correlationBusinessKey(),
                    payload.messageName(), payload.msg()));
            log.info("correlate-message.webhook.{} envId={} eventType={} name={} businessKey={} "
                            + "processInstanceId={}",
                    outcome.correlated() ? "done" : "no-match", payload.envId(), eventType,
                    payload.messageName(), payload.correlationBusinessKey(), outcome.processInstanceId());
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    new CorrelateMessageResult(outcome.correlated(), outcome.processInstanceId(),
                            payload.messageName(), outcome.processInstanceId())));
        } catch (IllegalArgumentException | IllegalStateException clientError) {
            return DaemonCommandOutcome.failed("correlate-message: " + message(clientError));
        } catch (Exception ex) {
            log.warn("correlate-message.webhook.failed envId={} eventType={} name={}: {}",
                    payload.envId(), payload.sqEventType(), payload.messageName(), ex.toString());
            return DaemonCommandOutcome.failed("correlate-message: " + message(ex));
        }
    }

    /**
     * A user-task approval decision from the control plane's decision service: completes exactly the
     * recorded task ({@code taskId}) on {@code activityId} of {@code workflowId} in the run,
     * publishing {@code variables.decision} on {@code task_output}.
     */
    private DaemonCommandOutcome handleUserTaskDecision(CorrelateMessagePayload payload) {
        try {
            Object decision = payload.variables() == null ? null
                    : payload.variables().get(MessageCorrelationService.DECISION_FIELD);
            MessageCorrelationOutcome outcome = correlationService.decideUserTask(
                    new MessageCorrelationService.UserTaskDecision(payload.taskId(), payload.activityId(),
                            payload.workflowId(), payload.correlationBusinessKey(),
                            decision == null ? null : decision.toString()));
            log.info("correlate-message.task.{} envId={} taskId={} nodeId={}",
                    outcome.correlated() ? "done" : "no-match", payload.envId(), payload.taskId(),
                    payload.activityId());
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    new CorrelateMessageResult(outcome.correlated(), outcome.executionId(), outcome.messageName(),
                            null)));
        } catch (Exception ex) {
            log.warn("correlate-message.task.failed envId={} taskId={}: {}",
                    payload.envId(), payload.taskId(), ex.toString());
            return DaemonCommandOutcome.failed("correlate-message: " + message(ex));
        }
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ApiKeyVerifyPayload(String refId, String keyId, String presentedSecret) {
    }

    /**
     * Inbound command payload (shared correlate-message wire contract). {@code correlationKey} and
     * {@code businessKey} may both be the run's {@code workflowExecutionId}; either is accepted.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record CorrelateMessagePayload(String envId, String messageName, String correlationKey,
                                   String businessKey, String eventKind, String activityId,
                                   Map<String, Object> variables, String sqEventType, String workflowId,
                                   String nodeId, Map<String, Object> msg, String taskId,
                                   JsonNode apiKeyVerification) {

        /** The correlation key to resolve the parked run: prefer {@code businessKey}, then {@code correlationKey}. */
        String correlationBusinessKey() {
            return StringUtils.isNotBlank(businessKey) ? businessKey : correlationKey;
        }

        boolean hasApiKeyVerification() {
            return apiKeyVerification != null && !apiKeyVerification.isNull() && !apiKeyVerification.isMissingNode();
        }
    }

    /**
     * Command-result {@code output} (FIXED shared correlate-message wire contract):
     * {@code {"correlated":<bool>,"executionId":"...?","messageName":"...","processInstanceId":"...?"}}.
     * {@code executionId}/{@code processInstanceId} are omitted (NON_NULL) on a no-match; {@code correlated} +
     * {@code messageName} are always present.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record CorrelateMessageResult(boolean correlated, String executionId, String messageName,
                                  String processInstanceId) {
    }
}
