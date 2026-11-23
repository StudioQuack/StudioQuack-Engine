package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.MessageCorrelationOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.SqEventType;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * {@code correlate-message}: the daemon side of client-side message-event correlation for self-hosted
 * environments. An external caller (a webhook) needs to deliver a BPMN message / intermediate message event to
 * a running workflow instance on this env's embedded Activiti engine.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CorrelateMessageCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (the control plane).
     */
    public static final String TYPE = "correlate-message";

    private final ObjectMapper objectMapper;
    private final MessageCorrelationService correlationService;

    /**
     * Null in slice tests that construct this handler by hand.
     */
    private com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
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
        // A halted engine takes no delivery at all — a start, a catch into a parked run, or a user-task
        // decision — so the caller sees the refusal and can retry, instead of a message consumed by a
        // run that cannot move.
        String haltRefusal = executionHaltGuard == null ? null : executionHaltGuard.refusal();
        if (haltRefusal != null) {
            return DaemonCommandOutcome.failed("correlate-message: " + haltRefusal);
        }

        // CORE (webhook addressing) — a new-style payload carries sqEventType, resolved server-side
        // by the BE relay (the control plane lookup) BEFORE forwarding: elementNodeId/name/businessKey are
        // already resolved, and msg is the raw payload to seal REF-only. A payload without
        // sqEventType is the pre-CORE legacy contract (messageName/eventKind), unaffected.
        if (StringUtils.isNotBlank(payload.sqEventType())) {
            return handleWebhook(payload);
        }
        if ("TASK".equalsIgnoreCase(StringUtils.trimToEmpty(payload.eventKind()))) {
            return handleUserTaskDecision(payload);
        }

        try {
            // Delegate to the shared correlation core — same REF-only guard the public HTTP ingress
            // uses.: dispatch by eventKind (null/absent ⇒ MESSAGE, the pre- default) so start /
            // timer / intermediate / signal / message all reach the engine.

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
     * CORE — the new-style webhook dispatch (fully-resolved address; {@code msg} sealed REF-only).
     */
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
            // Malformed address / REF-ONLY violation — the BE ingress maps this to 400.
            return DaemonCommandOutcome.failed("correlate-message: " + message(clientError));
        } catch (Exception ex) {
            log.warn("correlate-message.webhook.failed envId={} eventType={} name={}: {}",
                    payload.envId(), payload.sqEventType(), payload.messageName(), ex.toString());
            return DaemonCommandOutcome.failed("correlate-message: " + message(ex));
        }
    }

    /**
     * A user-task approval decision from the control plane's decision service: completes exactly the recorded task
     * ({@code taskId}) on {@code activityId} of {@code workflowId} in the run, publishing
     * {@code variables.decision} on {@code task_output}.
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

    /**
     * Inbound command payload (shared correlate-message wire contract). {@code correlationKey} and
     * {@code businessKey} may both be the run's {@code workflowExecutionId}; either is accepted.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record CorrelateMessagePayload(String envId, String messageName, String correlationKey,
                                   String businessKey, String eventKind, String activityId,
                                   Map<String, Object> variables, String sqEventType, String workflowId,
                                   String nodeId, Map<String, Object> msg, String taskId) {

        /**
         * The correlation key to resolve the parked run: prefer {@code businessKey}, then {@code correlationKey}.
         */
        String correlationBusinessKey() {
            return StringUtils.isNotBlank(businessKey) ? businessKey : correlationKey;
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
