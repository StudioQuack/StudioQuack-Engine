package com.studioquack.sqs.webhook;

import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.sandbox.helperchan.WebhookIngressOutcome;

import java.util.Map;

/**
 * Seam used by {@code IngestWebhookCommandHandler} to execute a bound node's own code
 * (Python / JS / TS warm pool script, custom Java/BYO container, or built-in library container)
 * in {@code invocationMode = "ingress"} with resolved {@code $secretRef} parameters.
 */
public interface WebhookNodeInvoker {

    WebhookInvocationResult invoke(WebhookInvocationInput input);

    record WebhookInvocationInput(
            String envId,
            String workflowId,
            String nodeId,
            String activeSelectRole,
            TemplateDto template,
            Map<String, Object> webhookRequest) {
    }

    record WebhookInvocationResult(
            boolean ok,
            WebhookIngressOutcome outcome,
            String errorCode,
            String errorMessage) {

        public static WebhookInvocationResult success(WebhookIngressOutcome outcome) {
            return new WebhookInvocationResult(true, outcome, null, null);
        }

        public static WebhookInvocationResult failure(String errorCode, String errorMessage) {
            return new WebhookInvocationResult(false, null, errorCode, errorMessage);
        }
    }
}
