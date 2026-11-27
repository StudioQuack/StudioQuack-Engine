package com.studioquack.sqs.sandbox.helperchan;

import java.util.List;
import java.util.Map;

/**
 * Structured ingress outcome reported by a node invoked in {@code invocationMode = "ingress"}
 * (via any of the three supported channels: {@code /v1/helper/webhook-*} HTTP endpoints,
 * {@code sqEvent:::} stdout markers, or stateful {@code /invoke} direct JSON keys).
 */
public record WebhookIngressOutcome(
        WebhookChallenge challenge,
        WebhookReject reject,
        List<WebhookEmittedEvent> events) {

    /** Hard per-invocation cap on emitted webhook events to bound memory and fan-out. */
    public static final int MAX_EVENTS = 100;

    public WebhookIngressOutcome {
        events = events == null ? List.of() : List.copyOf(events);
    }

    /** True when none of challenge, reject, or emitted events were reported. */
    public boolean isEmpty() {
        return challenge == null && reject == null && events.isEmpty();
    }

    /** Verification handshake response (e.g. GET challenge or URL-verification reply). */
    public record WebhookChallenge(int status, String contentType, String body) {
        public WebhookChallenge {
            status = status <= 0 ? 200 : status;
            contentType = (contentType == null || contentType.isBlank()) ? "text/plain" : contentType;
            body = body == null ? "" : body;
        }
    }

    /** Clean rejection raised by node code (e.g. invalid provider HMAC signature). */
    public record WebhookReject(int status, String reason) {
        public WebhookReject {
            status = status <= 0 ? 401 : status;
            reason = (reason == null || reason.isBlank()) ? "unauthorized" : reason;
        }
    }

    /** One normalized inbound event emitted by node code for engine correlation. */
    public record WebhookEmittedEvent(
            String idempotencyKey,
            String correlationKey,
            Map<String, Object> payload) {
        public WebhookEmittedEvent {
            payload = payload == null ? Map.of() : payload;
        }
    }
}
