package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * Wire body for helper-channel throw endpoints ({@code POST /v1/helper/escalate}, {@code /signal},
 * {@code /message}). Fields are endpoint-specific; unused ones stay null.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record HelperEventRequest(
        String escalationCode,
        String message,
        Map<String, Object> data,
        String signalName,
        String scope,
        String messageName,
        String correlationKey) {
}
