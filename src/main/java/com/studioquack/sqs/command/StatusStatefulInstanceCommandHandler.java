package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager.StatusResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles the BE-pushed {@code status-stateful-instance} command: {@code podman inspect} the tracked
 * instance's container state + healthcheck and report it so the BE reconciles the row. Wire contract: payload:
 * {envId, instanceId} result: {status, containerId, health} Never throws: an untracked instance reports
 * {@code status:UNKNOWN}; only a malformed / under-specified payload is a {@code failed} outcome.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatusStatefulInstanceCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "status-stateful-instance";

    private final ObjectMapper objectMapper;
    private final StatefulInstanceManager manager;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        JsonNode payload;
        try {
            payload = objectMapper.readTree(command.payload() == null ? "{}" : command.payload());
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("status-stateful-instance: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String instanceId = text(payload, "instanceId");
        if (instanceId == null || instanceId.isBlank()) {
            return DaemonCommandOutcome.failed("status-stateful-instance: payload missing 'instanceId'");
        }
        try {
            StatusResult result = manager.status(envId, instanceId);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("status-stateful-instance.failed env={} instanceId={}: {}", envId, instanceId, ex.toString());
            return DaemonCommandOutcome.failed("status-stateful-instance failed: " + message(ex));
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
