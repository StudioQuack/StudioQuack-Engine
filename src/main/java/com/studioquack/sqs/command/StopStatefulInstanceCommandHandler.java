package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager.StopResult;
import com.studioquack.sqs.sandbox.stateful.shell.StatefulShellSessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles the BE-pushed {@code stop-stateful-instance} command: stop + remove the named container for a
 * tracked instance (the persistent volume is KEPT so a later start reuses its data). Wire contract: payload:
 * {envId, instanceId} result: {status} status STOPPED Idempotent + never throws: stopping an untracked /
 * already-stopped instance still reports STOPPED.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StopStatefulInstanceCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "stop-stateful-instance";

    private final ObjectMapper objectMapper;
    private final StatefulInstanceManager manager;
    private final StatefulShellSessionManager shellSessions;

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
            return DaemonCommandOutcome.failed("stop-stateful-instance: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String instanceId = text(payload, "instanceId");
        if (instanceId == null || instanceId.isBlank()) {
            return DaemonCommandOutcome.failed("stop-stateful-instance: payload missing 'instanceId'");
        }
        try {
            StopResult result = manager.stop(envId, instanceId);
            try {
                shellSessions.destroySessionsForInstance(instanceId);
            } catch (Exception shellEx) {
                log.warn("stop-stateful-instance: shell session cleanup failed instanceId={}: {}",
                        instanceId, shellEx.toString());
            }
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("stop-stateful-instance.failed env={} instanceId={}: {}", envId, instanceId, ex.toString());
            return DaemonCommandOutcome.failed("stop-stateful-instance failed: " + message(ex));
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
