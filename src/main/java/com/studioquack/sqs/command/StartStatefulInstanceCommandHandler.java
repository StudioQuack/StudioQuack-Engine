package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager.StartResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles the BE-pushed {@code start-stateful-instance} command: bring up the env's single stateful instance
 * for an image ref (named container + named persistent volume, uncapped, NOT {@code --rm}) and report its
 * identity so the BE records {@code instanceId}/{@code containerId}. Wire contract (matches the BE
 * {@code StatefulInstanceServiceImpl} payload builder): payload: {envId, imageRef, stateful} result:
 * {instanceId, containerId, status, health} status RUNNING on success Stateful instances run uncapped (
 * {@link StatefulCaps#uncapped()}); any legacy {@code caps} object in older payloads is ignored.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StartStatefulInstanceCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "start-stateful-instance";

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
            return DaemonCommandOutcome.failed("start-stateful-instance: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String imageRef = text(payload, "imageRef");
        if (imageRef == null || imageRef.isBlank()) {
            return DaemonCommandOutcome.failed("start-stateful-instance: payload missing 'imageRef'");
        }
        boolean stateful = payload.path("stateful").asBoolean(true);
        StatefulCaps caps = StatefulCaps.uncapped();
        try {
            // One live instance per (env, imageRef). The manager enforces it by


            // Live container is never bounced out from under in-flight work.
            StartResult result = manager.start(envId, imageRef, stateful, caps);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("start-stateful-instance.failed env={} imageRef={}: {}", envId, imageRef, ex.toString());
            return DaemonCommandOutcome.failed("start-stateful-instance failed: " + message(ex));
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
