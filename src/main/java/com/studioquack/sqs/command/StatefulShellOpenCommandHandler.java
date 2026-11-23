package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.shell.StatefulShellSessionManager;
import com.studioquack.sqs.sandbox.stateful.shell.StatefulShellSessionManager.OpenResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handles the BE-pushed {@code stateful-shell-open} command (synchronous). Wire contract (matches the control
 * plane / BE): payload: {envId, instanceId, sessionId, cols, rows} result: {sessionId, shell} sessionId echoed
 * VERBATIM from the payload The backend mints {@code sessionId}; a missing or different sessionId in the
 * result is treated as a refusal.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatefulShellOpenCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type.
     */
    public static final String TYPE = "stateful-shell-open";

    private final ObjectMapper objectMapper;
    private final StatefulInstanceManager instanceManager;
    private final StatefulShellSessionManager shellSessions;
    private final OciCli cli;

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
            return DaemonCommandOutcome.failed("stateful-shell-open: malformed payload: " + message(parse));
        }

        String envId = text(payload, "envId");
        String instanceId = text(payload, "instanceId");
        String sessionId = text(payload, "sessionId");
        if (sessionId == null || sessionId.isBlank()) {
            return DaemonCommandOutcome.failed("stateful-shell-open: payload missing 'sessionId'");
        }
        if (instanceId == null || instanceId.isBlank()) {
            return DaemonCommandOutcome.failed("stateful-shell-open: payload missing 'instanceId'");
        }

        int cols = payload.path("cols").asInt(80);
        int rows = payload.path("rows").asInt(24);

        StatefulInstance inst = instanceManager.byInstanceId(instanceId).orElse(null);
        if (inst == null) {
            return DaemonCommandOutcome.failed(
                    "stateful-shell-open: unknown instanceId " + instanceId);
        }

        OpenResult result = shellSessions.open(envId, instanceId, sessionId, cols, rows, inst, cli);
        if (!result.ok()) {
            log.info("stateful-shell-open refused sessionId={} instanceId={}: {}",
                    sessionId, instanceId, result.error());
            return DaemonCommandOutcome.failed(
                    result.error() == null ? "stateful-shell-open: refused" : result.error());
        }

        // Echo the BE-minted sessionId VERBATIM — never mint a replacement.
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sessionId", sessionId);
        body.put("shell", result.shell() == null ? StatefulShellSessionManager.SHELL : result.shell());
        try {
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(body));
        } catch (Exception ex) {
            return DaemonCommandOutcome.failed("stateful-shell-open: serialize failed: " + message(ex));
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
