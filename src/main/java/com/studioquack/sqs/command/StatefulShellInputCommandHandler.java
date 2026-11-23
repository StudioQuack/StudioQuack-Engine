package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.shell.StatefulShellSessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles the BE-pushed {@code stateful-shell-input} command (fire-and-forget style). Wire contract: payload:
 * {envId, sessionId, data} result: {} on success (stdout/stderr stream via daemon→BE shell-output callback)
 * Unknown / already-closed sessions return {@link DaemonCommandOutcome#failed(String)} so the BE can surface
 * the error; output does not ride the command result.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatefulShellInputCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type.
     */
    public static final String TYPE = "stateful-shell-input";

    private final ObjectMapper objectMapper;
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
            return DaemonCommandOutcome.failed("stateful-shell-input: malformed payload: " + message(parse));
        }

        String sessionId = text(payload, "sessionId");
        if (sessionId == null || sessionId.isBlank()) {
            return DaemonCommandOutcome.failed("stateful-shell-input: payload missing 'sessionId'");
        }
        // Data may be empty (e.g. bare Enter) — still a valid write
        String data = text(payload, "data");
        if (data == null) {
            data = "";
        }

        try {
            shellSessions.writeInput(sessionId, data);
            return DaemonCommandOutcome.ok("{}");
        } catch (IllegalStateException unknown) {
            log.debug("stateful-shell-input: unknown/closed sessionId={}: {}",
                    sessionId, unknown.toString());
            return DaemonCommandOutcome.failed(message(unknown));
        } catch (Exception ex) {
            log.warn("stateful-shell-input failed sessionId={}: {}", sessionId, ex.toString());
            return DaemonCommandOutcome.failed("stateful-shell-input failed: " + message(ex));
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
