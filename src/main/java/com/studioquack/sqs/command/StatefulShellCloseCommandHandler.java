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
 * Handles the BE-pushed {@code stateful-shell-close} command. Wire contract: payload: {envId, sessionId,
 * reason?} result: {} Idempotent ok: {@link StatefulShellSessionManager#close} throws
 * {@link IllegalStateException} when the session is already gone.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatefulShellCloseCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type.
     */
    public static final String TYPE = "stateful-shell-close";

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
            return DaemonCommandOutcome.failed("stateful-shell-close: malformed payload: " + message(parse));
        }

        String sessionId = text(payload, "sessionId");
        if (sessionId == null || sessionId.isBlank()) {
            return DaemonCommandOutcome.failed("stateful-shell-close: payload missing 'sessionId'");
        }
        String reason = text(payload, "reason");
        if (reason == null || reason.isBlank()) {
            reason = StatefulShellSessionManager.REASON_CLOSED_BY_USER;
        }

        try {
            shellSessions.close(sessionId, reason);
            return DaemonCommandOutcome.ok("{}");
        } catch (IllegalStateException unknown) {
            // Idempotent: already closed / forgotten — still ok so BE/FE teardown is clean.
            log.debug("stateful-shell-close: session already gone sessionId={}: {}",
                    sessionId, unknown.toString());
            return DaemonCommandOutcome.ok("{}");
        } catch (Exception ex) {
            log.warn("stateful-shell-close failed sessionId={}: {}", sessionId, ex.toString());
            return DaemonCommandOutcome.failed("stateful-shell-close failed: " + message(ex));
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
