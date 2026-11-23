package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.maintenance.MaintenanceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * {@code logs}: returns a BOUNDED tail of the newest daemon log file. Accepts an optional JSON payload
 * {@code {"lines": <int>, "maxBytes": <long>}} to size the tail (both clamped by
 * {@link MaintenanceService#tailLogs(int, long)} so the result stays small enough for the BE recent-results
 * ring).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LogsCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "logs";

    /**
     * Default trailing-line count when the payload omits {@code lines}.
     */
    private static final int DEFAULT_LINES = 200;

    /**
     * Default trailing-byte cap when the payload omits {@code maxBytes} (64 KiB).
     */
    private static final long DEFAULT_MAX_BYTES = 65_536L;

    private final ObjectMapper objectMapper;
    private final MaintenanceService maintenanceService;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            int lines = DEFAULT_LINES;
            long maxBytes = DEFAULT_MAX_BYTES;
            String payload = command == null ? null : command.payload();
            if (payload != null && !payload.isBlank()) {
                JsonNode node = objectMapper.readTree(payload);
                if (node.hasNonNull("lines")) {
                    lines = node.get("lines").asInt(DEFAULT_LINES);
                }
                if (node.hasNonNull("maxBytes")) {
                    maxBytes = node.get("maxBytes").asLong(DEFAULT_MAX_BYTES);
                }
            }
            MaintenanceService.LogsResult result = maintenanceService.tailLogs(lines, maxBytes);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("logs failed: " + message(e));
        }
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
