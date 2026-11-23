package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.update.DaemonUpdateService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Counts the runs an update would kill — those with a node in flight — so the settings panel can warn before
 * the operator confirms. Message and timer waits are not in that registry and survive the restart.
 */
@Component
@RequiredArgsConstructor
public class UpdatePreviewCommandHandler implements DaemonCommandHandler {

    public static final String TYPE = "update-preview";

    private final ObjectMapper objectMapper;
    private final DaemonUpdateService updateService;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    Map.of("runningInstances", updateService.runningInstances())));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("update-preview failed: "
                    + (e.getMessage() == null ? e.toString() : e.getMessage()));
        }
    }
}
