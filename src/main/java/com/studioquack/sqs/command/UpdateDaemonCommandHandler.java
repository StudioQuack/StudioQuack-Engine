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
 * Starts an in-place daemon update. The result is the acceptance; once the release is verified, runs with a
 * node in flight are killed and this process is replaced.
 */
@Component
@RequiredArgsConstructor
public class UpdateDaemonCommandHandler implements DaemonCommandHandler {

    public static final String TYPE = "update-daemon";

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
                    Map.of("status", "accepted", "message", updateService.begin())));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("update-daemon failed: "
                    + (e.getMessage() == null ? e.toString() : e.getMessage()));
        }
    }
}
