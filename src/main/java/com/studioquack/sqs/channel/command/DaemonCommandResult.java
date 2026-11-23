package com.studioquack.sqs.channel.command;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Wire record the daemon ships in a heartbeat's {@code commandResults[]} after executing a pushed command. The
 * BE matches it by {@code commandId}, drops the command from its pending queue, and appends the blob to the
 * env's recent-results ring.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DaemonCommandResult(String commandId, String type, String status, String output, String error) {

    /**
     * Builds a wire result for {@code command} from its terminal {@code outcome}.
     */
    public static DaemonCommandResult of(DaemonCommand command, DaemonCommandOutcome outcome) {
        String output = (outcome.output() == null || outcome.output().isEmpty()) ? null : outcome.output();
        String error = (outcome.error() == null || outcome.error().isEmpty()) ? null : outcome.error();
        return new DaemonCommandResult(command.commandId(), command.type(), outcome.status(), output, error);
    }
}
