package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.runtime.RunTerminationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Repair — {@code cancel-instance}: terminates a run and removes it from the runtime (
 * {@link RunTerminationService}); the cancelled run stays recorded in history. Addressed by process-instance
 * identity (never an incidentKey).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CancelInstanceCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (must be added to the control plane).
     */
    public static final String TYPE = "cancel-instance";

    private final ObjectMapper objectMapper;
    private final RunTerminationService runTermination;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        RepairCommandPayload payload;
        try {
            payload = objectMapper.readValue(command.payload(), RepairCommandPayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("cancel-instance: malformed payload: " + message(parse));
        }
        if (payload == null || !payload.hasProcessInstance()) {
            return DaemonCommandOutcome.failed("cancel-instance: payload requires processInstanceId");
        }

        try {
            // The BE may address the run by its workflowExecutionId (business key); the service maps
            // it to the live PI. Idempotent: an already-gone instance is a no-op success.
            RunTerminationService.Outcome outcome =
                    runTermination.terminate(payload.processInstanceId(), payload.reasonOrDefault());
            if (outcome.terminated()) {
                log.info("repair.cancel-instance.done pi={} reason={} reapedDispatches={} abortedLlmStreams={}",
                        outcome.processInstanceId(), payload.reasonOrDefault(), outcome.reapedDispatches(),
                        outcome.abortedStreams());
            } else {
                log.info("repair.cancel-instance.noop pi={} (already ended/removed) reapedDispatches={} abortedLlmStreams={}",
                        outcome.processInstanceId(), outcome.reapedDispatches(), outcome.abortedStreams());
            }
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    new CancelResult(outcome.processInstanceId(), outcome.terminated())));
        } catch (Exception ex) {
            log.warn("repair.cancel-instance.failed pi={}: {}", payload.processInstanceId(), ex.toString());
            return DaemonCommandOutcome.failed("cancel-instance failed: " + message(ex));
        }
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Success output shipped to the BE.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record CancelResult(String processInstanceId, boolean cancelled) {
    }
}
