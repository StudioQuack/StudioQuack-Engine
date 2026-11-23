package com.studioquack.sqs.channel.command;

/**
 * SPI implemented by daemon subsystems that execute a BE-pushed command type. This is the extension point (the
 * BE push-command refactor) plugs into: it registers {@code deploy} and {@code start-run} handlers (deploy →
 * {@code BpmnGenerationService} + {@code SqsEngine.deploy}; start-run →
 * {@code SqsEngine.startLatest(processKey, businessKey=workflowExecutionId, REF seed vars)}) without touching
 * the channel transport.
 */
public interface DaemonCommandHandler {

    /**
     * The command type this handler serves, e.g. {@code "deploy"} or {@code "start-run"}. MUST be stable and match
     * the BE wire type.
     */
    String type();

    /**
     * Executes the command and returns its terminal outcome. May throw; the {@link DaemonCommandRegistry} maps a
     * thrown exception to a {@code failed} outcome with the exception message.
     *
     * @param command the pushed command (id + type + opaque JSON payload)
     * @return the terminal outcome reported back to the BE on the next heartbeat
     * @throws Exception on any unhandled failure (converted to a failed outcome)
     */
    DaemonCommandOutcome handle(DaemonCommand command) throws Exception;
}
