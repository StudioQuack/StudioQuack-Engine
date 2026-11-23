package com.studioquack.sqs.engine.halt;

/**
 * Work was refused because the engine is halted ({@link ExecutionHaltGuard}). The message starts with
 * the error code, so the control plane relay can map {@code Error_NodeQuotaExhausted} to its 429.
 */
public class ExecutionHaltedException extends RuntimeException {

    public ExecutionHaltedException(String refusal) {
        super(refusal);
    }
}
