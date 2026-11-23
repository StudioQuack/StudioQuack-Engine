package com.studioquack.sqs.sandbox.stateful.shell;

import java.util.Map;

/**
 * Daemon → the control plane shell-output callback seam. Implemented later by the HTTP callback
 * client (POST {@code The session manager depends only on this interface so unit tests and early
 * wiring need no HTTP stack.
 */
@FunctionalInterface
public interface ShellOutputCallback {

    /**
     * Deliver one output or closed frame for {@code sessionId} under {@code envId}. Implementations
     * must not throw; failures are logged by the caller/manager path.
     *
     * @param envId Environment the session was opened against.
     * @param sessionId The control plane-minted session id.
     */
    void postOutput(String envId, String sessionId, Map<String, Object> frame);
}
