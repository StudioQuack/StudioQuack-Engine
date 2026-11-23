package com.studioquack.sqs.sandbox.testrun;

import com.studioquack.sqs.sandbox.helperchan.HelperPipeChannel;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;

/**
 * Plants + launches the in-container helper broker and returns a live pipe, or {@code null}
 * when the broker could not start. Never throws out to the caller — log and return null.
 *
 * <p>Tests inject an implementation that returns null so {@code ProcessBuilder} is never used.
 */
@FunctionalInterface
public interface BrokerStarter {

    /**
     * @param cliPath     resolved podman/CLI path
     * @param containerId live test container id
     * @param router      the shared helper-channel router the pipe dispatches into
     * @return a live {@link HelperPipeChannel}, or {@code null} if the broker could not start
     */
    HelperPipeChannel start(String cliPath, String containerId, HelperRequestRouter router);
}
