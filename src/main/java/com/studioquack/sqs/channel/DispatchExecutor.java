package com.studioquack.sqs.channel;

import java.time.Duration;

/**
 * SPI that runs one exec-channel {@code dispatch-job} locally and returns the verbatim the control plane JSON
 * the BE expects back on {@code POST …/exec-results/{dispatchId}}. The actual sandbox execution is owned by
 * /E5 (the in-process Podman dispatch + engine resume).
 */
@FunctionalInterface
public interface DispatchExecutor {

    /**
     * Runs the dispatch and returns the JSON-encoded the control plane.
     *
     * @param requestJson the verbatim the control plane JSON the BE parked
     * @param deadline the max wall-clock the caller should honour
     * @return the control plane JSON to POST back to the BE
     * @throws Exception on any failure (the client posts to {@code …/error})
     */
    String dispatch(String requestJson, Duration deadline) throws Exception;
}
