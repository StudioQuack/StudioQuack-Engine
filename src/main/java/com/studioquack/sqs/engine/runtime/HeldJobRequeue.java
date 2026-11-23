package com.studioquack.sqs.engine.runtime;

import com.studioquack.sqs.engine.SqsEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.runtime.Job;
import org.springframework.stereotype.Component;

/**
 * The single scrub+move that brings a run HELD at {@code hold_<nodeId>} back to life: every repair
 * that requeues a failure-hold dead-letter job ({@code retry-incident}, {@code skip-step}) goes
 * through here so the two steps can never drift apart.
 *
 * <p><b>Why the scrub comes first.</b> {@code FailureHoldDelegate} re-throws (and the run re-holds)
 * while the {@link EngineVariableNames#ERROR_SENTINELS} family is set, so only a scrubbed requeue
 * passes the hold and loops the token along {@code flow_<nodeId>_hold_to_retry} back to
 * {@code node_<nodeId>}, which re-dispatches. The scrub is the NON-local
 * {@code removeVariables} walk (see {@link SqsEngine#removeVariables}): the sentinels may sit on
 * the process-instance scope while the job's execution is a child of it. The {@code __sq_skip_node}
 * pin is deliberately NOT scrubbed here — a skip-step pins it right before the requeue.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HeldJobRequeue {

    /** Retries granted when moving a dead-letter back to executable (>=1 so the executor picks it up). */
    private static final int RETRY_COUNT = 1;

    private final SqsEngine sqsEngine;

    /**
     * Scrubs the failure sentinels off the job's execution BEFORE moving the job back to
     * executable, so the re-run of the hold task passes and the failed node re-dispatches.
     */
    public void requeue(Job job) {
        sqsEngine.removeVariables(job.getExecutionId(), EngineVariableNames.ERROR_SENTINELS);
        sqsEngine.retryDeadLetterJob(job.getId(), RETRY_COUNT);
    }
}
