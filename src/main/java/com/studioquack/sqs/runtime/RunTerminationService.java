package com.studioquack.sqs.runtime;

import com.studioquack.sqs.aimodel.LlmStreamCancellationRegistry;
import com.studioquack.sqs.engine.SqsEngine;
import lombok.RequiredArgsConstructor;
import org.activiti.engine.runtime.ProcessInstance;
import org.springframework.stereotype.Component;

/**
 * Kills one run on this engine: marks it cancelled so late logs and statuses from its in-flight
 * nodes are suppressed, aborts its LLM streams, reaps its sandbox dispatches and deletes the
 * process instance. The engine's {@code PROCESS_CANCELLED} then reports the run CANCELED to the
 * control plane; history keeps it. Used by the control plane's {@code cancel-instance} and by an
 * in-place update, which kills the runs still working when it hands over.
 *
 * <p>Idempotent: a run that is already gone answers {@code terminated=false} after still reaping
 * any dispatch this daemon tracks for it — a process instance can vanish from the engine (deleted,
 * ended) while its background sandbox thread or container is still live.
 */
@Component
@RequiredArgsConstructor
public class RunTerminationService {

    private final SqsEngine sqsEngine;
    private final DispatchCancellationRegistry dispatchCancellationRegistry;
    private final CancelledRunGuard cancelledRunGuard;
    private final LlmStreamCancellationRegistry llmStreamCancellationRegistry;

    /** What one kill did; {@code terminated} is false when the run was already gone. */
    public record Outcome(String processInstanceId, boolean terminated, int reapedDispatches, int abortedStreams) {
    }

    /**
     * @param runId  the process-instance id, or the workflowExecutionId (business key) the control
     *               plane addresses runs by
     * @param reason recorded as the delete reason in the engine history
     */
    public Outcome terminate(String runId, String reason) {
        String pid = sqsEngine.resolveProcessInstanceId(runId);
        if (!sqsEngine.isProcessInstanceActive(pid)) {
            int reaped = dispatchCancellationRegistry.cancelRun(pid);
            // The PI is gone but its host-side LLM stream may still be pumping tokens (the container
            // reap raced it). runId doubles as the business key when the run was addressed by
            // workflowExecutionId — abort is a no-op when nothing matches.
            int aborted = abortLlmStreams(pid, runId);
            return new Outcome(pid, false, reaped, aborted);
        }
        // Mark the run cancelled BEFORE reaping/deleting so any late sandbox log/status the
        // in-flight dispatch(es) still produce is suppressed rather than resurrecting the run.
        ProcessInstance instance = sqsEngine.findProcessInstance(pid);
        String businessKey = instance == null ? null : instance.getBusinessKey();
        if (businessKey != null && !businessKey.isBlank()) {
            cancelledRunGuard.markCancelled(businessKey);
        }
        // Abort the run's in-flight LLM provider streams BEFORE the container reap: closing the
        // upstream read makes the local LLM server stop generating, so no late answer can be
        // produced (and nothing can land in the conversation history) after the kill.
        int aborted = abortLlmStreams(businessKey, runId);
        int reaped = dispatchCancellationRegistry.cancelRun(pid);
        sqsEngine.deleteProcessInstance(pid, reason);
        return new Outcome(pid, true, reaped, aborted);
    }

    /** Aborts in-flight LLM streams under every key the run may be addressed by (null-safe). */
    private int abortLlmStreams(String... runKeys) {
        int aborted = 0;
        for (String key : runKeys) {
            aborted += llmStreamCancellationRegistry.abortRun(key);
        }
        return aborted;
    }
}
