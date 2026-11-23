package com.studioquack.sqs.engine.listener;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.springframework.stereotype.Component;

/**
 * Records a run-index status when a process instance completes, fails, or is cancelled.
 * A failure here never rolls back the engine transaction.
 */
@Slf4j
@Component
public class SqsExecutionStatusListener implements ActivitiEventListener {

    /** Coarse status shown on the execution-history list. */
    public enum RunStatus { COMPLETED, FAILED, CANCELED }

    @Override
    public void onEvent(ActivitiEvent event) {
        try {
            RunStatus newStatus = mapStatus(event);
            if (newStatus == null) {
                return; // not a lifecycle event we track
            }
            String businessKey = extractBusinessKey(event);
            if (businessKey == null || businessKey.isBlank()) {
                log.debug("SQS status listener: no businessKey on {} — skipping", event.getType());
                return;
            }
            log.info("SQS run-index status -> {} for executionId={}", newStatus, businessKey);
        } catch (Exception ex) {
            // Never let a status-index write break the engine.
            log.warn("SQS status listener failed for event {}: {}", event.getType(), ex.getMessage());
        }
    }

    @Override
    public boolean isFailOnException() {
        return false;
    }

    RunStatus mapStatus(ActivitiEvent event) {
        return switch (event.getType()) {
            case PROCESS_COMPLETED -> endedInFailure(event) ? RunStatus.FAILED : RunStatus.COMPLETED;
            case PROCESS_COMPLETED_WITH_ERROR_END_EVENT -> RunStatus.FAILED;
            case PROCESS_CANCELLED -> RunStatus.CANCELED;
            default -> null;
        };
    }

    /**
     * Whether a run that Activiti calls "completed" actually ended on the sandbox-error path.
     *
     * <p>{@code PROCESS_COMPLETED_WITH_ERROR_END_EVENT} never fires for us: the shared
     * {@code Event_SandboxErrorEnd} the generator routes every caught {@code BpmnError} into is a plain
     * end event, deliberately so — an Activiti error end event with no catching boundary above it
     * aborts the instance with "no catching boundary event found" instead of ending it. The engine
     * therefore reports an ordinary completion, and reading only the event type marked every failed run
     * as successful, which is what kept a red step from ever reaching the live execution animation.
     *
     * <p>So we ask the run itself. The {@link EngineVariableNames#ERROR_CODE} sentinel is what made the
     * error gate raise the {@code BpmnError} in the first place, and it survives on the instance to the
     * end — the same signal {@code SubProcessCompletionListener} already uses to fail a sub-process.
     */
    private boolean endedInFailure(ActivitiEvent event) {
        DelegateExecution execution = executionOf(event);
        if (execution == null) {
            return false;
        }
        try {
            return EngineVariableNames.isFailureSentinel(execution.getVariable(EngineVariableNames.ERROR_CODE));
        } catch (RuntimeException ex) {
            // The instance is being torn down; an unreadable variable is not evidence of failure.
            log.debug("SQS status listener: could not read the error sentinel ({})", ex.toString());
            return false;
        }
    }

    private String extractBusinessKey(ActivitiEvent event) {
        DelegateExecution execution = executionOf(event);
        return execution == null ? null : execution.getProcessInstanceBusinessKey();
    }

    private DelegateExecution executionOf(ActivitiEvent event) {
        if (event instanceof ActivitiEntityEvent entityEvent
                && entityEvent.getEntity() instanceof DelegateExecution execution) {
            return execution;
        }
        return null;
    }
}
