package com.studioquack.sqs.engine.listener;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A sandbox failure routes through the shared {@code Event_SandboxErrorEnd}, which is a PLAIN end
 * event, so Activiti reports an ordinary {@code PROCESS_COMPLETED}. Mapping on the event type alone
 * therefore recorded every failed run as successful — and a run that reports success can never be
 * drawn as a red step in the live execution animation.
 */
class SqsExecutionStatusListenerTest {

    private final SqsExecutionStatusListener listener = new SqsExecutionStatusListener();

    private static ActivitiEvent event(ActivitiEventType type, Object errorCode) {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE)).thenReturn(errorCode);
        ActivitiEntityEvent event = mock(ActivitiEntityEvent.class);
        when(event.getType()).thenReturn(type);
        when(event.getEntity()).thenReturn(execution);
        return event;
    }

    @Test
    @DisplayName("a clean completion is COMPLETED")
    void cleanCompletion() {
        assertThat(listener.mapStatus(event(ActivitiEventType.PROCESS_COMPLETED, null)))
                .isEqualTo(SqsExecutionStatusListener.RunStatus.COMPLETED);
    }

    @Test
    @DisplayName("a completion carrying the error sentinel is FAILED")
    void completionWithSentinelIsFailure() {
        assertThat(listener.mapStatus(event(ActivitiEventType.PROCESS_COMPLETED, "DISPATCH_UNRESOLVED")))
                .as("the run reached the shared sandbox-error end; Activiti still calls that 'completed'")
                .isEqualTo(SqsExecutionStatusListener.RunStatus.FAILED);
    }

    @Test
    @DisplayName("a blank sentinel is not a failure")
    void blankSentinelIsNotFailure() {
        assertThat(listener.mapStatus(event(ActivitiEventType.PROCESS_COMPLETED, "   ")))
                .isEqualTo(SqsExecutionStatusListener.RunStatus.COMPLETED);
    }

    @Test
    @DisplayName("a real error end event is FAILED without inspecting variables")
    void errorEndEventIsFailure() {
        assertThat(listener.mapStatus(event(ActivitiEventType.PROCESS_COMPLETED_WITH_ERROR_END_EVENT, null)))
                .isEqualTo(SqsExecutionStatusListener.RunStatus.FAILED);
    }

    @Test
    @DisplayName("a cancellation is CANCELED")
    void cancellation() {
        assertThat(listener.mapStatus(event(ActivitiEventType.PROCESS_CANCELLED, null)))
                .isEqualTo(SqsExecutionStatusListener.RunStatus.CANCELED);
    }

    @Test
    @DisplayName("an untracked lifecycle event maps to nothing")
    void untrackedEvent() {
        assertThat(listener.mapStatus(event(ActivitiEventType.TASK_CREATED, null))).isNull();
    }

    @Test
    @DisplayName("an event with no execution entity never reads as a failure")
    void nonEntityEvent() {
        ActivitiEvent bare = mock(ActivitiEvent.class);
        when(bare.getType()).thenReturn(ActivitiEventType.PROCESS_COMPLETED);

        assertThat(listener.mapStatus(bare)).isEqualTo(SqsExecutionStatusListener.RunStatus.COMPLETED);
    }
}
