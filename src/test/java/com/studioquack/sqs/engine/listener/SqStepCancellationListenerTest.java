package com.studioquack.sqs.engine.listener;

import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import org.activiti.engine.delegate.event.ActivitiActivityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * {@link SqStepCancellationListener} maps engine cancel events onto the dispatch registry —
 * {@code wait_X} → {@code cancelStep(pi, X)}, whole-process → {@code cancelRun}.
 */
class SqStepCancellationListenerTest {

    private DispatchCancellationRegistry registry;
    private SqStepCancellationListener listener;

    @BeforeEach
    void setUp() {
        registry = mock(DispatchCancellationRegistry.class);
        listener = new SqStepCancellationListener(registry);
    }

    @Test
    void isFailOnException_isFalse() {
        assertThat(listener.isFailOnException()).isFalse();
    }

    @Test
    void activityCancelled_waitPrefix_callsCancelStepWithSuffix() {
        listener.onEvent(activityEvent(ActivitiEventType.ACTIVITY_CANCELLED, "wait_scriptN", "pi-1"));

        verify(registry).cancelStep("pi-1", "scriptN");
        verify(registry, never()).cancelRun(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void activityCancelled_nodePrefix_doesNotCallCancelStep() {
        listener.onEvent(activityEvent(ActivitiEventType.ACTIVITY_CANCELLED, "node_scriptN", "pi-1"));

        verify(registry, never()).cancelStep(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        verify(registry, never()).cancelRun(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void processCancelled_callsCancelRunOnly() {
        ActivitiEvent event = mock(ActivitiEvent.class);
        when(event.getType()).thenReturn(ActivitiEventType.PROCESS_CANCELLED);
        when(event.getProcessInstanceId()).thenReturn("pi-9");

        listener.onEvent(event);

        verify(registry).cancelRun("pi-9");
        verify(registry, never()).cancelStep(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void onEvent_nullEvent_isNoop() {
        assertThatCode(() -> listener.onEvent(null)).doesNotThrowAnyException();
        verifyNoInteractions(registry);
    }

    private static ActivitiEvent activityEvent(ActivitiEventType type, String activityId, String processInstanceId) {
        ActivitiActivityEvent event = mock(ActivitiActivityEvent.class);
        when(event.getType()).thenReturn(type);
        when(event.getActivityId()).thenReturn(activityId);
        when(event.getProcessInstanceId()).thenReturn(processInstanceId);
        return event;
    }
}
