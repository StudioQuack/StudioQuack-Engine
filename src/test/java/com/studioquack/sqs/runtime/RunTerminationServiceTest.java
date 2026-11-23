package com.studioquack.sqs.runtime;

import com.studioquack.sqs.aimodel.LlmStreamCancellationRegistry;
import com.studioquack.sqs.engine.SqsEngine;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The one kill path behind {@code cancel-instance} and the in-place update: a live run is marked
 * cancelled before its work is reaped and it is deleted; an already-gone run is never deleted again.
 */
class RunTerminationServiceTest {

    private final SqsEngine engine = mock(SqsEngine.class);
    private final DispatchCancellationRegistry dispatches = mock(DispatchCancellationRegistry.class);
    private final CancelledRunGuard guard = mock(CancelledRunGuard.class);
    private final LlmStreamCancellationRegistry streams = mock(LlmStreamCancellationRegistry.class);
    private final RunTerminationService service = new RunTerminationService(engine, dispatches, guard, streams);

    @Test
    void aLiveRunIsMarkedCancelledThenReapedThenDeleted() {
        ProcessInstance instance = mock(ProcessInstance.class);
        when(instance.getBusinessKey()).thenReturn("wfe-1");
        when(engine.resolveProcessInstanceId("wfe-1")).thenReturn("pi-1");
        when(engine.isProcessInstanceActive("pi-1")).thenReturn(true);
        when(engine.findProcessInstance("pi-1")).thenReturn(instance);
        when(dispatches.cancelRun("pi-1")).thenReturn(2);

        RunTerminationService.Outcome outcome = service.terminate("wfe-1", "killed by an engine update");

        assertThat(outcome).isEqualTo(new RunTerminationService.Outcome("pi-1", true, 2, 0));
        InOrder order = inOrder(guard, dispatches, engine);
        order.verify(guard).markCancelled("wfe-1");
        order.verify(dispatches).cancelRun("pi-1");
        order.verify(engine).deleteProcessInstance("pi-1", "killed by an engine update");
    }

    @Test
    void anAlreadyGoneRunIsReapedButNeverDeleted() {
        when(engine.resolveProcessInstanceId("pi-gone")).thenReturn("pi-gone");
        when(engine.isProcessInstanceActive("pi-gone")).thenReturn(false);

        RunTerminationService.Outcome outcome = service.terminate("pi-gone", "killed by an engine update");

        assertThat(outcome.terminated()).isFalse();
        verify(dispatches).cancelRun("pi-gone");
        verify(engine, never()).deleteProcessInstance(anyString(), anyString());
        verify(guard, never()).markCancelled(anyString());
    }
}
