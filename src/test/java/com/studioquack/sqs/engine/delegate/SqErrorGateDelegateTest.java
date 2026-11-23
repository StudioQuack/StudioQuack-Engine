package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import org.activiti.engine.delegate.BpmnError;
import org.activiti.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The post-wait error gate raises a {@link BpmnError} when the failure sentinel is present (routing
 * the token to the interrupting boundary → sandbox-error end) and is a zero-cost pass-through on
 * the success path.
 */
class SqErrorGateDelegateTest {

    private final SqErrorGateDelegate gate = new SqErrorGateDelegate();

    @Test
    void raises_bpmn_error_when_error_code_sentinel_is_present() {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE)).thenReturn("SANDBOX_FAILED");
        when(execution.getVariable(EngineVariableNames.ERROR_MESSAGE)).thenReturn("boom");

        BpmnError error = assertThrows(BpmnError.class, () -> gate.execute(execution));
        assertEquals("SANDBOX_FAILED", error.getErrorCode());
    }

    @Test
    void passes_through_on_success_when_no_sentinel() {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE)).thenReturn(null);
        assertDoesNotThrow(() -> gate.execute(execution));
        assertEquals(1, gate.getInvocationCount());
    }
}
