package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.status.RunStatusCallback;
import com.studioquack.sqs.status.TerminalLogHandoff;
import org.activiti.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit test for the failure-hold a failure sentinel parks the ROOT run as a dead-letter incident
 * (FAILED report + {@link FailureHeldException}); a cleared sentinel (the repair-retry pass) is a
 * pass-through so the token flows on to the retry target.
 */
class FailureHoldDelegateTest {

    private final RunStatusCallback callback = mock(RunStatusCallback.class);
    private final TerminalLogHandoff handoff = mock(TerminalLogHandoff.class);
    private final FailureHoldDelegate delegate =
            new FailureHoldDelegate(providerOf(callback), providerOf(handoff));

    @Test
    void the_runs_logs_are_handed_off_before_it_is_reported_failed() {
        DelegateExecution execution = execution("SANDBOX_FAILED", "boom");
        when(handoff.handOffLogs("wfe-1")).thenReturn(true);

        assertThrows(FailureHeldException.class, () -> delegate.execute(execution));

        // A held run never reaches a lifecycle end, so its BATCH rows must leave the daemon here.
        InOrder order = inOrder(handoff, callback);
        order.verify(handoff).handOffLogs("wfe-1");
        order.verify(callback).report(eq("wfe-1"), eq(RunStatusCallback.Status.FAILED),
                anyString(), any(), eq(true));
    }

    @Test
    void a_refused_handoff_reports_the_logs_as_not_durable() {
        DelegateExecution execution = execution("SANDBOX_FAILED", "boom");
        when(handoff.handOffLogs("wfe-1")).thenReturn(false);

        assertThrows(FailureHeldException.class, () -> delegate.execute(execution));

        verify(callback).report(eq("wfe-1"), eq(RunStatusCallback.Status.FAILED),
                anyString(), any(), eq(false));
    }

    @Test
    void sentinel_parks_the_run_as_an_incident() {
        DelegateExecution execution = execution("SCRIPT boom", "boom detail");
        when(execution.getProcessDefinitionId()).thenReturn("Process_wf1:1:abc");

        FailureHeldException held = assertThrows(FailureHeldException.class,
                () -> delegate.execute(execution));

        assertEquals("node scriptN failed [SCRIPT boom]: boom detail — held for repair",
                held.getMessage());
        assertEquals(0, held.getStackTrace().length,
                "control-flow exception: no stack trace on the incident row");
        // FAILED is reported with the bridge's argument shape: businessKey, detail, workflowId.
        verify(callback).report(eq("wfe-1"), eq(RunStatusCallback.Status.FAILED),
                anyString(), eq("wf1"), anyBoolean());
    }

    @Test
    void cleared_sentinel_is_a_pass_through_for_the_repair_retry() {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE)).thenReturn(null);

        assertDoesNotThrow(() -> delegate.execute(execution));
        verifyNoInteractions(callback);
    }

    @Test
    void blank_sentinel_is_a_pass_through() {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE)).thenReturn("  ");

        assertDoesNotThrow(() -> delegate.execute(execution));
        verifyNoInteractions(callback);
    }

    @Test
    void message_is_truncated_and_never_carries_more_than_the_cap() {
        String longMessage = "x".repeat(900);
        DelegateExecution execution = execution("SANDBOX_FAILED", longMessage);

        FailureHeldException held = assertThrows(FailureHeldException.class,
                () -> delegate.execute(execution));

        assertTrue(held.getMessage().length() < 600, "the incident message is capped");
        assertTrue(held.getMessage().contains("…"), "truncation is marked");
    }

    @Test
    void a_failed_status_report_does_not_mask_the_hold() {
        DelegateExecution execution = execution("SANDBOX_FAILED", "boom");
        doThrow(new RuntimeException("channel down")).when(callback)
                .report(anyString(), eq(RunStatusCallback.Status.FAILED),
                        anyString(), any(), anyBoolean());

        assertThrows(FailureHeldException.class, () -> delegate.execute(execution));
    }

    /** An execution parked at {@code hold_scriptN} carrying the given sentinel pair. */
    private DelegateExecution execution(String code, String message) {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE)).thenReturn(code);
        when(execution.getVariable(EngineVariableNames.ERROR_MESSAGE)).thenReturn(message);
        when(execution.getCurrentActivityId()).thenReturn("hold_scriptN");
        when(execution.getProcessInstanceBusinessKey()).thenReturn("wfe-1");
        return execution;
    }

    private static <T> ObjectProvider<T> providerOf(T bean) {
        @SuppressWarnings("unchecked")
        ObjectProvider<T> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(bean);
        return provider;
    }
}
