package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.activiti.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for the fail-route hold decision: a ROOT run holds (true); a call-activity child
 * (super execution), a function-port sub-PI child (registry) or an operator skip
 * ({@code SQ_SKIP}) ends at the shared error end (false); any evaluation fault falls back to
 * false so the run ends FAILED as before.
 */
class FailureHoldPolicyTest {

    private final SubProcessCompletionRegistry registry = mock(SubProcessCompletionRegistry.class);
    private final FailureHoldPolicy policy = new FailureHoldPolicy(providerOf(registry));

    @Test
    void root_run_holds() {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.isProcessInstanceType()).thenReturn(true);
        when(execution.getSuperExecutionId()).thenReturn(null);
        when(execution.getProcessInstanceBusinessKey()).thenReturn("wfe-root");
        when(registry.isSubPiChild("wfe-root")).thenReturn(false);

        assertTrue(policy.shouldHold(execution), "a root run is held for repair");
    }

    @Test
    void call_activity_child_does_not_hold() {
        // The boundary fires on a child scope: the policy must walk up to the PI root, which is
        // where the superExecutionId lives (same traversal as the child-context seeder).
        DelegateExecution piRoot = mock(DelegateExecution.class);
        when(piRoot.isProcessInstanceType()).thenReturn(true);
        when(piRoot.getSuperExecutionId()).thenReturn("super-exec-1");
        DelegateExecution leaf = mock(DelegateExecution.class);
        when(leaf.isProcessInstanceType()).thenReturn(false);
        when(leaf.getParent()).thenReturn(piRoot);

        assertFalse(policy.shouldHold(leaf),
                "a call-activity child ends FAILED — the parent owns the failure");
    }

    @Test
    void sub_pi_child_does_not_hold() {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.isProcessInstanceType()).thenReturn(true);
        when(execution.getSuperExecutionId()).thenReturn("");
        when(execution.getProcessInstanceBusinessKey()).thenReturn("wfe-subpi");
        when(registry.isSubPiChild("wfe-subpi")).thenReturn(true);

        assertFalse(policy.shouldHold(execution),
                "a helper sub-PI child ends FAILED — the calling function owns the failure");
    }

    @Test
    void missing_registry_still_holds_a_root_run() {
        FailureHoldPolicy noRegistry = new FailureHoldPolicy(providerOf(null));
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.isProcessInstanceType()).thenReturn(true);
        when(execution.getSuperExecutionId()).thenReturn(null);
        when(execution.getProcessInstanceBusinessKey()).thenReturn("wfe-root");

        assertTrue(noRegistry.shouldHold(execution),
                "no registry → cannot be a known sub-PI child → root holds");
    }

    @Test
    void operator_skip_never_holds() {
        // SQ_SKIP raised through the error gate lands on the system catch-all → failroute; a skip
        // is a deliberate abandon-node action, so even a root run ends at the shared error end.
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE))
                .thenReturn(EngineVariableNames.SKIP_ERROR_CODE);

        assertFalse(policy.shouldHold(execution),
                "an operator skip ends at the shared error end — it is never held for repair");
    }

    @Test
    void evaluation_fault_falls_back_to_end() {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.isProcessInstanceType()).thenThrow(new RuntimeException("engine tearing down"));
        when(execution.getCurrentActivityId()).thenReturn("hold_x");

        assertFalse(policy.shouldHold(execution),
                "a policy fault must never strand the run — it ends FAILED as before the hold path");
    }

    private static ObjectProvider<SubProcessCompletionRegistry> providerOf(SubProcessCompletionRegistry bean) {
        @SuppressWarnings("unchecked")
        ObjectProvider<SubProcessCompletionRegistry> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(bean);
        return provider;
    }
}
