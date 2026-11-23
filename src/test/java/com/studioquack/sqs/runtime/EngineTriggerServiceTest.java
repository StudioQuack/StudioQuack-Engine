package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import org.activiti.engine.ActivitiObjectNotFoundException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * The trigger seam: success completes the ReceiveTask with REF-ONLY vars, failure stamps the {@code
 * __sq_error_code} sentinel, and a non-whitelisted (PHI-bearing) variable is rejected at the
 * trigger boundary (the REF-ONLY whitelist audit requires).
 */
class EngineTriggerServiceTest {

    @Test
    void complete_triggers_with_ref_only_variables() {
        SqsEngine engine = mock(SqsEngine.class);
        EngineTriggerService svc = new EngineTriggerService(engine);

        ProcessContextRef ref = ProcessContextRef.builder().executionId("e1").build();
        svc.complete("exec-1", Map.of(EngineVariableNames.STUDIOQUACK_REF, ref));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> vars = ArgumentCaptor.forClass(Map.class);
        verify(engine).trigger(org.mockito.ArgumentMatchers.eq("exec-1"), vars.capture());
        assertEquals(ref, vars.getValue().get(EngineVariableNames.STUDIOQUACK_REF));
    }

    @Test
    void fail_stamps_error_code_sentinel_and_triggers() {
        SqsEngine engine = mock(SqsEngine.class);
        EngineTriggerService svc = new EngineTriggerService(engine);

        svc.fail("exec-2", "MY_CODE", "boom");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> vars = ArgumentCaptor.forClass(Map.class);
        verify(engine).trigger(org.mockito.ArgumentMatchers.eq("exec-2"), vars.capture());
        assertEquals("MY_CODE", vars.getValue().get(EngineVariableNames.ERROR_CODE));
        assertEquals("boom", vars.getValue().get(EngineVariableNames.ERROR_MESSAGE));
    }

    @Test
    void complete_rejects_non_ref_only_payload() {
        SqsEngine engine = mock(SqsEngine.class);
        EngineTriggerService svc = new EngineTriggerService(engine);

        assertThrows(IllegalStateException.class,
                () -> svc.complete("exec-3", Map.of("phiPayload", "patient data leak")),
                "PHI-bearing engine variable must be rejected by the REF-ONLY guard");
    }

    @Test
    void complete_swallowsActivitiObjectNotFound() {
        SqsEngine engine = mock(SqsEngine.class);
        doThrow(new ActivitiObjectNotFoundException("execution is null or doesn't exist"))
                .when(engine).trigger(anyString(), any());
        EngineTriggerService svc = new EngineTriggerService(engine);

        ProcessContextRef ref = ProcessContextRef.builder().executionId("e1").build();
        svc.complete("gone-exec", Map.of(EngineVariableNames.STUDIOQUACK_REF, ref));
    }

    @Test
    void fail_swallowsGoneExecutionMessage() {
        SqsEngine engine = mock(SqsEngine.class);
        doThrow(new IllegalStateException("execution gone"))
                .when(engine).trigger(anyString(), any());
        EngineTriggerService svc = new EngineTriggerService(engine);

        svc.fail("gone-exec", "SANDBOX_FAILED", "late");
    }

    @Test
    void complete_propagatesOtherExceptions() {
        SqsEngine engine = mock(SqsEngine.class);
        doThrow(new IllegalStateException("H2 store closed"))
                .when(engine).trigger(anyString(), any());
        EngineTriggerService svc = new EngineTriggerService(engine);

        ProcessContextRef ref = ProcessContextRef.builder().executionId("e1").build();
        assertThrows(IllegalStateException.class,
                () -> svc.complete("exec-1", Map.of(EngineVariableNames.STUDIOQUACK_REF, ref)));
    }
}
