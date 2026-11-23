package com.studioquack.sqs.execution.runtime;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Mapping Invariant 15 (REF-ONLY engine vars): only {@code studioquackRef} + non-PHI
 * routing/error/epoch tokens may reach the engine variable tables; the fat payload stays in
 * the sealed store. Exercises {@link RefOnlyVariableGuard} and {@link RefOnlyVarBuilder}.
 */
class RefOnlyVariableTest {

    @Test
    void guard_allows_only_ref_routing_error_epoch_tokens() {
        assertTrue(RefOnlyVariableGuard.isAllowed("studioquackRef"));
        assertTrue(RefOnlyVariableGuard.isAllowed("route__gw1"));
        assertTrue(RefOnlyVariableGuard.isAllowed("__sq_error_code"));
        assertTrue(RefOnlyVariableGuard.isAllowed("__sq_error_message"));
        assertTrue(RefOnlyVariableGuard.isAllowed("__sq_be_epoch"));

        assertFalse(RefOnlyVariableGuard.isAllowed("prompt"), "PHI-bearing key must be rejected");
        assertFalse(RefOnlyVariableGuard.isAllowed("executionContext"), "the fat blob must never be a var");
        assertFalse(RefOnlyVariableGuard.isAllowed("output_node1"));
    }

    @Test
    void assertRefOnly_throws_on_phi_key() {
        RefOnlyVariableGuard.assertRefOnly(Map.of("studioquackRef", "ref", "route__gw", "yes"));
        assertThrows(IllegalStateException.class,
                () -> RefOnlyVariableGuard.assertRefOnly(Map.of("studioquackRef", "ref", "prompt", "PHI")));
    }

    @Test
    void var_builder_emits_only_the_ref() {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId("cur");

        RefOnlyVarBuilder builder = new RefOnlyVarBuilder(new StubStore());
        Map<String, Object> vars = builder.toEngineVariables(ctx, 42L);

        assertEquals(1, vars.size(), "REF-ONLY: exactly one engine variable");
        assertTrue(vars.containsKey(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME));
        Object ref = vars.get(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME);
        assertTrue(ref instanceof ProcessContextRef, "the sole variable is the ref handle");
        // and the guard is satisfied by construction (no throw).
        RefOnlyVariableGuard.assertRefOnly(vars);
    }

    /** Minimal store: snapshot returns a PHI-free ref (ids only). */
    private static final class StubStore implements ExecutionContextStore {
        @Override
        public ProcessContextRef snapshot(ExecutionContext ctx, Long jobKey) {
            return ProcessContextRef.builder()
                    .executionId(ctx.getWorkflowExecutionId())
                    .currentNodeRefId("node_test")
                    .schemaVersion(2)
                    .build();
        }

        @Override
        public Optional<ExecutionContext> loadByRef(ProcessContextRef ref) {
            return Optional.empty();
        }

        @Override
        public List<InOutValueDto> loadInputsTargetingNode(ProcessContextRef ref, String currentNodeId) {
            return List.of();
        }

        @Override
        public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId) {
            return Map.of();
        }

        @Override
        public void saveOutputs(ProcessContextRef ref, List<InOutValueDto> outputs) {
        }

        @Override
        public boolean encryptionEnabled() {
            return false;
        }
    }
}
