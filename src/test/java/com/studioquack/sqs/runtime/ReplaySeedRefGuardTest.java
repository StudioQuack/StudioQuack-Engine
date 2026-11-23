package com.studioquack.sqs.runtime;

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
 * The replay seed-ref guard reconstructs the REF-ONLY seed from the sealed {@code
 * execution_context}, and fails fast with a deterministic repair code when the ref or its context
 * is unrecoverable (never returns an empty seed → never starts latest with no inputs).
 */
class ReplaySeedRefGuardTest {

    @Test
    void reconstructs_ref_only_seed_when_context_recoverable() {
        ReplaySeedRefGuard guard = new ReplaySeedRefGuard(new PresentStore());
        ProcessContextRef ref = ProcessContextRef.builder()
                .executionId("exec-1").workflowExecutionId("wfe-1")
                .currentNodeRefId("node_abc").schemaVersion(2).build();

        Map<String, Object> seed = guard.reconstructSeedVariables(ref);

        assertEquals(1, seed.size(), "REF-ONLY seed: exactly the studioquackRef");
        assertEquals(ref, seed.get(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME));
        assertFalse(seed.isEmpty(), "seed is never empty on the success path");
    }

    @Test
    void fails_fast_when_ref_missing_node_ref() {
        ReplaySeedRefGuard guard = new ReplaySeedRefGuard(new PresentStore());
        ProcessContextRef refWithoutNode = ProcessContextRef.builder().workflowExecutionId("wfe-1").build();

        ReplaySeedRefGuard.SeedRefUnrecoverableException ex = assertThrows(
                ReplaySeedRefGuard.SeedRefUnrecoverableException.class,
                () -> guard.reconstructSeedVariables(refWithoutNode));
        assertEquals(ReplaySeedRefGuard.CODE_SEED_REF_MISSING, ex.code());
    }

    @Test
    void fails_fast_when_context_row_unrecoverable() {
        ReplaySeedRefGuard guard = new ReplaySeedRefGuard(new EmptyStore());
        ProcessContextRef ref = ProcessContextRef.builder()
                .currentNodeRefId("node_gone").workflowExecutionId("wfe-1").build();

        ReplaySeedRefGuard.SeedRefUnrecoverableException ex = assertThrows(
                ReplaySeedRefGuard.SeedRefUnrecoverableException.class,
                () -> guard.reconstructSeedVariables(ref));
        assertEquals(ReplaySeedRefGuard.CODE_SEED_CONTEXT_UNRECOVERABLE, ex.code());
        assertTrue(ex.getMessage().contains("node_gone"));
    }

    @Test
    void fails_fast_when_ref_is_not_the_runs_seed() {
        // The ref LOADS (a mid-run dispatch context is recoverable), but the store names a
        // different row as the run's seed — replaying from it would not be a faithful restart.
        ReplaySeedRefGuard guard = new ReplaySeedRefGuard(new SeedAwareStore("node_seed"));
        ProcessContextRef midRunRef = ProcessContextRef.builder()
                .executionId("exec-1").workflowExecutionId("wfe-1")
                .currentNodeRefId("node_failed_attempt").schemaVersion(2).build();

        ReplaySeedRefGuard.SeedRefUnrecoverableException ex = assertThrows(
                ReplaySeedRefGuard.SeedRefUnrecoverableException.class,
                () -> guard.reconstructSeedVariables(midRunRef));
        assertEquals(ReplaySeedRefGuard.CODE_SEED_REF_NOT_SEED, ex.code());
        assertTrue(ex.getMessage().contains("node_failed_attempt"));
    }

    @Test
    void reconstructs_when_ref_is_the_runs_seed() {
        ReplaySeedRefGuard guard = new ReplaySeedRefGuard(new SeedAwareStore("node_seed"));
        ProcessContextRef seedRef = ProcessContextRef.builder()
                .executionId("exec-1").workflowExecutionId("wfe-1")
                .currentNodeRefId("node_seed").schemaVersion(2).build();

        Map<String, Object> seed = guard.reconstructSeedVariables(seedRef);

        assertEquals(seedRef, seed.get(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME));
    }

    /** Store whose context loads AND which can name the run's seed row. */
    private static class SeedAwareStore extends PresentStore {
        private final String seedNodeRefId;

        SeedAwareStore(String seedNodeRefId) {
            this.seedNodeRefId = seedNodeRefId;
        }

        @Override
        public Optional<ProcessContextRef> seedRefOf(String executionId) {
            return Optional.of(ProcessContextRef.builder()
                    .executionId(executionId).workflowExecutionId(executionId)
                    .currentNodeRefId(seedNodeRefId).schemaVersion(2).build());
        }
    }

    /** Store whose context is always recoverable. */
    private static class PresentStore extends StubStore {
        @Override
        public Optional<ExecutionContext> loadByRef(ProcessContextRef ref) {
            ExecutionContext ctx = new ExecutionContext();
            ctx.setWorkflowExecutionId(ref.getWorkflowExecutionId());
            return Optional.of(ctx);
        }
    }

    /** Store whose context row is missing (unrecoverable). */
    private static class EmptyStore extends StubStore {
        @Override
        public Optional<ExecutionContext> loadByRef(ProcessContextRef ref) {
            return Optional.empty();
        }
    }

    /** No-op base store. */
    private static class StubStore implements ExecutionContextStore {
        @Override public ProcessContextRef snapshot(ExecutionContext ctx, Long jobKey) { return null; }
        @Override public Optional<ExecutionContext> loadByRef(ProcessContextRef ref) { return Optional.empty(); }
        @Override public List<InOutValueDto> loadInputsTargetingNode(ProcessContextRef ref, String n) { return List.of(); }
        @Override public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId) { return Map.of(); }
        @Override public void saveOutputs(ProcessContextRef ref, List<InOutValueDto> outputs) { }
        @Override public boolean encryptionEnabled() { return false; }
    }
}
