package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Deterministic guard for the workflow <b>replay</b> path (item 7). A replay reconstructs
 * its seed variables from the encrypted {@code execution_context} addressed by a {@code studioquackRef}
 * (never from the fat blob on the engine wire — REF-ONLY). If that ref or its sealed context is
 * unrecoverable, this guard <b>fails fast</b> with a stable {@link SeedRefUnrecoverableException} so
 * the run is surfaced for repair instead of silently starting the latest process with an
 * empty variable map — which would run a workflow against no inputs and corrupt results.
 *
 * <p>The reconstructed seed is itself REF-ONLY ({@code {studioquackRef: <ref>}}) and re-asserted by
 * {@link RefOnlyVariableGuard} before it is returned.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReplaySeedRefGuard {

    /** Stable repair code: the ref itself is missing/blank (no {@code currentNodeRefId}). */
    public static final String CODE_SEED_REF_MISSING = "SEED_REF_MISSING";
    /** Stable repair code: the sealed {@code execution_context} row could not be loaded/opened. */
    public static final String CODE_SEED_CONTEXT_UNRECOVERABLE = "SEED_CONTEXT_UNRECOVERABLE";
    /** Stable repair code: the ref loads but is NOT the run's seed (a mid-run dispatch context). */
    public static final String CODE_SEED_REF_NOT_SEED = "SEED_REF_NOT_SEED";

    private final ExecutionContextStore contextStore;

    /**
     * Reconstructs the REF-ONLY seed variable map for a replay, or fails fast.
     *
     * @param ref the {@code studioquackRef} pointing at the sealed seed context
     * @return {@code {studioquackRef: <ref>}} — never empty
     * @throws SeedRefUnrecoverableException with a stable {@link SeedRefUnrecoverableException#code()}
     *                                       when the ref/context cannot be recovered
     */
    public Map<String, Object> reconstructSeedVariables(ProcessContextRef ref) {
        if (ref == null || StringUtils.isBlank(ref.getCurrentNodeRefId())) {
            throw new SeedRefUnrecoverableException(CODE_SEED_REF_MISSING,
                    "replay seed ref is missing or has no currentNodeRefId; cannot reconstruct seed "
                            + "variables — the run must be repaired, never started with an empty map.");
        }
        Optional<ExecutionContext> ctx;
        try {
            ctx = contextStore.loadByRef(ref);
        } catch (RuntimeException e) {
            throw new SeedRefUnrecoverableException(CODE_SEED_CONTEXT_UNRECOVERABLE,
                    "replay seed context failed to open for ref " + ref.getCurrentNodeRefId()
                            + " (" + e.getClass().getSimpleName() + "); run must be repaired.");
        }
        if (ctx.isEmpty()) {
            throw new SeedRefUnrecoverableException(CODE_SEED_CONTEXT_UNRECOVERABLE,
                    "replay seed context not found for ref " + ref.getCurrentNodeRefId()
                            + "; run must be repaired, never started with an empty variable map.");
        }
        assertSeedRef(ref);
        Map<String, Object> seed = new HashMap<>(1);
        seed.put(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME, ref);
        RefOnlyVariableGuard.assertRefOnly(seed);
        log.debug("replay.seed.reconstructed ref={} wfeId={}",
                ref.getCurrentNodeRefId(), ref.getWorkflowExecutionId());
        return seed;
    }

    /**
     * Asserts the resolved ref addresses the run's <b>seed</b> — the first {@code execution_context}
     * row of the business key (the start-run snapshot, i.e. the entry/trigger context). A ref that
     * loads but points at a later dispatch row (e.g. the failed node's context off a held run) would
     * replay the trigger against THAT node's context ({@code refNodeId ≠ trigger} → carried seed
     * inputs dropped) — never a faithful restart, so it fails fast for repair instead.
     *
     * <p>Skipped (load-only behaviour kept) when the store cannot identify a seed row for the
     * business key — purged rows, or a legacy run whose store is gone; the command handler already
     * WARN-falls-back in exactly that case. A store hiccup during the check is likewise non-fatal:
     * the load above already proved the ref opens.
     */
    private void assertSeedRef(ProcessContextRef ref) {
        String businessKey = StringUtils.firstNonBlank(ref.getWorkflowExecutionId(), ref.getExecutionId());
        Optional<ProcessContextRef> seedRef;
        try {
            seedRef = StringUtils.isBlank(businessKey) ? Optional.empty() : contextStore.seedRefOf(businessKey);
        } catch (RuntimeException e) {
            log.warn("replay.seed.assert-skipped ref={}: {}", ref.getCurrentNodeRefId(), e.toString());
            return;
        }
        if (seedRef.isEmpty()) {
            return;
        }
        if (!seedRef.get().getCurrentNodeRefId().equals(ref.getCurrentNodeRefId())) {
            throw new SeedRefUnrecoverableException(CODE_SEED_REF_NOT_SEED,
                    "replay ref " + ref.getCurrentNodeRefId() + " is not the run's seed context ("
                            + seedRef.get().getCurrentNodeRefId() + ") — a replay must restart from the "
                            + "first execution_context row of the business key, never a mid-run "
                            + "dispatch context; run must be repaired.");
        }
    }

    /** Deterministic, repairable failure raised when a replay seed ref/context is unrecoverable. */
    public static final class SeedRefUnrecoverableException extends RuntimeException {
        private final String code;

        public SeedRefUnrecoverableException(String code, String message) {
            super(message);
            this.code = code;
        }

        /** Stable machine code driving the repair path. */
        public String code() {
            return code;
        }
    }
}
