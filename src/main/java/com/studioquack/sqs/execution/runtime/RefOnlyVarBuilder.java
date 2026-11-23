package com.studioquack.sqs.execution.runtime;

import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the (tiny) variable map published onto an engine process instance for one node
 * dispatch. This is the daemon's REF-ONLY counterpart of the control plane
 * context-variable builder: the control plane supported {@code LEGACY}/{@code BOTH}/
 * {@code REF_ONLY} modes, but the daemon runs <b>REF_ONLY only</b> (Mapping Invariant 15) —
 * there is no config knob to re-introduce the fat blob.
 *
 * <p>Snapshotting the context into the {@link ExecutionContextStore} is the sole authoritative
 * writer of the sealed {@code execution_context} row; the returned {@link ProcessContextRef}
 * is the only handle that crosses onto the engine. {@link RefOnlyVariableGuard} asserts the
 * emitted map is REF-only before it leaves this builder.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RefOnlyVarBuilder {

    private final ExecutionContextStore executionContextStore;

    /**
     * Snapshots {@code ctx} and returns {@code {studioquackRef: <ref>}}. No fat
     * {@code executionContext} blob, no standalone {@code workflowExecutionId}, no
     * per-node {@code output_*} — exactly the REF-only shape.
     */
    public Map<String, Object> toEngineVariables(ExecutionContext ctx, Long jobKey) {
        ProcessContextRef ref = executionContextStore.snapshot(ctx, jobKey);
        Map<String, Object> out = new HashMap<>(1);
        out.put(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME, ref);
        RefOnlyVariableGuard.assertRefOnly(out);
        return out;
    }

    /** Convenience for callers without a job key (replay / test paths). */
    public Map<String, Object> toEngineVariables(ExecutionContext ctx) {
        return toEngineVariables(ctx, null);
    }
}
