package com.studioquack.sqs.execution.store;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.execution.context.ExecutionContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Persists and reloads the sealed node payload addressed by a {@code studioquackRef}.
 * {@link H2ExecutionContextStore} keeps it in encrypted-file H2.
 */
public interface ExecutionContextStore {

    /**
     * Seals a snapshot of {@code ctx} into a fresh {@code execution_context} row and returns
     * the tiny {@link ProcessContextRef} handle to publish on the engine. This is the ONLY
     * authoritative writer of the store.
     */
    ProcessContextRef snapshot(ExecutionContext ctx, Long jobKey);

    /** Rehydrates the full {@link ExecutionContext} addressed by {@code ref} (opens sealed fields). */
    Optional<ExecutionContext> loadByRef(ProcessContextRef ref);

    /**
 * Resolves the run's <b>seed</b> context ref: the first {@code execution_context} row minted
 * for the business key — the start-run snapshot (dispatch rows of the same run order after it
 * by {@code cdate}, then {@code revision}). The replay path restarts from THIS ref, never
 * from the instance's current {@code studioquackRef}: on a run held at {@code hold_<X>} that ref
 * is X's failed-dispatch context, and replaying it is not a faithful restart. Rows persist after
 * the run ends, so this also serves ENDED runs replayed from history. Default empty so existing
 * test stubs stay source-compatible; persist implementations override.
 */
    default Optional<ProcessContextRef> seedRefOf(String executionId) {
        return Optional.empty();
    }

    /**
     * REF-ONLY equivalent of the legacy per-node output scan: collects every completed
     * upstream attempt's outputs addressed to {@code currentNodeId}, deduped latest-wins.
     */
    List<InOutValueDto> loadInputsTargetingNode(ProcessContextRef ref, String currentNodeId);

    /**
     * Groups every completed attempt's outputs by the node that PRODUCED them
     * ({@link InOutValueDto#getNodeId()}, stamped by the fan-out) — retry-idempotent, latest-wins.
     * Never by the row label: the dispatch chain mints the next hop's row at the end of a node, so
     * the row labelled "A" carries A's successor's outputs.
     */
    Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId);

    /**
     * Filtered variant of {@link #collectOutputsByNode(String)}: only producers in {@code nodeIds}
     * contribute. The DuckTape Add-Node foreign-source delivery reads exactly the pinned nodes'
     * outputs. The default (full collect + filter) keeps test stubs source-compatible.
     */
    default Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId, Set<String> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return Map.of();
        }
        Map<String, List<InOutValueDto>> all = collectOutputsByNode(executionId);
        Map<String, List<InOutValueDto>> out = new LinkedHashMap<>();
        for (String nodeId : nodeIds) {
            List<InOutValueDto> outputs = all.get(nodeId);
            if (outputs != null) {
                out.put(nodeId, outputs);
            }
        }
        return out;
    }

    /** Persists the node's outputs onto its live row and marks it completed. */
    void saveOutputs(ProcessContextRef ref, List<InOutValueDto> outputs);

    /** @return true when rows are sealed at rest (a {@link com.studioquack.sqs.execution.crypto.ContextKeyProvider} key is configured). */
    boolean encryptionEnabled();

    /**
     * Records one function-port {@code studioquack.execute} invocation under the parent run.
     * Default is a no-op so existing test fakes keep compiling. Persist implementations treat
     * blank/null arguments as a no-op.
     */
    default void recordSubPiInvocation(String parentExecutionId, String childExecutionId,
                                       String callerNodeId, String subPiRootNodeId) {
    }

    /**
     * Invocations recorded for {@code parentExecutionId}, ordered by persisted id ascending.
     * Default returns an empty list. Blank/null parent yields an empty list.
     */
    default List<SubPiInvocation> subPiInvocationsOf(String parentExecutionId) {
        return List.of();
    }
}
