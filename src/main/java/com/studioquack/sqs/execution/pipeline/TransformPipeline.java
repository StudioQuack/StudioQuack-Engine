package com.studioquack.sqs.execution.pipeline;

import com.studioquack.sqs.execution.context.ExecutionContext;

import java.util.Map;

/**
 * The published client-side I/O-mapping + JOLT API that (in-process dispatch) calls
 * in place of the control plane's HTTP {@code daemon}. It encapsulates the
 * <b>2-phase inbound pipeline</b> and the post-sandbox output handling, preserving all 16
 * Mapping Invariants.
 *
 * <p>Contract for:
 * <ol>
 * <li>Populate an {@link ExecutionContext} with the workflow graph, {@code currentNodeId},
 * and the inbound values (either via {@code nextNodeInputList} + {@code createCurrentNode}
 * or by setting {@code currentNode.inputList} directly).</li>
 * <li>Call {@link #prepareInbound(ExecutionContext)} — this runs Phase 1 (pre-collect
 * before edge-clear) and Phase 2 (pre-bake inbound JOLT, build the flat input port
 * map, strip the control plane-applied specs, emit the daemon directive + function-port lanes).</li>
 * <li>Call {@code GenerateScriptService.buildScriptSource(ctx)} — MUST be after
 * {@code prepareInbound} so the baked {@code __SQ_CONTEXT__} reflects the pre-baked
 * inputs (Mapping Invariant 2), then dispatch to the sandbox with the prepared
 * {@code inputVariables}.</li>
 * <li>On sandbox return, call {@link #finalizeOutputs(ExecutionContext, Map)} to fan the
 * outputs onto {@code currentNode.outputList} (port fan-out + schema filter ONLY —
 * no post-sandbox outbound JOLT, Mapping Invariant 14).</li>
 * </ol>
 */
public interface TransformPipeline {

    /**
 * Runs the full inbound pipeline (Phases 1 + 2) against {@code ctx}, mutating it in place
 * (pre-baked inputList values, cleared graph) and returning the flat input port map +
 * daemon directives ships to the sandbox.
 */
    PreparedInbound prepareInbound(ExecutionContext ctx);

    /**
     * Prunes each {@code currentNode.inputList} entry's value against the TARGET port's schema
     * (post JOLT / edge-transform pre-step), rebuilds the flat input port map, and records
     * dropped top-level parameter names on {@link ExecutionContext#getDroppedInputParameters()}.
     * Best-effort — a filter failure logs a warning and leaves the entry untouched (Invariant 11).
     */
    PreparedInbound filterInboundAgainstPortSchemas(ExecutionContext ctx, PreparedInbound prepared);

    /**
     * Handles the sandbox's raw output map: strips daemon metadata keys, filters each value
     * against its output port schema, fans it out to downstream wires, and sets
     * {@code currentNode.outputList}. Applies NO JOLT (Mapping Invariant 14). Bridge-set
     * outputs are preserved when the sandbox returned only metadata.
     */
    void finalizeOutputs(ExecutionContext ctx, Map<String, Object> sandboxOutputs);
}
