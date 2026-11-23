package com.studioquack.sqs.runtime;

import java.util.List;

/**
 * Run-scoped record of an exclusive/inclusive gateway branch evaluation. Captured by
 * {@link GatewayRouteResolver} at route-token resolution time (the single place conditions
 * are evaluated) and consumed by the BPMN node run-log listener to render the gateway's
 * DuckTales panels — input (incoming msg), output (condition + msg passthrough) and the
 * echoes line ({@code activated: <full expression>}).
 *
 * <p>Security contract: carries the authored condition <em>text</em> and boolean outcomes
 * only — never resolved payload values, so the REF-only engine invariant is preserved.
 * Implementations must be bounded per run (memory) and never throw into the engine.
 */
public interface GatewayDecisionRecorder {

    /** Records one gateway evaluation for later log rendering. Never throws. */
    void record(GatewayDecision decision);

    /**
     * Latest decision recorded for {@code (workflowExecutionId, gatewayNodeId)}, or
     * {@code null} when none exists. Read-only; does not evict.
     */
    GatewayDecision latest(String workflowExecutionId, String gatewayNodeId);

    /**
     * Immutable snapshot of one gateway evaluation.
     * {@code defaultTaken} is an intended unconditioned pass; {@code deadEnd} is every
     * conditioned exit false with no unconditioned default. The two are mutually exclusive.
     * {@code selectedTargetNodeId} is the taken target, or
     * {@link GatewayRouteResolver#DEAD_END_SENTINEL} when {@code deadEnd}.
     */
    record GatewayDecision(
            String workflowExecutionId,
            String gatewayNodeId,
            String gatewayName,
            List<BranchEvaluation> evaluations,
            String selectedTargetNodeId,
            boolean defaultTaken,
            boolean deadEnd,
            long evaluatedAtMs) {
    }

    /**
     * One outgoing branch's outcome: the authored expression (as written, for the echoes
     * line), its boolean result, and a non-null eval/parse error marker when it failed.
     */
    record BranchEvaluation(
            String targetNodeId,
            String conditionExpression,
            boolean result,
            String error) {
    }
}
