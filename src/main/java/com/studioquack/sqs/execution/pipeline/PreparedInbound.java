package com.studioquack.sqs.execution.pipeline;

import com.studioquack.common.dto.flow.FunctionPortTransformsDto;
import com.studioquack.common.dto.flow.TransformationSpecDto;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Immutable result of {@link TransformPipeline#prepareInbound}. Everything needs to
 * dispatch a node to the in-process sandbox after the 2-phase inbound pipeline has run.
 *
 * @param inputVariables flat port map ({@code targetPortValue||portValue -> value})
 * the sandbox exposes via {@code studioquack.input(...)}, plus
 * the reserved {@code __sq_transformation__} /
 * {@code __sq_function_port_transforms__} directive keys.
 * @param daemonDirectives inbound transform specs left FOR the sandbox after the control plane-side
 * pre-bake + unmatched-JOLT filtering (Mapping Invariants 3, 4).
 * Also present under {@code __sq_transformation__} in
 * {@code inputVariables}; surfaced here for inspection/tests.
 * @param functionPortTransforms per-port request/response lanes (single-owner emit, Invariant 10).
 * @param beAppliedSpecs identity set of JOLT specs already applied the control plane-side during
 * pre-bake — stripped from {@code daemonDirectives} to avoid a
 * double transform (Invariant 3, IdentityHashMap identity).
 */
public record PreparedInbound(
        Map<String, Object> inputVariables,
        List<TransformationSpecDto> daemonDirectives,
        Map<String, FunctionPortTransformsDto> functionPortTransforms,
        Set<TransformationSpecDto> beAppliedSpecs) {

    /** An empty prepared result (no current node / nothing to dispatch). */
    public static PreparedInbound empty() {
        return new PreparedInbound(
                Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyMap(), Collections.emptySet());
    }
}
