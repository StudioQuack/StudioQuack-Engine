package com.studioquack.sqs.execution.transform;

import com.studioquack.common.dto.flow.TransformationSpecDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Function-port transform owner for {@code studioquack.execute()} request/response legs
 * ({@link #applyToValue}). Inbound JOLT on the node ctx is owned by
 * {@code DefaultTransformPipeline} — a second ctx-map apply path is deliberately unused
 * (it masked which owner ran the shift).
 *
 * <p>Script wins over JOLT per edge (Invariant 5). Failures are warnings, never throws
 * (Invariant 11).
 */
@Component
@RequiredArgsConstructor
public class TransformDirectiveApplier {

    /** Reserved input key carrying the transform directive list (pipeline / edge-pre-step). */
    public static final String DIRECTIVE_KEY = "__sq_transformation__";

    private final JoltTransformService joltTransformService;

    /** Result of {@link #applyToValue(List, Object)} — the transformed value + pending + warnings. */
    public record ValueResult(Object value, List<TransformationSpecDto> pending, List<String> warnings) {
    }

    /**
     * Applies JOLT to the whole value in spec order. Same script-wins rule as the pipeline.
     * This is the SINGLE owner of function-port transforms (Invariant 10).
     */
    public ValueResult applyToValue(List<TransformationSpecDto> specs, Object value) {
        List<String> warnings = new ArrayList<>();
        List<TransformationSpecDto> pending = new ArrayList<>();
        if (specs == null || specs.isEmpty()) {
            return new ValueResult(value, pending, warnings);
        }
        Set<EdgeKey> skipJolt = buildScriptCoveredEdges(specs);
        Object transformed = value;
        int idx = 0;
        for (TransformationSpecDto s : specs) {
            String kind = s.getKind();
            if ("jolt".equalsIgnoreCase(kind)) {
                if (isCoveredByScript(s, skipJolt)) {
                    idx++;
                    continue;
                }
                if (s.getJoltSpec() == null || s.getJoltSpec().isEmpty()) {
                    idx++;
                    continue;
                }
                try {
                    transformed = joltTransformService.transform(transformed, s.getJoltSpec());
                } catch (Exception e) {
                    warnings.add(warn(idx, s, null, e));
                }
            } else if ("script".equalsIgnoreCase(kind)) {
                pending.add(s);
            } else {
                warnings.add("transform[" + idx + "]: unknown kind " + kind + " (skipped)");
            }
            idx++;
        }
        return new ValueResult(transformed, pending, warnings);
    }

    private Set<EdgeKey> buildScriptCoveredEdges(List<TransformationSpecDto> specs) {
        Set<EdgeKey> out = new HashSet<>();
        for (TransformationSpecDto s : specs) {
            if ("script".equalsIgnoreCase(s.getKind())) {
                out.add(EdgeKey.of(s));
            }
        }
        return out;
    }

    private boolean isCoveredByScript(TransformationSpecDto s, Set<EdgeKey> covered) {
        if (covered.isEmpty()) {
            return false;
        }
        return covered.contains(EdgeKey.of(s)) || covered.contains(EdgeKey.EMPTY);
    }

    /** Full edge identity quad + direction — two directives sharing it operate on the same wire. */
    private record EdgeKey(String source, String target, String sourcePort, String targetPort, String direction) {
        static final EdgeKey EMPTY = new EdgeKey("", "", "", "", "");

        static EdgeKey of(TransformationSpecDto s) {
            return new EdgeKey(
                    nvl(s.getSourceNodeId()), nvl(s.getTargetNodeId()),
                    nvl(s.getSourcePortShortCode()), nvl(s.getTargetPortShortCode()), nvl(s.getDirection()));
        }

        private static String nvl(String v) {
            return v == null ? "" : v;
        }
    }

    private static String warn(int idx, TransformationSpecDto s, String detail, Exception e) {
        return "transform[" + idx + "] jolt " + (detail == null ? "" : detail + " ")
                + "edge=" + s.getSourceNodeId() + "->" + s.getTargetNodeId() + ": " + e.getMessage();
    }
}
