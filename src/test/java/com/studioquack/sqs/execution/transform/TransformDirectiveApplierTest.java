package com.studioquack.sqs.execution.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import com.studioquack.sqs.execution.TransformFixtures;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Function-port applyToValue contract — Invariants 5 (script wins) and 11 (warn, don't throw).
 * Ctx-map apply(Map) is gone; inbound JOLT lives in DefaultTransformPipeline.
 */
class TransformDirectiveApplierTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final TransformDirectiveApplier applier =
            new TransformDirectiveApplier(new JoltTransformService(mapper));

    private TransformationSpecDto jolt(String src, String tgt, String sport, String tport, List<Object> spec) {
        return TransformationSpecDto.builder()
                .kind("jolt").joltSpec(spec)
                .sourceNodeId(src).targetNodeId(tgt)
                .sourcePortShortCode(sport).targetPortShortCode(tport)
                .direction("input").build();
    }

    private TransformationSpecDto script(String src, String tgt, String sport, String tport, String body) {
        return TransformationSpecDto.builder()
                .kind("script").script(body).language("javascript")
                .sourceNodeId(src).targetNodeId(tgt)
                .sourcePortShortCode(sport).targetPortShortCode(tport)
                .direction("input").build();
    }

    /** applyToValue applies JOLT in order and honours script-wins. */
    @Test
    void applyToValue_transforms_value_and_defers_script() {
        var jolted = applier.applyToValue(
                specs(jolt("cur", "sub", "a", "x", TransformFixtures.shiftSpec("a", "x"))),
                TransformFixtures.map("a", 7));
        assertEquals(Map.of("x", 7), jolted.value());
        assertTrue(jolted.pending().isEmpty());

        var scripted = applier.applyToValue(
                specs(jolt("cur", "sub", "a", "x", TransformFixtures.shiftSpec("a", "x")),
                        script("cur", "sub", "a", "x", "return v;")),
                TransformFixtures.map("a", 7));
        assertEquals(Map.of("a", 7), scripted.value(), "script covers the edge → JOLT skipped");
        assertEquals(1, scripted.pending().size());
    }

    /** Invariant 11: a transform engine failure becomes a warning; the job is never failed. */
    @Test
    void applyToValue_transform_failure_is_a_warning_not_a_throw() {
        JoltTransformService throwing = new JoltTransformService(mapper) {
            @Override
            public Object transform(Object inputJson, List<Object> specJson) {
                throw new RuntimeException("boom");
            }
        };
        TransformDirectiveApplier failingApplier = new TransformDirectiveApplier(throwing);

        var result = failingApplier.applyToValue(
                specs(jolt("cur", "sub", "a", "x", TransformFixtures.shiftSpec("a", "x"))),
                TransformFixtures.map("a", 7));
        assertFalse(result.warnings().isEmpty(), "a JOLT failure must surface as a warning");
        assertEquals(Map.of("a", 7), result.value(), "value stays as-is when JOLT throws");
    }

    private static List<TransformationSpecDto> specs(TransformationSpecDto... s) {
        return new ArrayList<>(List.of(s));
    }
}
