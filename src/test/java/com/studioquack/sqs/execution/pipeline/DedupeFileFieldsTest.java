package com.studioquack.sqs.execution.pipeline;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Pins the LLM-envelope dedupe: files publish ONCE (the dedicated file field); the copy an older
 * in-container node-library left inside {@code result.output_files} is stripped before fan-out,
 * spill and the sandbox-log report — so the run-log panels stop rendering the same file twice and
 * the spiller stops writing it once per copy.
 */
class DedupeFileFieldsTest {

    @Test
    void stripsResultOutputFilesWhenDedicatedFileFieldIsPopulated() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("role", "assistant");
        result.put("content", List.of(Map.of("type", "text", "text", "done")));
        result.put("output_files", List.of(Map.of("name", "a.png", "data", "…")));
        result.put("finish_reason", "stop");
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("result", result);
        envelope.put("outputFiles", List.of(Map.of("name", "a.png", "data", "…")));

        Object out = DefaultTransformPipeline.dedupeFileFields(envelope);

        @SuppressWarnings("unchecked")
        Map<String, Object> outMap = (Map<String, Object>) out;
        @SuppressWarnings("unchecked")
        Map<String, Object> outResult = (Map<String, Object>) outMap.get("result");
        assertEquals(Map.of("role", "assistant",
                        "content", List.of(Map.of("type", "text", "text", "done")),
                        "finish_reason", "stop"),
                outResult);
        assertEquals(1, ((List<?>) outMap.get("outputFiles")).size());
    }

    @Test
    void leavesResultUntouchedWhenItCarriesTheOnlyFileCopy() {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("result", Map.of("output_files", List.of(Map.of("name", "a.png"))));

        Object out = DefaultTransformPipeline.dedupeFileFields(envelope);

        assertSame(envelope, out);
    }

    @Test
    void leavesNonEnvelopeValuesUntouched() {
        assertSame("plain", DefaultTransformPipeline.dedupeFileFields("plain"));
        Map<String, Object> noFiles = Map.of("result", Map.of("role", "assistant"));
        assertSame(noFiles, DefaultTransformPipeline.dedupeFileFields(noFiles));
        Map<String, Object> emptyFiles = Map.of(
                "result", Map.of("output_files", List.of()),
                "outputFiles", List.of());
        assertSame(emptyFiles, DefaultTransformPipeline.dedupeFileFields(emptyFiles));
    }
}
