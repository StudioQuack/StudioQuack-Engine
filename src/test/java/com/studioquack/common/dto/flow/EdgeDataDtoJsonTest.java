package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@code transformLanguage} / {@code joltNotation} survive Jackson round-trip; legacy blobs without
 * those keys deserialise as null so the collector default {@code "jolt"} stays migrate-friendly.
 */
class EdgeDataDtoJsonTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void roundTripsTransformLanguageAndJoltNotation() throws Exception {
        EdgeDataDto dto = new EdgeDataDto();
        dto.setTransformLanguage("python");
        dto.setJoltNotation("prompt = result");
        dto.setScriptContentInput("return payload");

        EdgeDataDto back = JSON.readValue(JSON.writeValueAsString(dto), EdgeDataDto.class);

        assertEquals("python", back.getTransformLanguage());
        assertEquals("prompt = result", back.getJoltNotation());
        assertEquals("return payload", back.getScriptContentInput());
    }

    @Test
    void absentFieldsDeserializeAsNull() throws Exception {
        // Pre- deploy blob: missing language fields ⇒ null; default "jolt" is consumer-side.
        String legacy = "{\"scriptContentInput\":\"\",\"edgeTransforms\":[]}";

        EdgeDataDto back = JSON.readValue(legacy, EdgeDataDto.class);

        assertNull(back.getTransformLanguage());
        assertNull(back.getJoltNotation());
    }
}
