package com.studioquack.sqs.sandbox.helperchan;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HelperExecuteService#describePayload} — the function-port input log line must show
 * what was addressed to a port (keys, counts, roles, sizes, tool names) while never writing
 * user content into the daemon log.
 */
class HelperExecutePayloadDescriptionTest {

    @Test
    void scalarPayloadsDescribeByShape() {
        assertEquals("payload=null", HelperExecuteService.describePayload(null));
        assertEquals("payload=string[12]", HelperExecuteService.describePayload("hello world!"));
        assertEquals("payload=list[2]", HelperExecuteService.describePayload(List.of(1, 2)));
    }

    @Test
    void llmPayloadsExposeRolesSizesAndToolNames() {
        Map<String, Object> payload = Map.of(
                "messages", List.of(
                        Map.of("role", "system", "content", "You are careful."),
                        Map.of("role", "user", "content", List.of(
                                Map.of("type", "text", "text", "Read this."),
                                Map.of("type", "image", "data", "AAAA")))),
                "tools", List.of(
                        Map.of("name", "lookup", "description", "find"),
                        Map.of("name", "delegate_to_agent")),
                "stream", true);
        String line = HelperExecuteService.describePayload(payload);

        assertTrue(line.contains("messages=2"), line);
        assertTrue(line.contains("user=1"), line);
        assertTrue(line.contains("system=1"), line);
        assertTrue(line.contains("contentChars=26"),
                line + " — string + text-block lengths, media bytes not counted");
        assertTrue(line.contains("tools=2"), line);
        assertTrue(line.contains("lookup"), line);
        assertTrue(line.contains("delegate_to_agent"), line);
    }

    @Test
    void messageTextNeverLeaksIntoTheLine() {
        String secret = "my private prompt text";
        String line = HelperExecuteService.describePayload(Map.of(
                "messages", List.of(Map.of("role", "user", "content", secret))));
        assertFalse(line.contains(secret), "shape only — never user content");
        assertFalse(line.contains("private"), line);
    }

    @Test
    void nonLlmMapsLogKeysOnly() {
        String line = HelperExecuteService.describePayload(Map.of("query", "x", "limit", 5));
        assertTrue(line.contains("keys="), line);
        assertTrue(line.contains("query"), line);
        assertFalse(line.contains("messages="), line);
    }

    @Test
    void inputFilesCountOnlyAndFileDataNeverLeaks() {
        String fileBytes = "BASE64_FILE_BYTES_MUST_NOT_APPEAR";
        String line = HelperExecuteService.describePayload(Map.of(
                "messages", List.of(Map.of(
                        "role", "user",
                        "content", Map.of("type", "image", "data", fileBytes))),
                "inputFiles", List.of(Map.of(
                        "name", "clip.png",
                        "mime_type", "image/png",
                        "data", fileBytes))));
        assertTrue(line.contains("inputFiles=1"), line);
        assertFalse(line.contains(fileBytes), "file data must not appear in the shape line");
        assertFalse(line.contains("clip.png"), line);
    }
}
