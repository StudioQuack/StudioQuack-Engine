package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Batch {@code stage-oci-build-files} tests — same deny-path style as
 * {@link StageOciBuildFileCommandHandlerTest}; unseal is overridden so tests do not
 * need real envelope crypto.
 */
class StageOciBuildFilesCommandHandlerTest {

    private static final String ENV = "env-1";
    private static final String BUILD_ID = "build-batch-1";

    private final ObjectMapper mapper = new ObjectMapper();
    private SqsChannelProperties channel;
    private OciBuildContextStore contextStore;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        channel = new SqsChannelProperties();
        channel.setEnvId(ENV);
        channel.setBearer("sqvm_test_bearer");
        contextStore = new OciBuildContextStore(tempDir.toString());
    }

    @Test
    void stages_many_files_in_one_command() throws Exception {
        StageOciBuildFilesCommandHandler handler = handlerWithFixedUnseal();
        ObjectNode payload = batchPayload(
                new String[][]{{"a.txt", "aaa"}, {"dir/b.txt", "bbbb"}, {"dir/c.txt", "cc"}});

        DaemonCommandOutcome out = handler.handle(cmd(payload));

        assertTrue(out.succeeded(), () -> out.error());
        assertEquals(3, contextStore.entryCount(BUILD_ID));
        JsonNode result = mapper.readTree(out.output());
        assertEquals(3, result.get("count").asInt());
        assertEquals(3, result.get("files").size());
    }

    @Test
    void refuses_empty_files_array() throws Exception {
        StageOciBuildFilesCommandHandler handler = handlerWithFixedUnseal();
        ObjectNode payload = mapper.createObjectNode();
        payload.put("envId", ENV);
        payload.put("buildId", BUILD_ID);
        payload.set("files", mapper.createArrayNode());

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("files"), () -> out.error());
    }

    @Test
    void refuses_missing_buildId() throws Exception {
        StageOciBuildFilesCommandHandler handler = handlerWithFixedUnseal();
        ObjectNode payload = batchPayload(new String[][]{{"a.txt", "aaa"}});
        payload.remove("buildId");

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("buildId"), () -> out.error());
    }

    @Test
    void one_bad_file_fails_whole_batch_and_reports_index() throws Exception {
        StageOciBuildFilesCommandHandler handler = handlerWithFixedUnseal();
        // Second entry's sha256 will not match the unsealed bytes.
        ObjectNode payload = batchPayload(new String[][]{{"a.txt", "aaa"}, {"bad.txt", "bbb"}});
        ((ObjectNode) payload.get("files").get(1)).put("sha256", "0".repeat(64));

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("file[1]"), () -> out.error());
        assertTrue(out.error().contains("bad.txt"), () -> out.error());
    }

    // --- helpers -----------------------------------------------------------

    private StageOciBuildFilesCommandHandler handlerWithFixedUnseal() {
        return new StageOciBuildFilesCommandHandler(mapper, contextStore, channel) {
            @Override
            byte[] unsealContent(JsonNode payload, JsonNode contentNode) {
                // Echo the expected content per file: the test payload carries the plain
                // bytes base64'd inside the placeholder envelope so each file differs.
                return java.util.Base64.getDecoder().decode(contentNode.get("ciphertext").asText());
            }
        };
    }

    /** entries: {path, plainText} — the placeholder envelope carries base64(plainText). */
    private ObjectNode batchPayload(String[][] entries) throws Exception {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("envId", ENV);
        payload.put("buildId", BUILD_ID);
        ArrayNode files = mapper.createArrayNode();
        for (String[] e : entries) {
            byte[] raw = e[1].getBytes(StandardCharsets.UTF_8);
            String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw));
            ObjectNode f = mapper.createObjectNode();
            f.put("path", e[0]);
            f.put("sizeBytes", raw.length);
            f.put("sha256", sha);
            ObjectNode content = mapper.createObjectNode();
            content.put("v", 1);
            content.put("ciphertext", java.util.Base64.getEncoder().encodeToString(raw));
            f.set("content", content);
            files.add(f);
        }
        payload.set("files", files);
        return payload;
    }

    private DaemonCommand cmd(ObjectNode payload) throws Exception {
        return new DaemonCommand("cmd-stage-batch", StageOciBuildFilesCommandHandler.TYPE_BATCH,
                "2026-08-20T00:00:00Z", mapper.writeValueAsString(payload));
    }

    private static void assertFailed(DaemonCommandOutcome out) {
        assertEquals(DaemonCommandOutcome.STATUS_FAILED, out.status(), () -> out.output());
    }
}
