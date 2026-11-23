package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Deny-path unit tests for {@link StageOciBuildFileCommandHandler}. Unseal is overridden so tests
 * do not need real envelope crypto.
 */
class StageOciBuildFileCommandHandlerTest {

    private static final String ENV = "env-1";
    private static final String BUILD_ID = "build-abc-123";
    private static final String GOOD_PATH = "src/main/Containerfile";

    private final ObjectMapper mapper = new ObjectMapper();
    private SqsChannelProperties channel;
    private OciBuildContextStore contextStore;
    private byte[] unsealBytes;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        channel = new SqsChannelProperties();
        channel.setEnvId(ENV);
        channel.setBearer("sqvm_test_bearer");
        contextStore = new OciBuildContextStore(tempDir.toString());
        unsealBytes = "staged-file-bytes".getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void refuses_path_traversal_dotdot() throws Exception {
        StageOciBuildFileCommandHandler handler = handlerWithFixedUnseal();
        ObjectNode payload = validPayload("../evil", unsealBytes);

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("..") || out.error().toLowerCase().contains("path"),
                () -> "expected path traversal refusal, got: " + out.error());
        assertEquals(0, contextStore.entryCount(BUILD_ID));
    }

    @Test
    void refuses_sizeBytes_over_5mib_without_writing() throws Exception {
        StageOciBuildFileCommandHandler handler = handlerWithFixedUnseal();
        ObjectNode payload = validPayload(GOOD_PATH, unsealBytes);
        payload.put("sizeBytes", StageOciBuildFileCommandHandler.MAX_STAGE_FILE_BYTES + 1);

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("sizeBytes") || out.error().contains("exceeds"),
                () -> out.error());
        // Cap check runs before unseal/write — context must stay empty.
        assertEquals(0, contextStore.entryCount(BUILD_ID));
        assertTrue(!contextStore.contextExists(BUILD_ID));
    }

    @Test
    void refuses_missing_content() throws Exception {
        StageOciBuildFileCommandHandler handler = handlerWithFixedUnseal();
        ObjectNode payload = validPayload(GOOD_PATH, unsealBytes);
        payload.remove("content");

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("content"), () -> out.error());
        assertEquals(0, contextStore.entryCount(BUILD_ID));
    }

    @Test
    void refuses_missing_buildId() throws Exception {
        StageOciBuildFileCommandHandler handler = handlerWithFixedUnseal();
        ObjectNode payload = validPayload(GOOD_PATH, unsealBytes);
        payload.remove("buildId");

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("buildId"), () -> out.error());
    }

    // --- helpers -----------------------------------------------------------

    private StageOciBuildFileCommandHandler handlerWithFixedUnseal() {
        return new StageOciBuildFileCommandHandler(mapper, contextStore, channel) {
            @Override
            byte[] unsealContent(JsonNode payload, JsonNode contentNode) {
                return unsealBytes;
            }
        };
    }

    private ObjectNode validPayload(String path, byte[] raw) throws Exception {
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw));
        ObjectNode payload = mapper.createObjectNode();
        payload.put("envId", ENV);
        payload.put("buildId", BUILD_ID);
        payload.put("path", path);
        payload.put("sizeBytes", raw.length);
        payload.put("sha256", sha);
        // Placeholder SecureEnvelope JSON object — unseal is overridden in tests.
        ObjectNode content = mapper.createObjectNode();
        content.put("v", 1);
        content.put("ciphertext", "dGVzdA==");
        payload.set("content", content);
        return payload;
    }

    private DaemonCommand cmd(ObjectNode payload) throws Exception {
        return new DaemonCommand("cmd-stage", StageOciBuildFileCommandHandler.TYPE,
                "2026-08-06T00:00:00Z", mapper.writeValueAsString(payload));
    }

    private static void assertFailed(DaemonCommandOutcome out) {
        assertEquals(DaemonCommandOutcome.STATUS_FAILED, out.status(),
                () -> "expected failed, got ok: " + out.output());
    }
}
