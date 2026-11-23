package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.AgenticInfo;
import com.studioquack.common.dto.flow.ConversationFileRefDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.files.ClientFileStore;
import com.studioquack.sqs.status.ConversationOutputCallback;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DuckbotBridgeTest {

    private static final String VOICE_BYTES = "fake-audio-bytes";
    private static final String VOICE_B64 =
            Base64.getEncoder().encodeToString(VOICE_BYTES.getBytes(StandardCharsets.UTF_8));
    private static final String VOICE_HANDLE = ClientFileStore.REF_PREFIX + "voice-1";

    @Test
    @DisplayName("start node publishes SOURCE-shaped values; the inbound leg owns the msg→prompt mapping")
    void startNodePublishesSourceShapedValueForTheInboundLegToMap() {
        // This test covers edge publication only; history lives in DuckbotBridgeConversationTest.
        DuckbotBridge bridge = newBridge(mock(ClientFileStore.class));

        NodeDto start = startWithEdge();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", "merhaba", true));

        bridge.applyStart(ctx);

        List<InOutValueDto> outputs = start.getOutputList();
        assertEquals(1, outputs.size());
        InOutValueDto out = outputs.get(0);
        assertEquals("output", out.getPortValue(), "port identity remains the source handle short-code");
        assertEquals("input", out.getTargetPortValue(), "target port identity remains the target handle short-code");

        Map<?, ?> payload = assertInstanceOf(Map.class, out.getValue());
        // Pre-shifting here is what broke the mapping: a shift only rewrites keys it can find, so
        // `{prompt:…}` no longer matched its own msg→prompt spec on the consuming node's inbound leg.
        assertEquals("merhaba", payload.get("msg"), "published under the SOURCE attribute");
        assertFalse(payload.containsKey("prompt"),
                "the target attribute is applied by the consuming node's inbound transform, not here");
        assertFalse(payload.containsKey("output"), "port name must not leak into the value payload");
        assertFalse(payload.containsKey("voices"), "no voices when AgenticInfo.voice is absent");
        assertFalse(payload.containsKey("voice"), "the old voice key must not be emitted");
    }

    @Test
    @DisplayName("two attribute mappings on ONE wire publish one combined value, not two colliding ones")
    void startNode_msgAndFilesOnTheSameWirePublishOneCombinedValue() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(ClientFileStore.REF_PREFIX + "a1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("a1", "a.gif", "image/gif", 3,
                        "gif".getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        // One visual edge, two mapped attributes — the designer emits one EdgeTransformDto per
        // attribute, both sharing source and target port. Emitting a DTO each gave them the identical
        // binding key, so the store and the inbound merge kept only the last one and the agent lost
        // the message.
        NodeDto start = startWithWire(
                transform("output", "msg", "prompt", "port-out", "port-in", "agent-node"),
                transform("output", "files", "files", "port-out", "port-in", "agent-node"));
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "merhaba", true);
        ai.setAttachments(new java.util.ArrayList<>(List.of(
                fileRef(ClientFileStore.REF_PREFIX + "a1", "a.gif"))));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        assertEquals(1, start.getOutputList().size(), "one wire is one output");
        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        assertEquals("merhaba", payload.get("msg"), "the message must survive alongside the files");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = assertInstanceOf(List.class, payload.get("files"));
        assertEquals("a.gif", files.get(0).get("name"));
    }

    @Test
    @DisplayName("distinct wires to the same node stay separate; msg wire skips sibling files")
    void startNode_distinctWiresToTheSameNodeStaySeparate() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(ClientFileStore.REF_PREFIX + "a1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("a1", "a.gif", "image/gif", 3,
                        "gif".getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        // Separate msg and files edges to the same agent — sibling injection on the msg wire would
        // duplicate attachments (Duck Tales saw 2× files for one upload).
        NodeDto start = startWithWire(
                transform("output", "msg", "prompt", "port-out-a", "port-in-a", "agent-node"),
                transform("files", "files", "files", "port-out-b", "port-in-b", "agent-node"));
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "merhaba", true);
        ai.setAttachments(new java.util.ArrayList<>(List.of(
                fileRef(ClientFileStore.REF_PREFIX + "a1", "a.gif"))));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        assertEquals(2, start.getOutputList().size(),
                "different source/target ports are different wires and must not be merged");
        Map<?, ?> msgPayload = null;
        Map<?, ?> filesPayload = null;
        for (InOutValueDto dto : start.getOutputList()) {
            Map<?, ?> value = assertInstanceOf(Map.class, dto.getValue());
            if ("files".equals(dto.getPortValue())) {
                filesPayload = value;
            } else {
                msgPayload = value;
            }
        }
        assertEquals("merhaba", msgPayload.get("msg"));
        assertFalse(msgPayload.containsKey("files"),
                "must not inject sibling files when a dedicated files wire targets the same node");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = assertInstanceOf(List.class, filesPayload.get("files"));
        assertEquals("a.gif", files.get(0).get("name"));
    }

    @Test
    @DisplayName("REPRO: start→Docling (msg) + start→A (msg,files) — Docling wire must carry the file")
    void repro_doclingWireMustCarryTheFile() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(ClientFileStore.REF_PREFIX + "a1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("a1", "a.gif", "image/gif", 3,
                        "gif".getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        // Exact wiring from the failing run: start → Docling on the msg attribute only, and
        // start → A on msg + files (two attribute rows on the A wire).
        NodeDto start = startWithWire(
                transform("output", "msg", "document", "port-out", "port-in-docling", "docling-node"),
                transform("output", "msg", "prompt", "port-out", "port-in-a", "agent-a"),
                transform("output", "files", "files", "port-out", "port-in-a", "agent-a"));
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "merhaba", true);
        ai.setAttachments(new java.util.ArrayList<>(List.of(
                fileRef(ClientFileStore.REF_PREFIX + "a1", "a.gif"))));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        InOutValueDto toDocling = null;
        InOutValueDto toA = null;
        for (InOutValueDto dto : start.getOutputList()) {
            if ("docling-node".equals(dto.getTargetNodeId())) toDocling = dto;
            if ("agent-a".equals(dto.getTargetNodeId())) toA = dto;
        }
        System.out.println("TO_DOCLING=" + (toDocling == null ? "null" : toDocling.getValue()));
        System.out.println("TO_A=" + (toA == null ? "null" : toA.getValue()));
        org.junit.jupiter.api.Assertions.assertNotNull(toDocling, "no output addressed to Docling at all");
        Map<?, ?> doclingPayload = assertInstanceOf(Map.class, toDocling.getValue());
        assertTrue(doclingPayload.containsKey("files"),
                "Docling's msg-only wire must still offer the files sibling — got keys "
                        + doclingPayload.keySet());
    }

    @Test
    @DisplayName("a files-named row whose value never ships does NOT suppress the sibling")
    void startNode_filesRowOverwrittenOnSameWire_stillInjectsSibling() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(ClientFileStore.REF_PREFIX + "a1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("a1", "a.gif", "image/gif", 3,
                        "gif".getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        // One wire, two rows sharing the msg source attribute: the files-valued row is overwritten
        // by the prompt row, so no published payload carries the attachments. Name-based suppression
        // would still see "files" and withhold the sibling — the only copy of the file, lost.
        NodeDto start = startWithWire(
                transform("output", "msg", "files", "port-out-a", "port-in-a", "docling-node"),
                transform("output", "msg", "text", "port-out-a", "port-in-a", "docling-node"));
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "merhaba", true);
        ai.setAttachments(new java.util.ArrayList<>(List.of(
                fileRef(ClientFileStore.REF_PREFIX + "a1", "a.gif"))));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        assertEquals(1, start.getOutputList().size(), "both rows share one wire → one DTO");
        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        assertEquals("merhaba", payload.get("msg"), "the later row wins the shared key");
        assertTrue(payload.containsKey("files"),
                "no wire actually ships the attachments, so the sibling must be injected");
    }

    @Test
    @DisplayName("a wire already carrying the files value is not re-offered it as a sibling")
    void startNode_filesValueOnTheWire_notDuplicatedAsSibling() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(ClientFileStore.REF_PREFIX + "a1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("a1", "a.gif", "image/gif", 3,
                        "gif".getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        // msg → files selector: the wire's payload carries the attachment list under msg.
        NodeDto start = startWithWire(
                transform("output", "msg", "files", "port-out-a", "port-in-a", "docling-node"));
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "merhaba", true);
        ai.setAttachments(new java.util.ArrayList<>(List.of(
                fileRef(ClientFileStore.REF_PREFIX + "a1", "a.gif"))));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        assertInstanceOf(List.class, payload.get("msg"), "the files value rides the wire under msg");
        assertFalse(payload.containsKey("files"),
                "the wire already ships the attachments — a sibling would duplicate them");
    }

    @Test
    @DisplayName("a files wire carries files even when the port handle is named generically")
    void startNode_filesAttributeOnAGenericPortStillCarriesFiles() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(ClientFileStore.REF_PREFIX + "a1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("a1", "a.gif", "image/gif", 3,
                        "gif".getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        // Port handle is "output", but the author mapped the `files` attribute. Selecting on the port
        // short code published the chat text under `files`, so the consumer got a string where it
        // expected file objects.
        NodeDto start = startWithPortEdge("output", "files", "files");
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "look at this", true);
        ai.setAttachments(new java.util.ArrayList<>(List.of(
                fileRef(ClientFileStore.REF_PREFIX + "a1", "a.gif"))));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = assertInstanceOf(List.class, payload.get("files"));
        assertEquals("a.gif", files.get(0).get("name"));
    }

    @Test
    @DisplayName("start node exposes voices as a files-shaped one-item list (post-ingest handle)")
    void startNode_publishesVoiceBase64() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(VOICE_HANDLE))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "voice-1", "clip.webm", "audio/webm", VOICE_BYTES.length(),
                VOICE_BYTES.getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        NodeDto start = startWithEdge();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "hello", true);
        ai.setVoice(voiceRef(VOICE_HANDLE, null));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        assertEquals("hello", payload.get("msg"));
        Map<String, Object> voice = assertVoiceClip(payload.get("voices"));
        assertFalse(payload.containsKey("voice"), "the old voice key must not be emitted");
        assertEquals(VOICE_B64, voice.get("data"), "the base64 clip rides inline under data");
        assertEquals("user prompt by voice command.webm", voice.get("name"));
        assertEquals("audio", voice.get("kind"));
        assertEquals("audio/webm", voice.get("mime_type"));
        assertEquals("sq-file", voice.get("source"));
        assertEquals(VOICE_HANDLE, voice.get("fileId"));
    }

    /** The voices port value is a one-item list in the files ContentBlock shape. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> assertVoiceClip(Object value) {
        List<Map<String, Object>> voice = assertInstanceOf(List.class, value,
                "voices is a list in the files item shape, not a bare base64 string");
        assertEquals(1, voice.size(), "one voice turn is one clip");
        return voice.get(0);
    }

    @Test
    @DisplayName("voice-only turn (null prompt) still publishes voices with empty msg path")
    void startNode_voiceOnly_publishesVoice() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(VOICE_HANDLE))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "voice-1", "clip.webm", "audio/webm", VOICE_BYTES.length(),
                VOICE_BYTES.getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        NodeDto start = startWithEdge();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo();
        ai.setConversationId("conversation-1");
        ai.setAgentMessageKey("agent");
        ai.setUserPrompt(null);
        ai.setVoice(voiceRef(VOICE_HANDLE, null));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        assertTrue(start.getOutputList() != null && !start.getOutputList().isEmpty());
        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        assertEquals(VOICE_B64, assertVoiceClip(payload.get("voices")).get("data"));
        assertFalse(payload.containsKey("voice"), "the old voice key must not be emitted");
        // Empty prompt is still published, so the consumer's mapping stays consistent.
        assertEquals("", payload.get("msg"));
    }

    @Test
    @DisplayName("absent voice omits the voices key")
    void startNode_noVoice_omitsKey() {
        DuckbotBridge bridge = newBridge(mock(ClientFileStore.class));
        NodeDto start = startWithEdge();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        ctx.setAgenticInfo(new AgenticInfo("c1", "agent", "text only", true));

        bridge.applyStart(ctx);

        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        assertFalse(payload.containsKey("voices"));
        assertFalse(payload.containsKey("voice"));
    }

    @Test
    @DisplayName("the files output port seals ContentBlocks with fileId only (no multi-MB data)")
    void startNode_filesPort_publishesContentBlocksWithoutData() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(ClientFileStore.REF_PREFIX + "a1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("a1", "a.gif", "image/gif", 3, "gif".getBytes(StandardCharsets.UTF_8))));
        when(store.resolve(eq(ClientFileStore.REF_PREFIX + "b1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("b1", "b.pdf", "application/pdf", 3, "pdf".getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        NodeDto start = startWithPortEdge("files", "files", "files");
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "look at these", true);
        ai.setAttachments(new java.util.ArrayList<>(List.of(
                fileRef(ClientFileStore.REF_PREFIX + "a1", "a.gif"),
                fileRef(ClientFileStore.REF_PREFIX + "b1", "b.pdf"))));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = assertInstanceOf(List.class, payload.get("files"));
        assertEquals(2, files.size());
        assertEquals("image", files.get(0).get("kind"));
        assertEquals("image/gif", files.get(0).get("mime_type"));
        assertEquals("sq-file", files.get(0).get("source"));
        assertEquals("a.gif", files.get(0).get("name"));
        assertFalse(files.get(0).containsKey("data"),
                "sealed context must not carry base64 — ContentBlockHydrator fills it at sandbox bake");
        assertEquals(ClientFileStore.REF_PREFIX + "a1", files.get(0).get("fileId"));
        assertEquals("document", files.get(1).get("kind"));
        assertFalse(payload.containsKey("msg"), "a dedicated files edge must not smuggle the prompt");
    }

    @Test
    @DisplayName("the voices output port publishes the clip as a files-shaped list, wired straight to a downstream node")
    void startNode_voicePort_publishesBase64() {
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(VOICE_HANDLE))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "voice-1", "clip.webm", "audio/webm", VOICE_BYTES.length(),
                VOICE_BYTES.getBytes(StandardCharsets.UTF_8))));
        DuckbotBridge bridge = newBridge(store);

        NodeDto start = startWithPortEdge("voices", "voices", "voices");
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(start);
        AgenticInfo ai = new AgenticInfo("conversation-1", "agent", "hear this", true);
        ai.setVoice(voiceRef(VOICE_HANDLE, null));
        ctx.setAgenticInfo(ai);

        bridge.applyStart(ctx);

        Map<?, ?> payload = assertInstanceOf(Map.class, start.getOutputList().get(0).getValue());
        assertEquals(VOICE_B64, assertVoiceClip(payload.get("voices")).get("data"),
                "the voices port carries the clip as a files-shaped list with base64 under data");
        assertFalse(payload.containsKey("voice"), "the old voice key must not be emitted");
        assertFalse(payload.containsKey("msg"), "a dedicated voices edge must not smuggle the prompt");
    }

    @Test
    @DisplayName("end node sends an answer file's BYTES to the backend, resolving the client handle")
    void endNode_resolvesFileHandleToBase64ForTheBackend() {
        byte[] gif = "GIF89a-bytes".getBytes(StandardCharsets.UTF_8);
        String handle = ClientFileStore.REF_PREFIX + "answer-1";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "answer-1", "chart.gif", "image/gif", gif.length, gif)));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of(
                "msg", "here is your chart",
                "files", List.of(Map.of("kind", "image", "mime_type", "image/gif",
                        "source", "sq-file", "name", "chart.gif", "fileId", handle)))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals(1, pushed.size());
        List<ConversationOutputCallback.OutputFile> files = pushed.get(0).files();
        assertEquals(1, files.size(), "the answer's file crosses to the backend");
        ConversationOutputCallback.OutputFile file = files.get(0);
        assertEquals("chart.gif", file.name());
        assertEquals("image/gif", file.mimeType());
        assertEquals(Base64.getEncoder().encodeToString(gif), file.contentBase64(),
                "a client handle is meaningless to the backend — the bytes must travel");
        assertTrue(pushed.get(0).voices().isEmpty(), "files-port audio stays files, never voices");
    }

    @Test
    @DisplayName("end node forwards voices from the voices port, resolving sq-file handles")
    void endNode_forwardsVoicesFromVoicesPort() {
        byte[] clip = VOICE_BYTES.getBytes(StandardCharsets.UTF_8);
        String handle = ClientFileStore.REF_PREFIX + "answer-voice";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "answer-voice", "reply.webm", "audio/webm", clip.length, clip)));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of(
                "msg", "here is the clip",
                "voices", List.of(Map.of("kind", "audio", "mime_type", "audio/webm",
                        "source", "sq-file", "name", "reply.webm", "fileId", handle)))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals(1, pushed.size());
        assertEquals("here is the clip", pushed.get(0).message());
        assertTrue(pushed.get(0).files().isEmpty(), "voices must not be stolen onto files");
        List<ConversationOutputCallback.OutputFile> voices = pushed.get(0).voices();
        assertEquals(1, voices.size(), "the answer's voice crosses to the backend");
        assertEquals("reply.webm", voices.get(0).name());
        assertEquals("audio/webm", voices.get(0).mimeType());
        assertEquals("audio", voices.get(0).kind());
        assertEquals(Base64.getEncoder().encodeToString(clip), voices.get(0).contentBase64());
    }

    @Test
    @DisplayName("end node: files-only audio (no voices key) is moved into voices, not attachments")
    void endNode_filesOnlyAudioMovesToVoices() {
        String data = Base64.getEncoder().encodeToString(VOICE_BYTES.getBytes(StandardCharsets.UTF_8));
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of(
                "msg", "attached audio",
                "files", List.of(Map.of("kind", "audio", "mime_type", "audio/webm",
                        "name", "memo.webm", "source", "base64", "data", data)))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertTrue(pushed.get(0).files().isEmpty(), "moved audio must not also attach");
        List<ConversationOutputCallback.OutputFile> voices = pushed.get(0).voices();
        assertEquals(1, voices.size(), "files-port audio becomes a voice when VOICE_KEYS found none");
        assertEquals("memo.webm", voices.get(0).name());
        assertEquals("audio/webm", voices.get(0).mimeType());
        assertEquals("audio", voices.get(0).kind());
        assertEquals(data, voices.get(0).contentBase64());
    }

    @Test
    @DisplayName("end node: when the voices key is present, files-port audio stays on files")
    void endNode_doesNotStealAudioFromFilesPortWhenVoicesKeyPresent() {
        String voiceData = Base64.getEncoder().encodeToString(VOICE_BYTES.getBytes(StandardCharsets.UTF_8));
        String fileAudio = Base64.getEncoder().encodeToString("files-port-clip".getBytes(StandardCharsets.UTF_8));
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of(
                "msg", "both ports",
                "voices", List.of(Map.of("kind", "audio", "mime_type", "audio/webm",
                        "name", "reply.webm", "source", "base64", "data", voiceData)),
                "files", List.of(Map.of("kind", "audio", "mime_type", "audio/webm",
                        "name", "memo.webm", "source", "base64", "data", fileAudio)))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals(1, pushed.get(0).files().size(), "VOICE_KEYS wins — do not steal files-port audio");
        assertEquals("memo.webm", pushed.get(0).files().get(0).name());
        assertEquals(fileAudio, pushed.get(0).files().get(0).contentBase64());
        assertEquals(1, pushed.get(0).voices().size());
        assertEquals("reply.webm", pushed.get(0).voices().get(0).name());
        assertEquals(voiceData, pushed.get(0).voices().get(0).contentBase64());
    }

    @Test
    @DisplayName("end node: files-only audio+image moves only the audio into voices")
    void endNode_filesOnlyAudioAndImageSplitsVoicesAndFiles() {
        String audioData = Base64.getEncoder().encodeToString(VOICE_BYTES.getBytes(StandardCharsets.UTF_8));
        String imageData = Base64.getEncoder().encodeToString("GIF89a-bytes".getBytes(StandardCharsets.UTF_8));
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of(
                "msg", "mixed files",
                "files", List.of(
                        Map.of("kind", "audio", "mime_type", "audio/webm",
                                "name", "memo.webm", "source", "base64", "data", audioData),
                        Map.of("kind", "image", "mime_type", "image/gif",
                                "name", "chart.gif", "source", "base64", "data", imageData)))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        List<ConversationOutputCallback.OutputFile> files = pushed.get(0).files();
        List<ConversationOutputCallback.OutputFile> voices = pushed.get(0).voices();
        assertEquals(1, files.size(), "image stays a file");
        assertEquals("chart.gif", files.get(0).name());
        assertEquals("image", files.get(0).kind());
        assertEquals(imageData, files.get(0).contentBase64());
        assertEquals(1, voices.size(), "only kind=audio is moved");
        assertEquals("memo.webm", voices.get(0).name());
        assertEquals("audio", voices.get(0).kind());
        assertEquals(audioData, voices.get(0).contentBase64());
    }

    @Test
    @DisplayName("end node: a voices-only map is not mistaken for the answer text")
    void endNode_voicesOnlyMapIsNotAnswerText() {
        String data = Base64.getEncoder().encodeToString(VOICE_BYTES.getBytes(StandardCharsets.UTF_8));
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of(
                "voices", List.of(Map.of("kind", "audio", "mime_type", "audio/webm",
                        "name", "only.webm", "source", "base64", "data", data)))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals("", pushed.get(0).message(), "a voices-only map is not the bubble text");
        assertTrue(pushed.get(0).files().isEmpty());
        assertEquals(1, pushed.get(0).voices().size());
        assertEquals("only.webm", pushed.get(0).voices().get(0).name());
    }

    @Test
    @DisplayName("end node keeps inline base64 output files as-is")
    void endNode_passesInlineBase64Through() {
        String data = Base64.getEncoder().encodeToString("report".getBytes(StandardCharsets.UTF_8));
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of(
                "msg", "done",
                "files", List.of(Map.of("mime_type", "application/pdf",
                        "name", "report.pdf", "source", "base64", "data", data)))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        ConversationOutputCallback.OutputFile file = pushed.get(0).files().get(0);
        assertEquals(data, file.contentBase64());
        assertEquals("document", file.kind(), "kind is inferred from the mime type when absent");
    }

    @Test
    @DisplayName("thinking-only top-level end still emits stream-end so the FE can settle")
    void endNode_thinkingOnlyTurnEmitsStreamEnd() {
        java.util.List<com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent> seen =
                new java.util.ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(seen::add);
        DuckbotBridge bridge = new DuckbotBridge(noopConversationCallback(),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());
        setStreamSink(bridge, sink);

        NodeDto end = new NodeDto();
        end.setId("duck-end");
        TemplateDto template = new TemplateDto();
        template.setCategory("conversion");
        template.setNodeType("end");
        end.setData(template);
        InOutValueDto thoughts = new InOutValueDto();
        thoughts.setSourcePortType("FUNCTION");
        thoughts.setNodeId("agent-1");
        thoughts.setValue(Map.of("thoughts", "plan the lookup"));
        end.setInputList(List.of(thoughts));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(end);
        ctx.setWorkflowExecutionId("run-1");
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertTrue(seen.stream().anyMatch(e -> "stream-end".equals(e.event())
                && "thinking".equals(e.channel())));
    }

    @Test
    @DisplayName("end node drops the late answer when the run was cancelled (no persist, no report)")
    void endNode_dropsAnswerForCancelledRun() {
        // The user pressed Stop while the LLM node was still generating: cancel-instance marked the
        // run, the in-flight call finished late, and its answer must NOT land in the conversation.
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());
        com.studioquack.sqs.runtime.CancelledRunGuard guard =
                new com.studioquack.sqs.runtime.CancelledRunGuard(600_000L);
        guard.markCancelled("run-1");
        setCancelledRunGuard(bridge, guard);

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("msg", "late answer after stop")));
        ctx.setWorkflowExecutionId("run-1");
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertTrue(pushed.isEmpty(), "a cancelled run's late answer is dropped, never reported");
    }

    @Test
    @DisplayName("end node still delivers when a DIFFERENT run was cancelled")
    void endNode_deliversWhenOtherRunCancelled() {
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());
        com.studioquack.sqs.runtime.CancelledRunGuard guard =
                new com.studioquack.sqs.runtime.CancelledRunGuard(600_000L);
        guard.markCancelled("run-other");
        setCancelledRunGuard(bridge, guard);

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("msg", "on-time answer")));
        ctx.setWorkflowExecutionId("run-1");
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals(1, pushed.size());
        assertEquals("on-time answer", pushed.get(0).message());
    }

    @Test
    @DisplayName("empty answer still publishes to the conversation (terminal SSE update)")
    void endNode_emptyAnswerStillPublishes() {
        java.util.List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(""));
        ctx.setWorkflowExecutionId("run-1");
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals(1, pushed.size());
        assertEquals("", pushed.get(0).message());
    }

    @Test
    @DisplayName("end node stamps the answer's source node id on conversation-output")
    void endNode_stampsSourceNodeIdOnConversationOutput() {
        java.util.List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        NodeDto end = new NodeDto();
        end.setId("duck-end");
        TemplateDto template = new TemplateDto();
        template.setCategory("conversion");
        template.setNodeType("end");
        end.setData(template);
        InOutValueDto in = new InOutValueDto();
        in.setNodeId("agent-1");
        in.setValue("the answer");
        end.setInputList(List.of(in));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(end);
        ctx.setWorkflowExecutionId("run-1");
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals("agent-1", pushed.get(0).nodeId());
        assertEquals("the answer", pushed.get(0).message());
    }

    @Test
    @DisplayName("end node falls back to the only scalar when the attribute name is unknown")
    void endNode_fallsBackToTheSingleScalar() {
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("summary_for_user", "unrecognised but unambiguous")));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals("unrecognised but unambiguous", pushed.get(0).message());
    }

    @Test
    @DisplayName("end node reports no files when the answer carried none")
    void endNode_textOnlyAnswerCarriesNoFiles() {
        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("msg", "plain answer")));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals("plain answer", pushed.get(0).message());
        assertTrue(pushed.get(0).files().isEmpty());
        assertTrue(pushed.get(0).voices().isEmpty());
    }

    @Test
    @DisplayName("end node: bare sq-file handle on the input becomes an answer file, not chat text")
    void endNode_bareHandleIsAttachmentNotMessage() {
        byte[] png = "PNG-BYTES".getBytes(StandardCharsets.UTF_8);
        String handle = ClientFileStore.REF_PREFIX + "bare-1";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "bare-1", "photo.png", "image/png", png.length, png)));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(handle));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals("", pushed.get(0).message(), "sq-file handles must never appear as bubble text");
        assertEquals(1, pushed.get(0).files().size());
        assertEquals(Base64.getEncoder().encodeToString(png), pushed.get(0).files().get(0).contentBase64());
        assertEquals("image", pushed.get(0).files().get(0).kind());
    }

    @Test
    @DisplayName("end node: msg that is an sq-file handle is promoted to an attachment")
    void endNode_msgHandlePromotedToAttachment() {
        byte[] gif = "GIF89a".getBytes(StandardCharsets.UTF_8);
        String handle = ClientFileStore.REF_PREFIX + "msg-file";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "msg-file", "out.gif", "image/gif", gif.length, gif)));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("msg", handle)));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals("", pushed.get(0).message());
        assertEquals(1, pushed.get(0).files().size());
        assertEquals("image/gif", pushed.get(0).files().get(0).mimeType());
    }

    @Test
    @DisplayName("end node: files list of bare handles resolves to bytes")
    void endNode_filesListOfHandles() {
        byte[] bytes = "DOC".getBytes(StandardCharsets.UTF_8);
        String handle = ClientFileStore.REF_PREFIX + "list-1";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "list-1", "note.txt", "text/plain", bytes.length, bytes)));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("files", List.of(handle))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals(1, pushed.get(0).files().size());
        assertEquals("note.txt", pushed.get(0).files().get(0).name());
    }

    /**
     * The reported break: an LLM helper returns its files as a JSON string, the oversized string is
     * spilled into the file store as a text blob named after its port, and the chat then offered a
     * downloadable {@code llm_output.txt} whose contents were a filename and a base64 blob. The user's
     * image is in there — it just was not usable.
     */
    @Test
    @DisplayName("end node opens a JSON file envelope instead of delivering it as a text attachment")
    void endNode_opensJsonEnvelopeFromASpilledTextBlob() {
        byte[] gif = "GIF89a-real-bytes".getBytes(StandardCharsets.UTF_8);
        String envelope = "{\"output_files\":[{\"name\":\"agentic_usecases.gif\","
                + "\"mime_type\":\"image/gif\",\"data\":\""
                + Base64.getEncoder().encodeToString(gif) + "\"}]}";
        String handle = ClientFileStore.REF_PREFIX + "llm-output";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "llm-output", "llm_output.txt", "text/plain",
                envelope.length(), envelope.getBytes(StandardCharsets.UTF_8))));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("files", List.of(handle))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        List<ConversationOutputCallback.OutputFile> files = pushed.get(0).files();
        assertEquals(1, files.size());
        assertEquals("agentic_usecases.gif", files.get(0).name(), "the real filename reaches the chat");
        assertEquals("image/gif", files.get(0).mimeType());
        assertEquals("image", files.get(0).kind(), "so the chat renders a thumbnail, not a download tile");
        assertEquals(Base64.getEncoder().encodeToString(gif), files.get(0).contentBase64(),
                "the image's own bytes travel, not the envelope's");
    }

    /** A JSON string handed straight to the files port is the same problem without the spill. */
    @Test
    @DisplayName("end node opens a JSON file envelope passed inline as text")
    void endNode_opensJsonEnvelopePassedInline() {
        byte[] png = "PNG-bytes".getBytes(StandardCharsets.UTF_8);
        String envelope = "[{\"name\":\"chart.png\",\"mime_type\":\"image/png\",\"data\":\""
                + Base64.getEncoder().encodeToString(png) + "\"}]";

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(envelope));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals("", pushed.get(0).message(), "the envelope is not also pasted into the bubble");
        assertEquals("chart.png", pushed.get(0).files().get(0).name());
    }

    /**
     * REGRESSION GUARD: a real JSON document the workflow produced must stay that document, not be
     * unwrapped as if it were an LLM file envelope ({@code result}/{@code output_files}/base64).
     */
    @Test
    @DisplayName("end node leaves a genuine JSON document alone")
    void endNode_leavesAGenuineJsonDocumentAlone() {
        String json = "{\"customers\":[{\"id\":1,\"name\":\"ACME\"}]}";
        String handle = ClientFileStore.REF_PREFIX + "report";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "report", "report.json", "application/json",
                json.length(), json.getBytes(StandardCharsets.UTF_8))));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("files", List.of(handle))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        assertEquals(1, pushed.get(0).files().size());
        assertEquals("report.json", pushed.get(0).files().get(0).name());
        assertEquals("application/json", pushed.get(0).files().get(0).mimeType(),
                "customers JSON is a document, not an expanded image envelope");
        assertEquals(Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)),
                pushed.get(0).files().get(0).contentBase64());
    }

    /**
     * Real spilled-LLM shape: {@code {"result":{"role","content","output_files":[...]}}} landed in
     * the file store as {@code llm_output.txt}. Opening only the top-level {@code output_files} key
     * is not enough — the nest under {@code result} must open, the assistant {@code content} becomes
     * the chat message, and the image bytes (not the envelope) reach the attachment.
     */
    @Test
    @DisplayName("end node opens nested result.output_files from a spilled LLM text blob")
    void endNode_opensNestedResultOutputFilesFromSpilledLlmOutput() {
        byte[] png = "PNG-nested-bytes".getBytes(StandardCharsets.UTF_8);
        String pngB64 = Base64.getEncoder().encodeToString(png);
        String envelope = "{\"result\":{\"role\":\"assistant\",\"content\":\"Here is your image\","
                + "\"output_files\":[{\"type\":\"image\",\"mime_type\":\"image/png\","
                + "\"source\":\"base64\",\"data\":\"" + pngB64 + "\"}]}}";
        String handle = ClientFileStore.REF_PREFIX + "llm-nested";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "llm-nested", "llm_output.txt", "text/plain",
                envelope.length(), envelope.getBytes(StandardCharsets.UTF_8))));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        // No msg on the end node — the assistant content lives inside the spilled envelope.
        ctx.setCurrentNode(endNodeWithInput(Map.of("files", List.of(handle))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        ConversationOutputCallback.ConversationOutput out = pushed.get(0);
        assertEquals("Here is your image", out.message(),
                "assistant content from the nested result becomes the bubble text");
        List<ConversationOutputCallback.OutputFile> files = out.files();
        assertEquals(1, files.size());
        assertEquals("image", files.get(0).kind());
        assertEquals("image/png", files.get(0).mimeType());
        assertTrue(files.get(0).name().endsWith(".png"),
                "synthesised name must carry a .png extension; got " + files.get(0).name());
        assertEquals(pngB64, files.get(0).contentBase64(),
                "the image's own bytes travel, not the envelope's");
    }

    /**
     * The literal shape `llm-model.py` emits for a generated image, including the name it assigns
     * (`generated_<hex>.<ext>`). The user sees that filename in the chat, so it must survive the
     * envelope verbatim rather than being replaced by a synthesised one.
     */
    @Test
    @DisplayName("end node keeps the producer's own filename for a generated image")
    void endNode_keepsProducerFilenameForGeneratedImage() {
        byte[] png = "PNG-generated".getBytes(StandardCharsets.UTF_8);
        String pngB64 = Base64.getEncoder().encodeToString(png);
        String envelope = "{\"result\":{\"role\":\"assistant\","
                + "\"content\":\"Generated 1 image(s) for prompt.\","
                + "\"output_files\":[{\"type\":\"image\",\"mime_type\":\"image/png\","
                + "\"source\":\"base64\",\"data\":\"" + pngB64 + "\","
                + "\"name\":\"generated_ab12cd34.png\"}],\"finish_reason\":\"stop\"}}";
        String handle = ClientFileStore.REF_PREFIX + "llm-generated";
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "llm-generated", "llm_output.txt", "text/plain",
                envelope.length(), envelope.getBytes(StandardCharsets.UTF_8))));

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(Map.of("files", List.of(handle))));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        ConversationOutputCallback.OutputFile file = pushed.get(0).files().get(0);
        assertEquals("generated_ab12cd34.png", file.name(),
                "the producer's filename is what the user recognises; never overwrite it");
        assertEquals("image/png", file.mimeType());
        assertEquals("image", file.kind(), "so the chat renders a thumbnail, not a download tile");
        assertEquals(pngB64, file.contentBase64());
        assertEquals("Generated 1 image(s) for prompt.", pushed.get(0).message());
    }

    /** Same nested LLM envelope without the file-store spill — JSON string on the end-node port. */
    @Test
    @DisplayName("end node opens nested result.output_files passed inline as text")
    void endNode_opensNestedResultOutputFilesPassedInline() {
        byte[] png = "PNG-inline-nested".getBytes(StandardCharsets.UTF_8);
        String pngB64 = Base64.getEncoder().encodeToString(png);
        String envelope = "{\"result\":{\"role\":\"assistant\",\"content\":\"Here is your image\","
                + "\"output_files\":[{\"type\":\"image\",\"mime_type\":\"image/png\","
                + "\"source\":\"base64\",\"data\":\"" + pngB64 + "\"}]}}";

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(envelope));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        ConversationOutputCallback.ConversationOutput out = pushed.get(0);
        assertEquals("Here is your image", out.message());
        assertEquals(1, out.files().size());
        assertEquals("image", out.files().get(0).kind());
        assertEquals("image/png", out.files().get(0).mimeType());
        assertTrue(out.files().get(0).name().endsWith(".png"));
        assertEquals(pngB64, out.files().get(0).contentBase64());
    }

    /**
     * Producers disagree on field names: {@code filename}/{@code content} instead of
     * {@code name}/{@code data}, and some put a {@code data:} URI in the content field.
     */
    @Test
    @DisplayName("end node accepts filename/content aliases and a data: URI payload")
    void endNode_acceptsFilenameContentAliasesAndDataUri() {
        byte[] png = "PNG-alias-bytes".getBytes(StandardCharsets.UTF_8);
        String pngB64 = Base64.getEncoder().encodeToString(png);
        String envelope = "{\"output_files\":["
                + "{\"filename\":\"chart.png\",\"mime_type\":\"image/png\",\"content\":\""
                + pngB64 + "\"},"
                + "{\"filename\":\"uri.png\",\"mime_type\":\"image/png\",\"content\":"
                + "\"data:image/png;base64," + pngB64 + "\"}"
                + "]}";

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(envelope));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        List<ConversationOutputCallback.OutputFile> files = pushed.get(0).files();
        assertEquals(2, files.size());
        assertEquals("chart.png", files.get(0).name());
        assertEquals("image/png", files.get(0).mimeType());
        assertEquals(pngB64, files.get(0).contentBase64(), "raw base64 in content alias");
        assertEquals("uri.png", files.get(1).name());
        assertEquals(pngB64, files.get(1).contentBase64(),
                "data: URI prefix stripped; attachment carries image bytes only");
    }

    /** A file block that names only its mime still needs a usable download name. */
    @Test
    @DisplayName("end node synthesises a filename from mime_type when name is absent")
    void endNode_synthesisesNameFromMimeWhenMissing() {
        byte[] png = "PNG-no-name".getBytes(StandardCharsets.UTF_8);
        String pngB64 = Base64.getEncoder().encodeToString(png);
        String envelope = "{\"output_files\":[{\"mime_type\":\"image/png\",\"data\":\""
                + pngB64 + "\"}]}";

        List<ConversationOutputCallback.ConversationOutput> pushed = new java.util.ArrayList<>();
        DuckbotBridge bridge = new DuckbotBridge(capturingCallback(pushed),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                mock(ClientFileStore.class), new com.fasterxml.jackson.databind.ObjectMapper());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(endNodeWithInput(envelope));
        ctx.setAgenticInfo(new AgenticInfo("conversation-1", "agent", null, true));

        bridge.applyEnd(ctx);

        ConversationOutputCallback.OutputFile file = pushed.get(0).files().get(0);
        assertEquals("image/png", file.mimeType());
        assertEquals("image", file.kind());
        assertEquals("image.png", file.name(), "mime image/png → synthesised image.png");
        assertEquals(pngB64, file.contentBase64());
    }

    private static NodeDto endNodeWithInput(Object value) {
        NodeDto end = new NodeDto();
        end.setId("duck-end");
        TemplateDto template = new TemplateDto();
        template.setCategory("conversion");
        template.setNodeType("end");
        end.setData(template);
        InOutValueDto in = new InOutValueDto();
        in.setNodeId("duck-end");
        in.setValue(value);
        end.setInputList(List.of(in));
        return end;
    }

    private static ConversationOutputCallback capturingCallback(
            List<ConversationOutputCallback.ConversationOutput> pushed) {
        return output -> {
            pushed.add(output);
            return List.of();
        };
    }

    private static DuckbotBridge newBridge(ClientFileStore store) {
        return new DuckbotBridge(noopConversationCallback(),
                mock(com.studioquack.sqs.conversation.ConversationStore.class),
                store, new com.fasterxml.jackson.databind.ObjectMapper());
    }

    private static void setStreamSink(DuckbotBridge bridge, DuckbotStreamSink sink) {
        try {
            var field = DuckbotBridge.class.getDeclaredField("streamSink");
            field.setAccessible(true);
            field.set(bridge, sink);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void setCancelledRunGuard(DuckbotBridge bridge,
                                             com.studioquack.sqs.runtime.CancelledRunGuard guard) {
        try {
            var field = DuckbotBridge.class.getDeclaredField("cancelledRunGuard");
            field.setAccessible(true);
            field.set(bridge, guard);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A start node with one authored edge from {@code sourcePort} → downstream, identity source path. */
    private static NodeDto startWithPortEdge(String sourcePort, String sourcePath, String targetPath) {
        NodeDto start = new NodeDto();
        start.setId("duck-start");
        TemplateDto template = new TemplateDto();
        template.setCategory("conversion");
        template.setNodeType("start");
        start.setData(template);

        EdgeTransformDto transform = new EdgeTransformDto();
        transform.setSourceNodeId("duck-start");
        transform.setTargetNodeId("agent-node");
        transform.setSourcePortShortCode(sourcePort);
        transform.setTargetPortShortCode(sourcePort);
        transform.setSourcePath(sourcePath);
        transform.setTargetPath(targetPath);
        start.setEdgeTransformListForCurrentNode(List.of(transform));
        return start;
    }

    /** A start node whose edge-transform list is exactly {@code transforms}. */
    private static NodeDto startWithWire(EdgeTransformDto... transforms) {
        NodeDto start = new NodeDto();
        start.setId("duck-start");
        TemplateDto template = new TemplateDto();
        template.setCategory("conversion");
        template.setNodeType("start");
        start.setData(template);
        start.setEdgeTransformListForCurrentNode(List.of(transforms));
        return start;
    }

    private static EdgeTransformDto transform(String sourcePort, String sourcePath, String targetPath,
                                             String sourcePortId, String targetPortId, String targetNodeId) {
        EdgeTransformDto transform = new EdgeTransformDto();
        transform.setSourceNodeId("duck-start");
        transform.setTargetNodeId(targetNodeId);
        transform.setSourcePortShortCode(sourcePort);
        transform.setTargetPortShortCode("input");
        transform.setSourcePortId(sourcePortId);
        transform.setTargetPortId(targetPortId);
        transform.setSourcePath(sourcePath);
        transform.setTargetPath(targetPath);
        return transform;
    }

    private static ConversationFileRefDto fileRef(String handle, String name) {
        ConversationFileRefDto ref = new ConversationFileRefDto();
        ref.setFileId(handle);
        ref.setName(name);
        // kind left null so DuckbotBridge infers from mime (gif→image, pdf→document).
        if (name != null && name.endsWith(".gif")) {
            ref.setMimeType("image/gif");
        } else if (name != null && name.endsWith(".pdf")) {
            ref.setMimeType("application/pdf");
        } else {
            ref.setMimeType("application/octet-stream");
        }
        return ref;
    }

    private static NodeDto startWithEdge() {
        NodeDto start = new NodeDto();
        start.setId("duck-start");
        TemplateDto template = new TemplateDto();
        template.setCategory("conversion");
        template.setNodeType("start");
        start.setData(template);

        EdgeTransformDto transform = new EdgeTransformDto();
        transform.setSourceNodeId("duck-start");
        transform.setTargetNodeId("x-node");
        transform.setSourcePortShortCode("output");
        transform.setTargetPortShortCode("input");
        transform.setSourcePath("msg");
        transform.setTargetPath("prompt");
        start.setEdgeTransformListForCurrentNode(List.of(transform));
        return start;
    }

    private static ConversationFileRefDto voiceRef(String fileId, String contentBase64) {
        ConversationFileRefDto ref = new ConversationFileRefDto();
        ref.setFileId(fileId);
        ref.setName("clip.webm");
        ref.setMimeType("audio/webm");
        ref.setKind("audio");
        ref.setSize(VOICE_BYTES.length());
        ref.setContentBase64(contentBase64);
        return ref;
    }

    private static ConversationOutputCallback noopConversationCallback() {
        return output -> List.of();
    }
}
