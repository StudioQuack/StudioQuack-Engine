package com.studioquack.sqs.files;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * File ContentBlock spilling on node outputs. Only file-typed content spills: a value becomes a
 * file because it is a ContentBlock (declared file type), never because an arbitrary
 * string/structure grew large.
 */
class ParameterSpillerTest {

    private static final int THRESHOLD = 1024;

    private ClientFileStore store;
    private ParameterSpiller spiller;

    @BeforeEach
    void setUp() {
        store = mock(ClientFileStore.class);
        when(store.store(anyString(), anyString(), anyString(), any(), any()))
                .thenReturn(ClientFileStore.REF_PREFIX + "new-id");
        spiller = new ParameterSpiller(store);
        ReflectionTestUtils.setField(spiller, "inlineThresholdBytes", THRESHOLD);
    }

    private static String big() {
        return "x".repeat(THRESHOLD * 2);
    }

    @Test
    @DisplayName("a value under the budget stays inline and never reaches the store")
    void smallValue_staysInline() {
        Object value = "small";

        assertSame(value, spiller.spillIfOversized(value, "out", "run-1"));
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("an oversized NON-FILE string stays inline (no whole-value file conversion)")
    void oversizedString_staysInline() {
        // The removed behaviour: a big text answer became an opaque port-named .txt download.
        Object value = big();
        assertSame(value, spiller.spillIfOversized(value, "out", "run-1"),
                "a large non-file value must not be turned into a file");
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("an oversized NON-FILE structure stays inline (no JSON-blob conversion)")
    void oversizedStructure_staysInline() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("blob", big());

        Object result = spiller.spillIfOversized(value, "payload", "run-1");

        assertSame(value, result, "a large non-file structure must not be turned into a file");
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("the spilled file is RUN-scoped so it dies with the run")
    void spilledFile_isRunScoped() {
        String base64 = java.util.Base64.getEncoder().encodeToString(new byte[THRESHOLD * 2]);
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("name", "f.bin");
        block.put("mime_type", "application/octet-stream");
        block.put("data", base64);
        spiller.spillIfOversized(List.of(block), "out", "run-42");

        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-42"), anyString(), any(), any());
    }

    /**
     * The reported break: an image travelling on a files port arrived in the chat named after the port
     * with a {@code .txt}/{@code .json} extension, because the whole port value was spilled as one
     * opaque blob. A file must keep its own name and media type, and stay a ContentBlock the rest of
     * the pipeline recognises.
     */
    @Test
    @DisplayName("an oversized file on a files port keeps its own name and media type")
    void oversizedContentBlock_spillsPerFile_keepingNameAndMime() {
        String base64 = java.util.Base64.getEncoder().encodeToString(new byte[THRESHOLD * 2]);
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "image");
        block.put("name", "agentic_usecases.gif");
        block.put("mime_type", "image/gif");
        block.put("data", base64);

        Object result = spiller.spillIfOversized(List.of(block), "files", "run-1");

        ArgumentCaptor<String> name = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> mime = ArgumentCaptor.forClass(String.class);
        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-1"), name.capture(), mime.capture(), any());
        assertEquals("agentic_usecases.gif", name.getValue(), "the file's own name survives the spill");
        assertEquals("image/gif", mime.getValue(), "so does its media type");

        List<?> spilled = org.junit.jupiter.api.Assertions.assertInstanceOf(List.class, result);
        Map<?, ?> rewritten = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, spilled.get(0));
        assertTrue(ClientFileStore.isRef(rewritten.get("fileId")), "bytes moved behind a handle");
        assertEquals("agentic_usecases.gif", rewritten.get("name"), "metadata stays inline and readable");
        assertEquals(null, rewritten.get("data"), "the multi-MB base64 is out of the context");
    }

    /**
     * An LLM node's {@code llm_output} is an envelope: the generated image sits at
     * {@code result.output_files[i].data}. Multi-MB base64 there must still spill — otherwise the
     * whole envelope rides the FUNCTION-return transport inline and risks truncation.
     */
    @Test
    @DisplayName("an oversized file nested in an llm_output envelope spills in place")
    void nestedEnvelopeFile_spills() {
        String base64 = java.util.Base64.getEncoder().encodeToString(new byte[THRESHOLD * 2]);
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("type", "image");
        block.put("name", "pic.png");
        block.put("mime_type", "image/png");
        block.put("data", base64);
        Map<String, Object> assistant = new LinkedHashMap<>();
        assistant.put("role", "assistant");
        assistant.put("content", List.of(Map.of("type", "text", "text", "here you go")));
        assistant.put("output_files", List.of(block));
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("result", assistant);

        Object result = spiller.spillIfOversized(envelope, "llm_output", "run-1");

        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-1"), eq("pic.png"), eq("image/png"), any());
        Map<?, ?> spilledEnvelope = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, result);
        Map<?, ?> spilledAssistant = org.junit.jupiter.api.Assertions.assertInstanceOf(
                Map.class, spilledEnvelope.get("result"));
        List<?> files = org.junit.jupiter.api.Assertions.assertInstanceOf(
                List.class, spilledAssistant.get("output_files"));
        Map<?, ?> spilledBlock = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, files.get(0));
        assertTrue(ClientFileStore.isRef(spilledBlock.get("fileId")), "nested bytes moved behind a handle");
        assertEquals(null, spilledBlock.get("data"));
        assertEquals("pic.png", spilledBlock.get("name"));
        // Untouched siblings keep their identity — the envelope is rewritten, not replaced.
        assertEquals("assistant", spilledAssistant.get("role"));
    }

    @Test
    @DisplayName("a small file on a files port is left completely alone")
    void smallContentBlock_staysInline() {
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("name", "tiny.png");
        block.put("mime_type", "image/png");
        block.put("data", java.util.Base64.getEncoder().encodeToString(new byte[16]));

        Object value = List.of(block);
        assertSame(value, spiller.spillIfOversized(value, "files", "run-1"));
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("many small values are each judged on their own, not in aggregate")
    void aggregateSize_isIrrelevant() {
        // The case a naive "is the output map > 1 MB" check gets wrong: a node with dozens of small
        // ports is normal and every one of them must stay readable inline.
        for (int i = 0; i < 100; i++) {
            Object value = "y".repeat(100);
            assertSame(value, spiller.spillIfOversized(value, "p" + i, "run-1"));
        }
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("an existing handle is not spilled again")
    void existingHandle_isLeftAlone() {
        // Re-spilling would chain handles and orphan the first file.
        Object ref = ClientFileStore.REF_PREFIX + "already";

        assertSame(ref, spiller.spillIfOversized(ref, "out", "run-1"));
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("a store failure keeps the value inline rather than losing the node's output")
    void storeFailure_keepsValueInline() {
        // Above the store's hard ceiling, or the store is unavailable. An oversized context is worse
        // than a small one; a lost output is worse than both.
        when(store.store(anyString(), anyString(), anyString(), any(), any()))
                .thenThrow(new IllegalArgumentException("too big"));
        Object value = big();

        assertSame(value, spiller.spillIfOversized(value, "out", "run-1"));
    }

    @Test
    @DisplayName("a non-positive threshold disables spilling instead of spilling everything")
    void nonPositiveThreshold_disablesSpilling() {
        // Reading it as "everything is oversized" would route every value on every port through the
        // file store on a single mis-set property.
        ReflectionTestUtils.setField(spiller, "inlineThresholdBytes", 0);
        Object value = big();

        assertSame(value, spiller.spillIfOversized(value, "out", "run-1"));
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("a store that declines without raising leaves the value inline, never blank")
    void storeReturningNoHandle_keepsValueInline() {
        when(store.store(anyString(), anyString(), anyString(), any(), any())).thenReturn(null);
        Object value = big();

        assertSame(value, spiller.spillIfOversized(value, "out", "run-1"),
                "substituting a blank handle would erase the node's output");
    }

    @Test
    @DisplayName("null passes through")
    void nullValue() {
        assertEquals(null, spiller.spillIfOversized(null, "out", "run-1"));
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("an oversized NON-FILE list stays inline")
    void oversizedList_staysInline() {
        List<String> value = List.of(big());

        assertSame(value, spiller.spillIfOversized(value, "items", "run-1"),
                "a list of large strings is not file content and must stay inline");
        verify(store, never()).store(anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("an oversized file under inputFiles spills; data gone; fileId set; name/mime kept")
    void inputFilesBlock_spills() {
        String base64 = java.util.Base64.getEncoder().encodeToString(new byte[THRESHOLD * 2]);
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("name", "attach.png");
        block.put("mime_type", "image/png");
        block.put("data", base64);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("inputFiles", List.of(block));

        Object result = spiller.spillIfOversized(payload, "llm_input", "run-1");

        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-1"), eq("attach.png"),
                eq("image/png"), any());
        Map<?, ?> spilled = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, result);
        List<?> files = org.junit.jupiter.api.Assertions.assertInstanceOf(
                List.class, spilled.get("inputFiles"));
        Map<?, ?> spilledBlock = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, files.get(0));
        assertTrue(ClientFileStore.isRef(spilledBlock.get("fileId")));
        assertEquals(null, spilledBlock.get("data"));
        assertEquals("attach.png", spilledBlock.get("name"));
        assertEquals("image/png", spilledBlock.get("mime_type"));
    }

    @Test
    @DisplayName("an oversized file under messages[].content[] spills; text sibling untouched")
    void messagesContentFileBlock_spillsOnlyTheFile() {
        String base64 = java.util.Base64.getEncoder().encodeToString(new byte[THRESHOLD * 2]);
        Map<String, Object> text = new LinkedHashMap<>();
        text.put("type", "text");
        text.put("text", "please read this");
        Map<String, Object> file = new LinkedHashMap<>();
        file.put("type", "image");
        file.put("name", "shot.jpg");
        file.put("mime_type", "image/jpeg");
        file.put("data", base64);
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("role", "user");
        message.put("content", List.of(text, file));
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("messages", List.of(message));

        Object result = spiller.spillIfOversized(payload, "llm_input", "run-1");

        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-1"), eq("shot.jpg"),
                eq("image/jpeg"), any());
        Map<?, ?> spilled = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, result);
        List<?> messages = org.junit.jupiter.api.Assertions.assertInstanceOf(
                List.class, spilled.get("messages"));
        Map<?, ?> spilledMsg = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, messages.get(0));
        List<?> content = org.junit.jupiter.api.Assertions.assertInstanceOf(
                List.class, spilledMsg.get("content"));
        Map<?, ?> spilledText = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, content.get(0));
        Map<?, ?> spilledFile = org.junit.jupiter.api.Assertions.assertInstanceOf(Map.class, content.get(1));
        assertEquals("please read this", spilledText.get("text"));
        assertEquals("text", spilledText.get("type"));
        assertTrue(ClientFileStore.isRef(spilledFile.get("fileId")));
        assertEquals(null, spilledFile.get("data"));
        assertEquals("shot.jpg", spilledFile.get("name"));
        assertEquals("image/jpeg", spilledFile.get("mime_type"));
    }
}
