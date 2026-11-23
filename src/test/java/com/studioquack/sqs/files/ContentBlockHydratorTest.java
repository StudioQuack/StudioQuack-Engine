package com.studioquack.sqs.files;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ContentBlockHydratorTest {

    @Test
    void hydrate_fillsDataFromFileStore() {
        String handle = ClientFileStore.REF_PREFIX + "f1";
        byte[] bytes = "hello-bytes".getBytes(StandardCharsets.UTF_8);
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("f1", "doc.pdf", "application/pdf", bytes.length, bytes)));

        Map<String, Object> block = new HashMap<>();
        block.put("kind", "document");
        block.put("mime_type", "application/pdf");
        block.put("source", "sq-file");
        block.put("name", "doc.pdf");
        block.put("fileId", handle);

        Map<String, Object> root = new HashMap<>();
        root.put("files", List.of(block));

        new ContentBlockHydrator(store).hydrate(root);

        assertEquals("base64", block.get("source"));
        assertEquals(Base64.getEncoder().encodeToString(bytes), block.get("data"));
        assertEquals(handle, block.get("fileId"));
    }

    @Test
    void hydrate_leavesExistingDataAlone() {
        ClientFileStore store = mock(ClientFileStore.class);
        Map<String, Object> block = new HashMap<>();
        block.put("fileId", ClientFileStore.REF_PREFIX + "f1");
        block.put("data", "already");
        block.put("source", "base64");

        new ContentBlockHydrator(store).hydrate(block);

        assertEquals("already", block.get("data"));
        assertFalse(org.mockito.Mockito.mockingDetails(store).getInvocations().stream()
                .anyMatch(i -> i.getMethod().getName().equals("resolve")));
    }

    @Test
    void hydrate_fillsInputFilesBlockAndPreservesFileId() {
        String handle = ClientFileStore.REF_PREFIX + "in-1";
        byte[] bytes = "input-bytes".getBytes(StandardCharsets.UTF_8);
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("in-1", "clip.png", "image/png", bytes.length, bytes)));

        Map<String, Object> block = new HashMap<>();
        block.put("kind", "image");
        block.put("name", "clip.png");
        block.put("mime_type", "image/png");
        block.put("fileId", handle);

        Map<String, Object> root = new HashMap<>();
        root.put("inputFiles", List.of(block));

        new ContentBlockHydrator(store).hydrate(root);

        assertEquals(Base64.getEncoder().encodeToString(bytes), block.get("data"));
        assertEquals(handle, block.get("fileId"));
        assertEquals("base64", block.get("source"));
    }

    @Test
    void hydrate_fillsMessagesContentBlockAndPreservesFileId() {
        String handle = ClientFileStore.REF_PREFIX + "msg-1";
        byte[] bytes = "msg-bytes".getBytes(StandardCharsets.UTF_8);
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("msg-1", "note.pdf", "application/pdf", bytes.length, bytes)));

        Map<String, Object> block = new HashMap<>();
        block.put("type", "document");
        block.put("name", "note.pdf");
        block.put("fileId", handle);

        Map<String, Object> message = new HashMap<>();
        message.put("role", "user");
        message.put("content", List.of(Map.of("type", "text", "text", "see file"), block));

        Map<String, Object> root = new HashMap<>();
        root.put("messages", List.of(message));

        new ContentBlockHydrator(store).hydrate(root);

        assertEquals(Base64.getEncoder().encodeToString(bytes), block.get("data"));
        assertEquals(handle, block.get("fileId"));
        assertEquals("base64", block.get("source"));
    }

    @Test
    void hydrate_fromSourceRefWhenFileIdMissing_writesFileId() {
        String handle = ClientFileStore.REF_PREFIX + "src-only";
        byte[] bytes = "from-source".getBytes(StandardCharsets.UTF_8);
        ClientFileStore store = mock(ClientFileStore.class);
        when(store.resolve(eq(handle))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("src-only", "a.bin", "application/octet-stream",
                        bytes.length, bytes)));

        Map<String, Object> block = new HashMap<>();
        block.put("kind", "file");
        block.put("source", handle);

        new ContentBlockHydrator(store).hydrate(block);

        assertEquals(Base64.getEncoder().encodeToString(bytes), block.get("data"));
        assertEquals(handle, block.get("fileId"));
        assertEquals("base64", block.get("source"));
    }

    @Test
    void hydrate_selfReferentialMapDoesNotStackOverflow() {
        ClientFileStore store = mock(ClientFileStore.class);
        Map<String, Object> cyclic = new HashMap<>();
        cyclic.put("self", cyclic);

        new ContentBlockHydrator(store).hydrate(cyclic);
    }
}
