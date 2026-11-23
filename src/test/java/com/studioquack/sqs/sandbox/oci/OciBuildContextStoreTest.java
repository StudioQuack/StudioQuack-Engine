package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Context materialisation, caps, cleanup, resolve-under-root defence. */
class OciBuildContextStoreTest {

    private static final String BUILD_ID = "550e8400-e29b-41d4-a716-446655440000";

    @TempDir
    Path tempDir;

    private OciBuildContextStore store;

    @BeforeEach
    void setUp() {
        store = new OciBuildContextStore(tempDir);
    }

    @Test
    void writeFile_createsNestedDirsAndTracksQuota() throws IOException {
        byte[] body = "class Index {}".getBytes(StandardCharsets.UTF_8);
        Path written = store.writeFile(BUILD_ID, "src/main/java/Index.java", body);

        assertTrue(Files.isRegularFile(written));
        assertEquals(new String(body, StandardCharsets.UTF_8), Files.readString(written));
        assertEquals(1, store.entryCount(BUILD_ID));
        assertEquals(body.length, store.totalBytes(BUILD_ID));
        assertTrue(store.contextExists(BUILD_ID));
        assertEquals(tempDir.resolve("oci-build").resolve(BUILD_ID).normalize(),
                store.contextDir(BUILD_ID));
    }

    @Test
    void writeFile_replaceSamePath_doesNotIncreaseEntryCount() throws IOException {
        store.writeFile(BUILD_ID, "Containerfile", "FROM a".getBytes(StandardCharsets.UTF_8));
        store.writeFile(BUILD_ID, "Containerfile", "FROM bbb".getBytes(StandardCharsets.UTF_8));

        assertEquals(1, store.entryCount(BUILD_ID));
        assertEquals(8, store.totalBytes(BUILD_ID)); // "FROM bbb"
    }

    @Test
    void writeFile_rejectsOversizeFile() {
        byte[] huge = new byte[(int) OciBuildContextStore.MAX_FILE_BYTES + 1];
        assertThrows(IllegalArgumentException.class,
                () -> store.writeFile(BUILD_ID, "big.bin", huge));
        assertEquals(0, store.entryCount(BUILD_ID));
        assertFalse(store.contextExists(BUILD_ID));
    }

    @Test
    void writeFile_allowsManyFiles() throws IOException {
        // No entry-count cap: 600 tiny files stay under the byte budgets and write.
        for (int i = 0; i < 600; i++) {
            store.writeFile(BUILD_ID, "f" + i + ".txt", "x".getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(600, store.entryCount(BUILD_ID));
    }

    @Test
    void writeFile_rejectsOverWorkspaceTotal() throws IOException {
        // Stay under per-file 5 MiB; exceed 50 MiB total with 11 × ~4.8 MiB.
        int chunk = (int) (4.8 * 1024 * 1024);
        byte[] payload = new byte[chunk];
        for (int i = 0; i < 10; i++) {
            store.writeFile(BUILD_ID, "chunk-" + i + ".bin", payload);
        }
        assertTrue(store.totalBytes(BUILD_ID) <= OciBuildContextStore.MAX_WORKSPACE_BYTES);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> store.writeFile(BUILD_ID, "chunk-overflow.bin", payload));
        assertTrue(ex.getMessage().toLowerCase().contains("50"));
    }

    @Test
    void writeFile_rejectsTraversalPaths() {
        byte[] body = "x".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class,
                () -> store.writeFile(BUILD_ID, "../x", body));
        assertThrows(IllegalArgumentException.class,
                () -> store.writeFile(BUILD_ID, "/abs", body));
        assertThrows(IllegalArgumentException.class,
                () -> store.writeFile(BUILD_ID, "a\\b", body));
        assertThrows(IllegalArgumentException.class,
                () -> store.writeFile(BUILD_ID, "a/./b", body));
        assertThrows(IllegalArgumentException.class,
                () -> store.writeFile(BUILD_ID, "", body));
        assertThrows(IllegalArgumentException.class,
                () -> store.writeFile(BUILD_ID, "a/b/", body));
    }

    @Test
    void ensureUnderRoot_refusesEscapingResolvedPath() {
        Path root = tempDir.resolve("oci-build").resolve(BUILD_ID);
        Path outside = tempDir.resolve("outside.txt");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> OciBuildContextStore.ensureUnderRoot(root, outside));
        assertTrue(ex.getMessage().toLowerCase().contains("escapes"));
    }

    @Test
    void deleteContext_removesTreeAndQuota() throws IOException {
        store.writeFile(BUILD_ID, "src/a.txt", "hello".getBytes(StandardCharsets.UTF_8));
        store.writeFile(BUILD_ID, "src/nested/b.txt", "world".getBytes(StandardCharsets.UTF_8));
        assertTrue(store.contextExists(BUILD_ID));

        store.deleteContext(BUILD_ID);

        assertFalse(store.contextExists(BUILD_ID));
        assertEquals(0, store.entryCount(BUILD_ID));
        assertEquals(0L, store.totalBytes(BUILD_ID));
        assertFalse(Files.exists(tempDir.resolve("oci-build").resolve(BUILD_ID)));
        // Best-effort: second delete must not throw.
        store.deleteContext(BUILD_ID);
    }
}
