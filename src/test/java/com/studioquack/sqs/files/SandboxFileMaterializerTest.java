package com.studioquack.sqs.files;

import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.oci.ContainerBootstrap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Turning {@code sq-file:} handles into files the node script can open. Substitution has to leave
 * the script with a usable path, and the file list has to be exactly what gets planted — no
 * duplicates (the same handle appears in both parameters and port inputs) and no silent overwrites
 * (two different files can share a basename).
 */
class SandboxFileMaterializerTest {

    private ClientFileStore store;
    private SandboxFileMaterializer materializer;

    @BeforeEach
    void setUp() {
        store = mock(ClientFileStore.class);
        materializer = new SandboxFileMaterializer(store);
    }

    private String given(String id, String name, String content) {
        String ref = ClientFileStore.REF_PREFIX + id;
        when(store.resolve(ref)).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                id, name, "application/pdf", content.length(), content.getBytes(StandardCharsets.UTF_8))));
        return ref;
    }

    private static Map<String, Object> mutableMap(String key, Object value) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(key, value);
        return m;
    }

    @Test
    @DisplayName("a handle is replaced by the path the file will occupy in the container")
    void handle_becomesPath() {
        String ref = given("id-1", "contract.pdf", "DATA");
        Map<String, Object> params = mutableMap("doc", ref);

        List<SandboxJobRequest.AttachedFile> files = materializer.materialize(params);

        assertEquals(ContainerBootstrap.FILES_DIR + "/contract.pdf", params.get("doc"));
        assertEquals(1, files.size());
        assertEquals("contract.pdf", files.get(0).name());
        assertArrayEquals("DATA".getBytes(StandardCharsets.UTF_8), files.get(0).content());
    }

    @Test
    @DisplayName("handles nested in maps and lists are found")
    void nestedHandles_areFound() {
        String ref = given("id-2", "nested.pdf", "N");
        Map<String, Object> inner = mutableMap("file", ref);
        List<Object> list = new ArrayList<>(List.of(inner));
        Map<String, Object> outer = mutableMap("items", list);

        List<SandboxJobRequest.AttachedFile> files = materializer.materialize(outer);

        assertEquals(ContainerBootstrap.FILES_DIR + "/nested.pdf", inner.get("file"));
        assertEquals(1, files.size());
    }

    @Test
    @DisplayName("a handle inside a list element is substituted")
    void listElement_isSubstituted() {
        String ref = given("id-3", "a.pdf", "A");
        List<Object> list = new ArrayList<>(List.of(ref, "not-a-ref"));

        materializer.materialize(list);

        assertEquals(ContainerBootstrap.FILES_DIR + "/a.pdf", list.get(0));
        assertEquals("not-a-ref", list.get(1));
    }

    @Test
    @DisplayName("the same handle in two places resolves and travels once")
    void duplicateHandle_isDeduplicated() {
        // A parameter and a port input routinely carry the same handle; sending the bytes twice
        // would double the stdin payload for no benefit.
        String ref = given("id-4", "shared.pdf", "S");
        Map<String, Object> params = mutableMap("doc", ref);
        Map<String, Object> inputs = mutableMap("alsoDoc", ref);

        List<SandboxJobRequest.AttachedFile> files = materializer.materialize(params, inputs);

        assertEquals(1, files.size(), "one copy of the bytes");
        assertEquals(params.get("doc"), inputs.get("alsoDoc"), "both point at the same path");
        verify(store, times(1)).resolve(any());
    }

    @Test
    @DisplayName("two different files sharing a basename do not overwrite each other")
    void nameCollision_isDisambiguated() {
        // Writing both to one path would silently hand the node the wrong content.
        String first = given("aaaaaaaa-1", "report.pdf", "FIRST");
        String second = given("bbbbbbbb-2", "report.pdf", "SECOND");
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("a", first);
        params.put("b", second);

        List<SandboxJobRequest.AttachedFile> files = materializer.materialize(params);

        assertEquals(2, files.size());
        assertTrue(((String) params.get("a")).endsWith("/report.pdf"), "the first keeps the clean name");
        assertTrue(((String) params.get("b")).endsWith("-report.pdf"), params.get("b").toString());
        assertEquals(2, files.stream().map(SandboxJobRequest.AttachedFile::name).distinct().count(),
                "the planted names must be distinct");
    }

    @Test
    @DisplayName("an unresolvable handle is left in place rather than pointing at nothing")
    void missingFile_leavesTheHandle() {
        // Substituting a path to a file that was never planted surfaces as a confusing
        // FileNotFoundError inside the user's script; leaving the handle names the real problem.
        String ref = ClientFileStore.REF_PREFIX + "gone";
        when(store.resolve(ref)).thenReturn(Optional.empty());
        Map<String, Object> params = mutableMap("doc", ref);

        List<SandboxJobRequest.AttachedFile> files = materializer.materialize(params);

        assertEquals(ref, params.get("doc"));
        assertTrue(files.isEmpty());
    }

    @Test
    @DisplayName("values that are not handles are untouched")
    void nonHandles_areUntouched() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("text", "hello");
        params.put("num", 42);
        params.put("gridfs", "sq-ref:gridfs:abc");
        params.put("nothing", null);

        assertTrue(materializer.materialize(params).isEmpty());

        assertEquals("hello", params.get("text"));
        assertEquals(42, params.get("num"));
        assertEquals("sq-ref:gridfs:abc", params.get("gridfs"),
                "the backend's GridFS ref is a different mechanism and not ours to resolve");
    }

    @Test
    @DisplayName("an inline ../main.py name is flattened so it cannot overwrite /sandbox/main.*")
    void inlineTraversalName_isSanitizedToBasename() {
        String evil = java.util.Base64.getEncoder().encodeToString("EVIL".getBytes(StandardCharsets.UTF_8));
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("name", "../main.py");
        item.put("data", evil);
        Map<String, Object> params = mutableMap("doc", item);

        List<SandboxJobRequest.AttachedFile> files = materializer.materialize(params);

        assertEquals(1, files.size());
        assertEquals("main.py", files.get(0).name());
        assertTrue(files.get(0).name().indexOf('/') < 0);
        assertArrayEquals("EVIL".getBytes(StandardCharsets.UTF_8), files.get(0).content());
        assertEquals("doc", params.keySet().iterator().next(), "the item map stays; path is not substituted");
    }

    @Test
    @DisplayName("null trees are tolerated")
    void nullTrees() {
        assertTrue(materializer.materialize((Object) null).isEmpty());
        assertTrue(materializer.materialize().isEmpty());
    }

    @Test
    @DisplayName("an immutable list is left alone rather than crashing the dispatch")
    void immutableList_doesNotThrow() {
        String ref = given("id-5", "x.pdf", "X");
        List<Object> immutable = List.of(ref);

        List<SandboxJobRequest.AttachedFile> files = materializer.materialize(immutable);

        // The file still resolves; only the substitution could not be written back.
        assertEquals(1, files.size());
        assertEquals(ref, immutable.get(0));
    }
}
