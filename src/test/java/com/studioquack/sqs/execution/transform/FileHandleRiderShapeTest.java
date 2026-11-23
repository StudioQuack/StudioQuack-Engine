package com.studioquack.sqs.execution.transform;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shape guards on {@link FileHandleRider}. The rider recovers a {@code sq-file:} handle that a
 * leaf-mapped file ContentBlock lost, so its target test must be narrow: an agent envelope is
 * full of ContentBlocks that are NOT files, and attaching a handle to one of those would let
 * hydrate replace its payload with unrelated file bytes.
 */
class FileHandleRiderShapeTest {

    private static final String HANDLE = "sq-file:abc123";

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    /** One resolvable file in the source tree — the sole candidate a nameless target could take. */
    private static Object sourceWithOneHandle() {
        return map("doc", map("name", "report.pdf", "mime_type", "application/pdf", "fileId", HANDLE));
    }

    @Test
    void leafMappedFileBlock_getsTheHandleBack() {
        Map<String, Object> target = map("inputFiles",
                map("name", "report.pdf", "mime_type", "application/pdf", "data", ""));

        FileHandleRider.rideHandles(sourceWithOneHandle(), target);

        @SuppressWarnings("unchecked")
        Map<String, Object> block = (Map<String, Object>) target.get("inputFiles");
        assertEquals(HANDLE, block.get("fileId"), "a leaf-mapped file recovers its transport handle");
    }

    /**
     * An agent text block clears the two-signal bar ({@code kind} + {@code data}) and has no
     * name, so without a file marker it would take the sole candidate and hydrate would
     * overwrite its text.
     */
    @Test
    void textContentBlock_isNotTreatedAsAFile() {
        Map<String, Object> target = map("thinking", map("kind", "text", "data", "hello there"));

        FileHandleRider.rideHandles(sourceWithOneHandle(), target);

        @SuppressWarnings("unchecked")
        Map<String, Object> block = (Map<String, Object>) target.get("thinking");
        assertFalse(block.containsKey("fileId"), "a text block is not a file and must stay untouched");
        assertEquals("hello there", block.get("data"), "its payload must survive");
    }

    /** {@code {name, email}} was never a file; it also carries no file marker. */
    @Test
    void contactObject_isNotTreatedAsAFile() {
        Map<String, Object> target = map("contact", map("name", "report.pdf", "email", "a@b.c"));

        FileHandleRider.rideHandles(sourceWithOneHandle(), target);

        @SuppressWarnings("unchecked")
        Map<String, Object> block = (Map<String, Object>) target.get("contact");
        assertFalse(block.containsKey("fileId"), "a contact sharing a name with a file is not a file");
    }

    /** Bytes already present ⇒ nothing to fetch, and a handle would invite hydrate to replace them. */
    @Test
    void blockThatAlreadyCarriesBytes_isLeftAlone() {
        Map<String, Object> target = map("inputFiles",
                map("name", "clip.webm", "mime_type", "audio/webm", "data", "AAAABBBB"));

        FileHandleRider.rideHandles(sourceWithOneHandle(), target);

        @SuppressWarnings("unchecked")
        Map<String, Object> block = (Map<String, Object>) target.get("inputFiles");
        assertFalse(block.containsKey("fileId"), "a populated block needs no handle");
        assertEquals("AAAABBBB", block.get("data"), "its inline bytes must survive");
    }

    /** {@code kind: file} marks a file even without a mime type. */
    @Test
    void kindFile_countsAsAFileMarker() {
        Map<String, Object> target = map("inputFiles", map("kind", "file", "name", "report.pdf"));

        FileHandleRider.rideHandles(sourceWithOneHandle(), target);

        @SuppressWarnings("unchecked")
        Map<String, Object> block = (Map<String, Object>) target.get("inputFiles");
        assertEquals(HANDLE, block.get("fileId"), "kind=file is a file marker");
    }

    /** Ambiguity is never guessed: two same-named candidates attach nothing. */
    @Test
    void ambiguousCandidates_attachNothing() {
        List<Object> sources = new ArrayList<>();
        sources.add(map("name", "dup.pdf", "mime_type", "application/pdf", "fileId", "sq-file:one"));
        sources.add(map("name", "dup.pdf", "mime_type", "application/pdf", "fileId", "sq-file:two"));
        Map<String, Object> target = map("inputFiles",
                map("name", "dup.pdf", "mime_type", "application/pdf", "data", ""));

        FileHandleRider.rideHandles(map("files", sources), target);

        @SuppressWarnings("unchecked")
        Map<String, Object> block = (Map<String, Object>) target.get("inputFiles");
        assertFalse(block.containsKey("fileId"), "two handles for one name must not be guessed");
        assertTrue(((String) block.get("data")).isEmpty(), "and nothing is fabricated in data");
    }
}
