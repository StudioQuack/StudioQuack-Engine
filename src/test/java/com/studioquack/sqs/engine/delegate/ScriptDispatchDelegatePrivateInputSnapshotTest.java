package com.studioquack.sqs.engine.delegate;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The sandbox-facing port map must be a PRIVATE snapshot. {@code prepared.inputVariables()} holds
 * the very Map/List instances the persisted execution context holds, and the hydrator / file
 * materialiser / secret resolver all mutate in place. A shallow copy let a file block's
 * {@code fileId} become the in-container path and its multi-MB {@code data} land in the persisted
 * context, the sandbox-log {@code input} and every value carried downstream.
 */
class ScriptDispatchDelegatePrivateInputSnapshotTest {

    private final ScriptDispatchDelegate delegate = new ScriptDispatchDelegate(
            null, null, null, null, null, null, null, null, null,
            null, null, null, null,
            null, null, null, null, null, null, null,
            new ObjectMapper(),
            null, null,
            new MockEnvironment(),
            null, null, null, null, null);

    @Test
    void nested_port_values_are_deep_copied_so_in_place_mutation_never_reaches_the_context() {
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "image");
        block.put("name", "shot.png");
        block.put("fileId", "sq-file:abc");
        List<Object> files = new ArrayList<>(List.of(block));
        Map<String, Object> port = new LinkedHashMap<>();
        port.put("files", files);
        Map<String, Object> inputVariables = new LinkedHashMap<>();
        inputVariables.put("input", port);

        Map<String, Object> snapshot = delegate.privateInputSnapshot(inputVariables);

        @SuppressWarnings("unchecked")
        Map<String, Object> copiedPort = (Map<String, Object>) snapshot.get("input");
        assertNotSame(port, copiedPort, "the port value is a fresh map");
        @SuppressWarnings("unchecked")
        Map<String, Object> copiedBlock = (Map<String, Object>) ((List<Object>) copiedPort.get("files")).get(0);
        assertNotSame(block, copiedBlock, "the nested block is a fresh map");
        assertEquals(block, copiedBlock, "the copy is value-identical");

        // What the hydrator + materialiser do to the sandbox copy …
        copiedBlock.put("data", "iVBORw0KGgo=");
        copiedBlock.put("fileId", "/sandbox/files/shot.png");

        // … never shows on the context-owned block.
        assertEquals("sq-file:abc", block.get("fileId"), "the persisted handle survives");
        assertFalse(block.containsKey("data"), "no bytes leak into the persisted context");
    }

    @Test
    void reserved_directive_entries_and_scalars_stay_by_reference() {
        List<Object> directives = new ArrayList<>(List.of(Map.of("op", "shift")));
        Map<String, Object> inputVariables = new LinkedHashMap<>();
        inputVariables.put("__sq_transformation__", directives);
        inputVariables.put("prompt", "hello");
        inputVariables.put("count", 3);

        Map<String, Object> snapshot = delegate.privateInputSnapshot(inputVariables);

        assertSame(directives, snapshot.get("__sq_transformation__"),
                "typed directive payloads are read as-is by later stages");
        assertEquals("hello", snapshot.get("prompt"));
        assertEquals(3, snapshot.get("count"));
        assertEquals(inputVariables.keySet(), snapshot.keySet(), "every port survives, in order");
    }
}
