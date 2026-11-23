package com.studioquack.sqs.execution.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.sqs.execution.TransformFixtures;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JOLT engine + spec-builder tests: shift idempotency, the double-apply "no {} data loss"
 * regression, and the schema-coercion precedence (Mapping Invariant 13). Pure unit tests.
 */
class JoltTransformServiceTest {

    private final JoltTransformService jolt = new JoltTransformService(new ObjectMapper());

    @Test
    void shift_maps_source_key_to_target_key() {
        Object out = jolt.transform(TransformFixtures.map("result", "hello"),
                TransformFixtures.shiftSpec("result", "prompt"));
        assertEquals(Map.of("prompt", "hello"), out);
    }

    /** Idempotency: a shift whose source path is absent returns the input UNCHANGED (not {}). */
    @Test
    void shift_is_idempotent_when_no_source_path_matches() {
        Map<String, Object> alreadyShifted = TransformFixtures.map("prompt", "hello");
        Object out = jolt.transform(alreadyShifted, TransformFixtures.shiftSpec("result", "prompt"));
        assertEquals(Map.of("prompt", "hello"), out, "no-match shift must be a no-op, never {}");
    }

    /**
     * Schema-shaped producers publish LIST-valued roots (Docling's {@code texts} is declared
     * {@code type: "list"} of ContentBlocks). An authored {@code texts.data → prompt} row must
     * navigate into the list and deliver the leaf — without it the path dies at the list, the
     * shift reports no match, and the consumer receives the producer's raw payload (an unknown
     * {@code texts} parameter) untransformed.
     */
    @Test
    void shift_navigates_a_list_valued_root_to_the_leaf() {
        Map<String, Object> block = TransformFixtures.map(
                "kind", "document", "mime_type", "text/markdown", "source", "text",
                "name", "clip.md", "data", "converted text");
        Object out = jolt.transform(TransformFixtures.map("texts", List.of(block)),
                TransformFixtures.shiftSpec("texts.data", "prompt"));
        assertEquals(Map.of("prompt", "converted text"), out,
                "texts.data must unwrap the single list element's leaf, not die at the list");
    }

    /**
     * Function-port Add-Node merge delivers a LIST of invocation payloads as the message
     * value. {@code name → prompt} must walk that list root: one invocation unwraps, two
     * collect in order.
     */
    @Test
    void shift_walks_a_list_input_root_and_unwraps_a_single_hit() {
        Object out = jolt.transform(List.of(Map.of("name", "n1")),
                TransformFixtures.shiftSpec("name", "prompt"));
        assertEquals(Map.of("prompt", "n1"), out,
                "a list of one invocation unwraps to the leaf, not a 1-element list");
    }

    @Test
    void shift_walks_a_list_input_root_and_collects_every_invocation() {
        Object out = jolt.transform(List.of(Map.of("name", "n1"), Map.of("name", "n2")),
                TransformFixtures.shiftSpec("name", "prompt"));
        assertEquals(Map.of("prompt", List.of("n1", "n2")), out);
    }

    /** Several list elements → the leaf values collect into a list, in order. */
    @Test
    void shift_collects_leaves_from_every_list_element() {
        Object out = jolt.transform(TransformFixtures.map("files",
                        List.of(Map.of("name", "a.pdf"), Map.of("name", "b.pdf"))),
                TransformFixtures.shiftSpec("files.name", "names"));
        assertEquals(Map.of("names", List.of("a.pdf", "b.pdf")), out);
    }

    /** A list root addressed without a leaf still delivers the list itself. */
    @Test
    void shift_list_root_without_leaf_delivers_the_list() {
        List<Map<String, Object>> blocks = List.of(Map.of("data", "x"));
        Object out = jolt.transform(TransformFixtures.map("voices", blocks),
                TransformFixtures.shiftSpec("voices", "files"));
        assertEquals(Map.of("files", blocks), out);
    }

    /**
     * Double-apply regression (the exact silent-data-loss trap): applying the same
     * {@code result -> prompt} shift twice must still yield {@code {prompt}}, never {@code {}}.
     */
    @Test
    void double_apply_shift_does_not_wipe_to_empty_map() {
        List<Object> spec = TransformFixtures.shiftSpec("result", "prompt");
        Object first = jolt.transform(TransformFixtures.map("result", "x"), spec);
        assertEquals(Map.of("prompt", "x"), first);
        Object second = jolt.transform(first, spec);
        assertEquals(Map.of("prompt", "x"), second, "second apply must NOT produce {} (BE+daemon double-apply safety)");
    }

    /** Invariant 13: the consuming port's schema type wins over a stale EdgeTransform.targetDataType. */
    @Test
    void convertJoltSpec_prefers_port_schema_type_over_targetDataType() {
        EdgeTransformDto t = TransformFixtures.transform("raw", "count", "nodeB", "schema", "string");
        // Port schema declares count:integer — must override the persisted "string".
        List<Object> spec = jolt.convertJoltSpec(List.of(t), Map.of("count", "integer"));

        Map<String, Object> modify = findOp(spec, "modify-overwrite-beta");
        assertTrue(modify != null, "integer coercion must be emitted as a modify op");
        assertEquals("=toInteger", asSpec(modify).get("count"));
        assertTrue(findOp(spec, "custom-to-string") == null,
                "schema type (integer) wins → NO custom-to-string coercion from the stale targetDataType");
    }

    /** Without a port schema, the persisted targetDataType still drives coercion (backend parity). */
    @Test
    void convertJoltSpec_falls_back_to_targetDataType_without_schema() {
        EdgeTransformDto t = TransformFixtures.transform("raw", "label", "nodeB", "schema", "string");
        List<Object> spec = jolt.convertJoltSpec(List.of(t), null);
        Map<String, Object> toStr = findOp(spec, "custom-to-string");
        assertTrue(toStr != null, "string targetDataType → custom-to-string op");
        assertEquals("toString", asSpec(toStr).get("label"));
    }

    @Test
    void modify_coerces_numeric_string_to_integer() {
        // shift keeps n, then modify coerces it.
        Object out = jolt.transform(TransformFixtures.map("n", "5"),
                List.of(op("shift", TransformFixtures.map("n", List.of("n"))),
                        op("modify-overwrite-beta", TransformFixtures.map("n", "=toInteger"))));
        assertEquals(5L, ((Map<?, ?>) out).get("n"));
    }

    @Test
    void custom_to_string_stringifies_nested_object() {
        Object out = jolt.transform(TransformFixtures.map("payload", TransformFixtures.map("a", 1)),
                List.of(op("custom-to-string", TransformFixtures.map("payload", "toString"))));
        assertEquals("{\"a\":1}", ((Map<?, ?>) out).get("payload"));
    }

    /**
     * Duckbot full-history {@code msg} is [{role, content}, …]. A string consumer (Jira summary,
     * custom-node text) must get the current user turn, not that JSON array.
     */
    @Test
    void custom_to_string_extracts_current_user_turn_from_chat_transcript() {
        List<Map<String, String>> thread = List.of(
                Map.of("role", "user", "content", "first question"),
                Map.of("role", "assistant", "content", "SQA-1"),
                Map.of("role", "user", "content", "WWW new bug task"));
        EdgeTransformDto t = TransformFixtures.transform("msg", "msg", "nodeB", "schema", "string");
        Object out = jolt.transform(TransformFixtures.map("msg", thread), jolt.convertJoltSpec(List.of(t)));
        assertEquals("WWW new bug task", ((Map<?, ?>) out).get("msg"));
    }

    @Test
    void custom_to_string_extracts_content_from_a_single_chat_turn() {
        Object out = jolt.transform(
                TransformFixtures.map("msg", List.of(Map.of("role", "user", "content", "attach file test"))),
                List.of(op("custom-to-string", TransformFixtures.map("msg", "toString"))));
        assertEquals("attach file test", ((Map<?, ?>) out).get("msg"));
    }

    @Test
    void custom_to_string_leaves_a_non_chat_list_as_json() {
        Object out = jolt.transform(TransformFixtures.map("payload", List.of("a", "b")),
                List.of(op("custom-to-string", TransformFixtures.map("payload", "toString"))));
        assertEquals("[\"a\",\"b\"]", ((Map<?, ?>) out).get("payload"));
    }

    @Test
    void empty_spec_returns_input_verbatim() {
        Map<String, Object> in = TransformFixtures.map("a", 1);
        assertEquals(in, jolt.transform(in, List.of()));
    }

    @Test
    void non_map_input_survives_shift() {
        assertFalse(jolt.transform("plain-string", TransformFixtures.shiftSpec("a", "b")) instanceof Map);
    }

    /**
     * REF-only invariant: a file ContentBlock that reaches JOLT with both {@code fileId} and
     * inline base64 must exit carrying only the ref — never {@code data}.
     */
    @Test
    void transform_file_content_block_keeps_ref_and_strips_base64() {
        String ref = "sq-file:abc-123";
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "document");
        block.put("mime_type", "application/pdf");
        block.put("source", "base64");
        block.put("name", "report.pdf");
        block.put("fileId", ref);
        block.put("data", "JVBERi0xLjQK" + "A".repeat(256));

        List<Object> files = new ArrayList<>();
        files.add(block);
        Map<String, Object> in = TransformFixtures.map("files", files);

        Object out = jolt.transform(in, TransformFixtures.shiftSpec("files", "attachments"));

        @SuppressWarnings("unchecked")
        Map<String, Object> outMap = (Map<String, Object>) out;
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> attachments = (List<Map<String, Object>>) outMap.get("attachments");
        Map<String, Object> got = attachments.get(0);

        assertEquals(ref, got.get("fileId"));
        assertEquals("sq-file", got.get("source"));
        assertEquals("report.pdf", got.get("name"));
        assertEquals("application/pdf", got.get("mime_type"));
        assertNull(got.get("data"), "JOLT must never see / emit inline file base64");
        assertFalse(got.containsKey("data"));
        assertFalse(got.containsKey("content"));
        assertFalse(got.containsKey("base64"));
    }

    /** Empty / null spec is still a transform entry — bytes must be stripped, not passed through. */
    @Test
    void transform_empty_spec_still_strips_inline_file_bytes() {
        String ref = "sq-file:empty-spec-1";
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "document");
        block.put("source", "base64");
        block.put("fileId", ref);
        block.put("data", "JVBERi0xLjQK" + "B".repeat(128));
        Map<String, Object> in = TransformFixtures.map("files", List.of(block));

        Object out = jolt.transform(in, List.of());

        @SuppressWarnings("unchecked")
        Map<String, Object> outMap = (Map<String, Object>) out;
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = (List<Map<String, Object>>) outMap.get("files");
        Map<String, Object> got = files.get(0);
        assertEquals(ref, got.get("fileId"));
        assertEquals("sq-file", got.get("source"));
        assertNull(got.get("data"));
    }

    /* -------------------- Catalog operator coverage -------------------- */

    @Test
    void catalog_operator_null_emits_legacy_spec_shape() {
        EdgeTransformDto t = TransformFixtures.transform("raw", "label", "nodeB", "schema", "string");
        List<Object> withNull = jolt.convertJoltSpec(List.of(t), null);
        EdgeTransformDto t2 = TransformFixtures.transform("raw", "label", "nodeB", "schema", "string");
        t2.setOperator(null);
        t2.setArgs(null);
        List<Object> withExplicitNull = jolt.convertJoltSpec(List.of(t2), null);
        assertEquals(withNull, withExplicitNull);
    }

    @Test
    void catalog_toInteger_via_operator() {
        EdgeTransformDto t = catalog("n", "n", "toInteger", null);
        List<Object> spec = jolt.convertJoltSpec(List.of(t));
        assertTrue(findOp(spec, "modify-overwrite-beta") != null);
        Object out = jolt.transform(TransformFixtures.map("n", "5"), spec);
        assertEquals(5L, ((Map<?, ?>) out).get("n"));
    }

    @Test
    void catalog_toUpper_toLower_trim() {
        EdgeTransformDto up = catalog("name", "name", "toUpper", null);
        Object upper = jolt.transform(TransformFixtures.map("name", "hello"), jolt.convertJoltSpec(List.of(up)));
        assertEquals("HELLO", ((Map<?, ?>) upper).get("name"));

        EdgeTransformDto low = catalog("name", "name", "toLower", null);
        Object lower = jolt.transform(TransformFixtures.map("name", "HELLO"), jolt.convertJoltSpec(List.of(low)));
        assertEquals("hello", ((Map<?, ?>) lower).get("name"));

        EdgeTransformDto tr = catalog("name", "name", "trim", null);
        Object trimmed = jolt.transform(TransformFixtures.map("name", "  hi  "), jolt.convertJoltSpec(List.of(tr)));
        assertEquals("hi", ((Map<?, ?>) trimmed).get("name"));
    }

    @Test
    void catalog_leftPad_with_literal_args() {
        EdgeTransformDto t = catalog("code", "code", "leftPad", List.of("5", "\"0\""));
        List<Object> spec = jolt.convertJoltSpec(List.of(t));
        Map<String, Object> mod = findOp(spec, "modify-overwrite-beta");
        assertTrue(mod != null);
        assertTrue(String.valueOf(asSpec(mod).get("code")).contains("leftPad"));
        Object out = jolt.transform(TransformFixtures.map("code", "7"), spec);
        assertEquals("00007", ((Map<?, ?>) out).get("code"));
    }

    @Test
    void catalog_size_and_firstElement_and_elementAt() {
        EdgeTransformDto size = catalog("items", "n", "size", null);
        Object sized = jolt.transform(TransformFixtures.map("items", List.of("a", "b", "c")),
                jolt.convertJoltSpec(List.of(size)));
        assertEquals(3, ((Map<?, ?>) sized).get("n"));

        EdgeTransformDto first = catalog("items", "items", "firstElement", null);
        Object firstOut = jolt.transform(TransformFixtures.map("items", List.of("a", "b", "c")),
                jolt.convertJoltSpec(List.of(first)));
        assertEquals("a", ((Map<?, ?>) firstOut).get("items"));

        EdgeTransformDto at = catalog("items", "items", "elementAt", List.of("1"));
        Object atOut = jolt.transform(TransformFixtures.map("items", List.of("a", "b", "c")),
                jolt.convertJoltSpec(List.of(at)));
        assertEquals("b", ((Map<?, ?>) atOut).get("items"));
    }

    @Test
    void catalog_default_remove_sort_cardinality() {
        EdgeTransformDto def = catalog(null, "b", "default", List.of("\"x\""));
        Object defOut = jolt.transform(TransformFixtures.map("a", 1), jolt.convertJoltSpec(List.of(def)));
        assertEquals("x", ((Map<?, ?>) defOut).get("b"));
        assertEquals(1, ((Map<?, ?>) defOut).get("a"));

        EdgeTransformDto rem = catalog(null, "b", "remove", null);
        Object remOut = jolt.transform(TransformFixtures.map("a", 1, "b", 2), jolt.convertJoltSpec(List.of(rem)));
        assertEquals(1, ((Map<?, ?>) remOut).get("a"));
        assertFalse(((Map<?, ?>) remOut).containsKey("b"));

        EdgeTransformDto sort = catalog(null, null, "sort", null);
        sort.setTargetPath(null);
        Object sortOut = jolt.transform(TransformFixtures.map("z", 1, "a", 2), jolt.convertJoltSpec(List.of(sort)));
        assertEquals(List.copyOf(((Map<?, ?>) sortOut).keySet()), List.of("a", "z"));

        EdgeTransformDto card = catalog("x", "x", "cardinality", List.of("ONE"));
        Object cardOut = jolt.transform(TransformFixtures.map("x", List.of(1, 2)), jolt.convertJoltSpec(List.of(card)));
        assertEquals(1, ((Map<?, ?>) cardOut).get("x"));
    }

    @Test
    void catalog_concat_multi_source_via_scratch_args() {
        EdgeTransformDto t = catalog("first", "full", "concat", List.of("\" \"", "source.last"));
        List<Object> spec = jolt.convertJoltSpec(List.of(t));
        // Scratch shift + cleanup remove must be present for source-ref args.
        assertTrue(findOp(spec, "remove") != null, "scratch __args cleanup remove expected");
        Object out = jolt.transform(TransformFixtures.map("first", "John", "last", "Doe"), spec);
        assertEquals("John Doe", ((Map<?, ?>) out).get("full"));
        assertFalse(((Map<?, ?>) out).containsKey("__args"), "scratch keys must be cleaned up");
    }

    @Test
    void catalog_shift_idempotency_still_holds_with_operator_rows() {
        EdgeTransformDto t = catalog("result", "prompt", "toUpper", null);
        List<Object> spec = jolt.convertJoltSpec(List.of(t));
        Object first = jolt.transform(TransformFixtures.map("result", "hi"), spec);
        assertEquals("HI", ((Map<?, ?>) first).get("prompt"));
        // Second apply: shift source "result" absent → passthrough; toUpper on missing prompt is soft.
        Object second = jolt.transform(first, spec);
        assertEquals("HI", ((Map<?, ?>) second).get("prompt"));
    }

    /* -------------------- Authored row-value defaults -------------------- */

    /** Connected row, upstream delivered nothing → the authored target value fills the gap. */
    @Test
    void authored_target_default_applies_when_connection_delivers_nothing() {
        EdgeTransformDto t = TransformFixtures.transform("result", "prompt", "nodeB", "schema", "string");
        t.setTargetPathValue("fallback");
        List<Object> spec = jolt.convertJoltSpec(List.of(t), null);

        Object out = jolt.transform(TransformFixtures.map("other", "x"), spec);
        assertEquals("fallback", ((Map<?, ?>) out).get("prompt"),
                "no data on the wire → the authored default must be sent");
    }

    /** Connected row, upstream delivered data → the real value wins over the default. */
    @Test
    void authored_target_default_never_overwrites_delivered_data() {
        EdgeTransformDto t = TransformFixtures.transform("result", "prompt", "nodeB", "schema", "string");
        t.setTargetPathValue("fallback");
        List<Object> spec = jolt.convertJoltSpec(List.of(t), null);

        Object out = jolt.transform(TransformFixtures.map("result", "hi"), spec);
        assertEquals("hi", ((Map<?, ?>) out).get("prompt"));
    }

    /** Source-side default: the port produced nothing → the authored value is sent instead. */
    @Test
    void authored_source_default_feeds_the_mapping_when_port_output_is_missing() {
        EdgeTransformDto t = TransformFixtures.transform("result", "prompt", "nodeB", "schema", "string");
        t.setSourcePathValue("seed");
        List<Object> spec = jolt.convertJoltSpec(List.of(t), null);

        Object out = jolt.transform(TransformFixtures.map("unrelated", 1), spec);
        assertEquals("seed", ((Map<?, ?>) out).get("prompt"),
                "source default is planted before the shift so the mapping carries it");
    }

    /** Container rows author JSON text — the default must land as structured data. */
    @Test
    void authored_container_default_parses_json_into_structured_data() {
        EdgeTransformDto t = TransformFixtures.transform("result", "meta", "nodeB", "schema", null);
        t.setTargetPathValue("{\"retries\":3}");
        List<Object> spec = jolt.convertJoltSpec(List.of(t), null);

        Object out = jolt.transform(TransformFixtures.map("other", "x"), spec);
        assertEquals(Map.of("retries", 3), ((Map<?, ?>) out).get("meta"));
    }

    /** Blank authored values add no default op — the spec stays byte-identical to before. */
    @Test
    void blank_authored_values_emit_no_default_op() {
        EdgeTransformDto t = TransformFixtures.transform("result", "prompt", "nodeB", "schema", "string");
        t.setTargetPathValue("   ");
        t.setSourcePathValue(null);
        List<Object> spec = jolt.convertJoltSpec(List.of(t), null);
        assertTrue(findOp(spec, "default") == null, "blank values must not emit a Defaultr op");
    }

    /**
     * BOTH ports carry an authored default and the connection comes back empty: the source-side
     * default is planted before the shift, so the receiver always sees a filled wire and the
     * target-side default NEVER fires. This is the intended precedence — the provider's default
     * travels; the receiver's default exists only for a wire the provider left truly empty.
     */
    @Test
    void both_defaults_set_source_default_wins_when_connection_is_empty() {
        EdgeTransformDto t = TransformFixtures.transform("result", "prompt", "nodeB", "schema", "string");
        t.setSourcePathValue("provider-default");
        t.setTargetPathValue("receiver-default");
        List<Object> spec = jolt.convertJoltSpec(List.of(t), null);

        Object out = jolt.transform(TransformFixtures.map("unrelated", 1), spec);
        assertEquals("provider-default", ((Map<?, ?>) out).get("prompt"),
                "source default fills the wire first — the target default must never apply");
    }

    /**
     * An explicit {@code null} on the wire is "empty" too: Defaultr fires on null, so the source
     * default replaces it before the shift; with no source default the target default catches it.
     */
    @Test
    void explicit_null_on_the_wire_behaves_like_missing() {
        EdgeTransformDto withSource = TransformFixtures.transform("result", "prompt", "nodeB", "schema", "string");
        withSource.setSourcePathValue("provider-default");
        withSource.setTargetPathValue("receiver-default");
        Object out = jolt.transform(TransformFixtures.map("result", null),
                jolt.convertJoltSpec(List.of(withSource), null));
        assertEquals("provider-default", ((Map<?, ?>) out).get("prompt"),
                "null from the provider must be replaced by the provider's default");

        EdgeTransformDto targetOnly = TransformFixtures.transform("result", "prompt", "nodeB", "schema", "string");
        targetOnly.setTargetPathValue("receiver-default");
        Object out2 = jolt.transform(TransformFixtures.map("result", null),
                jolt.convertJoltSpec(List.of(targetOnly), null));
        assertEquals("receiver-default", ((Map<?, ?>) out2).get("prompt"),
                "null on the wire with no source default falls to the receiver's default");
    }

    /**
     * AI Agent {@code tools.response} (wrapper) ← MCP Server {@code status / data / http_status},
     * rows in fan-connect order: every mapped field reaches the agent, as-is, under the wrapper.
     */
    @Test
    void wrapper_target_keeps_every_mapped_source_as_its_own_child() {
        List<EdgeTransformDto> rows = new ArrayList<>();
        for (String field : List.of("status", "data", "http_status")) {
            rows.add(TransformFixtures.transform(field, "response", "agent", "outputSchema", "wrapper"));
        }
        List<Object> spec = jolt.convertJoltSpec(rows, Map.of("response", "wrapper"));

        Map<String, Object> data = TransformFixtures.map("tools", List.of(TransformFixtures.map("name", "ask")));
        Object out = jolt.transform(TransformFixtures.map("status", "ok", "data", data, "http_status", 200), spec);

        assertEquals(TransformFixtures.map("response",
                        TransformFixtures.map("status", "ok", "data", data, "http_status", 200)), out,
                "three rows onto one wrapper must not overwrite each other down to the last value");
    }

    /** A port-root row (blank source path) has no key to nest under — it is the wrapper's whole value. */
    @Test
    void wrapper_target_takes_a_port_root_source_whole() {
        EdgeTransformDto root = TransformFixtures.transform("", "response", "agent", "outputSchema", "wrapper");
        Object out = jolt.transform(TransformFixtures.map("status", "ok", "http_status", 200),
                jolt.convertJoltSpec(List.of(root), Map.of("response", "wrapper")));
        assertEquals(TransformFixtures.map("response",
                TransformFixtures.map("status", "ok", "http_status", 200)), out);
    }

    private static EdgeTransformDto catalog(String sourcePath, String targetPath,
                                            String operator, List<String> args) {
        EdgeTransformDto t = TransformFixtures.transform(
                sourcePath == null ? "" : sourcePath,
                targetPath == null ? "" : targetPath,
                "nodeB", null, null);
        t.setOperator(operator);
        t.setArgs(args);
        return t;
    }

    private static Map<String, Object> op(String operation, Map<String, Object> spec) {
        return TransformFixtures.map("operation", operation, "spec", spec);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> findOp(List<Object> spec, String operation) {
        for (Object o : spec) {
            Map<String, Object> m = (Map<String, Object>) o;
            if (operation.equals(m.get("operation"))) {
                return m;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asSpec(Map<String, Object> op) {
        return (Map<String, Object>) op.get("spec");
    }
}
