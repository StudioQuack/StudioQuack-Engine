package com.studioquack.sqs.sandbox.secrets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-node secret-scoping invariant, pinned as a regression test. One node's container must
 * never receive another node's resolved secret (: "Scope resolution to the current node only, never
 * the whole graph").
 */
class SecretPerNodeScopingTest {

    /** Refs/values — 24-hex ids, per the {@code sec_<24hex>:vN} wire shape. */
    private static final String REF_A = "sec_0000000000000000000000aa:v1";
    private static final String REF_B = "sec_0000000000000000000000bb:v1";
    private static final String VALUE_A = "value-A-plaintext";
    private static final String VALUE_B = "value-B-plaintext";

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Strict stub keystore: answers ONLY the two bound refs and records every ref it is asked for,
     * so the test can prove B's ref is never even LOOKED UP while A's subtree resolves.
     */
    private final List<String> requestedRefs = new ArrayList<>();
    private final SecretKeyStore keyStore = ref -> {
        requestedRefs.add(ref.wire());
        return switch (ref.wire()) {
            case REF_A -> Optional.of(new SecretKeyStore.ResolvedSecret("DB_PASSWORD_A", VALUE_A));
            case REF_B -> Optional.of(new SecretKeyStore.ResolvedSecret("API_KEY_B", VALUE_B));
            default -> Optional.empty();
        };
    };
    private final SecretResolver resolver = new SecretResolver(keyStore);

    /** Node A carries handles to REF_A only (object-wrapper + compact-string forms). */
    private static NodeDto nodeA() {
        ParameterDto pw = new ParameterDto();
        pw.setId("p-a1");
        pw.setShortCode("db_password");
        pw.setType("password");
        pw.setValue(new LinkedHashMap<>(Map.of("$secretRef", REF_A)));
        TemplateDto data = new TemplateDto();
        data.setId("tpl-a");
        data.setLabel("node A");
        data.setConfigurationParameters(List.of(pw));
        NodeDto n = new NodeDto();
        n.setId("nodeA");
        n.setData(data);
        n.setInputList(new ArrayList<>(List.of(
                new InOutValueDto("dbPassword", "$secretRef:" + REF_A),
                new InOutValueDto("query", "select 1"))));
        return n;
    }

    /** Node B carries handles to REF_B only (object-wrapper + JSON-stringified-wrapper forms). */
    private static NodeDto nodeB() {
        ParameterDto key = new ParameterDto();
        key.setId("p-b1");
        key.setShortCode("api_key");
        key.setType("api_key");
        key.setValue(new LinkedHashMap<>(Map.of("$secretRef", REF_B)));
        TemplateDto data = new TemplateDto();
        data.setId("tpl-b");
        data.setLabel("node B");
        data.setConfigurationParameters(List.of(key));
        NodeDto n = new NodeDto();
        n.setId("nodeB");
        n.setData(data);
        n.setInputList(new ArrayList<>(List.of(
                new InOutValueDto("apiKey", "{\"$secretRef\":\"" + REF_B + "\"}"))));
        return n;
    }

    @Test
    void resolving_node_A_subtree_never_touches_node_B() {
        // Two independent trees — the resolver must not need (or mutate) anything outside the
        // subtree it was handed.
        Map<String, Object> subtreeA = mapper.convertValue(nodeA(),
                new TypeReference<Map<String, Object>>() {});
        Map<String, Object> subtreeB = mapper.convertValue(nodeB(),
                new TypeReference<Map<String, Object>>() {});

        Set<String> resolved = resolver.resolveInline(subtreeA);

        assertEquals(Set.of(VALUE_A), resolved,
                "A's dispatch resolves exactly A's distinct plaintext values");
        assertFalse(resolved.contains(VALUE_B), "B's plaintext is never returned for A's subtree");
        assertTrue(deepContains(subtreeA, VALUE_A), "A's handles collapsed to plaintext in place");
        assertFalse(deepContains(subtreeA, REF_A),
                "no unresolved ref string of A survives inside A's resolved subtree");
        assertFalse(deepContains(subtreeA, VALUE_B), "A's subtree carries nothing of B");
        assertFalse(deepContains(subtreeA, REF_B), "A's subtree never even names B's ref");

        // B's own tree is byte-for-byte untouched: every handle still an unresolved placeholder.
        assertFalse(deepContains(subtreeB, VALUE_B), "B's subtree was not resolved as a side effect");
        assertTrue(deepContains(subtreeB, REF_B), "B's handles remain opaque $secretRef placeholders");

        // Fail-closed custody: the keystore was never even ASKED for B's ref during A's resolution.
        assertFalse(requestedRefs.contains(REF_B),
                "keystore never looked up B's ref while resolving A (requested=" + requestedRefs + ")");
    }

    @Test
    void assembled_sandbox_context_for_A_carries_value_A_but_never_value_B() {
        // Run-shaped fixture: one graph, two nodes, A is the node being dispatched.
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("wfe-1");
        ctx.setWorkflowId("wf-1");
        ctx.setEnvId("env-1");
        ctx.setCurrentNodeId("nodeA");
        ctx.setCurrentNode(nodeA());
        ctx.setNodes(new ArrayList<>(List.of(nodeA(), nodeB())));
        ctx.setEdges(new ArrayList<>(List.of(new EdgeDto())));

        // --- Mirror ScriptDispatchDelegate's assembly order exactly -------------------------
        // 1. Capture the graph BEFORE the pipeline clears it off the context.
        List<NodeDto> graphNodes = new ArrayList<>(ctx.getNodes());
        List<EdgeDto> graphEdges = new ArrayList<>(ctx.getEdges());
        // 2. prepareInbound's clearNodeAndEdges() — toContextMap then sees a GRAPH-LESS context.
        ctx.clearNodeAndEdges();
        Map<String, Object> sandboxContext = mapper.convertValue(ctx,
                new TypeReference<Map<String, Object>>() {});
        assertFalse(sandboxContext.containsKey("nodes") && sandboxContext.get("nodes") != null,
                "the private snapshot is graph-less before the explicit re-attach");
        // 3. The flat input variables (prepared.inputVariables()) — A's ports only.
        Map<String, Object> sandboxInputVars = new LinkedHashMap<>();
        sandboxInputVars.put("dbPassword", "$secretRef:" + REF_A);
        sandboxInputVars.put("query", "select 1");
        // 4. Resolve JUST-IN-TIME, scoped to the current node + its inputs — never the graph.
        Set<String> secretLiterals = new LinkedHashSet<>();
        secretLiterals.addAll(resolver.resolveInline(sandboxContext.get("currentNode")));
        secretLiterals.addAll(resolver.resolveInline(sandboxInputVars));
        // 5. Re-attach the graph AFTER resolution, as UNRESOLVED handles (delegate lines ~409-414).
        sandboxContext.put("nodes", mapper.convertValue(graphNodes,
                new TypeReference<List<Map<String, Object>>>() {}));
        sandboxContext.put("edges", mapper.convertValue(graphEdges,
                new TypeReference<List<Map<String, Object>>>() {}));

        // --- The sandbox-facing pair (executionContext + inputVariables) ----------------------
        Map<String, Object> sandboxFacing = new LinkedHashMap<>();
        sandboxFacing.put("executionContext", sandboxContext);
        sandboxFacing.put("inputVariables", sandboxInputVars);

        assertTrue(deepContains(sandboxFacing, VALUE_A), "A's own plaintext reached A's container");
        assertFalse(deepContains(sandboxFacing, VALUE_B),
                "B's plaintext appears NOWHERE in A's sandbox-facing context");
        assertEquals(Set.of(VALUE_A), secretLiterals,
                "the redactor is seeded with exactly A's resolved literals — never B's");

        // B's ref survives only as an unresolved handle inside B's own nodes[] entry.
        List<String> refBPaths = new ArrayList<>();
        collectPathsContaining(sandboxFacing, REF_B, "", refBPaths);
        assertFalse(refBPaths.isEmpty(), "the static graph copy still names B's ref as a handle");
        for (String path : refBPaths) {
            assertTrue(path.startsWith("executionContext.nodes["),
                    "B's ref appears only inside the re-attached graph, not in currentNode/inputs: " + path);
            assertTrue(path.contains("$secretRef"),
                    "B's ref survives only as an unresolved $secretRef placeholder: " + path);
        }
        // And the nodes[] entry carrying it is B's — never A's definition smuggling a foreign ref.
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) sandboxContext.get("nodes");
        for (Map<String, Object> node : nodes) {
            boolean isB = "nodeB".equals(node.get("id"));
            assertEquals(isB, deepContains(node, REF_B),
                    "REF_B handle lives exclusively in nodeB's nodes[] entry");
        }

        // Fail-closed custody on the full assembly: B's ref was never looked up for A's dispatch.
        assertFalse(requestedRefs.contains(REF_B),
                "keystore never looked up B's ref while assembling A's dispatch");
    }

    /* ------------------------------ walk helpers ------------------------------ */

    /** True when any string leaf of the (Map/List/String) tree CONTAINS {@code needle}. */
    static boolean deepContains(Object tree, String needle) {
        if (tree == null) {
            return false;
        }
        if (tree instanceof String s) {
            return s.contains(needle);
        }
        if (tree instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (String.valueOf(e.getKey()).contains(needle) || deepContains(e.getValue(), needle)) {
                    return true;
                }
            }
            return false;
        }
        if (tree instanceof List<?> list) {
            for (Object e : list) {
                if (deepContains(e, needle)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Collects the JSON-ish path of every string leaf containing {@code needle} (keys included). */
    static void collectPathsContaining(Object tree, String needle, String path, List<String> out) {
        if (tree == null) {
            return;
        }
        if (tree instanceof String s) {
            if (s.contains(needle)) {
                out.add(path + "=\"" + s + "\"");
            }
            return;
        }
        if (tree instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                String key = String.valueOf(e.getKey());
                if (key.contains(needle)) {
                    out.add(path + "." + key + " (key)");
                }
                collectPathsContaining(e.getValue(), needle,
                        path.isEmpty() ? key : path + "." + key, out);
            }
            return;
        }
        if (tree instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                collectPathsContaining(list.get(i), needle, path + "[" + i + "]", out);
            }
        }
    }
}
