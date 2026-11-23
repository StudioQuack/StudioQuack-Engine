package com.studioquack.sqs.sandbox.secrets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.transform.GenerateScriptService;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * No ref enumeration: the sandbox-facing context must expose ONLY the refs its own node uses, and
 * even those only as resolved plaintext (current node) or opaque per-node handles (the static graph
 * copy). A hostile script reading its whole baked {@code __SQ_CONTEXT__} / helper-channel {@code
 * full-context} must find
 */
class SandboxContextRefEnumerationTest {

    private static final String REF_A = "sec_0000000000000000000000aa:v1";
    private static final String REF_B = "sec_0000000000000000000000bb:v1";
    private static final String VALUE_A = "value-A-plaintext";
    private static final String VALUE_B = "value-B-plaintext";

    /** The ref wire shape, for find()-style scanning of arbitrary string leaves. */
    private static final Pattern REF_SHAPED = Pattern.compile("sec_[0-9a-f]{24}:v[1-9][0-9]{0,5}");

    /** Key names that would amount to an environment-wide secret inventory on the sandbox context. */
    private static final Set<String> FORBIDDEN_INVENTORY_KEYS = Set.of(
            "secretrefs", "availablesecrets", "secrets", "secretlist", "allsecrets",
            "environmentsecrets", "secretslist", "secretkeys");

    private final ObjectMapper mapper = new ObjectMapper();
    private final SecretKeyStore keyStore = ref -> switch (ref.wire()) {
        case REF_A -> Optional.of(new SecretKeyStore.ResolvedSecret("DB_PASSWORD_A", VALUE_A));
        case REF_B -> Optional.of(new SecretKeyStore.ResolvedSecret("API_KEY_B", VALUE_B));
        default -> Optional.empty();
    };
    private final SecretResolver resolver = new SecretResolver(keyStore);

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

    /**
     * The delegate's two sandbox-facing products, kept as separate as production keeps them:
     * {@code executionContext} is what {@code bakeContextBase64} freezes and what the helper job
     * serves; {@code inputVariables} rides the job beside it (never inside the baked blob).
     */
    private record Assembly(Map<String, Object> executionContext,
                            Map<String, Object> inputVariables,
                            Set<String> secretLiterals) {

        /** Single walk root for tree-wide assertions ({@code executionContext.*} / {@code inputVariables.*}). */
        Map<String, Object> walkRoot() {
            return Map.of("executionContext", executionContext, "inputVariables", inputVariables);
        }
    }

    /**
     * Assembles node A's dispatch EXACTLY as {@code ScriptDispatchDelegate} does: graph-less
     * private snapshot → resolve current node + inputs in place → re-attach the graph as
     * unresolved handles.
     */
    private Assembly assembleForNodeA() {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("wfe-1");
        ctx.setWorkflowId("wf-1");
        ctx.setEnvId("env-1");
        ctx.setCurrentNodeId("nodeA");
        ctx.setCurrentNode(nodeA());
        ctx.setNodes(new ArrayList<>(List.of(nodeA(), nodeB())));
        ctx.setEdges(new ArrayList<>(List.of(new EdgeDto())));

        List<NodeDto> graphNodes = new ArrayList<>(ctx.getNodes());
        List<EdgeDto> graphEdges = new ArrayList<>(ctx.getEdges());
        ctx.clearNodeAndEdges(); // what prepareInbound does before toContextMap

        Map<String, Object> sandboxContext = mapper.convertValue(ctx,
                new TypeReference<Map<String, Object>>() {});
        Map<String, Object> sandboxInputVars = new LinkedHashMap<>();
        sandboxInputVars.put("dbPassword", "$secretRef:" + REF_A);
        sandboxInputVars.put("query", "select 1");

        Set<String> secretLiterals = new LinkedHashSet<>();
        secretLiterals.addAll(resolver.resolveInline(sandboxContext.get("currentNode")));
        secretLiterals.addAll(resolver.resolveInline(sandboxInputVars));

        sandboxContext.put("nodes", mapper.convertValue(graphNodes,
                new TypeReference<List<Map<String, Object>>>() {}));
        sandboxContext.put("edges", mapper.convertValue(graphEdges,
                new TypeReference<List<Map<String, Object>>>() {}));
        return new Assembly(sandboxContext, sandboxInputVars, secretLiterals);
    }

    @Test
    void assembled_context_has_no_secret_inventory_and_no_bare_refs() {
        Assembly assembly = assembleForNodeA();
        Map<String, Object> root = assembly.walkRoot();

        // (1) No inventory-shaped key anywhere in the tree.
        List<String> keys = new ArrayList<>();
        collectKeys(root, keys);
        for (String key : keys) {
            assertFalse(FORBIDDEN_INVENTORY_KEYS.contains(key.toLowerCase(java.util.Locale.ROOT)),
                    "sandbox context must not carry an environment-wide inventory key: " + key);
        }

        // (2) Every ref-shaped string leaf is a recognised HANDLE form — never a bare ref, and
        //     never an element of a ref array.
        List<String[]> refLeaves = new ArrayList<>();
        collectRefShapedLeaves(root, "", refLeaves);
        assertFalse(refLeaves.isEmpty(), "fixture sanity: the graph copy does carry ref handles");
        for (String[] pathAndValue : refLeaves) {
            assertHandleForm(pathAndValue[0], pathAndValue[1]);
        }

        // (3) B's ref appears ONLY inside B's own nodes[] entry, as unresolved handles.
        List<String> refBPaths = new ArrayList<>();
        SecretPerNodeScopingTest.collectPathsContaining(root, REF_B, "", refBPaths);
        assertEquals(2, refBPaths.size(),
                "B has exactly two handles and both survive only in its graph copy: " + refBPaths);
        for (String path : refBPaths) {
            assertTrue(path.startsWith("executionContext.nodes["),
                    "B's ref must not appear outside the re-attached graph: " + path);
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes =
                (List<Map<String, Object>>) assembly.executionContext().get("nodes");
        for (Map<String, Object> node : nodes) {
            assertEquals("nodeB".equals(node.get("id")),
                    SecretPerNodeScopingTest.deepContains(node, REF_B),
                    "REF_B is confined to nodeB's own nodes[] entry");
        }

        // (4) Plaintext asymmetry: A's own value is present (its container legitimately gets it);
        //     B's value is nowhere — not even once.
        assertTrue(SecretPerNodeScopingTest.deepContains(root, VALUE_A));
        assertFalse(SecretPerNodeScopingTest.deepContains(root, VALUE_B),
                "B's plaintext must be absent from A's entire sandbox-facing context");
        assertEquals(Set.of(VALUE_A), assembly.secretLiterals(),
                "only A's resolved literals are seeded for redaction");
    }

    @Test
    void the_baked_context_blob_carries_the_same_shape() throws Exception {
        Assembly assembly = assembleForNodeA();
        // The real bake the delegate uses for both the script path (__SQ_CONTEXT__) and the
        // entrypoint path (SQ_CONTEXT) — same map, same serializer.
        GenerateScriptService generate = new GenerateScriptService(mapper, null);
        String baked = generate.bakeContextBase64(assembly.executionContext());

        String json = new String(Base64.getDecoder().decode(baked), StandardCharsets.UTF_8);
        assertTrue(json.contains(VALUE_A), "baked context carries A's own resolved plaintext");
        assertFalse(json.contains(VALUE_B), "baked context never carries B's plaintext");
        assertFalse(json.contains("secretRefs"), "baked context carries no inventory key");
        assertFalse(json.contains("availableSecrets"), "baked context carries no inventory key");

        // The decoded blob is the same tree: re-run the handle-form assertion on it.
        Map<String, Object> decoded = mapper.readValue(json,
                new TypeReference<Map<String, Object>>() {});
        List<String[]> refLeaves = new ArrayList<>();
        collectRefShapedLeaves(decoded, "", refLeaves);
        assertFalse(refLeaves.isEmpty(), "baked blob keeps the graph's unresolved handles");
        for (String[] pathAndValue : refLeaves) {
            assertHandleForm(pathAndValue[0], pathAndValue[1]);
        }
    }

    @Test
    void the_helper_channel_serves_the_same_shape() {
        Assembly assembly = assembleForNodeA();
        HelperRequestRouter router = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(new AiModelProviderHolder(), mapper),
                null);
        // registerHelperJob copies job.executionContext() onto the HelperJob verbatim — the map
        // under test IS what the in-container SDK can read back over the channel.
        String token = router.register(HelperJob.builder("job-a", "python")
                .executionContext(assembly.executionContext()).build());

        HelperRequestRouter.RoutedResponse full =
                router.route("GET", "full-context", token, new byte[0]);
        assertEquals(200, full.status());
        String fullBody = new String(full.body(), StandardCharsets.UTF_8);
        assertTrue(fullBody.contains(VALUE_A), "full-context serves A's own plaintext");
        assertFalse(fullBody.contains(VALUE_B),
                "full-context never serves B's plaintext to A's container");
        assertFalse(fullBody.contains(REF_B),
                "D-23: full-context must not name another node's $secretRef");

        HelperRequestRouter.RoutedResponse currentNode =
                router.route("GET", "current-node", token, new byte[0]);
        assertEquals(200, currentNode.status());
        String nodeBody = new String(currentNode.body(), StandardCharsets.UTF_8);
        assertTrue(nodeBody.contains(VALUE_A), "current-node resolved A's handles");
        assertFalse(nodeBody.contains(REF_A), "current-node holds no leftover A ref string");
        assertFalse(nodeBody.contains(REF_B), "current-node never names B's ref");
        assertFalse(nodeBody.contains(VALUE_B), "current-node never carries B's plaintext");
    }

    /* ------------------------------ walk helpers ------------------------------ */

    /** A ref-shaped leaf is legitimate only as one of the six recognised {@code $secretRef} handle forms. */
    private static void assertHandleForm(String path, String leaf) {
        boolean recognisedHandle = path.endsWith("$secretRef")
                || leaf.startsWith("$secretRef:")
                || leaf.startsWith("{\"$secretRef\":");
        assertTrue(recognisedHandle,
                "ref-shaped string outside a $secretRef handle (bare ref / enumeration?) at "
                        + path + " = " + leaf);
    }

    private static void collectKeys(Object tree, List<String> out) {
        if (tree instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.add(String.valueOf(e.getKey()));
                collectKeys(e.getValue(), out);
            }
        } else if (tree instanceof List<?> list) {
            for (Object e : list) {
                collectKeys(e, out);
            }
        }
    }

    /** Collects [path, leaf] for every string leaf containing a ref-shaped substring. */
    private static void collectRefShapedLeaves(Object tree, String path, List<String[]> out) {
        if (tree == null) {
            return;
        }
        if (tree instanceof String s) {
            if (REF_SHAPED.matcher(s).find()) {
                out.add(new String[]{path, s});
            }
            return;
        }
        if (tree instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                String key = String.valueOf(e.getKey());
                collectRefShapedLeaves(e.getValue(),
                        path.isEmpty() ? key : path + "." + key, out);
            }
            return;
        }
        if (tree instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                collectRefShapedLeaves(list.get(i), path + "[" + i + "]", out);
            }
        }
    }
}
