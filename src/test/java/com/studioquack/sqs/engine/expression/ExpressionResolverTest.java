package com.studioquack.sqs.engine.expression;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/** {@link ExpressionResolver} — grammar, coercion, bounds, ref scopes. */
@ExtendWith(MockitoExtension.class)
class ExpressionResolverTest {

    @Mock
    private ExecutionContextStore contextStore;

    private ExpressionResolver resolver;
    private ExecutionContext ctx;
    /** Cart producer from setUp — reused when a test adds extra nodes. */
    private NodeDto cartNode;

    @BeforeEach
    void setUp() {
        resolver = new ExpressionResolver(contextStore);
        ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("wfe-1");

        TemplateDto data = new TemplateDto();
        data.setNodeShortCode("cart");
        cartNode = new NodeDto();
        cartNode.setId("node-cart");
        cartNode.setData(data);
        ctx.setNodes(List.of(cartNode));

        Map<String, Object> wf = new HashMap<>();
        wf.put("maxRetry", 3);
        wf.put("label", "alpha");
        ctx.setWorkflowVariables(wf);

        Map<String, Object> project = new HashMap<>();
        project.put("apiBaseUrl", "https://api.example");
        project.put("enabled", true);
        ctx.setProjectVariables(project);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("total", 150);
        List<Map<String, Object>> items = List.of(
                Map.of("name", "a"),
                Map.of("name", "widget")
        );
        payload.put("items", items);

        InOutValueDto out = new InOutValueDto("out", payload);
        // Lenient: not every test exercises @node resolution.
        lenient().when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of("node-cart", List.of(out)));
    }

    // ---- resolveValue / passthrough ---------------------------------------

    @Test
    void resolveValue_passthrough_without_dollar_paren() {
        String plain = "hello world";
        assertSame(plain, resolver.resolveValue(plain, ctx));
        assertEquals("42", resolver.resolveValue("42", ctx));
    }

    @Test
    void resolveValue_wf_and_project_refs() {
        assertEquals(3, resolver.resolveValue("$(wf.maxRetry)", ctx));
        assertEquals("https://api.example", resolver.resolveValue("$(project.apiBaseUrl)", ctx));
    }

    @Test
    void resolveValue_node_path_into_primary_output() {
        assertEquals(150, resolver.resolveValue("$(@cart.total)", ctx));
        assertEquals("widget", resolver.resolveValue("$(@cart.items[1].name)", ctx));
    }

    @Test
    void resolveValue_mixed_string_substitution() {
        assertEquals("retries=3", resolver.resolveValue("retries=$(wf.maxRetry)", ctx));
    }

    @Test
    void substituteInString_skips_secretRef() {
        String in = "x=$secretRef:sec_abcdef0123456789abcdef01:v1 y=$(wf.label)";
        assertEquals("x=$secretRef:sec_abcdef0123456789abcdef01:v1 y=alpha",
                resolver.substituteInString(in, ctx));
    }

    // ---- conditions: comparisons / logical / parens -----------------------

    @Test
    void evaluateCondition_numeric_comparisons() {
        assertTrue(resolver.evaluateCondition("$(wf.maxRetry) == 3", ctx));
        assertTrue(resolver.evaluateCondition("$(wf.maxRetry) != 1", ctx));
        assertTrue(resolver.evaluateCondition("$(@cart.total) > 100", ctx));
        assertTrue(resolver.evaluateCondition("$(@cart.total) >= 150", ctx));
        assertTrue(resolver.evaluateCondition("$(wf.maxRetry) < 10", ctx));
        assertTrue(resolver.evaluateCondition("$(wf.maxRetry) <= 3", ctx));
        assertFalse(resolver.evaluateCondition("$(@cart.total) < 10", ctx));
    }

    @Test
    void evaluateCondition_string_equality_and_coercion() {
        assertTrue(resolver.evaluateCondition("$(wf.label) == 'alpha'", ctx));
        assertTrue(resolver.evaluateCondition("'5' == 5", ctx)); // both numeric after coerce
        assertTrue(resolver.evaluateCondition("'foo' != 'bar'", ctx));
    }

    @Test
    void evaluateCondition_double_and_single_quoted_empty_literals() {
        assertTrue(resolver.evaluateCondition("'' == \"\"", ctx));
        assertTrue(resolver.evaluateCondition("'' == ''", ctx));
        assertTrue(resolver.evaluateCondition("\"\" == \"\"", ctx));
        assertFalse(resolver.evaluateCondition("'hello' == \"\"", ctx));
        assertFalse(resolver.evaluateCondition("'hello' == ''", ctx));
        assertFalse(resolver.evaluateCondition("\"hello\" == \"\"", ctx));
        assertFalse(resolver.evaluateCondition("\"hello\" == ''", ctx));
    }

    @Test
    void evaluateCondition_mixed_quote_styles_compare_equal() {
        assertTrue(resolver.evaluateCondition("'foo' == \"foo\"", ctx));
        assertTrue(resolver.evaluateCondition("\"foo\" == 'foo'", ctx));
    }

    @Test
    void evaluateCondition_wf_label_empty_vs_alpha_both_quote_styles() {
        ctx.getWorkflowVariables().put("label", "");
        assertTrue(resolver.evaluateCondition("$(wf.label) == \"\"", ctx));
        assertTrue(resolver.evaluateCondition("$(wf.label) == ''", ctx));
        ctx.getWorkflowVariables().put("label", "alpha");
        assertFalse(resolver.evaluateCondition("$(wf.label) == \"\"", ctx));
        assertFalse(resolver.evaluateCondition("$(wf.label) == ''", ctx));
    }

    @Test
    void evaluateCondition_bang_equals_typo_is_parse_false() {
        assertFalse(resolver.evaluateCondition("'a' =! ''", ctx));
        assertEquals("parse", resolver.evaluateConditionOutcome("'a' =! ''", ctx).error());
        assertFalse(resolver.evaluateConditionOutcome("'a' =! ''", ctx).result());
        assertFalse(resolver.evaluateCondition("@missing.port.x =! \"\"", ctx));
        assertEquals("parse", resolver.evaluateConditionOutcome("@missing.port.x =! \"\"", ctx).error());
        assertFalse(resolver.evaluateConditionOutcome("@missing.port.x =! \"\"", ctx).result());
    }

    @Test
    void evaluateCondition_ordering_on_non_numbers_is_false() {
        assertFalse(resolver.evaluateCondition("'a' > 'b'", ctx));
        assertFalse(resolver.evaluateCondition("$(wf.label) < 'zzz'", ctx));
    }

    @Test
    void evaluateCondition_logical_and_parens_and_not() {
        assertTrue(resolver.evaluateCondition("$(wf.maxRetry) == 3 && $(@cart.total) > 100", ctx));
        assertTrue(resolver.evaluateCondition("false || $(project.enabled) == true", ctx));
        assertTrue(resolver.evaluateCondition("!(false)", ctx));
        assertTrue(resolver.evaluateCondition("($(wf.maxRetry) == 1) || ($(wf.maxRetry) == 3)", ctx));
        assertFalse(resolver.evaluateCondition("true && false", ctx));
    }

    // ---- functions --------------------------------------------------------

    @Test
    void evaluateCondition_functions() {
        assertTrue(resolver.evaluateCondition("contains('hello','ell')", ctx));
        assertTrue(resolver.evaluateCondition("startsWith($(project.apiBaseUrl),'https')", ctx));
        assertTrue(resolver.evaluateCondition("endsWith($(wf.label),'pha')", ctx));
        assertTrue(resolver.evaluateCondition("exists($(wf.maxRetry))", ctx));
        assertFalse(resolver.evaluateCondition("exists($(wf.missing))", ctx));
        assertFalse(resolver.evaluateCondition("contains('ab','z')", ctx));
    }

    // ---- error-is-false + bounds ------------------------------------------

    @Test
    void evaluateCondition_parse_error_is_false() {
        assertFalse(resolver.evaluateCondition("$((bad", ctx));
        assertFalse(resolver.evaluateCondition("unknownFunc(1)", ctx));
        assertFalse(resolver.evaluateCondition("1 + 2", ctx));
        assertFalse(resolver.evaluateCondition(null, ctx));
        assertFalse(resolver.evaluateCondition("   ", ctx));
    }

    @Test
    void evaluateCondition_rejects_overlong_expression() {
        String huge = "true" + " && true".repeat(400);
        assertTrue(huge.length() > ExpressionResolver.MAX_EXPR_LEN);
        assertFalse(resolver.evaluateCondition(huge, ctx));
    }

    @Test
    void substituteInString_caps_ref_count() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ExpressionResolver.MAX_REFS_PER_VALUE + 5; i++) {
            sb.append("$(wf.label)");
        }
        String out = resolver.substituteInString(sb.toString(), ctx);
        // First 64 resolved to "alpha"; remainder left as literal $(…)
        int alphas = out.split("alpha", -1).length - 1;
        assertEquals(ExpressionResolver.MAX_REFS_PER_VALUE, alphas);
        assertTrue(out.contains("$("));
    }

    @Test
    void resolveValue_missing_node_returns_null() {
        assertNull(resolver.resolveValue("$(@nope.total)", ctx));
    }

    @Test
    void literals_true_false_null() {
        assertTrue(resolver.evaluateCondition("true", ctx));
        assertFalse(resolver.evaluateCondition("false", ctx));
        assertFalse(resolver.evaluateCondition("null", ctx));
        assertTrue(resolver.evaluateCondition("null == null", ctx));
    }

    // Port-aware + global aliases + bare refs

    /** Node with shortCode, same TemplateDto + NodeDto shape as setUp. */
    private static NodeDto nodeWithShortCode(String id, String shortCode) {
        TemplateDto data = new TemplateDto();
        data.setNodeShortCode(shortCode);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(data);
        return node;
    }

    /** Sealed output; portId optional (match prefers portId, then portValue). */
    private static InOutValueDto portOutput(String portValue, String portId, Object value) {
        InOutValueDto o = new InOutValueDto(portValue, value);
        if (portId != null) {
            o.setPortId(portId);
        }
        return o;
    }

    @Test
    void resolveValue_portAware_selectsMatchingPort() {
        InOutValueDto side = portOutput("output_side", null, Map.of("total", 1));
        InOutValueDto main = portOutput("output_main", "pid-main",
                Map.of("orders", Map.of("a", "x")));
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of("node-cart", List.of(side, main)));

        assertEquals("x", resolver.resolveValue("$(@cart.output_main.orders.a)", ctx));
        assertEquals("x", resolver.resolveValue("$(@cart.pid-main.orders.a)", ctx));
        // First sealed port is independently addressable; output_main must not have used it.
        assertEquals(1, resolver.resolveValue("$(@cart.output_side.total)", ctx));
        assertNull(resolver.resolveValue("$(@cart.output_main.total)", ctx));
    }

    @Test
    void resolveValue_nonPortFirstSegment_keepsLegacyFirstOutput() {
        // setUp first output portValue="out", payload total=150 — `total` is a field, not a port.
        assertEquals(150, resolver.resolveValue("$(@cart.total)", ctx));
    }

    @Test
    void evaluateCondition_bareAtPortPath_equalsLiteral() {
        NodeDto node5 = nodeWithShortCode("node-5", "node_5");
        ctx.setNodes(List.of(cartNode, node5));
        InOutValueDto side = portOutput("output_side", null, Map.of("total", 1));
        InOutValueDto main = portOutput("output_main", "pid-main",
                Map.of("orders", Map.of("a", "x")));
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of(
                        "node-cart", List.of(portOutput("out", null, Map.of("total", 150))),
                        "node-5", List.of(side, main)));

        assertTrue(resolver.evaluateCondition("@node_5.output_main.orders.a == 'x'", ctx));
        assertFalse(resolver.evaluateCondition("@node_5.output_main.orders.a == 'nope'", ctx));
    }

    @Test
    void evaluateCondition_workflowAndProjectGlobals_insideAndOutsideDollar() {
        assertTrue(resolver.evaluateCondition("$(Workflow_Globals.maxRetry) == 3", ctx));
        assertTrue(resolver.evaluateCondition("Workflow_Globals.maxRetry == 3", ctx));
        assertTrue(resolver.evaluateCondition("$(Project_Globals.enabled) == true", ctx));
        assertTrue(resolver.evaluateCondition("Project_Globals.apiBaseUrl == 'https://api.example'", ctx));
        assertTrue(resolver.evaluateCondition("$(wf.maxRetry) == 3", ctx));
    }

    @Test
    void evaluateCondition_missingPortOrNode_isFalseNoThrow() {
        assertDoesNotThrow(() -> {
            assertFalse(resolver.evaluateCondition("$(@nope.output_main.x) == 'x'", ctx));
            assertFalse(resolver.evaluateCondition("@missing.foo == 1", ctx));
            assertFalse(resolver.evaluateCondition("$(@cart.no_such_port.zzz) == 'x'", ctx));
            assertNull(resolver.resolveValue("$(@nope.output_main.x)", ctx));
            assertNull(resolver.resolveValue("$(@cart.no_such_port.zzz)", ctx));
            assertNull(resolver.resolveValue("@missing.foo", ctx));
        });
    }

    @Test
    void evaluateCondition_overlong_stillFalse() {
        String huge = "Workflow_Globals.maxRetry == 3" + " && true".repeat(400);
        assertTrue(huge.length() > ExpressionResolver.MAX_EXPR_LEN);
        assertFalse(resolver.evaluateCondition(huge, ctx));
    }

    @Test
    void resolveValue_soleBareRef_andPassthrough() {
        assertEquals(150, resolver.resolveValue("@cart.total", ctx));
        assertEquals(3, resolver.resolveValue("Workflow_Globals.maxRetry", ctx));
        assertEquals("https://api.example", resolver.resolveValue("Project_Globals.apiBaseUrl", ctx));
    }

    // ---- list (n) / [n] and (*) wildcard ----------------------------------

    @Test
    void resolveValue_parenIndex_equalsBracketIndex() {
        assertEquals("widget", resolver.resolveValue("$(@cart.items[1].name)", ctx));
        assertEquals("widget", resolver.resolveValue("$(@cart.items(1).name)", ctx));
        assertEquals(resolver.resolveValue("$(@cart.items[0].name)", ctx),
                resolver.resolveValue("$(@cart.items(0).name)", ctx));
    }

    @Test
    void evaluateCondition_wildcard_anyItemMatches() {
        assertTrue(resolver.evaluateCondition("$(@cart.items(*).name) == 'widget'", ctx));
        assertTrue(resolver.evaluateCondition("$(@cart.items(*).name) == 'a'", ctx));
        assertFalse(resolver.evaluateCondition("$(@cart.items(*).name) == 'nope'", ctx));
    }

    @Test
    void evaluateCondition_nestedWildcard_andBareAt() {
        NodeDto node5 = nodeWithShortCode("node-5", "node_5");
        ctx.setNodes(List.of(cartNode, node5));
        Map<String, Object> payload = Map.of(
                "batches", List.of(
                        Map.of("items", List.of(Map.of("sku", "aaa"), Map.of("sku", "bbb"))),
                        Map.of("items", List.of(Map.of("sku", "ccc")))));
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of("node-5", List.of(portOutput("main_output", null, payload))));

        assertTrue(resolver.evaluateCondition(
                "@node_5.main_output.batches(*).items(*).sku == 'ccc'", ctx));
        assertFalse(resolver.evaluateCondition(
                "@node_5.main_output.batches(*).items(*).sku == 'zzz'", ctx));
        assertTrue(resolver.evaluateCondition(
                "$(@node_5.main_output.batches(*).items(*).sku) == 'aaa'", ctx));
    }

    @Test
    void evaluateCondition_outOfRangeIndex_isFalseNoThrow() {
        assertDoesNotThrow(() -> {
            assertFalse(resolver.evaluateCondition("$(@cart.items(9).name) == 'a'", ctx));
            assertFalse(resolver.evaluateCondition("@cart.items(99).name == 'a'", ctx));
            assertNull(resolver.resolveValue("$(@cart.items(9).name)", ctx));
        });
    }

    @Test
    void resolveValue_wildcard_truncatesAtCap() {
        List<Map<String, Object>> many = new java.util.ArrayList<>();
        for (int i = 0; i < ExpressionResolver.MAX_WILDCARD_VALUES + 40; i++) {
            many.add(Map.of("n", i));
        }
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of("node-cart", List.of(portOutput("out", null, Map.of("nums", many)))));

        Object expanded = resolver.resolveValue("$(@cart.nums(*).n)", ctx);
        assertTrue(expanded instanceof List, "wildcard resolveValue unwraps to a List");
        assertEquals(ExpressionResolver.MAX_WILDCARD_VALUES, ((List<?>) expanded).size());
        // Last kept index is 511; 512+ were truncated so == 551 is false.
        assertTrue(resolver.evaluateCondition("$(@cart.nums(*).n) == 0", ctx));
        assertTrue(resolver.evaluateCondition(
                "$(@cart.nums(*).n) == " + (ExpressionResolver.MAX_WILDCARD_VALUES - 1), ctx));
        assertFalse(resolver.evaluateCondition(
                "$(@cart.nums(*).n) == " + ExpressionResolver.MAX_WILDCARD_VALUES, ctx));
    }

    @Test
    void evaluateCondition_wildcard_withAndOr() {
        assertTrue(resolver.evaluateCondition(
                "$(@cart.items(*).name) == 'widget' && $(wf.maxRetry) == 3", ctx));
        assertTrue(resolver.evaluateCondition(
                "$(@cart.items(*).name) == 'nope' || $(@cart.items(*).name) == 'a'", ctx));
        assertFalse(resolver.evaluateCondition(
                "$(@cart.items(*).name) == 'nope' && $(wf.maxRetry) == 3", ctx));
        assertTrue(resolver.evaluateCondition("@cart.items(*).name == 'widget'", ctx));
    }

    /**
     * Live shape: {@code @ai_agent.main_output.final_answer}. Sealed value is the sandbox map
     * {@code {final_answer: <string>}} after {@code finalizeOutputs} fan-out
     * (portValue=main_output, portId = portValue or UUID).
     */
    @Test
    void evaluateCondition_aiAgent_mainOutput_finalAnswer_liveShape_bothPortIdVariants() {
        NodeDto agent = nodeWithShortCode("882fdb05-4c1a-4e2b-9f3a-7b8c9d0e1f2a", "ai_agent");
        ctx.setNodes(List.of(cartNode, agent));
        Map<String, Object> sealed = Map.of("final_answer", "hello");

        // Variant A: portId equals portValue (selector hits the portId loop).
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of(agent.getId(),
                        List.of(portOutput("main_output", "main_output", sealed))));
        assertEquals("hello", resolver.resolveValue("@ai_agent.main_output.final_answer", ctx));
        assertEquals("hello", resolver.resolveValue("$(@ai_agent.main_output.final_answer)", ctx));
        assertTrue(resolver.evaluateCondition("@ai_agent.main_output.final_answer == \"hello\"", ctx));
        assertFalse(resolver.evaluateCondition("@ai_agent.main_output.final_answer == \" \"", ctx));

        // Variant B: UUID portId — selector must match portValue, not portId.
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of(agent.getId(),
                        List.of(portOutput("main_output", "c0ffee00-1111-2222-3333-444455556666", sealed))));
        assertEquals("hello", resolver.resolveValue("@ai_agent.main_output.final_answer", ctx));
        assertTrue(resolver.evaluateCondition("@ai_agent.main_output.final_answer == \"hello\"", ctx));
        assertFalse(resolver.evaluateCondition("@ai_agent.main_output.final_answer == \" \"", ctx));
    }

    @Test
    void evaluateCondition_aiAgent_mainOutput_finalAnswer_nonEmptyRealAnswer_spaceLiteralIsFalse() {
        NodeDto agent = nodeWithShortCode("882fdb05-4c1a-4e2b-9f3a-7b8c9d0e1f2a", "ai_agent");
        ctx.setNodes(List.of(cartNode, agent));
        String answer = "The capital of France is Paris.";
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of(agent.getId(),
                        List.of(portOutput("main_output", "c0ffee00-1111-2222-3333-444455556666",
                                Map.of("final_answer", answer)))));

        assertEquals(answer, resolver.resolveValue("@ai_agent.main_output.final_answer", ctx));
        assertTrue(resolver.evaluateCondition(
                "@ai_agent.main_output.final_answer == \"The capital of France is Paris.\"", ctx));
        assertFalse(resolver.evaluateCondition("@ai_agent.main_output.final_answer == \" \"", ctx));
    }
}
