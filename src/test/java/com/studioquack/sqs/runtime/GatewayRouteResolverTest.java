package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.SchemaItemDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.expression.ExpressionResolver;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/** Gateway route-token resolver — legacy scalar path plus typed-condition evaluation. */
@ExtendWith(MockitoExtension.class)
class GatewayRouteResolverTest {

    @Mock
    private ExecutionContextStore contextStore;

    private ExpressionResolver expressionResolver;
    private GatewayRouteResolver resolver;
    private ExecutionContext ctx;

    @BeforeEach
    void setUp() {
        expressionResolver = new ExpressionResolver(contextStore);
        resolver = new GatewayRouteResolver(expressionResolver);
        ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("wfe-1");
        ctx.setWorkflowVariables(Map.of("score", 10));
    }

    private NodeDto node(String id, String bpmnType, String nodeType) {
        TemplateDto t = new TemplateDto();
        t.setBpmnType(bpmnType);
        t.setNodeType(nodeType);
        NodeDto n = new NodeDto();
        n.setId(id);
        n.setData(t);
        return n;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto e = new EdgeDto();
        e.setId("e_" + source + "_" + target);
        e.setSource(source);
        e.setTarget(target);
        return e;
    }

    private EdgeDto conditionedEdge(String source, String target, String condition, Boolean isDefault) {
        EdgeDto e = edge(source, target);
        EdgeDataDto data = new EdgeDataDto();
        data.setCondition(condition);
        data.setIsDefaultFlow(isDefault);
        e.setData(data);
        return e;
    }

    /** Gateway node carrying a `condition` configuration parameter (definition-JSON lane). */
    private NodeDto gatewayWithConditionParam(String id, String condition) {
        NodeDto n = node(id, "exclusive.gateway", "gateway");
        ParameterDto p = new ParameterDto();
        p.setShortCode("condition");
        p.setValue(condition);
        n.getData().setConfigurationParameters(new java.util.ArrayList<>(List.of(p)));
        return n;
    }

    private InOutValueDto output(String nodeId, String portValue, Object value, String targetNodeId) {
        InOutValueDto o = new InOutValueDto(portValue, value);
        o.setNodeId(nodeId);
        o.setTargetNodeId(targetNodeId);
        return o;
    }

    /** Cart producer on ctx + two sealed ports (side first so first-output would be wrong). */
    private void stubCartTwoPorts() {
        NodeDto cart = node("node-cart", "custom.sandbox", "serviceTask");
        cart.getData().setNodeShortCode("cart");
        ctx.setNodes(List.of(cart));
        InOutValueDto side = new InOutValueDto("output_side", Map.of("total", 1));
        InOutValueDto main = new InOutValueDto("output_main", Map.of("total", 200));
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of("node-cart", List.of(side, main)));
    }

    @Test
    void resolves_route_token_from_output_addressed_to_gateway() {
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"));
        List<EdgeDto> edges = List.of(edge("scriptN", "gw"));
        List<InOutValueDto> outputs = List.of(output("scriptN", "decision", "APPROVE", "gw"));

        Map<String, Object> tokens = resolver.resolveRouteTokens(nodes, edges, "scriptN", outputs);

        assertEquals("APPROVE", tokens.get("gw"), "output addressed to the gateway drives its route token");
    }

    @Test
    void single_output_single_gateway_fallback() {
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"));
        List<EdgeDto> edges = List.of(edge("scriptN", "gw"));
        // targetNodeId NOT set (connection map not built) — the single-output fallback still resolves it.
        List<InOutValueDto> outputs = List.of(output("scriptN", "out", "B", null));

        Map<String, Object> tokens = resolver.resolveRouteTokens(nodes, edges, "scriptN", outputs);

        assertEquals("B", tokens.get("gw"), "single output feeding a single gateway is the route value");
    }

    @Test
    void no_gateway_downstream_yields_no_tokens() {
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("endN", "end.event", "end"));
        List<EdgeDto> edges = List.of(edge("scriptN", "endN"));
        List<InOutValueDto> outputs = List.of(output("scriptN", "out", "X", "endN"));

        Map<String, Object> tokens = resolver.resolveRouteTokens(nodes, edges, "scriptN", outputs);

        assertTrue(tokens.isEmpty(), "a node feeding no gateway produces no routing tokens");
    }

    @Test
    void missing_output_value_yields_no_token_for_that_gateway() {
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gwA", "exclusive.gateway", "gateway"),
                node("gwB", "inclusive.gateway", "gateway"));
        // Two gateways → the single-output fallback is ambiguous and must NOT guess.
        List<EdgeDto> edges = List.of(edge("scriptN", "gwA"), edge("scriptN", "gwB"));
        List<InOutValueDto> outputs = List.of(output("scriptN", "out", "only", null));

        Map<String, Object> tokens = resolver.resolveRouteTokens(nodes, edges, "scriptN", outputs);

        assertNull(tokens.get("gwA"), "ambiguous single output must not be guessed onto multiple gateways");
        assertNull(tokens.get("gwB"), "no token → the gateway takes its default flow or raises an incident");
    }

    @Test
    void typed_condition_first_true_wins() {
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchB", "custom.sandbox", "serviceTask"),
                node("branchC", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "$(wf.score) > 100", false),
                conditionedEdge("gw", "branchB", "$(wf.score) > 5", false),
                conditionedEdge("gw", "branchC", "true", false));

        Map<String, Object> tokens = resolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals("branchB", tokens.get("gw"), "first true typed condition's target is the route token");
        assertFalse(tokens.containsValue("branchC"), "later true edges must not win");
    }

    @Test
    void conditioned_isDefaultFlow_false_must_not_set_defaultTaken() {
        // Mystery M1 (c): a conditioned edge that also has isDefaultFlow=true and evaluates
        // false must NOT be selected as the default — defaultTaken stays false; token is
        // the dead-end sentinel so Activiti JUEL stays defined.
        RecordingDecisionRecorder recorder = new RecordingDecisionRecorder();
        GatewayRouteResolver recordingResolver =
                new GatewayRouteResolver(expressionResolver, providerOf(recorder));

        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "false", true));

        Map<String, Object> tokens = recordingResolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals(GatewayRouteResolver.DEAD_END_SENTINEL, tokens.get("gw"),
                "false conditioned+isDefaultFlow is a dead-end, not an omitted token");
        GatewayDecisionRecorder.GatewayDecision decision = recorder.latest("wfe-1", "gw");
        assertFalse(decision.defaultTaken(),
                "defaultTaken must stay false when the only isDefaultFlow edge also has a condition");
        assertTrue(decision.deadEnd());
        assertEquals(GatewayRouteResolver.DEAD_END_SENTINEL, decision.selectedTargetNodeId(),
                "a false conditioned edge must not be selected as defaultTaken");
    }

    @Test
    void typed_condition_default_when_all_false() {
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchDefault", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "false", false),
                conditionedEdge("gw", "branchDefault", null, true));

        Map<String, Object> tokens = resolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals("branchDefault", tokens.get("gw"), "default flow target when every typed condition is false");
    }

    @Test
    void typed_condition_no_token_when_all_false_and_no_default() {
        RecordingDecisionRecorder recorder = new RecordingDecisionRecorder();
        GatewayRouteResolver recordingResolver =
                new GatewayRouteResolver(expressionResolver, providerOf(recorder));

        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "false", false));

        Map<String, Object> tokens = recordingResolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals(GatewayRouteResolver.DEAD_END_SENTINEL, tokens.get("gw"),
                "typed path always sets a token; sentinel when every conditioned exit is false");
        GatewayDecisionRecorder.GatewayDecision decision = recorder.latest("wfe-1", "gw");
        assertTrue(decision.deadEnd());
        assertFalse(decision.defaultTaken());
        assertEquals(GatewayRouteResolver.DEAD_END_SENTINEL, decision.selectedTargetNodeId());
    }

    @Test
    void unconditioned_outgoing_without_isDefaultFlow_is_default_when_condition_false() {
        // User graph: producer → exclusive gw with empty/unconditioned exit (no isDefaultFlow)
        // plus a conditioned exit that evaluates false → intended default pass.
        RecordingDecisionRecorder recorder = new RecordingDecisionRecorder();
        GatewayRouteResolver recordingResolver =
                new GatewayRouteResolver(expressionResolver, providerOf(recorder));

        List<NodeDto> nodes = List.of(
                node("producer", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("emptyTarget", "custom.sandbox", "serviceTask"),
                node("condTarget", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("producer", "gw"),
                edge("gw", "emptyTarget"),
                conditionedEdge("gw", "condTarget",
                        "@ai_agent.main_output.final_answer == \" \"", false));

        Map<String, Object> tokens = recordingResolver.resolveRouteTokens(
                nodes, edges, "producer", List.of(), ctx);

        assertEquals("emptyTarget", tokens.get("gw"),
                "unconditioned outgoing is the default even without isDefaultFlow");
        GatewayDecisionRecorder.GatewayDecision decision = recorder.latest("wfe-1", "gw");
        assertEquals("emptyTarget", decision.selectedTargetNodeId());
        assertTrue(decision.defaultTaken());
        assertFalse(decision.deadEnd());
    }

    @Test
    void unconditioned_outgoing_without_isDefaultFlow_condition_true_wins() {
        // Same graph as unconditioned_outgoing_without_isDefaultFlow_is_default_when_condition_false,
        // with the authored condition evaluating true.
        NodeDto agent = node("node-ai", "custom.sandbox", "serviceTask");
        agent.getData().setNodeShortCode("ai_agent");
        ctx.setNodes(List.of(agent));
        InOutValueDto main = new InOutValueDto("main_output", Map.of("final_answer", " "));
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of("node-ai", List.of(main)));

        RecordingDecisionRecorder recorder = new RecordingDecisionRecorder();
        GatewayRouteResolver recordingResolver =
                new GatewayRouteResolver(expressionResolver, providerOf(recorder));

        List<NodeDto> nodes = List.of(
                node("producer", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("emptyTarget", "custom.sandbox", "serviceTask"),
                node("condTarget", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("producer", "gw"),
                edge("gw", "emptyTarget"),
                conditionedEdge("gw", "condTarget",
                        "@ai_agent.main_output.final_answer == \" \"", false));

        Map<String, Object> tokens = recordingResolver.resolveRouteTokens(
                nodes, edges, "producer", List.of(), ctx);

        assertEquals("condTarget", tokens.get("gw"),
                "true conditioned exit wins over the unconditioned default");
        GatewayDecisionRecorder.GatewayDecision decision = recorder.latest("wfe-1", "gw");
        assertEquals("condTarget", decision.selectedTargetNodeId());
        assertFalse(decision.defaultTaken());
        assertFalse(decision.deadEnd());
    }

    @Test
    void node_condition_param_applies_to_edge_without_typed_condition() {
        // Definition-JSON `condition` parameter on the gateway (ctx.score=10 → >5 true).
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                gatewayWithConditionParam("gw", "$(wf.score) > 5"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchB", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", null, false),
                conditionedEdge("gw", "branchB", "false", false));

        Map<String, Object> tokens = resolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals("branchA", tokens.get("gw"),
                "edge without a typed condition falls back to the node's condition parameter");
    }

    @Test
    void edge_condition_wins_over_node_condition_param() {
        // Node param would route to branchA (false here), but the edge's own condition wins.
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                gatewayWithConditionParam("gw", "false"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchB", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "false", false),
                conditionedEdge("gw", "branchB", "$(wf.score) > 5", false));

        Map<String, Object> tokens = resolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals("branchB", tokens.get("gw"), "typed edge condition takes precedence over the node param");
    }

    @Test
    void typed_condition_path_ignores_legacy_scalar_output() {
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "true", false));
        List<InOutValueDto> outputs = List.of(output("scriptN", "decision", "LEGACY", "gw"));

        Map<String, Object> tokens = resolver.resolveRouteTokens(
                nodes, edges, "scriptN", outputs, ctx);

        assertEquals("branchA", tokens.get("gw"), "typed path must not be overwritten by scalar output");
    }

    @Test
    void typed_condition_portAwareNodeRef_selectsCorrectPort() {
        stubCartTwoPorts();
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchB", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "$(@cart.output_main.total) > 100", false),
                conditionedEdge("gw", "branchB", null, true));

        Map<String, Object> tokens = resolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals("branchA", tokens.get("gw"),
                "port-aware output_main.total=200; first-output total=1 would not match");
    }

    @Test
    void typed_condition_bareAtAndWorkflowGlobals() {
        stubCartTwoPorts();

        // (a) bare @port path — first-output total=1 would not satisfy > 100
        List<NodeDto> nodesA = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchB", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edgesA = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "@cart.output_main.total > 100", false),
                conditionedEdge("gw", "branchB", "false", false));
        Map<String, Object> tokensA = resolver.resolveRouteTokens(
                nodesA, edgesA, "scriptN", List.of(), ctx);
        assertEquals("branchA", tokensA.get("gw"), "bare @cart.output_main.total > 100");

        // (b) bare Workflow_Globals (ctx.score=10)
        List<NodeDto> nodesB = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchScore", "custom.sandbox", "serviceTask"),
                node("branchMiss", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edgesB = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchScore", "Workflow_Globals.score > 5", false),
                conditionedEdge("gw", "branchMiss", "false", false));
        Map<String, Object> tokensB = resolver.resolveRouteTokens(
                nodesB, edgesB, "scriptN", List.of(), ctx);
        assertEquals("branchScore", tokensB.get("gw"), "bare Workflow_Globals.score > 5");

        // missing node → false, default flow, no throw
        List<NodeDto> nodesC = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchGhost", "custom.sandbox", "serviceTask"),
                node("branchDefault", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edgesC = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchGhost", "@ghost.port.x == 1", false),
                conditionedEdge("gw", "branchDefault", null, true));
        Map<String, Object> tokensC = assertDoesNotThrow(
                () -> resolver.resolveRouteTokens(nodesC, edgesC, "scriptN", List.of(), ctx));
        assertEquals("branchDefault", tokensC.get("gw"),
                "missing @ghost is false; default-flow target is used");
    }

    @Test
    void typed_condition_wildcardAny_andParenIndex() {
        NodeDto cart = node("node-cart", "custom.sandbox", "serviceTask");
        cart.getData().setNodeShortCode("cart");
        ctx.setNodes(List.of(cart));
        InOutValueDto out = new InOutValueDto("out", Map.of(
                "orders_list", List.of(
                        Map.of("price", 10),
                        Map.of("price", 80)),
                "items", List.of(
                        Map.of("name", "a"),
                        Map.of("name", "widget"))));
        when(contextStore.collectOutputsByNode("wfe-1"))
                .thenReturn(Map.of("node-cart", List.of(out)));

        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchB", "custom.sandbox", "serviceTask"));

        Map<String, Object> any = resolver.resolveRouteTokens(nodes, List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "@cart.orders_list(*).price > 50", false),
                conditionedEdge("gw", "branchB", null, true)),
                "scriptN", List.of(), ctx);
        assertEquals("branchA", any.get("gw"), "any order price > 50");

        Map<String, Object> none = resolver.resolveRouteTokens(nodes, List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "@cart.orders_list(*).price > 500", false),
                conditionedEdge("gw", "branchB", null, true)),
                "scriptN", List.of(), ctx);
        assertEquals("branchB", none.get("gw"), "no price > 500 → default");

        Map<String, Object> idx = resolver.resolveRouteTokens(nodes, List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "$(@cart.items(1).name) == 'widget'", false),
                conditionedEdge("gw", "branchB", null, true)),
                "scriptN", List.of(), ctx);
        assertEquals("branchA", idx.get("gw"), "(1) index matches [1]");
    }

    @Test
    void schemaItems_only_condition_selects_target_not_scalar() {
        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchDefault", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                schemaConditionedEdge("gw", "branchA", "$(wf.score) > 5"),
                conditionedEdge("gw", "branchDefault", null, true));
        List<InOutValueDto> outputs = List.of(output("scriptN", "decision", "LEGACY", "gw"));

        Map<String, Object> tokens = resolver.resolveRouteTokens(
                nodes, edges, "scriptN", outputs, ctx);

        assertEquals("branchA", tokens.get("gw"),
                "schemaItems-only condition must take the typed path, not scalar/default");
    }

    @Test
    void blank_typed_condition_does_not_fall_through_to_schemaItems() {
        EdgeDto blocked = edge("gw", "branchA");
        EdgeDataDto data = new EdgeDataDto();
        data.setCondition("");
        SchemaItemDto row = new SchemaItemDto();
        row.setShortCode("condition");
        row.setValue("$(wf.score) > 5");
        data.setSchemaItems(List.of(row));
        blocked.setData(data);

        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                blocked);

        Map<String, Object> tokens = resolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertNull(tokens.get("gw"),
                "blank typed condition must not fall through to schemaItems");
    }

    @Test
    void records_gateway_decision_for_each_branch() {
        RecordingDecisionRecorder recorder = new RecordingDecisionRecorder();
        GatewayRouteResolver recordingResolver =
                new GatewayRouteResolver(expressionResolver, providerOf(recorder));

        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                labeledGateway("gw", "Choice"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchB", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "$(wf.score) > 100", false),
                conditionedEdge("gw", "branchB", "$(wf.score) > 5", false));

        Map<String, Object> tokens = recordingResolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals("branchB", tokens.get("gw"));
        GatewayDecisionRecorder.GatewayDecision decision = recorder.latest("wfe-1", "gw");
        assertEquals("Choice", decision.gatewayName());
        assertEquals("branchB", decision.selectedTargetNodeId());
        assertFalse(decision.defaultTaken());
        assertFalse(decision.deadEnd());
        assertEquals(2, decision.evaluations().size());
        assertEquals("$(wf.score) > 100", decision.evaluations().get(0).conditionExpression());
        assertFalse(decision.evaluations().get(0).result());
        assertNull(decision.evaluations().get(0).error());
        assertEquals("$(wf.score) > 5", decision.evaluations().get(1).conditionExpression());
        assertTrue(decision.evaluations().get(1).result());
        assertNull(decision.evaluations().get(1).error());
    }

    @Test
    void leftover_feel_only_stays_on_scalar_path() {
        // FEEL transform row is generation-only; runtime must still publish the scalar output token
        // Activiti JUEL compares (not evaluate the FEEL as).
        NodeDto gw = node("gw", "exclusive.gateway", "gateway");
        EdgeTransformDto feel = new EdgeTransformDto();
        feel.setTargetNodeId("branchA");
        feel.setSourcePath("conditionExpression");
        feel.setSourcePathValue("= route__gw = 'APPROVE'");
        gw.setEdgeTransformListForCurrentNode(List.of(feel));

        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                gw,
                node("branchA", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                edge("gw", "branchA"));
        List<InOutValueDto> outputs = List.of(output("scriptN", "decision", "APPROVE", "gw"));

        Map<String, Object> tokens = resolver.resolveRouteTokens(nodes, edges, "scriptN", outputs, ctx);

        assertEquals("APPROVE", tokens.get("gw"),
                "leftover FEEL must not steal the typed path; scalar output remains the token");
    }

    @Test
    void parse_typo_marks_branch_false_and_takes_default() {
        RecordingDecisionRecorder recorder = new RecordingDecisionRecorder();
        GatewayRouteResolver recordingResolver =
                new GatewayRouteResolver(expressionResolver, providerOf(recorder));

        List<NodeDto> nodes = List.of(
                node("scriptN", "custom.sandbox", "serviceTask"),
                node("gw", "exclusive.gateway", "gateway"),
                node("branchA", "custom.sandbox", "serviceTask"),
                node("branchDefault", "custom.sandbox", "serviceTask"));
        List<EdgeDto> edges = List.of(
                edge("scriptN", "gw"),
                conditionedEdge("gw", "branchA", "'a' =! \"\"", false),
                conditionedEdge("gw", "branchDefault", null, true));

        Map<String, Object> tokens = recordingResolver.resolveRouteTokens(
                nodes, edges, "scriptN", List.of(), ctx);

        assertEquals("branchDefault", tokens.get("gw"), "parse failure is no-match; default is taken");
        GatewayDecisionRecorder.GatewayDecision decision = recorder.latest("wfe-1", "gw");
        assertTrue(decision.defaultTaken());
        assertFalse(decision.deadEnd());
        assertEquals("branchDefault", decision.selectedTargetNodeId());
        GatewayDecisionRecorder.BranchEvaluation bad = decision.evaluations().get(0);
        assertEquals("branchA", bad.targetNodeId());
        assertFalse(bad.result());
        assertEquals("parse", bad.error());
    }

    private EdgeDto schemaConditionedEdge(String source, String target, String expression) {
        EdgeDto e = edge(source, target);
        EdgeDataDto data = new EdgeDataDto();
        SchemaItemDto row = new SchemaItemDto();
        row.setShortCode("condition");
        row.setValue(expression);
        data.setSchemaItems(List.of(row));
        e.setData(data);
        return e;
    }

    private NodeDto labeledGateway(String id, String label) {
        NodeDto n = node(id, "exclusive.gateway", "gateway");
        n.getData().setLabel(label);
        return n;
    }

    private static <T> ObjectProvider<T> providerOf(T bean) {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                return bean;
            }

            @Override
            public T getIfAvailable() {
                return bean;
            }

            @Override
            public T getIfUnique() {
                return bean;
            }

            @Override
            public Iterator<T> iterator() {
                return bean == null ? List.<T>of().iterator() : List.of(bean).iterator();
            }
        };
    }

    /** In-test recorder — InMemoryGatewayDecisionRecorder is not in this tree. */
    private static final class RecordingDecisionRecorder implements GatewayDecisionRecorder {
        private final Map<String, GatewayDecision> byKey = new HashMap<>();

        @Override
        public void record(GatewayDecision decision) {
            byKey.put(decision.workflowExecutionId() + "|" + decision.gatewayNodeId(), decision);
        }

        @Override
        public GatewayDecision latest(String workflowExecutionId, String gatewayNodeId) {
            return byKey.get(workflowExecutionId + "|" + gatewayNodeId);
        }
    }
}
