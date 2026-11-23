package com.studioquack.sqs.engine.expression;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.SchemaItemDto;
import com.studioquack.common.dto.flow.TemplateDto;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Precedence of FE {@code edgeHasGatewayCondition} carriers on the daemon helper.
 */
class GatewayConditionCarriersTest {

    @Test
    void typed_blank_does_not_fall_through_to_schemaItems() {
        EdgeDto edge = edgeWithSchemaCondition("$(wf.score) > 5");
        edge.getData().setCondition("");

        assertNull(GatewayConditionCarriers.authoredExpression(edge, gatewayWithParam("$(wf.score) > 99")),
                "blank typed condition is empty — no fallthrough");
        assertFalse(GatewayConditionCarriers.hasAuthoredExpression(edge, null));
    }

    @Test
    void typed_whitespace_does_not_fall_through() {
        EdgeDto edge = edgeWithSchemaCondition("$(wf.score) > 5");
        edge.getData().setCondition("   ");

        assertNull(GatewayConditionCarriers.authoredExpression(edge, null),
                "whitespace typed condition is empty — no fallthrough");
    }

    @Test
    void typed_null_falls_through_to_schemaItems() {
        EdgeDto edge = edgeWithSchemaCondition("$(wf.score) > 5");
        edge.getData().setCondition(null);

        assertEquals("$(wf.score) > 5",
                GatewayConditionCarriers.authoredExpression(edge, null));
        assertTrue(GatewayConditionCarriers.hasAuthoredExpression(edge, null));
    }

    @Test
    void typed_non_blank_wins_over_every_lower_carrier() {
        EdgeDto edge = edgeWithSchemaCondition("schema-lose");
        edge.getData().setCondition("$(wf.score) > 1");
        edge.getData().setIsDefaultFlow(true);

        SchemaItemDto legacy = new SchemaItemDto();
        legacy.setShortCode("conditionExpression");
        legacy.setValue("legacy-lose");
        edge.getData().getSchemaItems().add(legacy);

        assertEquals("$(wf.score) > 1",
                GatewayConditionCarriers.authoredExpression(edge, gatewayWithParam("param-lose")));
    }

    @Test
    void isDefaultFlow_yields_null() {
        // Pure default: no typed / schema / node-param carrier — only the flag.
        EdgeDto edge = bareEdge("gw1", "a1");
        EdgeDataDto data = new EdgeDataDto();
        data.setIsDefaultFlow(true);
        edge.setData(data);

        assertNull(GatewayConditionCarriers.authoredExpression(edge, gatewayWithParam(null)),
                "explicit default branch with no carrier has no expression");
    }

    @Test
    void isDefaultFlow_does_not_suppress_schemaItems_condition() {
        // Condition wins over isDefaultFlow: a schemaItems carrier on a flagged-default
        // edge is still an authored expression (false must not become defaultTaken).
        EdgeDto edge = edgeWithSchemaCondition("$(wf.score) > 5");
        edge.getData().setCondition(null);
        edge.getData().setIsDefaultFlow(true);

        assertEquals("$(wf.score) > 5",
                GatewayConditionCarriers.authoredExpression(edge, gatewayWithParam("$(wf.score) > 99")));
        assertTrue(GatewayConditionCarriers.hasAuthoredExpression(edge, null));
    }

    @Test
    void schemaItems_condition_is_used_when_typed_absent() {
        EdgeDto edge = edgeWithSchemaCondition("$(@cart.total) > 100");

        assertEquals("$(@cart.total) > 100",
                GatewayConditionCarriers.authoredExpression(edge, null));
    }

    @Test
    void node_condition_param_applies_when_edge_has_no_own_carrier() {
        EdgeDto edge = bareEdge("gw1", "a1");

        assertEquals("$(wf.score) > 5",
                GatewayConditionCarriers.authoredExpression(edge, gatewayWithParam("$(wf.score) > 5")));
    }

    @Test
    void empty_node_param_is_absent() {
        EdgeDto edge = bareEdge("gw1", "a1");

        assertNull(GatewayConditionCarriers.authoredExpression(edge, gatewayWithParam("")),
                "empty node condition parameter is absent");
        assertNull(GatewayConditionCarriers.authoredExpression(edge, gatewayWithParam("   ")));
    }

    @Test
    void legacy_schemaItems_conditionExpression() {
        EdgeDto edge = bareEdge("gw1", "a1");
        EdgeDataDto data = new EdgeDataDto();
        SchemaItemDto row = new SchemaItemDto();
        row.setShortCode("conditionExpression");
        row.setValue("= route__gw1 = 'approved'");
        data.setSchemaItems(new ArrayList<>(List.of(row)));
        edge.setData(data);

        assertEquals("= route__gw1 = 'approved'",
                GatewayConditionCarriers.authoredExpression(edge, null));
    }

    @Test
    void legacy_transform_row_is_last() {
        EdgeDto edge = bareEdge("gw1", "a1");
        NodeDto gw = gatewayWithParam(null);
        EdgeTransformDto row = new EdgeTransformDto();
        row.setTargetNodeId("a1");
        row.setSourcePath("conditionExpression");
        row.setSourcePathValue("= route__gw1 = 'approved'");
        gw.setEdgeTransformListForCurrentNode(List.of(row));

        assertEquals("= route__gw1 = 'approved'",
                GatewayConditionCarriers.authoredExpression(edge, gw));
    }

    @Test
    void schemaItems_condition_wins_over_legacy_carriers() {
        EdgeDto edge = edgeWithSchemaCondition("$(wf.score) > 5");
        SchemaItemDto legacy = new SchemaItemDto();
        legacy.setShortCode("conditionExpression");
        legacy.setValue("legacy-lose");
        edge.getData().getSchemaItems().add(legacy);

        NodeDto gw = gatewayWithParam("param-lose");
        EdgeTransformDto row = new EdgeTransformDto();
        row.setTargetNodeId("a1");
        row.setSourcePath("conditionExpression");
        row.setSourcePathValue("= transform-lose");
        gw.setEdgeTransformListForCurrentNode(List.of(row));

        assertEquals("$(wf.score) > 5",
                GatewayConditionCarriers.authoredExpression(edge, gw));
    }

    @Test
    void leftover_feel_or_juel_detection() {
        assertTrue(GatewayConditionCarriers.leftoverFeelOrJuel("= route__gw1 = 'approved'"));
        assertTrue(GatewayConditionCarriers.leftoverFeelOrJuel("${route__gw1 == 'approved'}"));
        assertFalse(GatewayConditionCarriers.leftoverFeelOrJuel("$(wf.score) > 5"));
        assertFalse(GatewayConditionCarriers.leftoverFeelOrJuel("@cart.total > 100"));
        assertFalse(GatewayConditionCarriers.leftoverFeelOrJuel(null));
    }

    @Test
    void leftover_feel_is_not_a_t42_expression() {
        EdgeDto edge = bareEdge("gw1", "a1");
        EdgeDataDto data = new EdgeDataDto();
        SchemaItemDto row = new SchemaItemDto();
        row.setShortCode("conditionExpression");
        row.setValue("= route__gw1 = 'approved'");
        data.setSchemaItems(new ArrayList<>(List.of(row)));
        edge.setData(data);

        assertEquals("= route__gw1 = 'approved'",
                GatewayConditionCarriers.authoredExpression(edge, null));
        assertNull(GatewayConditionCarriers.t42Expression(edge, null),
                "leftover FEEL stays off the typed evaluation path");
        assertFalse(GatewayConditionCarriers.hasAnyT42Expression(List.of(edge), null));
    }

    private static EdgeDto bareEdge(String source, String target) {
        EdgeDto e = new EdgeDto();
        e.setId("e_" + source + "_" + target);
        e.setSource(source);
        e.setTarget(target);
        return e;
    }

    private static EdgeDto edgeWithSchemaCondition(String value) {
        EdgeDto e = bareEdge("gw1", "a1");
        EdgeDataDto data = new EdgeDataDto();
        SchemaItemDto row = new SchemaItemDto();
        row.setShortCode("condition");
        row.setValue(value);
        data.setSchemaItems(new ArrayList<>(List.of(row)));
        e.setData(data);
        return e;
    }

    private static NodeDto gatewayWithParam(String condition) {
        TemplateDto t = new TemplateDto();
        t.setBpmnType("exclusive.gateway");
        if (condition != null) {
            ParameterDto p = new ParameterDto();
            p.setShortCode("condition");
            p.setValue(condition);
            t.setConfigurationParameters(new ArrayList<>(List.of(p)));
        }
        NodeDto n = new NodeDto();
        n.setId("gw1");
        n.setData(t);
        return n;
    }
}
