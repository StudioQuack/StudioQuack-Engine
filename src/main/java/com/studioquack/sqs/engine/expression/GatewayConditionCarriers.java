package com.studioquack.sqs.engine.expression;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.SchemaItemDto;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolves the authored exclusive/inclusive-gateway branch expression from the five
 * carriers the designer accepts. Mirrors FE {@code edgeHasGatewayCondition}
 * ({@code sq-frontend/src/lib/workflow/gatewayEdgeConditions.ts}) / REF-only:
 * the daemon evaluates the expression against the sealed context and compiles a
 * non-PHI {@code route__<gwId>} token — payload values never become engine variables.
 *
 * <p>Precedence (exact FE order, except {@code isDefaultFlow} no longer hides a
 * filled carrier — a default flow must not also carry a condition; when both are
 * set the condition wins):
 * <ol>
 * <li>typed {@code edge.data.condition} — wins when it is a {@link String}
 * (Jackson {@code null} = absent / fall through; {@code ""} or whitespace =
 * empty, <em>no</em> fallthrough);</li>
 * <li>edge {@code data.schemaItems} row {@code shortCode == "condition"} with a
 * non-blank value;</li>
 * <li>gateway node {@code configurationParameters} row {@code shortCode == "condition"}
 * (applies to edges without their own); empty string is absent;</li>
 * <li>legacy: edge {@code schemaItems} {@code shortCode == "conditionExpression"},
 * then the gateway-node {@code edgeTransformList} FEEL row
 * ({@code sourcePath=conditionExpression} matching {@code targetNodeId}).</li>
 * </ol>
 * {@code data.isDefaultFlow == true} is a default <em>marker</em>, not a carrier:
 * it yields no expression only when every carrier above is empty.
 */
public final class GatewayConditionCarriers {

    private GatewayConditionCarriers() {
    }

    /**
     * Trimmed authored expression for this outgoing edge, or {@code null} when the
     * edge is the default branch / has no filled carrier.
     */
    public static String authoredExpression(EdgeDto edge, NodeDto gatewayNode) {
        if (edge == null) {
            return nodeConditionParam(gatewayNode);
        }
        EdgeDataDto data = edge.getData();

        // 1. Typed data.condition: present String (incl. blank) wins — no fallthrough.
        if (data != null && data.getCondition() != null) {
            return StringUtils.trimToNull(data.getCondition());
        }

        // 2. Edge port-schema `condition` row (DuckTape schemaItems).
        // isDefaultFlow must not hide this — CONDITION wins when both are set.
        String schemaCondition = schemaRowValue(data != null ? data.getSchemaItems() : null, "condition");
        if (schemaCondition != null) {
            return schemaCondition;
        }

        // 3. Gateway node `condition` configuration parameter.
        String nodeParam = nodeConditionParam(gatewayNode);
        if (nodeParam != null) {
            return nodeParam;
        }

        // 4a. Legacy edge schemaItems `conditionExpression`.
        String legacySchema = schemaRowValue(data != null ? data.getSchemaItems() : null, "conditionExpression");
        if (legacySchema != null) {
            return legacySchema;
        }

        // 4b. Legacy gateway-node FEEL transform row (sourcePath=conditionExpression).
        return legacyTransformRow(gatewayNode, edge.getTarget());
    }

    /** True when {@link #authoredExpression} is non-blank. */
    public static boolean hasAuthoredExpression(EdgeDto edge, NodeDto gatewayNode) {
        return StringUtils.isNotBlank(authoredExpression(edge, gatewayNode));
    }

    /**
 * expression for runtime evaluation, or {@code null} when the authored
 * carrier is leftover FEEL/JUEL (those stay on the scalar route-token path).
 */
    public static String t42Expression(EdgeDto edge, NodeDto gatewayNode) {
        String authored = authoredExpression(edge, gatewayNode);
        if (authored == null || leftoverFeelOrJuel(authored)) {
            return null;
        }
        return authored;
    }

    /**
 * True when any outgoing edge has a authored expression, or the gateway
 * node's {@code condition} parameter is a non-blank expression. Leftover
 * FEEL/JUEL does <em>not</em> take the typed path (Activiti still compares the
 * scalar {@code route__} token via {@code FeelToJuelTranslator}).
 */
    public static boolean hasAnyT42Expression(List<EdgeDto> outgoing, NodeDto gatewayNode) {
        if (outgoing != null) {
            for (EdgeDto edge : outgoing) {
                if (StringUtils.isNotBlank(t42Expression(edge, gatewayNode))) {
                    return true;
                }
            }
        }
        String nodeParam = nodeConditionParam(gatewayNode);
        return StringUtils.isNotBlank(nodeParam) && !leftoverFeelOrJuel(nodeParam);
    }

    /**
 * Leftover FEEL ({@code = …}) or already-JUEL ({@code ${…}}) that generation
 * must still run through {@code FeelToJuelTranslator}. expressions
 * ({@code $(…)}, {@code @…}, bare globals) do not match.
 */
    public static boolean leftoverFeelOrJuel(String expression) {
        if (expression == null) {
            return false;
        }
        String trimmed = expression.trim();
        return trimmed.startsWith("=") || trimmed.startsWith("${");
    }

    /**
     * First schema row with {@code shortCode} and a non-blank value.
     * {@code SchemaItemDto.value} is {@link Object} — stringify like FE
     * {@code String(value).trim()} / {@link String#valueOf(Object)}.
     */
    static String schemaRowValue(List<SchemaItemDto> rows, String shortCode) {
        if (rows == null || shortCode == null) {
            return null;
        }
        for (SchemaItemDto item : rows) {
            if (item == null || !shortCode.equals(item.getShortCode())) {
                continue;
            }
            Object value = item.getValue();
            if (value == null) {
                continue;
            }
            String text = String.valueOf(value).trim();
            if (!text.isEmpty()) {
                return text;
            }
        }
        return null;
    }

    /** Node {@code condition} parameter; empty string is absent. */
    static String nodeConditionParam(NodeDto gatewayNode) {
        if (gatewayNode == null || gatewayNode.getData() == null) {
            return null;
        }
        return StringUtils.trimToNull(gatewayNode.getData().findParameterValueByShortCode("condition"));
    }

    /** Legacy FEEL row on the gateway node matching {@code targetNodeId}. */
    static String legacyTransformRow(NodeDto gatewayNode, String targetNodeId) {
        if (gatewayNode == null) {
            return null;
        }
        return Optional.ofNullable(gatewayNode.getEdgeTransformListForCurrentNode())
                .orElse(Collections.emptyList())
                .stream()
                .filter(t -> Objects.equals(t.getTargetNodeId(), targetNodeId)
                        && "conditionExpression".equals(t.getSourcePath()))
                .map(EdgeTransformDto::getSourcePathValue)
                .filter(StringUtils::isNotBlank)
                .map(String::trim)
                .findFirst()
                .orElse(null);
    }
}
