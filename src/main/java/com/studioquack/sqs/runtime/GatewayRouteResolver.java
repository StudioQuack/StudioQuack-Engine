package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.expression.ExpressionResolver;
import com.studioquack.sqs.engine.expression.GatewayConditionCarriers;
import com.studioquack.sqs.execution.context.ExecutionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Computes the execution-local gateway routing tokens ({@code route__<gatewayId>}) a completing node
 * must publish so the generated JUEL conditions ({@code ${route__<gatewayId> == 'value'}}) resolve
 * when the token reaches the gateway (item 5 / typed conditions).
 *
 * <p><b>Typed-condition path</b> (when any outgoing main-flow edge of a fed gateway has a
 * authored expression — see {@link GatewayConditionCarriers#hasAnyT42Expression} —
 * leftover FEEL/JUEL stays on the scalar path): evaluate those expressions in
 * edge-list order via {@link ExpressionResolver}; the token value is the first true edge's
 * <em>target nodeId</em>. If none are true, an unconditioned outgoing is the default
 * (prefer {@code isDefaultFlow==true}, else the first unconditioned in edge-list order).
 * A conditioned+isDefaultFlow edge that evaluated false is never selected. When every
 * exit is conditioned and all are false the token is {@link #DEAD_END_SENTINEL} so
 * Activiti's {@code ${route__… == 'target'}} comparisons stay defined (all false) and
 * take the generated default / synthetic error-end. The typed path always sets the token.
 *
 * <p><b>Legacy scalar path</b> (no typed conditions on that gateway — pure function of graph + outputs):
 * <ol>
 * <li>the node output explicitly addressed to a gateway ({@code output.targetNodeId == gatewayId})
 * provides that gateway's route value — the primary path once outputs are fanned out;</li>
 * <li>otherwise, for each main-flow edge {@code currentNode → gateway}, the output on the edge's
 * source port ({@code sourceHandle}) provides the value;</li>
 * <li>otherwise, when the node has exactly one output value and feeds exactly one gateway, that
 * single value is used.</li>
 * </ol>
 */
@Slf4j
@Component
public class GatewayRouteResolver {

    /**
     * Typed-path token when every outgoing is conditioned and all evaluated false.
     * Node-id-shaped, REF-only — never a payload. Makes every
     * {@code ${route__gw == 'target'}} false so Activiti takes the generated
     * default / synthetic {@code Event_SandboxErrorEnd} instead of throwing
     * {@code PropertyNotFoundException}.
     */
    public static final String DEAD_END_SENTINEL = "__dead_end__";

    /** Optional for unit tests that only exercise the legacy scalar path. */
    private final ExpressionResolver expressionResolver;

    /** Optional — absent in unit tests that construct with the 1-arg ctor. */
    private final ObjectProvider<GatewayDecisionRecorder> decisionRecorder;

    /** Keeps existing unit tests compiling: {@code new GatewayRouteResolver(expressionResolver)}. */
    public GatewayRouteResolver(ExpressionResolver expressionResolver) {
        this(expressionResolver, null);
    }

    @Autowired
    public GatewayRouteResolver(ExpressionResolver expressionResolver,
                                ObjectProvider<GatewayDecisionRecorder> decisionRecorder) {
        this.expressionResolver = expressionResolver;
        this.decisionRecorder = decisionRecorder;
    }

    /**
     * Backward-compatible overload — typed-condition evaluation sees a null context (literals only).
     */
    public Map<String, Object> resolveRouteTokens(List<NodeDto> nodes, List<EdgeDto> edges,
                                                  String currentNodeId, List<InOutValueDto> outputs) {
        return resolveRouteTokens(nodes, edges, currentNodeId, outputs, null);
    }

    /**
     * @param nodes         the workflow nodes (pre-clear snapshot)
     * @param edges         the workflow edges (pre-clear snapshot)
     * @param currentNodeId the node whose outputs may drive downstream gateways
     * @param outputs       the node's fanned-out outputs (post {@code finalizeOutputs}); may be empty
     *                      when a typed-condition gateway is evaluated from sealed context alone
     * @param ctx           sealed execution context for typed {@code $(…)} condition evaluation; nullable
     * @return {@code gatewayNodeId -> route scalar}; empty when the node feeds no gateway
     */
    public Map<String, Object> resolveRouteTokens(List<NodeDto> nodes, List<EdgeDto> edges,
                                                  String currentNodeId, List<InOutValueDto> outputs,
                                                  ExecutionContext ctx) {
        Map<String, Object> tokens = new LinkedHashMap<>();
        if (StringUtils.isBlank(currentNodeId)) {
            return tokens;
        }
        Set<String> gatewayIds = gatewayNodeIds(nodes);
        if (gatewayIds.isEmpty()) {
            return tokens;
        }
        List<EdgeDto> edgeList = edges == null ? List.of() : edges;

        // Gateways this node feeds via a main-flow edge — typed conditions take precedence per gateway.
        Set<String> typedGateways = new java.util.HashSet<>();
        List<EdgeDto> toGateways = edgeList.stream()
                .filter(e -> currentNodeId.equals(e.getSource()) && gatewayIds.contains(e.getTarget()))
                .filter(this::isMainFlowEdge)
                .collect(Collectors.toList());
        for (EdgeDto inbound : toGateways) {
            String gatewayId = inbound.getTarget();
            if (typedGateways.contains(gatewayId)) {
                continue;
            }
            List<EdgeDto> outgoing = outgoingMainFlowEdges(edgeList, gatewayId);
            NodeDto gatewayNode = findNode(nodes, gatewayId);
            if (!GatewayConditionCarriers.hasAnyT42Expression(outgoing, gatewayNode)) {
                continue;
            }
            typedGateways.add(gatewayId);
            Object token = resolveTypedConditionToken(outgoing, ctx, gatewayNode);
            tokens.put(gatewayId, token);
        }

        // Legacy scalar resolution for gateways without typed outgoing conditions.
        if (outputs == null || outputs.isEmpty()) {
            if (!tokens.isEmpty()) {
                log.debug("gateway.route.tokens node={} tokens={}", currentNodeId, tokens.keySet());
            }
            return tokens;
        }

        // 1) Outputs explicitly addressed to a gateway (targetNodeId == gatewayId).
        for (InOutValueDto out : outputs) {
            String target = out.getTargetNodeId();
            if (target != null && gatewayIds.contains(target) && out.getValue() != null
                    && !typedGateways.contains(target)) {
                tokens.put(target, out.getValue());
            }
        }

        // 2) For each main-flow edge currentNode -> gateway not yet resolved, match by source port.
        for (EdgeDto edge : toGateways) {
            String gatewayId = edge.getTarget();
            if (typedGateways.contains(gatewayId) || tokens.containsKey(gatewayId)) {
                continue;
            }
            Object value = valueForPort(outputs, portValue(edge.getSourceHandle()));
            if (value == null && toGateways.size() == 1 && singleOutputValue(outputs) != null) {
                value = singleOutputValue(outputs); // 3) single-output → single-gateway fallback
            }
            if (value != null) {
                tokens.put(gatewayId, value);
            }
        }

        if (!tokens.isEmpty()) {
            log.debug("gateway.route.tokens node={} tokens={}", currentNodeId, tokens.keySet());
        }
        return tokens;
    }

    /**
     * First true authored condition → target nodeId; else an unconditioned default
     * (explicit {@code isDefaultFlow}, else first unconditioned outgoing); else
     * {@link #DEAD_END_SENTINEL}. Never returns null — the typed path always sets
     * the token. A conditioned+{@code isDefaultFlow} edge that evaluated false is
     * never selected. Records a {@link GatewayDecisionRecorder.GatewayDecision}.
     */
    private Object resolveTypedConditionToken(List<EdgeDto> outgoing, ExecutionContext ctx,
                                              NodeDto gatewayNode) {
        List<GatewayDecisionRecorder.BranchEvaluation> evaluations = new ArrayList<>();
        String selected = null;
        if (expressionResolver == null) {
            log.warn("gateway.route.typed-condition skipped: ExpressionResolver not available");
            for (EdgeDto edge : outgoing) {
                String expr = GatewayConditionCarriers.t42Expression(edge, gatewayNode);
                evaluations.add(new GatewayDecisionRecorder.BranchEvaluation(
                        edge.getTarget(), expr, false, null));
            }
        } else {
            for (EdgeDto edge : outgoing) {
                String expr = GatewayConditionCarriers.t42Expression(edge, gatewayNode);
                boolean result = false;
                String error = null;
                if (expr != null) {
                    ExpressionResolver.ConditionOutcome outcome =
                            expressionResolver.evaluateConditionOutcome(expr, ctx);
                    result = outcome.result();
                    error = outcome.error();
                }
                evaluations.add(new GatewayDecisionRecorder.BranchEvaluation(
                        edge.getTarget(), expr, result, error));
                if (selected == null && result) {
                    selected = edge.getTarget();
                }
            }
        }
        boolean defaultTaken = false;
        boolean deadEnd = false;
        if (selected == null) {
            EdgeDto explicitDefault = null;
            EdgeDto firstUnconditioned = null;
            for (EdgeDto edge : outgoing) {
                // Unconditioned outgoing is default. Conditioned+isDefaultFlow that
                // evaluated false is never selected (condition wins).
                if (GatewayConditionCarriers.hasAuthoredExpression(edge, gatewayNode)) {
                    continue;
                }
                if (firstUnconditioned == null) {
                    firstUnconditioned = edge;
                }
                EdgeDataDto data = edge.getData();
                if (data != null && Boolean.TRUE.equals(data.getIsDefaultFlow())) {
                    explicitDefault = edge;
                    break;
                }
            }
            EdgeDto chosen = explicitDefault != null ? explicitDefault : firstUnconditioned;
            if (chosen != null) {
                selected = chosen.getTarget();
                defaultTaken = true;
            } else {
                selected = DEAD_END_SENTINEL;
                deadEnd = true;
                // No payload values — ids and counts only (REF-only / no secrets in logs).
                log.warn("gateway.route.dead-end gateway={} outgoing={} — no true condition and no unconditioned default",
                        gatewayNode != null ? gatewayNode.getId() : "?", outgoing.size());
            }
        }
        recordDecision(ctx, gatewayNode, outgoing, evaluations, selected, defaultTaken, deadEnd);
        return selected;
    }

    private static NodeDto findNode(List<NodeDto> nodes, String id) {
        if (nodes == null || id == null) {
            return null;
        }
        for (NodeDto n : nodes) {
            if (n != null && id.equals(n.getId())) {
                return n;
            }
        }
        return null;
    }

    private static String gatewayName(NodeDto gatewayNode, String fallbackId) {
        if (gatewayNode == null) {
            return fallbackId;
        }
        TemplateDto data = gatewayNode.getData();
        if (data != null && StringUtils.isNotBlank(data.getLabel())) {
            return data.getLabel();
        }
        if (data != null && StringUtils.isNotBlank(data.getName())) {
            return data.getName();
        }
        return StringUtils.defaultIfBlank(gatewayNode.getId(), fallbackId);
    }

    /** Never throws — a recorder failure must not affect routing. */
    private void recordDecision(ExecutionContext ctx, NodeDto gatewayNode, List<EdgeDto> outgoing,
                                List<GatewayDecisionRecorder.BranchEvaluation> evaluations,
                                String selectedTargetNodeId, boolean defaultTaken, boolean deadEnd) {
        try {
            if (decisionRecorder == null) {
                return;
            }
            GatewayDecisionRecorder recorder = decisionRecorder.getIfAvailable();
            if (recorder == null) {
                return;
            }
            String gatewayId = gatewayNode != null ? gatewayNode.getId()
                    : (outgoing.isEmpty() ? "" : outgoing.get(0).getSource());
            String wfeId = ctx != null && ctx.getWorkflowExecutionId() != null
                    ? ctx.getWorkflowExecutionId() : "";
            recorder.record(new GatewayDecisionRecorder.GatewayDecision(
                    wfeId,
                    gatewayId,
                    gatewayName(gatewayNode, gatewayId),
                    List.copyOf(evaluations),
                    selectedTargetNodeId,
                    defaultTaken,
                    deadEnd,
                    System.currentTimeMillis()));
        } catch (RuntimeException ex) {
            log.warn("gateway.route.decision-record failed: {}", ex.getMessage());
        }
    }

    /** Outgoing main-flow edges from {@code gatewayId}, preserving list order. */
    private List<EdgeDto> outgoingMainFlowEdges(List<EdgeDto> edges, String gatewayId) {
        return edges.stream()
                .filter(e -> gatewayId.equals(e.getSource()))
                .filter(this::isMainFlowEdge)
                .collect(Collectors.toList());
    }

    /**
     * Main-flow sequence edge: not ASSOCIATION and not a boundary-event wiring edge.
     * Absent/null {@code flowType} is treated as SEQUENCE.
     */
    private boolean isMainFlowEdge(EdgeDto edge) {
        if (edge == null) {
            return false;
        }
        EdgeDataDto data = edge.getData();
        if (data == null) {
            return true;
        }
        if (StringUtils.isNotBlank(data.getBoundaryEventId())) {
            return false;
        }
        String flowType = data.getFlowType();
        return flowType == null || !"ASSOCIATION".equalsIgnoreCase(flowType.trim());
    }

    /** All node ids whose effective type is an exclusive/inclusive gateway. */
    private Set<String> gatewayNodeIds(List<NodeDto> nodes) {
        if (nodes == null) {
            return Set.of();
        }
        return nodes.stream()
                .filter(n -> isGateway(n.getData()))
                .map(NodeDto::getId)
                .collect(Collectors.toSet());
    }

    private boolean isGateway(TemplateDto data) {
        if (data == null) {
            return false;
        }
        String bpmn = data.getBpmnType() == null ? "" : data.getBpmnType().toLowerCase(Locale.ROOT);
        if (bpmn.equals("exclusive.gateway") || bpmn.equals("inclusive.gateway")) {
            return true;
        }
        return "gateway".equalsIgnoreCase(data.getNodeType());
    }

    /** The value of the node output on {@code portValue} (matches portValue or portId), or null. */
    private Object valueForPort(List<InOutValueDto> outputs, String portValue) {
        if (portValue == null) {
            return null;
        }
        for (InOutValueDto out : outputs) {
            if (portValue.equals(out.getPortValue()) || portValue.equals(out.getPortId())) {
                return out.getValue();
            }
        }
        return null;
    }

    /** The single output's value, or null when there is not exactly one non-null output value. */
    private Object singleOutputValue(List<InOutValueDto> outputs) {
        Object found = null;
        for (InOutValueDto out : outputs) {
            if (out.getValue() == null) {
                continue;
            }
            if (found != null) {
                return null; // more than one candidate → ambiguous
            }
            found = out.getValue();
        }
        return found;
    }

    /** Handle wire format is {@code "{PORTTYPE}-{portValue}"}; return the portValue after the dash. */
    private String portValue(String handle) {
        if (StringUtils.isBlank(handle)) {
            return null;
        }
        int dash = handle.indexOf('-');
        return dash >= 0 && dash < handle.length() - 1 ? handle.substring(dash + 1).trim() : handle.trim();
    }
}
