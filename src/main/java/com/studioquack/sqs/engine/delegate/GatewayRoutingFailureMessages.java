package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder.GatewayDecision;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Builds the ENGINE-kind failure text when dispatch resume throws because a downstream
 * gateway had no matching branch (missing {@code route__} token / JUEL Unknown property).
 *
 * <p>Output is gateway id + a fixed non-PHI sentence only — never {@code e.getMessage()},
 * never expression values, never payload.
 */
final class GatewayRoutingFailureMessages {

    static final String CONDITION_MISS = "no outgoing flow matched — condition miss";
    static final String DEAD_END_SENTINEL = "__dead_end__";

    private static final String GATEWAY_PREFIX = "gateway ";
    private static final String SEP = " — ";

    private GatewayRoutingFailureMessages() {
    }

    /**
     * Routing-miss without calling {@code deadEnd()} (W1 may add that flag in parallel):
     * default was not taken and no real target was selected.
     */
    static boolean isRoutingMiss(GatewayDecision decision) {
        if (decision == null || decision.defaultTaken()) {
            return false;
        }
        String selected = decision.selectedTargetNodeId();
        return selected == null || DEAD_END_SENTINEL.equals(selected);
    }

    /**
     * @param genericPrefix {@code "dispatch error: "} or {@code "duckbot boundary error: "}
     */
    static String message(Exception e,
                          String workflowExecutionId,
                          String currentNodeId,
                          List<NodeDto> nodes,
                          List<EdgeDto> edges,
                          GatewayDecisionRecorder recorder,
                          String genericPrefix) {
        String gatewayId = resolveGatewayId(
                e, workflowExecutionId, currentNodeId, nodes, edges, recorder);
        if (gatewayId != null && !gatewayId.isBlank()) {
            return GATEWAY_PREFIX + gatewayId + SEP + CONDITION_MISS;
        }
        String prefix = genericPrefix == null || genericPrefix.isBlank()
                ? "dispatch error: " : genericPrefix;
        return prefix + (e == null ? "Exception" : e.getClass().getSimpleName());
    }

    /**
     * Prefer a recorded miss on a gateway this node feeds; else parse a {@code route__}
     * property name from an Unknown-property / PropertyNotFound exception (id only).
     */
    static String resolveGatewayId(Exception e,
                                   String workflowExecutionId,
                                   String currentNodeId,
                                   List<NodeDto> nodes,
                                   List<EdgeDto> edges,
                                   GatewayDecisionRecorder recorder) {
        List<String> fed = gatewaysFedBy(currentNodeId, nodes, edges);
        GatewayDecision miss = latestMiss(recorder, workflowExecutionId, fed);
        if (miss != null && miss.gatewayNodeId() != null && !miss.gatewayNodeId().isBlank()) {
            return miss.gatewayNodeId();
        }
        if (!isRouteTokenFailure(e)) {
            return null;
        }
        String parsed = parseRouteGatewayId(e);
        if (parsed == null || parsed.isBlank()) {
            return "unknown";
        }
        String mapped = mapSanitizedToNodeId(parsed, nodes, fed);
        if (recorder != null && workflowExecutionId != null) {
            GatewayDecision byMapped = latestQuiet(recorder, workflowExecutionId, mapped);
            if (isRoutingMiss(byMapped) && byMapped.gatewayNodeId() != null) {
                return byMapped.gatewayNodeId();
            }
        }
        return mapped;
    }

    /** Package-visible: outgoing exclusive/inclusive gateways from {@code nodeId}. */
    static List<String> gatewaysFedBy(String nodeId, List<NodeDto> nodes, List<EdgeDto> edges) {
        Set<String> gatewayIds = new LinkedHashSet<>();
        if (nodes != null) {
            for (NodeDto n : nodes) {
                if (isGateway(n) && n.getId() != null) {
                    gatewayIds.add(n.getId());
                }
            }
        }
        List<String> fed = new ArrayList<>();
        if (nodeId == null || edges == null) {
            return fed;
        }
        for (EdgeDto edge : edges) {
            if (edge == null || !nodeId.equals(edge.getSource())) {
                continue;
            }
            String target = edge.getTarget();
            if (target != null && gatewayIds.contains(target) && !fed.contains(target)) {
                fed.add(target);
            }
        }
        return fed;
    }

    private static GatewayDecision latestMiss(GatewayDecisionRecorder recorder,
                                              String workflowExecutionId,
                                              List<String> gatewayIds) {
        if (recorder == null || workflowExecutionId == null || gatewayIds == null) {
            return null;
        }
        for (String gatewayId : gatewayIds) {
            GatewayDecision decision = latestQuiet(recorder, workflowExecutionId, gatewayId);
            if (isRoutingMiss(decision)) {
                return decision;
            }
        }
        return null;
    }

    private static GatewayDecision latestQuiet(GatewayDecisionRecorder recorder,
                                               String workflowExecutionId,
                                               String gatewayId) {
        try {
            return recorder.latest(workflowExecutionId, gatewayId);
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    /**
     * True when the throwable chain is an Activiti/JUEL missing {@code route__} property.
     * Reads messages only to classify and extract an identifier — never to copy into output.
     */
    static boolean isRouteTokenFailure(Exception e) {
        String text = throwableText(e);
        if (text == null) {
            return false;
        }
        boolean route = text.contains(EngineVariableNames.ROUTE_PREFIX);
        String lower = text.toLowerCase(Locale.ROOT);
        boolean unknown = lower.contains("unknown property")
                || lower.contains("propertynotfound")
                || lower.contains("property or field");
        return route && unknown;
    }

    /**
     * Identifier after {@code route__} — {@code [A-Za-z0-9_]} only. Stops before
     * {@code ==}, quotes, or {@code }} so expression values never leave this method.
     */
    static String parseRouteGatewayId(Exception e) {
        String text = throwableText(e);
        if (text == null) {
            return null;
        }
        int idx = text.indexOf(EngineVariableNames.ROUTE_PREFIX);
        if (idx < 0) {
            return null;
        }
        int start = idx + EngineVariableNames.ROUTE_PREFIX.length();
        int end = start;
        while (end < text.length()) {
            char c = text.charAt(end);
            if (c == '_' || Character.isLetterOrDigit(c)) {
                end++;
            } else {
                break;
            }
        }
        return end == start ? null : text.substring(start, end);
    }

    /** Map a hyphen-stripped token suffix back to the authored node id when the graph is present. */
    private static String mapSanitizedToNodeId(String sanitized,
                                               List<NodeDto> nodes,
                                               List<String> preferredIds) {
        List<String> candidates = new ArrayList<>();
        if (preferredIds != null) {
            candidates.addAll(preferredIds);
        }
        if (nodes != null) {
            for (NodeDto n : nodes) {
                if (n != null && n.getId() != null && !candidates.contains(n.getId())) {
                    candidates.add(n.getId());
                }
            }
        }
        for (String id : candidates) {
            String token = EngineVariableNames.routeTokenVariableName(id);
            String suffix = token.startsWith(EngineVariableNames.ROUTE_PREFIX)
                    ? token.substring(EngineVariableNames.ROUTE_PREFIX.length())
                    : token;
            if (sanitized.equals(suffix) || sanitized.equals(id)) {
                return id;
            }
        }
        return sanitized;
    }

    private static boolean isGateway(NodeDto node) {
        if (node == null) {
            return false;
        }
        TemplateDto data = node.getData();
        if (data == null) {
            return false;
        }
        String bpmn = data.getBpmnType() == null ? "" : data.getBpmnType().toLowerCase(Locale.ROOT);
        if ("exclusive.gateway".equals(bpmn) || "inclusive.gateway".equals(bpmn)) {
            return true;
        }
        return "gateway".equalsIgnoreCase(data.getNodeType());
    }

    /** Class names + messages of the cause chain — classification only, never logged as-is. */
    private static String throwableText(Throwable e) {
        if (e == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        Set<Throwable> seen = new LinkedHashSet<>();
        for (Throwable t = e; t != null && seen.add(t); t = t.getCause()) {
            sb.append(t.getClass().getName()).append(' ');
            sb.append(t.getClass().getSimpleName()).append(' ');
            if (t.getMessage() != null) {
                sb.append(t.getMessage()).append(' ');
            }
        }
        return sb.toString();
    }
}
