package com.studioquack.sqs.execution.transform;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.FunctionPortTransformsDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.SchemaDto;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Stateless collector that builds the {@code TransformationSpecDto} list for the current
 * node's inbound edges + function-port request/response lanes. Ported verbatim from the
 * control plane {@code TransformationCollectorServiceImpl}: it does NOT apply JOLT, it only
 * <em>emits</em> the spec list the pipeline ships to the sandbox.
 *
 * <p>The consuming port's declared schema type is passed into
 * {@link JoltTransformService#convertJoltSpec(List, Map)} so the coercion lane respects the
 * port DTO's authoritative type over a stale {@code EdgeTransform.targetDataType}
 * (Mapping Invariant 13).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransformationCollector {

    /** BFS look-through bound — a runaway cycle or a huge fan-in must not walk the whole graph. */
    private static final int GATEWAY_PRODUCER_WALK_CAP = 64;

    private final JoltTransformService joltTransformService;

    /** Insertion-ordered producer of a look-through gateway. First inbound port wins. */
    private record UpstreamProducer(String nodeId, String portShortCode) {}

    public List<TransformationSpecDto> collectForCurrentNode(ExecutionContext ctx) {
        if (ctx == null) {
            return Collections.emptyList();
        }
        NodeDto currentNode = ctx.getCurrentNode();
        List<EdgeDto> edges = ctx.getEdges();
        if (currentNode == null || CollectionUtils.isEmpty(edges)) {
            return Collections.emptyList();
        }
        String currentNodeId = currentNode.getId();
        if (StringUtils.isEmpty(currentNodeId)) {
            return Collections.emptyList();
        }

        List<TransformationSpecDto> specs = new ArrayList<>();
        for (EdgeDto edge : edges) {
            if (edge == null || !currentNodeId.equals(edge.getTarget())) {
                continue;
            }
            EdgeDataDto edgeData = edge.getData();
            if (edgeData == null) {
                continue;
            }
            // Active language wins: non-jolt (incl. legacy script-only edges) omits jolt entirely.
            String effectiveLanguage = resolveEffectiveTransformLanguage(edgeData);
            if ("jolt".equalsIgnoreCase(effectiveLanguage)) {
                // Explicit jolt (or default/missing) — rebuild from edgeTransforms only.
                // Leftover scriptContentInput is retained by FE for tab switch but not emitted.
                String targetPortShortCode = extractShortCode(edge.getTargetHandle());
                Map<String, String> consumerPortTypes =
                        resolvePortSchemaTypes(currentNode, targetPortShortCode, false);
                collectJoltSpec(ctx, edge, edgeData, currentNodeId, specs, consumerPortTypes);
            } else {
                collectScriptSpec(ctx, edge, edgeData, currentNodeId, specs, effectiveLanguage);
            }
        }
        return specs;
    }

    /**
 * Resolves the edge's effective transform language for inbound collection.
 * <ol>
 * <li>Non-blank {@code transformLanguage} → trimmed value</li>
 * <li>Else non-empty legacy {@code scriptContentInput} →
 * {@code scriptLanguageInput} or {@code "javascript"}</li>
 * <li>Else {@code "jolt"} (identical to pre- default)</li>
 * </ol>
 */
    public static String resolveEffectiveTransformLanguage(EdgeDataDto edgeData) {
        if (StringUtils.isNotBlank(edgeData.getTransformLanguage())) {
            return edgeData.getTransformLanguage().trim();
        }
        if (StringUtils.isNotEmpty(edgeData.getScriptContentInput())) {
            return StringUtils.isNotBlank(edgeData.getScriptLanguageInput())
                    ? edgeData.getScriptLanguageInput().trim()
                    : "javascript";
        }
        return "jolt";
    }

    /**
     * The screen defines the message. Every inbound edge whose DuckTape screen carries at least one
     * data mapping row (Simple / jolt lane) is <em>authored</em>: only its rows flow, and a wire on
     * that edge that has no rows of its own contributes nothing — its raw payload must not pass
     * through beside the fields the screen mapped (an unmapped Duckbot `files` beside a pinned
     * tile's `file` surfaced as "Unknown parameters: input.files"). An edge with no rows at all
     * stays the legacy raw pass-through. Script-lane edges are owned by the transform hop and are
     * not listed.
     *
     * @return {@code producerNodeId|targetPortShortCode} for every wire the pre-bake may blank when
     *         no spec shaped it: the edge source, or each look-through producer of a gateway source
     */
    public Set<String> authoredWireKeys(ExecutionContext ctx) {
        Set<String> keys = new java.util.LinkedHashSet<>();
        if (ctx == null || ctx.getCurrentNode() == null || CollectionUtils.isEmpty(ctx.getEdges())) {
            return keys;
        }
        String currentNodeId = ctx.getCurrentNode().getId();
        for (EdgeDto edge : ctx.getEdges()) {
            if (edge == null || edge.getData() == null || !StringUtils.equals(currentNodeId, edge.getTarget())) {
                continue;
            }
            if (!"jolt".equalsIgnoreCase(resolveEffectiveTransformLanguage(edge.getData()))) {
                continue;
            }
            boolean gatewaySourced = isGateway(findNodeById(ctx, edge.getSource()));
            if (inboundDataRows(edge.getData(), currentNodeId, gatewaySourced).isEmpty()) {
                continue;
            }
            String targetPort = extractShortCode(edge.getTargetHandle());
            if (gatewaySourced) {
                for (UpstreamProducer p : resolveUpstreamProducers(ctx, edge.getSource())) {
                    keys.add(p.nodeId() + "|" + targetPort);
                }
            } else {
                keys.add(edge.getSource() + "|" + targetPort);
            }
        }
        return keys;
    }

    /**
     * The edge's data mapping rows that target this node. A row with no targetNodeId targets this
     * node by construction — it rides on an edge whose target IS the current node. ForeignEdgeInputResolver
     * already tolerates a blank targetNodeId when DELIVERING the pinned source's payload; rejecting
     * the same row here delivered the value but dropped its mapping, so a gateway-edge voices→files
     * row arrived as {voices:…} and was never renamed. A gateway's advisory OUTPUT rows
     * ({@code condition} / {@code conditionExpression}) select a branch — GatewayConditionCarriers
     * reads them. Shifting one onto the payload would publish the expression text as if the author
     * had mapped a field, so they are dropped from the data lane; scoped to the gateway source and
     * to sourcePath, since on an ordinary edge {@code condition} is just a producer field name.
     */
    private static List<EdgeTransformDto> inboundDataRows(EdgeDataDto edgeData, String currentNodeId,
                                                          boolean gatewaySourced) {
        if (CollectionUtils.isEmpty(edgeData.getEdgeTransforms())) {
            return List.of();
        }
        List<EdgeTransformDto> rows = edgeData.getEdgeTransforms().stream()
                .filter(Objects::nonNull)
                .filter(t -> StringUtils.isEmpty(t.getTargetNodeId())
                        || currentNodeId.equals(t.getTargetNodeId()))
                .collect(Collectors.toList());
        if (gatewaySourced) {
            rows.removeIf(t -> isRoutingCarrierPath(t.getSourcePath()));
        }
        return rows;
    }

    private void collectJoltSpec(ExecutionContext ctx, EdgeDto edge, EdgeDataDto edgeData, String currentNodeId,
                                 List<TransformationSpecDto> out, Map<String, String> targetPortSchemaTypes) {
        boolean gatewaySourced = isGateway(findNodeById(ctx, edge.getSource()));
        List<EdgeTransformDto> inboundTransforms = inboundDataRows(edgeData, currentNodeId, gatewaySourced);
        if (inboundTransforms.isEmpty()) {
            return;
        }
        if (gatewaySourced) {
            collectGatewayJoltSpecs(ctx, edge, inboundTransforms, currentNodeId, out, targetPortSchemaTypes);
        } else {
            collectNonGatewayJoltSpecs(edge, inboundTransforms, currentNodeId, out, targetPortSchemaTypes);
        }
    }

    /**
     * Existing grouping — byte-identical for non-gateway edges. Primary group is the wire source;
     * pinned C / $globals stay on their own sourceNodeId (null port = match by node alone).
     */
    private void collectNonGatewayJoltSpecs(EdgeDto edge, List<EdgeTransformDto> inboundTransforms,
                                            String currentNodeId, List<TransformationSpecDto> out,
                                            Map<String, String> targetPortSchemaTypes) {
        // Group rows by their OWN sourceNodeId. DuckTape "Add Node" pins earlier nodes as extra
        // source tiles on this edge, so a row's sourceNodeId can differ from the edge's source.
        // Each group becomes its own spec so the inbound pre-bake applies the group's shift to
        // the inputList entry of the node that actually produced the value — the primary wire's
        // entry and every pinned node's delivered entry each yield one target-shaped message on
        // the consuming port's list. Rows with no sourceNodeId (legacy) belong to the edge's
        // source group, keeping the pre-grouping wire shape byte-identical.
        Map<String, List<EdgeTransformDto>> rowsBySource = new LinkedHashMap<>();
        // Edge-source group first for stable ordering (primary wire precedes pinned tiles).
        rowsBySource.put(edge.getSource(), new ArrayList<>());
        for (EdgeTransformDto t : inboundTransforms) {
            String rowSource = StringUtils.isEmpty(t.getSourceNodeId()) ? edge.getSource() : t.getSourceNodeId();
            rowsBySource.computeIfAbsent(rowSource, k -> new ArrayList<>()).add(t);
        }
        for (Map.Entry<String, List<EdgeTransformDto>> group : rowsBySource.entrySet()) {
            if (group.getValue().isEmpty()) {
                continue;
            }
            boolean primary = Objects.equals(group.getKey(), edge.getSource());
            try {
                List<Object> joltSpec = joltTransformService.convertJoltSpec(group.getValue(), targetPortSchemaTypes);
                if (CollectionUtils.isEmpty(joltSpec)) {
                    continue;
                }
                out.add(TransformationSpecDto.builder()
                        .kind("jolt")
                        .joltSpec(joltSpec)
                        .sourceNodeId(group.getKey())
                        .targetNodeId(currentNodeId)
                        // Foreign groups match the delivered entry by node alone (null = any of
                        // that node's ports); the primary group keeps the edge's source handle.
                        .sourcePortShortCode(primary ? extractShortCode(edge.getSourceHandle()) : null)
                        .targetPortShortCode(extractShortCode(edge.getTargetHandle()))
                        .direction("input")
                        .build());
            } catch (Exception e) {
                log.warn("TransformationCollector: JOLT spec build failed for edge {}->{} (source={}): {}",
                        edge.getSource(), currentNodeId, group.getKey(), e.getMessage());
            }
        }
    }

    /**
     * Look-through: ConnectionBuilder fans producer payloads straight to the consumer
     * ({@code nodeId=A}, {@code targetNodeId=B}). After {@code clearNodeAndEdges} the matcher
     * has no graph, so a gateway-stamped spec cannot match A's entry — never emit the gateway id.
     */
    private void collectGatewayJoltSpecs(ExecutionContext ctx, EdgeDto edge,
                                         List<EdgeTransformDto> inboundTransforms, String currentNodeId,
                                         List<TransformationSpecDto> out,
                                         Map<String, String> targetPortSchemaTypes) {
        List<UpstreamProducer> producers = resolveUpstreamProducers(ctx, edge.getSource());
        Map<String, List<EdgeTransformDto>> rowsBySource = new LinkedHashMap<>();
        Map<String, String> producerPorts = new LinkedHashMap<>();
        // Seed producer groups first (stable BFS order; enables merge when a pin names the same predecessor).
        for (UpstreamProducer p : producers) {
            rowsBySource.put(p.nodeId(), new ArrayList<>());
            producerPorts.put(p.nodeId(), p.portShortCode());
        }
        for (EdgeTransformDto t : inboundTransforms) {
            String rowSource = StringUtils.isEmpty(t.getSourceNodeId()) ? null : t.getSourceNodeId();
            boolean primaryish = rowSource == null || rowSource.equals(edge.getSource());
            if (primaryish) {
                // Fan-in: each producer gets its own spec with the same look-through rows.
                // Empty producer list: drop (do not emit a gateway-id spec).
                for (UpstreamProducer p : producers) {
                    rowsBySource.get(p.nodeId()).add(t);
                }
            } else {
                rowsBySource.computeIfAbsent(rowSource, k -> new ArrayList<>()).add(t);
            }
        }
        for (Map.Entry<String, List<EdgeTransformDto>> group : rowsBySource.entrySet()) {
            if (group.getValue().isEmpty()) {
                continue;
            }
            String groupKey = group.getKey();
            // Never stamp a gateway — look-through left no inputList entry addressed to it.
            if (Objects.equals(groupKey, edge.getSource()) || isGateway(findNodeById(ctx, groupKey))) {
                continue;
            }
            try {
                List<Object> joltSpec = joltTransformService.convertJoltSpec(group.getValue(), targetPortSchemaTypes);
                if (CollectionUtils.isEmpty(joltSpec)) {
                    continue;
                }
                boolean resolvedProducer = producerPorts.containsKey(groupKey);
                out.add(TransformationSpecDto.builder()
                        .kind("jolt")
                        .joltSpec(joltSpec)
                        .sourceNodeId(groupKey)
                        .targetNodeId(currentNodeId)
                        .sourcePortShortCode(resolvedProducer ? producerPorts.get(groupKey) : null)
                        .targetPortShortCode(extractShortCode(edge.getTargetHandle()))
                        .direction("input")
                        .build());
            } catch (Exception e) {
                log.warn("TransformationCollector: JOLT spec build failed for edge {}->{} (source={}): {}",
                        edge.getSource(), currentNodeId, groupKey, e.getMessage());
            }
        }
    }

    private void collectScriptSpec(ExecutionContext ctx, EdgeDto edge, EdgeDataDto edgeData, String currentNodeId,
                                   List<TransformationSpecDto> out, String language) {
        String script = edgeData.getScriptContentInput();
        if (StringUtils.isEmpty(script)) {
            return;
        }
        // Same defect as JOLT: stamping the gateway id cannot match a look-through producer entry.
        // One script hop per edge, so a fan-in gateway binds the FIRST resolved producer rather
        // than emitting a spec per producer — those would all carry the same target port key and
        // collide when EdgeTransformPreStep merges the hop results back onto that one port.
        if (isGateway(findNodeById(ctx, edge.getSource()))) {
            List<UpstreamProducer> producers = resolveUpstreamProducers(ctx, edge.getSource());
            if (producers.isEmpty()) {
                return;
            }
            UpstreamProducer bound = producers.get(0);
            if (producers.size() > 1) {
                log.warn("TransformationCollector: gateway edge {}->{} has {} upstream producers; "
                                + "the script transform binds {}",
                        edge.getSource(), currentNodeId, producers.size(), bound.nodeId());
            }
            out.add(TransformationSpecDto.builder()
                    .kind("script")
                    .script(script)
                    .language(language)
                    .sourceNodeId(bound.nodeId())
                    .targetNodeId(currentNodeId)
                    .sourcePortShortCode(bound.portShortCode())
                    .targetPortShortCode(extractShortCode(edge.getTargetHandle()))
                    .direction("input")
                    .build());
            return;
        }
        out.add(TransformationSpecDto.builder()
                .kind("script")
                .script(script)
                .language(language)
                .sourceNodeId(edge.getSource())
                .targetNodeId(currentNodeId)
                .sourcePortShortCode(extractShortCode(edge.getSourceHandle()))
                .targetPortShortCode(extractShortCode(edge.getTargetHandle()))
                .direction("input")
                .build());
    }

    private String extractShortCode(String handle) {
        return (handle != null && handle.split("-").length == 2) ? handle.split("-")[1] : null;
    }

    /**
     * Private on this class on purpose: ConnectionBuilder, GatewayRouteResolver and
     * BpmnGenerationService each keep their own predicate — no shared helper.
     */
    private static boolean isGateway(NodeDto n) {
        if (n == null || n.getData() == null) {
            return false;
        }
        if ("gateway".equalsIgnoreCase(n.getData().getNodeType())) {
            return true;
        }
        String bpmnType = n.getData().getBpmnType();
        return bpmnType != null && bpmnType.toLowerCase(Locale.ROOT).contains("gateway");
    }

    /** A gateway's advisory routing row, which never carries payload. */
    private static boolean isRoutingCarrierPath(String path) {
        String trimmed = StringUtils.trimToEmpty(path);
        return "condition".equalsIgnoreCase(trimmed) || "conditionExpression".equalsIgnoreCase(trimmed);
    }

    /** Main-flow data lane only — skip FUNCTION / blank handles. */
    private static boolean isMainFlowIncoming(EdgeDto edge) {
        String sourceHandle = edge.getSourceHandle();
        String targetHandle = edge.getTargetHandle();
        if (StringUtils.isEmpty(sourceHandle) || StringUtils.isEmpty(targetHandle)) {
            return false;
        }
        return sourceHandle.startsWith("OUTPUT-") && targetHandle.startsWith("INPUT-");
    }

    /**
     * BFS look-through of incoming main-flow edges. Unique by {@code nodeId}, first port wins.
     * The start gateway is visited first so it can never appear as a producer.
     */
    private List<UpstreamProducer> resolveUpstreamProducers(ExecutionContext ctx, String gatewayId) {
        if (ctx == null || StringUtils.isEmpty(gatewayId)
                || CollectionUtils.isEmpty(ctx.getNodes())
                || CollectionUtils.isEmpty(ctx.getEdges())) {
            return List.of();
        }
        Set<String> visited = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        visited.add(gatewayId);
        queue.add(gatewayId);

        Map<String, UpstreamProducer> byId = new LinkedHashMap<>();
        List<EdgeDto> edges = ctx.getEdges();
        walk:
        while (!queue.isEmpty()) {
            String current = queue.poll();
            for (EdgeDto edge : edges) {
                if (edge == null || !StringUtils.equals(current, edge.getTarget())) {
                    continue;
                }
                if (!isMainFlowIncoming(edge)) {
                    continue;
                }
                String sourceId = edge.getSource();
                if (StringUtils.isEmpty(sourceId) || visited.contains(sourceId)) {
                    continue;
                }
                if (visited.size() >= GATEWAY_PRODUCER_WALK_CAP) {
                    break walk;
                }
                visited.add(sourceId);
                NodeDto sourceNode = findNodeById(ctx, sourceId);
                if (isGateway(sourceNode)) {
                    queue.add(sourceId);
                } else {
                    byId.putIfAbsent(sourceId,
                            new UpstreamProducer(sourceId, extractShortCode(edge.getSourceHandle())));
                }
            }
        }
        return new ArrayList<>(byId.values());
    }

    // ------------------------------------------------------------------
    // Function-port transforms — request lane (Source tab) + response lane
    // (Target tab). Single-owner: emitted only at the caller node's outbound
    // FUNCTION edges (Mapping Invariant 10).
    // ------------------------------------------------------------------

    public Map<String, FunctionPortTransformsDto> collectFunctionPortTransforms(ExecutionContext ctx) {
        if (ctx == null) {
            return Collections.emptyMap();
        }
        NodeDto currentNode = ctx.getCurrentNode();
        List<EdgeDto> edges = ctx.getEdges();
        if (currentNode == null || CollectionUtils.isEmpty(edges)) {
            return Collections.emptyMap();
        }
        String currentNodeId = currentNode.getId();
        if (StringUtils.isEmpty(currentNodeId)) {
            return Collections.emptyMap();
        }

        Map<String, FunctionPortTransformsDto> out = new LinkedHashMap<>();
        for (EdgeDto edge : edges) {
            if (edge == null || !currentNodeId.equals(edge.getSource())) {
                continue;
            }
            String sourceHandle = edge.getSourceHandle();
            if (StringUtils.isEmpty(sourceHandle) || !sourceHandle.startsWith("FUNCTION-")) {
                continue;
            }
            String portValue = extractShortCode(sourceHandle);
            if (StringUtils.isEmpty(portValue)) {
                continue;
            }
            EdgeDataDto edgeData = edge.getData();
            if (edgeData == null) {
                continue;
            }
            FunctionPortTransformsDto entry = out.computeIfAbsent(portValue, k -> FunctionPortTransformsDto.builder()
                    .request(new ArrayList<>())
                    .response(new ArrayList<>())
                    .build());

            NodeDto targetNode = findNodeById(ctx, edge.getTarget());
            String targetPortShortCode = extractShortCode(edge.getTargetHandle());
            Map<String, String> requestSchemaTypes = resolvePortSchemaTypes(targetNode, targetPortShortCode, false);
            Map<String, String> responseSchemaTypes = resolvePortSchemaTypes(currentNode, portValue, true);
            collectFunctionPortJoltSpecs(edge, edgeData, currentNodeId, portValue, entry,
                    requestSchemaTypes, responseSchemaTypes);
            collectFunctionPortScriptSpecs(edge, edgeData, currentNodeId, portValue, entry);
        }
        return out;
    }

    private void collectFunctionPortJoltSpecs(EdgeDto edge, EdgeDataDto edgeData, String currentNodeId,
                                              String portValue, FunctionPortTransformsDto entry,
                                              Map<String, String> requestSchemaTypes,
                                              Map<String, String> responseSchemaTypes) {
        if (CollectionUtils.isEmpty(edgeData.getEdgeTransforms())) {
            return;
        }
        List<EdgeTransformDto> requestTransforms = edgeData.getEdgeTransforms().stream()
                .filter(Objects::nonNull)
                .filter(t -> isRequestSchemaType(t.getTargetSchemaType()))
                .collect(Collectors.toList());
        List<EdgeTransformDto> responseTransforms = edgeData.getEdgeTransforms().stream()
                .filter(Objects::nonNull)
                .filter(t -> isResponseSchemaType(t.getTargetSchemaType()))
                .collect(Collectors.toList());

        if (!requestTransforms.isEmpty()) {
            try {
                List<Object> joltSpec = joltTransformService.convertJoltSpec(requestTransforms, requestSchemaTypes);
                if (CollectionUtils.isNotEmpty(joltSpec)) {
                    entry.getRequest().add(TransformationSpecDto.builder()
                            .kind("jolt").joltSpec(joltSpec)
                            .sourceNodeId(currentNodeId).targetNodeId(edge.getTarget())
                            .sourcePortShortCode(portValue)
                            .targetPortShortCode(extractShortCode(edge.getTargetHandle()))
                            .direction("input").build());
                }
            } catch (Exception e) {
                log.warn("FunctionPortCollector: request JOLT build failed for port {}: {}", portValue, e.getMessage());
            }
        }

        if (!responseTransforms.isEmpty()) {
            try {
                List<Object> joltSpec = joltTransformService.convertJoltSpec(responseTransforms, responseSchemaTypes);
                if (CollectionUtils.isNotEmpty(joltSpec)) {
                    entry.getResponse().add(TransformationSpecDto.builder()
                            .kind("jolt").joltSpec(joltSpec)
                            .sourceNodeId(edge.getTarget()).targetNodeId(currentNodeId)
                            .sourcePortShortCode(extractShortCode(edge.getTargetHandle()))
                            .targetPortShortCode(portValue)
                            .direction("output").build());
                }
            } catch (Exception e) {
                log.warn("FunctionPortCollector: response JOLT build failed for port {}: {}", portValue, e.getMessage());
            }
        }
    }

    private void collectFunctionPortScriptSpecs(EdgeDto edge, EdgeDataDto edgeData, String currentNodeId,
                                                String portValue, FunctionPortTransformsDto entry) {
        String reqScript = edgeData.getScriptContentInput();
        if (StringUtils.isNotBlank(reqScript)) {
            String lang = StringUtils.defaultIfEmpty(edgeData.getScriptLanguageInput(), "javascript");
            entry.getRequest().add(TransformationSpecDto.builder()
                    .kind("script").script(reqScript).language(lang)
                    .sourceNodeId(currentNodeId).targetNodeId(edge.getTarget())
                    .sourcePortShortCode(portValue)
                    .targetPortShortCode(extractShortCode(edge.getTargetHandle()))
                    .direction("input").build());
        }
        String respScript = edgeData.getScriptContentOutput();
        if (StringUtils.isNotBlank(respScript)) {
            String lang = StringUtils.defaultIfEmpty(edgeData.getScriptLanguageOutput(), "javascript");
            entry.getResponse().add(TransformationSpecDto.builder()
                    .kind("script").script(respScript).language(lang)
                    .sourceNodeId(edge.getTarget()).targetNodeId(currentNodeId)
                    .sourcePortShortCode(extractShortCode(edge.getTargetHandle()))
                    .targetPortShortCode(portValue)
                    .direction("output").build());
        }
    }

    private static boolean isRequestSchemaType(String schemaType) {
        if (schemaType == null) {
            return false;
        }
        return "schema".equalsIgnoreCase(schemaType) || "inputSchema".equalsIgnoreCase(schemaType);
    }

    private static boolean isResponseSchemaType(String schemaType) {
        return "outputSchema".equalsIgnoreCase(schemaType);
    }

    private static NodeDto findNodeById(ExecutionContext ctx, String nodeId) {
        if (ctx == null || StringUtils.isEmpty(nodeId)) {
            return null;
        }
        List<NodeDto> nodes = ctx.getNodes();
        if (CollectionUtils.isEmpty(nodes)) {
            return null;
        }
        for (NodeDto n : nodes) {
            if (n != null && nodeId.equals(n.getId())) {
                return n;
            }
        }
        return null;
    }

    private static Map<String, String> resolvePortSchemaTypes(NodeDto node, String portShortCode,
                                                              boolean outputSchemaLane) {
        if (node == null || StringUtils.isEmpty(portShortCode) || node.getData() == null) {
            return Collections.emptyMap();
        }
        PortDto port = node.getData().findPortByPortName(portShortCode);
        if (port == null) {
            return Collections.emptyMap();
        }
        List<SchemaDto> schemaItems;
        if (outputSchemaLane) {
            schemaItems = port.getOutputSchema();
        } else {
            schemaItems = port.getInputSchema();
            if (CollectionUtils.isEmpty(schemaItems)) {
                schemaItems = port.getSchema();
            }
        }
        if (CollectionUtils.isEmpty(schemaItems)) {
            return Collections.emptyMap();
        }
        Map<String, String> out = new LinkedHashMap<>();
        flattenSchemaTypes(schemaItems, null, out);
        return out;
    }

    private static void flattenSchemaTypes(List<SchemaDto> items, String prefix, Map<String, String> out) {
        for (SchemaDto item : items) {
            if (item == null || StringUtils.isEmpty(item.getShortCode()) || StringUtils.isEmpty(item.getType())) {
                continue;
            }
            String dotted = StringUtils.isEmpty(prefix)
                    ? item.getShortCode() : prefix + "." + item.getShortCode();
            out.putIfAbsent(item.getShortCode(), item.getType());
            out.put(dotted, item.getType());
            if (CollectionUtils.isNotEmpty(item.getChildren())) {
                flattenSchemaTypes(item.getChildren(), dotted, out);
            }
        }
    }
}
