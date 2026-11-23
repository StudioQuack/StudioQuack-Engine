package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.studioquack.sqs.sandbox.helperchan.StreamRouteSink.StreamFrame;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Graph-routed live streaming: a port-tagged {@code emit-partial} leaves the emitting node through
 * the edges of that named source port, takes the edge's JOLT per frame, and lands on every
 * live-render sink (Duckbot end) and every {@code data.streamThrough=true} target ({@link
 * StreamSessionManager} → the node's {@code POST …/invoke/stream}).
 */
@Slf4j
@Component
public class StreamEdgeRouter {

    /** Max stream-through hops in one chain; a cycle or deeper path terminates with a warn. */
    public static final int MAX_ROUTE_DEPTH = 8;

    /** File-handle prefix — stream frames are text-only and must not carry refs. */
    private static final String FILE_REF_PREFIX = "sq-file:";

    /** Bound on the per-workflow graph cache (a graph never changes mid-run for its run's frames). */
    private static final int GRAPH_CACHE_CAP = 128;

    private final StreamRouteSink sink;
    private final JoltTransformService jolt;
    private final SubProcessCompletionRegistry subProcessRegistry;
    private final StreamSessionManager streamSessions;
    private final GraphLookup graphLookup;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Deployed-graph views keyed by workflowId. The baked job context carries no graph ({@code
     * ExecutionContext.clearNodeAndEdges()} — REF-only), so without this fallback {@link #route} could
     * never resolve the target node and every live frame was dropped.
     */
    private final Map<String, GraphView> graphCache = Collections.synchronizedMap(
            new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, GraphView> eldest) {
                    return size() > GRAPH_CACHE_CAP;
                }
            });

    public StreamEdgeRouter(@Nullable StreamRouteSink sink,
                            @Nullable JoltTransformService jolt,
                            @Nullable SubProcessCompletionRegistry subProcessRegistry) {
        this(sink, jolt, subProcessRegistry, (StreamSessionManager) null, (GraphLookup) null);
    }

    /** Legacy slice-test seam (no graph fallback). */
    public StreamEdgeRouter(@Nullable StreamRouteSink sink,
                            @Nullable JoltTransformService jolt,
                            @Nullable SubProcessCompletionRegistry subProcessRegistry,
                            @Nullable StreamSessionManager streamSessions) {
        this(sink, jolt, subProcessRegistry, streamSessions, (GraphLookup) null);
    }

    /** Test seam: graph fallback without the store bean. */
    StreamEdgeRouter(@Nullable StreamRouteSink sink,
                     @Nullable JoltTransformService jolt,
                     @Nullable SubProcessCompletionRegistry subProcessRegistry,
                     @Nullable StreamSessionManager streamSessions,
                     @Nullable GraphLookup graphLookup) {
        this.sink = sink;
        this.jolt = jolt;
        this.subProcessRegistry = subProcessRegistry;
        this.streamSessions = streamSessions;
        this.graphLookup = graphLookup;
    }

    @Autowired
    public StreamEdgeRouter(@Nullable StreamRouteSink sink,
                            @Nullable JoltTransformService jolt,
                            @Nullable SubProcessCompletionRegistry subProcessRegistry,
                            @Lazy @Nullable StreamSessionManager streamSessions,
                            @Nullable DeployedArtifactStore artifactStore) {
        this(sink, jolt, subProcessRegistry, streamSessions,
                artifactStore == null ? null
                        : workflowId -> artifactStore.loadDeployedGraph(workflowId).orElse(null));
    }

    /** Deployed-graph lookup seam — keeps slice tests free of the store's repository wiring. */
    @FunctionalInterface
    interface GraphLookup {
        @Nullable
        DeployedArtifactStore.DeployedGraph load(String workflowId);
    }

    /** Map-shaped nodes/edges of one deployed graph (the shape the baked EC would have carried). */
    private record GraphView(List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
    }

    /**
     * Routes one delta emitted on {@code port} by the job's current node. Returns
     * {@code true} when at least one sink or stream-through session accepted the frame
     * (caller then suppresses the legacy conversation side-channel for this frame).
     */
    public boolean route(HelperJob job, String port, String channel, String delta) {
        return route(job, port, channel, delta, RouteChain.root());
    }

    /**
     * Same as {@link #route(HelperJob, String, String, String)} with an explicit chain for
     * stream-OUT re-entry (depth + visited). Package-visible for {@link StreamSessionManager}.
     */
    boolean route(HelperJob job, String port, String channel, String delta, RouteChain chain) {
        if (job == null || port == null || port.isBlank()) {
            return false;
        }
        if (containsFileRef(delta)) {
            log.warn("stream-route: dropped non-text frame (file ref) node={} port={}",
                    str(job.executionContext() == null ? null : job.executionContext().get("currentNodeId")),
                    port);
            return false;
        }
        Map<String, Object> ec = job.executionContext();
        if (ec == null || ec.isEmpty()) {
            return false;
        }
        String wfeId = str(ec.get("workflowExecutionId"));
        String nodeId = str(ec.get("currentNodeId"));
        if (wfeId.isEmpty() || nodeId.isEmpty()) {
            return false;
        }
        RouteChain incoming = chain == null ? RouteChain.root() : chain;
        if (incoming.depth() >= MAX_ROUTE_DEPTH) {
            log.warn("stream-route: max depth {} exceeded wfe={} node={} — terminating chain",
                    MAX_ROUTE_DEPTH, wfeId, nodeId);
            return false;
        }
        if (subProcessRegistry != null && subProcessRegistry.isSubPiChild(wfeId)) {
            return false;
        }
        // Seq space is the parent run even if coords still carry a raw child id.
        String reportableWfeId = reportableRunId(wfeId);
        if (incoming.seen(reportableWfeId, nodeId)) {
            log.warn("stream-route: cycle detected wfe={} node={} depth={} — terminating chain",
                    reportableWfeId, nodeId, incoming.depth());
            return false;
        }
        Map<?, ?> currentNode = ec.get("currentNode") instanceof Map<?, ?> cn ? cn : null;
        if (currentNode == null) {
            return false;
        }
        List<TargetRef> targets = targetsForPort(currentNode, port);
        if (targets.isEmpty()) {
            return false;
        }
        RouteChain next = incoming.plus(reportableWfeId, nodeId);
        // The baked EC never carries the graph (clearNodeAndEdges — REF-ONLY): resolve target
        // nodes + edge transforms from the deployed graph, cached per workflowId.
        List<?> nodes = ec.get("nodes") instanceof List<?> n ? n : List.of();
        List<?> edges = ec.get("edges") instanceof List<?> e ? e : List.of();
        if (nodes.isEmpty()) {
            GraphView view = graphViewFor(str(ec.get("workflowId")));
            if (view == null) {
                return false;
            }
            nodes = view.nodes();
            edges = view.edges();
        }
        ConversationStreamCoordinator.AgenticCoords coords = ConversationStreamCoordinator.coordsOf(job);
        boolean delivered = false;
        for (TargetRef target : targets) {
            Map<String, Object> targetNode = nodeById(nodes, target.targetNodeId());
            if (targetNode == null) {
                continue;
            }
            String routed = applyEdgeJolt(edges, nodeId, target, port, delta);
            if (routed == null) {
                continue;
            }
            if (sink != null && sink.isStreamTarget(targetNode) && coords != null) {
                try {
                    sink.onFrame(new StreamFrame(reportableRunId(coords.workflowExecutionId()),
                            coords.conversationId(),
                            coords.agentMessageKey(), coords.nodeId(), target.targetNodeId(),
                            channel, routed, nodeLabelOf(nodes, currentNode, nodeId)));
                    delivered = true;
                } catch (RuntimeException e) {
                    log.warn("stream-route: sink rejected frame node={} port={} target={}: {}",
                            nodeId, port, target.targetNodeId(), e.toString());
                }
            } else if (streamSessions != null && isStreamThrough(targetNode)
                    && !next.seen(reportableWfeId, target.targetNodeId())) {
                try {
                    if (streamSessions.accept(job, port, target.targetNodeId(), target.targetPortValue(),
                            targetNode, channel, routed, next)) {
                        delivered = true;
                    }
                } catch (RuntimeException e) {
                    log.warn("stream-route: stream-through failed node={} port={} target={}: {}",
                            nodeId, port, target.targetNodeId(), e.toString());
                }
            }
        }
        return delivered;
    }

    /**
     * The deployed graph for {@code workflowId} as map-shaped nodes/edges, cached per workflow.
     * Null when no lookup is wired (slice tests) or the workflow has no persisted graph; both
     * are cached so a graph-less run never re-reads the store per frame.
     */
    private GraphView graphViewFor(String workflowId) {
        if (workflowId == null || workflowId.isBlank() || graphLookup == null) {
            return null;
        }
        GraphView cached = graphCache.get(workflowId);
        if (cached != null) {
            return cached.nodes().isEmpty() ? null : cached;
        }
        GraphView loaded = loadGraphView(workflowId);
        graphCache.put(workflowId, loaded == null ? new GraphView(List.of(), List.of()) : loaded);
        return loaded;
    }

    private GraphView loadGraphView(String workflowId) {
        try {
            DeployedArtifactStore.DeployedGraph graph = graphLookup.load(workflowId);
            if (graph == null || graph.nodes() == null || graph.nodes().isEmpty()) {
                return null;
            }
            List<Map<String, Object>> nodes = mapper.convertValue(graph.nodes(), new TypeReference<>() {});
            List<Map<String, Object>> edges = mapper.convertValue(
                    graph.edges() == null ? List.of() : graph.edges(), new TypeReference<>() {});
            return new GraphView(nodes, edges);
        } catch (RuntimeException e) {
            log.debug("stream-route: deployed graph load failed workflow={}: {}", workflowId, e.toString());
            return null;
        }
    }

    /** Closes the sink sessions and stream-through sessions this job's node opened. */
    public void onJobFinished(HelperJob job) {
        if (job == null) {
            return;
        }
        Map<String, Object> ec = job.executionContext();
        String wfeId = ec == null ? "" : str(ec.get("workflowExecutionId"));
        String nodeId = ec == null ? "" : str(ec.get("currentNodeId"));
        // Same skip as route: a function-port child must never emit a stray stream-end
        // into the parent's seq space (onSourceFinished remaps via resolveReportableRunId).
        if (subProcessRegistry != null && !wfeId.isEmpty() && subProcessRegistry.isSubPiChild(wfeId)) {
            return;
        }
        ConversationStreamCoordinator.AgenticCoords coords = ConversationStreamCoordinator.coordsOf(job);
        if (sink != null && coords != null) {
            try {
                sink.onSourceFinished(reportableRunId(coords.workflowExecutionId()), coords.nodeId());
            } catch (RuntimeException e) {
                log.warn("stream-route: source-finish failed node={}: {}", coords.nodeId(), e.toString());
            }
        }
        if (streamSessions != null && !wfeId.isEmpty() && !nodeId.isEmpty()) {
            try {
                streamSessions.closeBySource(wfeId, nodeId);
            } catch (RuntimeException e) {
                log.warn("stream-route: stream-through close failed node={}: {}", nodeId, e.toString());
            }
        }
    }

    /** True when {@code data.streamThrough} is the Boolean {@code true}. */
    static boolean isStreamThrough(Map<String, Object> targetNode) {
        if (targetNode == null || !(targetNode.get("data") instanceof Map<?, ?> data)) {
            return false;
        }
        Object v = data.get("streamThrough");
        return Boolean.TRUE.equals(v);
    }

    /** True when {@code delta} carries an {@code sq-file:} handle. Never logs the value. */
    static boolean containsFileRef(String delta) {
        return delta != null && delta.contains(FILE_REF_PREFIX);
    }

    /**
     * The target's sole OUTPUT port value when the definition lists exactly one; otherwise
     * empty (caller drops the frame). No hardcoded port names.
     */
    @SuppressWarnings("unchecked")
    static String soleOutputPort(Map<String, Object> targetNode) {
        if (targetNode == null || !(targetNode.get("data") instanceof Map<?, ?> data)) {
            return "";
        }
        if (!(data.get("ports") instanceof List<?> ports)) {
            return "";
        }
        String found = null;
        for (Object raw : ports) {
            if (!(raw instanceof Map<?, ?> port)) {
                continue;
            }
            Object type = port.get("type");
            if (type == null || !"OUTPUT".equalsIgnoreCase(String.valueOf(type))) {
                continue;
            }
            String value = port.get("value") == null ? "" : String.valueOf(port.get("value")).trim();
            if (value.isEmpty()) {
                continue;
            }
            if (found != null) {
                return "";
            }
            found = value;
        }
        return found == null ? "" : found;
    }

    /** Outbound (targetNodeId, targetPortValue) pairs for the source port, from connectionList. */
    private List<TargetRef> targetsForPort(Map<?, ?> currentNode, String port) {
        Object connListRaw = currentNode.get("connectionList");
        if (!(connListRaw instanceof List<?> connList)) {
            return List.of();
        }
        for (Object rawConn : connList) {
            if (!(rawConn instanceof Map<?, ?> conn) || !port.equals(conn.get("portValue"))) {
                continue;
            }
            if (!(conn.get("connectionDetailList") instanceof List<?> details)) {
                return List.of();
            }
            List<TargetRef> out = new ArrayList<>();
            for (Object dRaw : details) {
                if (!(dRaw instanceof Map<?, ?> d)) {
                    continue;
                }
                String targetNodeId = str(d.get("nodeId"));
                if (targetNodeId.isEmpty()) {
                    continue;
                }
                String targetPort = d.get("portDto") instanceof Map<?, ?> pd ? str(pd.get("value")) : "";
                out.add(new TargetRef(targetNodeId, targetPort));
            }
            return out;
        }
        return List.of();
    }

    private static Map<String, Object> nodeById(List<?> nodes, String nodeId) {
        for (Object raw : nodes) {
            if (raw instanceof Map<?, ?> node && nodeId.equals(str(node.get("id")))) {
                @SuppressWarnings("unchecked")
                Map<String, Object> typed = (Map<String, Object>) node;
                return typed;
            }
        }
        return null;
    }

    /**
     * Applies the edge's DuckTape JOLT to the frame envelope {@code {sourcePort: delta}} and reads the
     * delta back under the target port's attribute.
     */
    private String applyEdgeJolt(List<?> edges, String sourceNodeId, TargetRef target,
                                 String port, String delta) {
        List<EdgeTransformDto> rows = edgeTransformRows(edges, sourceNodeId, target, port);
        if (rows.isEmpty()) {
            return delta;
        }
        if (jolt == null) {
            return delta;
        }
        try {
            List<Object> spec = jolt.convertJoltSpec(rows);
            if (spec == null || spec.isEmpty()) {
                return delta;
            }
            Object out = jolt.transform(Map.of(port, delta), spec);
            if (out instanceof Map<?, ?> map && !target.targetPortValue().isEmpty()) {
                Object v = map.get(target.targetPortValue());
                if (v instanceof String s && !s.isEmpty()) {
                    return s;
                }
            }
        } catch (RuntimeException e) {
            log.debug("stream-route: edge jolt skipped {}→{}: {}", sourceNodeId, target.targetNodeId(), e.toString());
        }
        return delta;
    }

    /**
     * DuckTape rows on the source→target edge whose source handle/port equals the emit
     * {@code port} and whose target handle matches {@code target} (same pairing as
     * {@link #targetsForPort}). Sibling edges of the same node pair (e.g. thoughts FUNCTION
     * vs main_output OUTPUT into one Duckbot end) must not share transforms.
     */
    private List<EdgeTransformDto> edgeTransformRows(List<?> edges, String sourceNodeId,
                                                     TargetRef target, String port) {
        List<EdgeTransformDto> rows = new ArrayList<>();
        String targetNodeId = target.targetNodeId();
        for (Object rawEdge : edges) {
            if (!(rawEdge instanceof Map<?, ?> edge)) {
                continue;
            }
            if (!sourceNodeId.equals(str(edge.get("source"))) || !targetNodeId.equals(str(edge.get("target")))) {
                continue;
            }
            if (!portMatchesHandle(str(edge.get("sourceHandle")), port)) {
                continue;
            }
            if (!portMatchesHandle(str(edge.get("targetHandle")), target.targetPortValue())) {
                continue;
            }
            if (!(edge.get("data") instanceof Map<?, ?> data)
                    || !(data.get("edgeTransforms") instanceof List<?> transforms)) {
                continue;
            }
            for (Object rawRow : transforms) {
                if (!(rawRow instanceof Map<?, ?> row)) {
                    continue;
                }
                String rowTarget = str(row.get("targetNodeId"));
                String rowSource = str(row.get("sourceNodeId"));
                if (!targetNodeId.equals(rowTarget)) {
                    continue;
                }
                if (!rowSource.isEmpty() && !sourceNodeId.equals(rowSource)) {
                    continue;
                }
                try {
                    rows.add(mapper.convertValue(row, EdgeTransformDto.class));
                } catch (IllegalArgumentException e) {
                    log.debug("stream-route: unparseable edgeTransform row on {}→{}: {}",
                            sourceNodeId, targetNodeId, e.toString());
                }
            }
        }
        return rows;
    }

    /** React-flow handle is {@code TYPE-port} (e.g. {@code OUTPUT-main_output}) or the bare port. */
    static boolean portMatchesHandle(String handle, String port) {
        if (port == null || port.isBlank()) {
            return true;
        }
        if (handle == null || handle.isBlank()) {
            return true;
        }
        if (port.equals(handle)) {
            return true;
        }
        String[] parts = handle.split("-", 2);
        return parts.length == 2 && port.equals(parts[1]);
    }

    /** Top-level run the control plane registered; raw id when this is already top-level or unknown. */
    private String reportableRunId(String wfeId) {
        if (wfeId == null || wfeId.isEmpty() || subProcessRegistry == null) {
            return wfeId == null ? "" : wfeId;
        }
        try {
            String resolved = subProcessRegistry.resolveReportableRunId(wfeId);
            return resolved == null || resolved.isBlank() ? wfeId : resolved;
        } catch (RuntimeException e) {
            return wfeId;
        }
    }

    /** {@code data.label} on currentNode, else the matching graph node, else nodeId. */
    static String nodeLabelOf(List<?> nodes, Map<?, ?> currentNode, String nodeId) {
        String fromCurrent = labelFrom(currentNode);
        if (!fromCurrent.isEmpty()) {
            return fromCurrent;
        }
        String fromListed = labelFrom(nodeById(nodes, nodeId));
        if (!fromListed.isEmpty()) {
            return fromListed;
        }
        return nodeId == null ? "" : nodeId;
    }

    private static String labelFrom(Map<?, ?> node) {
        if (node != null && node.get("data") instanceof Map<?, ?> data) {
            String label = str(data.get("label"));
            if (!label.isBlank()) {
                return label;
            }
        }
        return "";
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private record TargetRef(String targetNodeId, String targetPortValue) {
    }

    /**
     * Stream-through recursion state: hop count plus the {@code (wfeId, nodeId)} identities
     * already visited on this chain.
     */
    public record RouteChain(int depth, Set<String> visited) {
        public static RouteChain root() {
            return new RouteChain(0, Set.of());
        }

        public boolean seen(String wfeId, String nodeId) {
            return visited.contains(key(wfeId, nodeId));
        }

        public RouteChain plus(String wfeId, String nodeId) {
            Set<String> next = new LinkedHashSet<>(visited);
            next.add(key(wfeId, nodeId));
            return new RouteChain(depth + 1, Collections.unmodifiableSet(next));
        }

        private static String key(String wfeId, String nodeId) {
            return wfeId + '\0' + nodeId;
        }
    }
}
