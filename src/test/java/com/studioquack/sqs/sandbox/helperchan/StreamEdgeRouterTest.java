package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.delegate.DuckbotStreamSink;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Graph-routed streaming: a port-tagged emit-partial travels the emitting node's edges to a
 * Duckbot-end sink (with per-frame edge JOLT) instead of bypassing the graph.
 */
class StreamEdgeRouterTest {

    /** Minimal graph: agent node-a --thoughts/main_output--> duckbot end node-end. */
    private static Map<String, Object> ec(List<Map<String, Object>> edgeTransforms) {
        Map<String, Object> ai = new LinkedHashMap<>();
        ai.put("conversationId", "conv-1");
        ai.put("agentMessageKey", "amk-1");

        Map<String, Object> thoughtsConn = new LinkedHashMap<>();
        thoughtsConn.put("portValue", "thoughts");
        thoughtsConn.put("connectionDetailList", List.of(Map.of(
                "nodeId", "node-end", "portDto", Map.of("value", "msg"))));
        Map<String, Object> outputConn = new LinkedHashMap<>();
        outputConn.put("portValue", "main_output");
        outputConn.put("connectionDetailList", List.of(Map.of(
                "nodeId", "node-end", "portDto", Map.of("value", "msg"))));
        Map<String, Object> currentNode = new LinkedHashMap<>();
        currentNode.put("id", "node-a");
        currentNode.put("connectionList", List.of(thoughtsConn, outputConn));

        Map<String, Object> endNode = Map.of("id", "node-end", "data",
                Map.of("category", "conversion", "nodeType", "end"));
        Map<String, Object> agentNode = Map.of("id", "node-a", "data",
                Map.of("category", "ai", "nodeType", ""));

        Map<String, Object> edgeData = new LinkedHashMap<>();
        edgeData.put("edgeTransforms", edgeTransforms);
        Map<String, Object> edge = new LinkedHashMap<>();
        edge.put("source", "node-a");
        edge.put("target", "node-end");
        edge.put("data", edgeData);

        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("workflowExecutionId", "wfe-1");
        ec.put("currentNodeId", "node-a");
        ec.put("agenticInfo", ai);
        ec.put("currentNode", currentNode);
        ec.put("nodes", List.of(agentNode, endNode));
        ec.put("edges", List.of(edge));
        return ec;
    }

    /** Same graph with distinct thoughts FUNCTION + main_output OUTPUT edges (the reported pairing). */
    private static Map<String, Object> ecPairedEdges(List<Map<String, Object>> thoughtsTransforms,
                                                     List<Map<String, Object>> outputTransforms) {
        Map<String, Object> base = ec(List.of());
        base.put("edges", List.of(
                edge("FUNCTION-thoughts", "INPUT-msg", thoughtsTransforms),
                edge("OUTPUT-main_output", "INPUT-msg", outputTransforms)));
        return base;
    }

    private static Map<String, Object> edge(String sourceHandle, String targetHandle,
                                            List<Map<String, Object>> transforms) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("edgeTransforms", transforms);
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("source", "node-a");
        e.put("target", "node-end");
        e.put("sourceHandle", sourceHandle);
        e.put("targetHandle", targetHandle);
        e.put("data", data);
        return e;
    }

    private static SubProcessCompletionRegistry registry() {
        return new SubProcessCompletionRegistry(null, null, null, null, null, null, false);
    }

    private static HelperJob job(Map<String, Object> ec) {
        return HelperJob.builder("j1", "python").executionContext(ec).build();
    }

    @Test
    void portTaggedFrameRoutesToDuckbotEndWithSourceIdentity() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, null);

        assertTrue(router.route(job(ec(List.of())), "thoughts", "thinking", "planning…"));
        router.onJobFinished(job(ec(List.of())));

        assertEquals(3, seen.size());
        assertEquals("stream-start", seen.get(0).event());
        assertEquals("thinking", seen.get(0).channel());
        assertEquals("stream-delta", seen.get(1).event());
        assertEquals("planning…", seen.get(1).delta());
        assertEquals("stream-end", seen.get(2).event());
        // The frame carries the SOURCE node identity — chained agents never share a buffer.
        assertEquals("node-a", seen.get(1).nodeId());
        assertEquals("node-a", seen.get(1).nodeLabel());
        assertEquals("conv-1", seen.get(1).conversationId());
        assertEquals("amk-1", seen.get(1).agentMessageKey());
    }

    @Test
    void edgeJoltRenamesTheFrameAttributePerFrame() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink,
                new JoltTransformService(new ObjectMapper()), null);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("sourceNodeId", "node-a");
        row.put("targetNodeId", "node-end");
        row.put("sourcePath", "main_output");
        row.put("targetPath", "msg");

        assertTrue(router.route(job(ec(List.of(row))), "main_output", "answer", "tok"));

        assertEquals(2, seen.size());
        assertEquals("tok", seen.get(1).delta());
    }

    @Test
    void blankPortAndUnwiredPortDoNotRoute() {
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> { });
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, null);
        HelperJob job = job(ec(List.of()));
        assertFalse(router.route(job, null, "answer", "x"));
        assertFalse(router.route(job, "", "answer", "x"));
        assertFalse(router.route(job, "no_such_port", "answer", "x"));
    }

    @Test
    void subPiChildFramesNeverRoute() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        SubProcessCompletionRegistry registry = registry();
        registry.register(new SubProcessCompletionRegistry.SubPiLink(
                "child-wfe", "parent-wfe", "parent-job", "env", "proc", null));
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, registry);
        Map<String, Object> childEc = new LinkedHashMap<>(ec(List.of()));
        childEc.put("workflowExecutionId", "child-wfe");

        assertFalse(router.route(job(childEc), "thoughts", "thinking", "x"));
        assertTrue(seen.isEmpty());
    }

    @Test
    void nonDuckbotTargetIsNotDeliveredLive() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, null);
        Map<String, Object> ec = ec(List.of());
        // Repoint the thoughts edge at a plain node — no live render, completion value only.
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) ec.get("nodes");
        List<Map<String, Object>> plainNodes = List.of(nodes.get(0),
                Map.of("id", "node-end", "data", Map.of("category", "ai", "nodeType", "")));
        ec.put("nodes", plainNodes);

        assertFalse(router.route(job(ec), "thoughts", "thinking", "x"));
        assertTrue(seen.isEmpty());
    }

    @Test
    void frameCarriesSourceNodeLabelFromEcNodes() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, null);
        Map<String, Object> graph = ec(List.of());
        graph.put("nodes", List.of(
                Map.of("id", "node-a", "data", Map.of("category", "ai", "nodeType", "", "label", "Planner")),
                Map.of("id", "node-end", "data", Map.of("category", "conversion", "nodeType", "end"))));

        assertTrue(router.route(job(graph), "thoughts", "thinking", "planning…"));
        assertEquals("Planner", seen.get(1).nodeLabel());
        assertEquals("node-a", seen.get(1).nodeId());
    }

    @Test
    void paramStrictNonMatchingTransformPassesRawStreamToken() {
        // Stream tokens are an unshaped preview of the final value — dropping them silenced the
        // live channel. Completion-time param routing is unchanged (TransformPipeline).
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink,
                new JoltTransformService(new ObjectMapper()), null);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("sourceNodeId", "node-a");
        row.put("targetNodeId", "node-end");
        row.put("sourcePath", "main_output");
        row.put("targetPath", "msg");

        assertTrue(router.route(job(ec(List.of(row))), "thoughts", "thinking", "planning"));
        assertEquals("planning", seen.get(1).delta());
    }

    @Test
    void mainOutputTokensStreamThroughWhenOutputEdgeHasNoTransform() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, null);

        assertTrue(router.route(job(ecPairedEdges(List.of(), List.of())), "main_output", "answer", "tok"));
        assertEquals("tok", seen.get(1).delta());
        assertEquals("answer", seen.get(1).channel());
    }

    @Test
    void mainOutputMatchingTransformIsAppliedPerFrame() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink,
                new JoltTransformService(new ObjectMapper()), null);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("sourceNodeId", "node-a");
        row.put("targetNodeId", "node-end");
        row.put("sourcePath", "main_output");
        row.put("targetPath", "msg");
        row.put("operator", "toUpper");

        assertTrue(router.route(job(ecPairedEdges(List.of(), List.of(row))), "main_output", "answer", "tok"));
        assertEquals("TOK", seen.get(1).delta());
    }

    @Test
    void mainOutputNonMatchingTransformOnSamePortEdgePassesRaw() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink,
                new JoltTransformService(new ObjectMapper()), null);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("sourceNodeId", "node-a");
        row.put("targetNodeId", "node-end");
        row.put("sourcePath", "other_field");
        row.put("targetPath", "msg");

        assertTrue(router.route(job(ecPairedEdges(List.of(), List.of(row))), "main_output", "answer", "tok"));
        assertEquals("tok", seen.get(1).delta());
    }

    @Test
    void thoughtsTransformOnSiblingEdgeDoesNotDropMainOutputTokens() {
        // The reported bug: thoughts FUNCTION + main_output OUTPUT both target the same Duckbot
        // end. Merged node-pair JOLT swallowed {main_output: token}. Port-pair filter keeps it.
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink,
                new JoltTransformService(new ObjectMapper()), null);
        Map<String, Object> thoughtsRow = new LinkedHashMap<>();
        thoughtsRow.put("sourceNodeId", "node-a");
        thoughtsRow.put("targetNodeId", "node-end");
        thoughtsRow.put("sourcePath", "thoughts");
        thoughtsRow.put("targetPath", "msg");

        assertTrue(router.route(job(ecPairedEdges(List.of(thoughtsRow), List.of())),
                "main_output", "answer", "hello"));
        assertEquals("hello", seen.get(1).delta());
        assertEquals("answer", seen.get(1).channel());

        seen.clear();
        assertTrue(router.route(job(ecPairedEdges(List.of(thoughtsRow), List.of())),
                "thoughts", "thinking", "planning"));
        assertEquals("planning", seen.get(1).delta());
        assertEquals("thinking", seen.get(1).channel());
    }

    @Test
    void childWfeFrameLandsOnParentSeqCounter() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        SubProcessCompletionRegistry registry = new SubProcessCompletionRegistry(
                null, null, null, null, null, null, false) {
            @Override
            public boolean isSubPiChild(String workflowExecutionId) {
                return false;
            }
        };
        registry.register(new SubProcessCompletionRegistry.SubPiLink(
                "child-wfe", "wfe-1", "parent-job", "env", "proc", null));
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, registry);

        assertTrue(router.route(job(ec(List.of())), "thoughts", "thinking", "parent"));
        int lastParentSeq = seen.get(seen.size() - 1).seq();

        Map<String, Object> childEc = new LinkedHashMap<>(ec(List.of()));
        childEc.put("workflowExecutionId", "child-wfe");
        assertTrue(router.route(job(childEc), "thoughts", "thinking", "child"));
        int childDeltaSeq = seen.stream()
                .filter(e -> "child".equals(e.delta()))
                .mapToInt(ConversationStreamEvent::seq)
                .findFirst().orElse(-1);
        assertTrue(childDeltaSeq > lastParentSeq, "remapped child frame must continue parent seq, not restart at 0");
        assertEquals("stream-delta", seen.get(seen.size() - 1).event());
    }

    @Test
    void childJobFinishEmitsNoStreamEnd() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        SubProcessCompletionRegistry registry = registry();
        registry.register(new SubProcessCompletionRegistry.SubPiLink(
                "child-wfe", "wfe-1", "parent-job", "env", "proc", null));
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, registry);

        assertTrue(router.route(job(ec(List.of())), "thoughts", "thinking", "live"));
        Map<String, Object> childEc = new LinkedHashMap<>(ec(List.of()));
        childEc.put("workflowExecutionId", "child-wfe");
        router.onJobFinished(job(childEc));

        assertTrue(seen.stream().noneMatch(e -> "stream-end".equals(e.event())),
                "a function-port child finish must not close the parent's live stream");
        router.onJobFinished(job(ec(List.of())));
        assertTrue(seen.stream().anyMatch(e -> "stream-end".equals(e.event())));
    }

    @Test
    void rawPassthroughWhenEdgeHasNoTransforms() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink,
                new JoltTransformService(new ObjectMapper()), null);

        assertTrue(router.route(job(ec(List.of())), "thoughts", "thinking", "raw"));
        assertEquals("raw", seen.get(1).delta());
    }

    @Test
    void fileRefDeltaIsDropped() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, null);

        assertFalse(router.route(job(ec(List.of())), "thoughts", "thinking", "see sq-file:abc"));
        assertTrue(seen.isEmpty());
    }

    // ---- Deployed-graph fallback (the production baked EC carries NO nodes/edges) ----

    /** The real baked job context: connectionList on currentNode, no graph keys, workflowId set. */
    private static Map<String, Object> bakedEc() {
        Map<String, Object> baked = ec(List.of());
        baked.remove("nodes");
        baked.remove("edges");
        baked.put("workflowId", "wf-1");
        return baked;
    }

    private static DeployedArtifactStore.DeployedGraph deployedGraph(List<Map<String, Object>> transforms) {
        ObjectMapper m = new ObjectMapper();
        Map<String, Object> agentNode = Map.of("id", "node-a", "data",
                Map.of("category", "ai", "nodeType", "", "label", "Planner"));
        Map<String, Object> endNode = Map.of("id", "node-end", "data",
                Map.of("category", "conversion", "nodeType", "end"));
        Map<String, Object> edgeData = new LinkedHashMap<>();
        edgeData.put("edgeTransforms", transforms);
        Map<String, Object> edgeMap = new LinkedHashMap<>();
        edgeMap.put("source", "node-a");
        edgeMap.put("target", "node-end");
        edgeMap.put("sourceHandle", "OUTPUT-main_output");
        edgeMap.put("targetHandle", "INPUT-msg");
        edgeMap.put("data", edgeData);
        return new DeployedArtifactStore.DeployedGraph(
                List.of(m.convertValue(agentNode, NodeDto.class), m.convertValue(endNode, NodeDto.class)),
                List.of(m.convertValue(edgeMap, EdgeDto.class)));
    }

    @Test
    void bakedEcWithoutGraphRoutesViaDeployedGraphFallback() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        int[] loads = {0};
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, null, null,
                workflowId -> {
                    loads[0]++;
                    return deployedGraph(List.of());
                });

        assertTrue(router.route(job(bakedEc()), "main_output", "answer", "tok"));
        assertEquals("tok", seen.get(1).delta());
        assertEquals("answer", seen.get(1).channel());
        // The source label resolves from the deployed graph when the baked currentNode has none.
        assertEquals("Planner", seen.get(1).nodeLabel());

        // Second frame hits the cache — no per-token store reads on the stream hot path.
        assertTrue(router.route(job(bakedEc()), "main_output", "answer", "tok2"));
        assertEquals(1, loads[0]);
    }

    @Test
    void graphFallbackAppliesEdgeJoltPerFrame() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        StreamEdgeRouter router = new StreamEdgeRouter(sink,
                new JoltTransformService(new ObjectMapper()), null, null,
                workflowId -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("sourceNodeId", "node-a");
                    row.put("targetNodeId", "node-end");
                    row.put("sourcePath", "main_output");
                    row.put("targetPath", "msg");
                    row.put("operator", "toUpper");
                    return deployedGraph(List.of(row));
                });

        assertTrue(router.route(job(bakedEc()), "main_output", "answer", "tok"));
        assertEquals("TOK", seen.get(1).delta());
    }

    @Test
    void missingGraphStillDropsTheFrame() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        int[] loads = {0};
        StreamEdgeRouter router = new StreamEdgeRouter(sink, null, null, null,
                workflowId -> {
                    loads[0]++;
                    return null;
                });

        assertFalse(router.route(job(bakedEc()), "main_output", "answer", "tok"));
        assertTrue(seen.isEmpty());
        // The miss is cached: no store hit per token for a graph-less run.
        assertFalse(router.route(job(bakedEc()), "main_output", "answer", "tok2"));
        assertEquals(1, loads[0]);
    }
}
