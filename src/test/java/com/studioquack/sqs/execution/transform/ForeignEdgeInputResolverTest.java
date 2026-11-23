package com.studioquack.sqs.execution.transform;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DuckTape Add-Node delivery: a mapping row pinned to a node with no wire into the consuming
 * node must re-address that node's persisted output onto the consumer's input list — whatever
 * the edge's transform language. Rows pinned from the virtual "Workflow/Project Globals" cards
 * materialize a {key: value} message off the sealed context's variable maps instead.
 */
class ForeignEdgeInputResolverTest {

    /** Minimal store: serves prepared outputs per node and records the filtered read's node ids. */
    private static final class StubStore implements ExecutionContextStore {
        private final Map<String, List<InOutValueDto>> byNode = new LinkedHashMap<>();
        private Set<String> lastRequestedIds;

        StubStore withOutputs(String nodeId, List<InOutValueDto> outputs) {
            byNode.put(nodeId, outputs);
            return this;
        }

        @Override
        public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId) {
            return byNode;
        }

        @Override
        public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId, Set<String> nodeIds) {
            lastRequestedIds = nodeIds;
            Map<String, List<InOutValueDto>> out = new LinkedHashMap<>();
            for (String id : nodeIds) {
                if (byNode.containsKey(id)) {
                    out.put(id, byNode.get(id));
                }
            }
            return out;
        }

        @Override
        public ProcessContextRef snapshot(ExecutionContext ctx, Long jobKey) {
            return null;
        }

        @Override
        public Optional<ExecutionContext> loadByRef(ProcessContextRef ref) {
            return Optional.empty();
        }

        @Override
        public List<InOutValueDto> loadInputsTargetingNode(ProcessContextRef ref, String currentNodeId) {
            return List.of();
        }

        @Override
        public void saveOutputs(ProcessContextRef ref, List<InOutValueDto> outputs) {
        }

        @Override
        public boolean encryptionEnabled() {
            return false;
        }
    }

    private static ProcessContextRef ref() {
        return ProcessContextRef.builder().executionId("exec-1").build();
    }

    /** A's persisted fan-out entry: port out_a payload addressed to its real downstream B. */
    private static InOutValueDto producedByA(Object value) {
        InOutValueDto dto = new InOutValueDto();
        dto.setNodeId("A");
        dto.setPortId("out_a");
        dto.setPortValue("out");
        dto.setTargetNodeId("B");
        dto.setTargetPortId("in_b");
        dto.setTargetPortValue("in");
        dto.setValue(value);
        return dto;
    }

    /** Foreign mapping row on edge B→C: pin A's out_a port onto C's in_c port, path a→f. */
    private static EdgeTransformDto foreignRow() {
        EdgeTransformDto row = TransformFixtures.transform("a", "f", "C", "schema", null);
        row.setSourceNodeId("A");
        row.setSourcePortId("out_a");
        row.setTargetPortId("in_c");
        return row;
    }

    /** Virtual-card mapping row on edge B→C: pin one global key onto C's in_c port. */
    private static EdgeTransformDto globalsRow(String virtualSourceId, String globalKey) {
        EdgeTransformDto row = TransformFixtures.transform(globalKey, "f", "C", "schema", null);
        row.setSourceNodeId(virtualSourceId);
        row.setTargetPortId("in_c");
        return row;
    }

    @Test
    void deliversPinnedNodeOutputAsExtraMessage() {
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(foreignRow())));
        StubStore store = new StubStore().withOutputs("A",
                List.of(producedByA(TransformFixtures.map("a", 1, "b", 2))));

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge), null, null);

        assertEquals(1, out.size());
        InOutValueDto delivered = out.get(0);
        assertEquals("A", delivered.getNodeId(), "message keeps its producer identity");
        assertEquals("C", delivered.getTargetNodeId(), "re-addressed onto the consuming node");
        assertEquals("in", delivered.getTargetPortValue(), "the edge's target port carries the message");
        assertEquals("in_c", delivered.getTargetPortId());
        assertEquals(Map.of("a", 1, "b", 2), delivered.getValue(), "the producer's port payload rides along");
        assertEquals("B", store.byNode.get("A").get(0).getTargetNodeId(),
                "the persisted entry is never mutated in place");
    }

    @Test
    void scriptEdgeStillDeliversRawForeignMessage() {
        // Delivery is language-agnostic: the raw pinned message rides along on a script edge too —
        // shaping (author script or JOLT) stays the collector's / script's job downstream.
        EdgeDto edge = TransformFixtures.edgeWithScript("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(foreignRow())), "return payload", "python");
        StubStore store = new StubStore().withOutputs("A", List.of(producedByA(Map.of("a", 1))));

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge), null, null);

        assertEquals(1, out.size());
        assertEquals(Map.of("a", 1), out.get(0).getValue());
        assertEquals("C", out.get(0).getTargetNodeId());
    }

    @Test
    void singleOutputProducerFallsBackToItsOnlyEntry() {
        // Neither the row's port id nor its short code matches, but a single-output producer is
        // unambiguous — deliver its one entry instead of silently dropping the row.
        EdgeTransformDto row = foreignRow();
        row.setSourcePortId("out_stale");
        row.setSourcePortShortCode("stale");
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(row)));
        StubStore store = new StubStore().withOutputs("A", List.of(producedByA(Map.of("a", 1))));

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge), null, null);

        assertEquals(1, out.size());
        assertEquals("out_a", out.get(0).getPortId(), "the producer's single entry delivers as-is");
        assertEquals(Map.of("a", 1), out.get(0).getValue());
    }

    @Test
    void multiOutputProducerWithNoPortMatchStillDrops() {
        // The single-entry fallback must NOT fire when the producer fanned out more than one port.
        EdgeTransformDto row = foreignRow();
        row.setSourcePortId("out_stale");
        row.setSourcePortShortCode("stale");
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(row)));
        InOutValueDto second = producedByA(Map.of("b", 2));
        second.setPortId("out_b");
        StubStore store = new StubStore().withOutputs("A",
                List.of(producedByA(Map.of("a", 1)), second));

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge), null, null);

        assertTrue(out.isEmpty(), "ambiguous producer with no port identity match delivers nothing");
    }

    @Test
    void workflowGlobalsRowMaterializesKeyedMessage() {
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(globalsRow("$workflowGlobals", "maxRetries"))));
        StubStore store = new StubStore();

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge),
                Map.of("maxRetries", 3, "label", "wf-a"), Map.of("region", "eu"));

        assertEquals(1, out.size());
        InOutValueDto msg = out.get(0);
        assertEquals("$workflowGlobals", msg.getNodeId(), "message keeps its virtual producer identity");
        assertEquals("$globals", msg.getPortId());
        assertEquals("GLOBALS", msg.getPortValue());
        assertEquals("C", msg.getTargetNodeId(), "addressed onto the consuming node");
        assertEquals("in_c", msg.getTargetPortId());
        assertEquals("in", msg.getTargetPortValue(), "the edge's target port carries the message");
        assertFalse(msg.isGateway());
        assertEquals(Map.of("maxRetries", 3), msg.getValue(),
                "only the key named by the row's sourcePath is materialized");
        assertNull(store.lastRequestedIds, "virtual global producers must not hit the store scan");
    }

    @Test
    void projectGlobalsRowMaterializesKeyedMessage() {
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(globalsRow("$projectGlobals", "region"))));
        StubStore store = new StubStore();

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge),
                Map.of("maxRetries", 3), Map.of("region", "eu", "tier", "standard"));

        assertEquals(1, out.size());
        InOutValueDto msg = out.get(0);
        assertEquals("$projectGlobals", msg.getNodeId());
        assertEquals("$globals", msg.getPortId());
        assertEquals("GLOBALS", msg.getPortValue());
        assertEquals("C", msg.getTargetNodeId());
        assertEquals(Map.of("region", "eu"), msg.getValue());
        assertNull(store.lastRequestedIds, "virtual global producers must not hit the store scan");
    }

    @Test
    void missingGlobalKeyIsSimplyAbsent() {
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(globalsRow("$workflowGlobals", "deletedKey"))));
        StubStore store = new StubStore();

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge),
                Map.of("maxRetries", 3), null);

        assertEquals(1, out.size());
        assertEquals(Map.of(), out.get(0).getValue(),
                "a key with no sealed value is absent — never a null placeholder");
    }

    @Test
    void twoGlobalKeysOnSameTargetPortFoldIntoOneMessage() {
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(
                        globalsRow("$workflowGlobals", "maxRetries"),
                        globalsRow("$workflowGlobals", "label"))));
        StubStore store = new StubStore();

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge),
                TransformFixtures.map("maxRetries", 3, "label", "wf-a"), null);

        assertEquals(1, out.size(), "one message per (virtual source, target port)");
        assertEquals(Map.of("maxRetries", 3, "label", "wf-a"), out.get(0).getValue());
    }

    @Test
    void producerThatNeverRanDeliversNothing() {
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(foreignRow())));
        StubStore store = new StubStore(); // no rows for A — it never produced output

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge), null, null);

        assertTrue(out.isEmpty());
    }

    @Test
    void dedupesPerNodePortAndTargetPort() {
        // Two rows mapping different fields off the SAME pinned port → one delivered message.
        EdgeTransformDto second = foreignRow();
        second.setSourcePath("b");
        second.setTargetPath("g");
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(foreignRow(), second)));
        StubStore store = new StubStore().withOutputs("A", List.of(producedByA(Map.of("a", 1, "b", 2))));

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge), null, null);

        assertEquals(1, out.size(), "one message per (node, port, target port) — the JOLT group shapes all rows off it");
    }

    @Test
    void readsOnlyPinnedNodes() {
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(foreignRow())));
        StubStore store = new StubStore()
                .withOutputs("A", List.of(producedByA(Map.of("a", 1))))
                .withOutputs("stranger", List.of(producedByA(Map.of("x", 9))));

        ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge), null, null);

        assertEquals(Set.of("A"), store.lastRequestedIds,
                "only explicitly pinned nodes are read — nothing else leaks into this scope");
    }

    @Test
    void primarySourceRowsAreNotForeign() {
        // A row naming the edge's own source arrives over the real wire — never duplicated.
        EdgeTransformDto primary = TransformFixtures.transform("result", "f", "C", "schema", null);
        primary.setSourceNodeId("B");
        EdgeDto edge = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(primary)));
        StubStore store = new StubStore().withOutputs("B", List.of(producedByA(Map.of("result", 1))));

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "C", List.of(edge), null, null);

        assertTrue(out.isEmpty());
    }

    /** C's outputs on a gw→B edge. */
    private static InOutValueDto producedByC(Object value) {
        InOutValueDto dto = new InOutValueDto();
        dto.setNodeId("C");
        dto.setPortId("out_c");
        dto.setPortValue("out");
        dto.setTargetNodeId("other");
        dto.setTargetPortId("in_other");
        dto.setTargetPortValue("in");
        dto.setValue(value);
        return dto;
    }

    /** Pinned C row riding a gateway edge into B's prompt. */
    private static EdgeTransformDto pinnedCOnGateway() {
        EdgeTransformDto row = TransformFixtures.transform("a", "prompt", "B", "schema", null);
        row.setSourceNodeId("C");
        row.setSourcePortId("out_c");
        row.setTargetPortId("in_prompt");
        return row;
    }

    /** Pinned C on a gw→B edge still delivers — gateway source does not swallow foreign rows. */
    @Test
    void gatewayEdge_pinnedC_stillDelivers() {
        EdgeDto edge = TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(pinnedCOnGateway())));
        StubStore store = new StubStore().withOutputs("C",
                List.of(producedByC(TransformFixtures.map("a", 1))));

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "B", List.of(edge), null, null);

        assertEquals(1, out.size());
        assertEquals("C", out.get(0).getNodeId());
        assertEquals("B", out.get(0).getTargetNodeId());
    }

    /**
     * A row that names the gateway id is the edge's own source — skipped, never a foreign
     * store read. lastRequestedIds must not contain {@code gw}.
     */
    @Test
    void gatewayEdge_rowNamingGatewayId_isSkippedAsOwnSource() {
        EdgeTransformDto row = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        row.setSourceNodeId("gw");
        EdgeDto edge = TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(row)));
        InOutValueDto gwOut = new InOutValueDto();
        gwOut.setNodeId("gw");
        gwOut.setPortId("out_out");
        gwOut.setPortValue("out");
        gwOut.setValue(Map.of("result", 1));
        StubStore store = new StubStore()
                .withOutputs("gw", List.of(gwOut))
                .withOutputs("C", List.of(producedByC(Map.of("a", 1))));

        List<InOutValueDto> out = ForeignEdgeInputResolver.resolve(store, ref(), "B", List.of(edge), null, null);

        assertTrue(out.isEmpty());
        assertTrue(store.lastRequestedIds == null || !store.lastRequestedIds.contains("gw"),
                "gateway id must never be treated as a foreign producer");
    }
}
