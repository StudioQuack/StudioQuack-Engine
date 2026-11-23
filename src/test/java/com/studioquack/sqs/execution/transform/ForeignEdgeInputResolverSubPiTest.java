package com.studioquack.sqs.execution.transform;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.store.SubPiInvocation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Function-port index fallback: a pinned node with no parent-scope outputs is resolved
 * through {@code subPiInvocationsOf}, and every invocation's matched port value is merged
 * into one list-wrapped message (even a single invocation). Parent-scope hits stay unwrapped.
 */
class ForeignEdgeInputResolverSubPiTest {

    /**
     * Execution-scoped stub: outputs keyed by (executionId, nodeId). Never serves another
     * execution's rows. Records every filtered collect so tests can pin {@code Set.of(D)}
     * and reject an unfiltered whole-child scan.
     */
    private static final class StubStore implements ExecutionContextStore {
        private final Map<String, Map<String, List<InOutValueDto>>> byExecution = new LinkedHashMap<>();
        private final Map<String, List<SubPiInvocation>> invocationsByParent = new LinkedHashMap<>();
        private final List<CollectCall> filteredCollects = new ArrayList<>();
        private final List<String> unfilteredCollects = new ArrayList<>();
        String lastRequestedExecutionId;
        Set<String> lastRequestedIds;

        record CollectCall(String executionId, Set<String> nodeIds) {
        }

        StubStore withOutputs(String executionId, String nodeId, List<InOutValueDto> outputs) {
            byExecution.computeIfAbsent(executionId, k -> new LinkedHashMap<>()).put(nodeId, outputs);
            return this;
        }

        List<InOutValueDto> storedOutputs(String executionId, String nodeId) {
            Map<String, List<InOutValueDto>> byNode = byExecution.get(executionId);
            return byNode == null ? List.of() : byNode.getOrDefault(nodeId, List.of());
        }

        List<CollectCall> filteredCollects() {
            return filteredCollects;
        }

        List<String> unfilteredCollects() {
            return unfilteredCollects;
        }

        @Override
        public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId) {
            unfilteredCollects.add(executionId);
            Map<String, List<InOutValueDto>> byNode = byExecution.get(executionId);
            return byNode == null ? Map.of() : byNode;
        }

        @Override
        public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId, Set<String> nodeIds) {
            lastRequestedExecutionId = executionId;
            lastRequestedIds = nodeIds;
            filteredCollects.add(new CollectCall(executionId,
                    nodeIds == null ? null : Set.copyOf(nodeIds)));
            Map<String, List<InOutValueDto>> byNode = byExecution.get(executionId);
            Map<String, List<InOutValueDto>> out = new LinkedHashMap<>();
            if (byNode == null || nodeIds == null) {
                return out;
            }
            for (String id : nodeIds) {
                if (byNode.containsKey(id)) {
                    out.put(id, byNode.get(id));
                }
            }
            return out;
        }

        @Override
        public void recordSubPiInvocation(String parentExecutionId, String childExecutionId,
                                          String callerNodeId, String subPiRootNodeId) {
            invocationsByParent.computeIfAbsent(parentExecutionId, k -> new ArrayList<>())
                    .add(new SubPiInvocation(childExecutionId, callerNodeId, subPiRootNodeId));
        }

        @Override
        public List<SubPiInvocation> subPiInvocationsOf(String parentExecutionId) {
            List<SubPiInvocation> list = invocationsByParent.get(parentExecutionId);
            return list == null ? List.of() : list;
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
        return ProcessContextRef.builder().executionId("parent").build();
    }

    /** Pinned row on A→B: D.out_d → B.in_b, path name→prompt. */
    private static EdgeTransformDto pinnedRow() {
        return pinnedRow("out_d", "in_b");
    }

    private static EdgeTransformDto pinnedRow(String sourcePortId, String targetPortId) {
        EdgeTransformDto row = TransformFixtures.transform("name", "prompt", "B", "schema", null);
        row.setSourceNodeId("D");
        row.setSourcePortId(sourcePortId);
        row.setTargetPortId(targetPortId);
        return row;
    }

    private static EdgeDto edgeAB(EdgeTransformDto... rows) {
        return TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(rows)));
    }

    private static InOutValueDto producedByD(Object value) {
        return producedByD("out_d", value, null);
    }

    private static InOutValueDto producedByD(String portId, Object value, Object unfilteredValue) {
        InOutValueDto dto = new InOutValueDto();
        dto.setNodeId("D");
        dto.setPortId(portId);
        dto.setPortValue("out");
        dto.setValue(value);
        dto.setUnfilteredValue(unfilteredValue);
        return dto;
    }

    private static List<InOutValueDto> resolve(StubStore store, EdgeDto edge) {
        return ForeignEdgeInputResolver.resolve(store, ref(), "B", List.of(edge), null, null);
    }

    /** Every child collect must be the filtered {@code Set.of(D)} overload — never a whole-child scan. */
    private static void assertChildCollectsAreFilteredToPinned(StubStore store, String pinnedId) {
        assertTrue(store.unfilteredCollects().isEmpty(),
                "resolver must never call the unfiltered collectOutputsByNode(executionId) scan");
        for (StubStore.CollectCall call : store.filteredCollects()) {
            if ("parent".equals(call.executionId())) {
                continue;
            }
            assertEquals(Set.of(pinnedId), call.nodeIds(),
                    "child collectOutputsByNode must use the filtered Set.of(" + pinnedId + ") overload");
        }
    }

    @Test
    void twoInvocationsMergeIntoOrderedList() {
        StubStore store = new StubStore();
        store.recordSubPiInvocation("parent", "child1", "A", "sub");
        store.recordSubPiInvocation("parent", "child2", "A", "sub");
        Map<String, Object> v1 = TransformFixtures.map("name", "n1");
        Map<String, Object> v2 = TransformFixtures.map("name", "n2");
        store.withOutputs("child1", "D", List.of(producedByD(v1)));
        store.withOutputs("child2", "D", List.of(producedByD(v2)));

        List<InOutValueDto> out = resolve(store, edgeAB(pinnedRow()));

        assertEquals(1, out.size());
        InOutValueDto delivered = out.get(0);
        assertEquals("B", delivered.getTargetNodeId());
        assertFalse(delivered.isGateway());
        assertEquals(List.of(Map.of("name", "n1"), Map.of("name", "n2")), delivered.getValue());
        assertChildCollectsAreFilteredToPinned(store, "D");

        // Deep-copy: mutating the delivered list must not touch persisted child entries.
        @SuppressWarnings("unchecked")
        List<Object> deliveredList = (List<Object>) delivered.getValue();
        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) deliveredList.get(0);
        assertNotSame(v1, first);
        first.put("name", "MUTATED");
        deliveredList.add(TransformFixtures.map("name", "extra"));
        assertEquals(Map.of("name", "n1"), store.storedOutputs("child1", "D").get(0).getValue());
        assertEquals(Map.of("name", "n2"), store.storedOutputs("child2", "D").get(0).getValue());
        assertEquals("n1", v1.get("name"));
        assertEquals("n2", v2.get("name"));
    }

    @Test
    void singleInvocationWrapsAsListOfOne() {
        StubStore store = new StubStore();
        store.recordSubPiInvocation("parent", "child1", "A", "sub");
        store.withOutputs("child1", "D", List.of(producedByD(TransformFixtures.map("name", "n1"))));

        List<InOutValueDto> out = resolve(store, edgeAB(pinnedRow()));

        assertEquals(1, out.size());
        assertEquals(List.of(Map.of("name", "n1")), out.get(0).getValue(),
                "a single invocation is still list-wrapped — never the bare map");
        assertEquals("B", out.get(0).getTargetNodeId());
        assertFalse(out.get(0).isGateway());
        assertChildCollectsAreFilteredToPinned(store, "D");
    }

    @Test
    void excludesInvocationsFromDifferentCaller() {
        StubStore store = new StubStore();
        store.recordSubPiInvocation("parent", "childX", "OTHER", "sub");
        store.recordSubPiInvocation("parent", "child1", "A", "sub");
        store.withOutputs("childX", "D", List.of(producedByD(TransformFixtures.map("name", "x"))));
        store.withOutputs("child1", "D", List.of(producedByD(TransformFixtures.map("name", "n1"))));

        List<InOutValueDto> out = resolve(store, edgeAB(pinnedRow()));

        assertEquals(1, out.size());
        assertEquals(List.of(Map.of("name", "n1")), out.get(0).getValue(),
                "only invocations whose callerNodeId equals the edge source (A) contribute");
        List<String> childReads = store.filteredCollects().stream()
                .map(StubStore.CollectCall::executionId)
                .filter(id -> !"parent".equals(id))
                .toList();
        assertEquals(List.of("child1"), childReads, "OTHER-caller childX must not be consulted");
        assertChildCollectsAreFilteredToPinned(store, "D");
    }

    @Test
    void resolvesNestedGrandchildWithoutCallerFilter() {
        StubStore store = new StubStore();
        store.recordSubPiInvocation("parent", "child1", "A", "sub");
        // Nested level has no caller filter — grandchild is included regardless of caller.
        store.recordSubPiInvocation("child1", "grandchild", "NESTED", "sub2");
        store.withOutputs("grandchild", "D", List.of(producedByD(TransformFixtures.map("name", "n1"))));

        List<InOutValueDto> out = resolve(store, edgeAB(pinnedRow()));

        assertEquals(1, out.size());
        assertEquals(List.of(Map.of("name", "n1")), out.get(0).getValue());
        assertEquals("B", out.get(0).getTargetNodeId());
        assertFalse(out.get(0).isGateway());
        List<String> childReads = store.filteredCollects().stream()
                .map(StubStore.CollectCall::executionId)
                .filter(id -> !"parent".equals(id))
                .toList();
        assertEquals(List.of("child1", "grandchild"), childReads);
        assertChildCollectsAreFilteredToPinned(store, "D");
    }

    @Test
    void skipsChildWherePinnedNodeAbsent() {
        StubStore store = new StubStore();
        store.recordSubPiInvocation("parent", "child1", "A", "sub");
        store.recordSubPiInvocation("parent", "child2", "A", "sub");
        store.recordSubPiInvocation("parent", "child3", "A", "sub");
        store.withOutputs("child1", "D", List.of(producedByD(TransformFixtures.map("name", "v1"))));
        store.withOutputs("child3", "D", List.of(producedByD(TransformFixtures.map("name", "v3"))));

        List<InOutValueDto> out = resolve(store, edgeAB(pinnedRow()));

        assertEquals(1, out.size());
        assertEquals(List.of(Map.of("name", "v1"), Map.of("name", "v3")), out.get(0).getValue(),
                "a child with no D outputs is skipped — the merge is not padded");
        assertChildCollectsAreFilteredToPinned(store, "D");
    }

    @Test
    void parentScopeHitTakesPrecedenceUnwrapped() {
        StubStore store = new StubStore();
        store.withOutputs("parent", "D", List.of(producedByD(TransformFixtures.map("name", "parent"))));
        store.recordSubPiInvocation("parent", "child1", "A", "sub");
        store.recordSubPiInvocation("parent", "child2", "A", "sub");
        store.withOutputs("child1", "D", List.of(producedByD(TransformFixtures.map("name", "n1"))));
        store.withOutputs("child2", "D", List.of(producedByD(TransformFixtures.map("name", "n2"))));

        List<InOutValueDto> out = resolve(store, edgeAB(pinnedRow()));

        assertEquals(1, out.size());
        InOutValueDto delivered = out.get(0);
        assertFalse(delivered.getValue() instanceof List,
                "parent-scope hit stays unwrapped — not list-wrapped");
        assertEquals(Map.of("name", "parent"), delivered.getValue());
        assertEquals("B", delivered.getTargetNodeId());
        assertFalse(delivered.isGateway());
        // Children must not be consulted once parent scope produced D.
        List<String> execIds = store.filteredCollects().stream()
                .map(StubStore.CollectCall::executionId)
                .toList();
        assertEquals(List.of("parent"), execIds);
        assertEquals(Set.of("D"), store.lastRequestedIds);
        assertEquals("parent", store.lastRequestedExecutionId);
        assertTrue(store.unfilteredCollects().isEmpty());
    }

    @Test
    void emptyIndexReturnsEmptyWithoutThrowing() {
        StubStore store = new StubStore();
        // Isolation pin: another execution's D must not leak into parent scope.
        store.withOutputs("other-exec", "D", List.of(producedByD(TransformFixtures.map("name", "leak"))));

        List<InOutValueDto> out = assertDoesNotThrow(
                () -> resolve(store, edgeAB(pinnedRow())));

        assertTrue(out.isEmpty());
        assertEquals("parent", store.lastRequestedExecutionId);
        assertEquals(Set.of("D"), store.lastRequestedIds);
        assertTrue(store.filteredCollects().stream().noneMatch(c -> !"parent".equals(c.executionId())));
        assertTrue(store.unfilteredCollects().isEmpty());
    }

    @Test
    void multiPortRowsEachMergeIntoOwnList() {
        EdgeTransformDto row1 = pinnedRow("out_d1", "in_b1");
        EdgeTransformDto row2 = pinnedRow("out_d2", "in_b2");
        StubStore store = new StubStore();
        store.recordSubPiInvocation("parent", "child1", "A", "sub");
        store.recordSubPiInvocation("parent", "child2", "A", "sub");
        store.withOutputs("child1", "D", List.of(
                producedByD("out_d1", TransformFixtures.map("a", 1), null),
                producedByD("out_d2", TransformFixtures.map("b", 2), null)));
        store.withOutputs("child2", "D", List.of(
                producedByD("out_d1", TransformFixtures.map("a", 11), null),
                producedByD("out_d2", TransformFixtures.map("b", 22), null)));

        List<InOutValueDto> out = resolve(store, edgeAB(row1, row2));

        assertEquals(2, out.size(), "one merged message per (node, source port, target port)");
        InOutValueDto d1 = out.get(0);
        InOutValueDto d2 = out.get(1);
        assertEquals("out_d1", d1.getPortId());
        assertEquals("out_d2", d2.getPortId());
        assertEquals(List.of(Map.of("a", 1), Map.of("a", 11)), d1.getValue());
        assertEquals(List.of(Map.of("b", 2), Map.of("b", 22)), d2.getValue());
        assertEquals("in_b1", d1.getTargetPortId());
        assertEquals("in_b2", d2.getTargetPortId());
        assertEquals("B", d1.getTargetNodeId());
        assertEquals("B", d2.getTargetNodeId());
        assertFalse(d1.isGateway());
        assertFalse(d2.isGateway());
        assertChildCollectsAreFilteredToPinned(store, "D");
        for (StubStore.CollectCall call : store.filteredCollects()) {
            if (!"parent".equals(call.executionId())) {
                assertEquals(Set.of("D"), call.nodeIds());
            }
        }
    }
}
