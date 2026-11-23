package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.InOutValueDto;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repair merge precedence ({@code ScriptDispatchDelegate#mergeNodeInputs}): the normal path keeps
 * "upstream wins" and only honours carried rows on the ref's minted node; the operator override
 * stored by {@code apply-inputs} for (run, node) folds LAST — it applies even when the dispatching
 * node is not the ref's minted node (a retried mid-flow node re-dispatches with its predecessor's
 * ref), it WINS over upstream on a binding-key overlap, newly added parameters land as new keys,
 * and an explicit empty override clears the inbound list.
 */
class ScriptDispatchDelegateMergeInputsTest {

    /** One input wire row; the binding key is nodeId|portId|portValue|targetNodeId|targetPortId. */
    private static InOutValueDto wire(String producer, String portValue, String targetNode,
                                      String targetPort, Object value) {
        InOutValueDto dto = new InOutValueDto();
        dto.setNodeId(producer);
        dto.setPortId("p-" + producer);
        dto.setPortValue(portValue);
        dto.setTargetNodeId(targetNode);
        dto.setTargetPortId(targetPort);
        dto.setValue(value);
        return dto;
    }

    @Test
    void normalPath_upstreamWins_onOverlap() {
        InOutValueDto carried = wire("entry", "prompt", "X", "in", "carried-value");
        InOutValueDto upstream = wire("entry", "prompt", "X", "in", "upstream-value");

        List<InOutValueDto> merged = ScriptDispatchDelegate.mergeNodeInputs(
                "X", "X", List.of(carried), List.of(upstream), null);

        assertEquals(1, merged.size());
        assertEquals("upstream-value", merged.get(0).getValue(), "normal path: upstream wins");
    }

    @Test
    void normalPath_ignoresCarried_whenNodeIsNotTheRefsMintedNode() {
        InOutValueDto carried = wire("W", "prompt", "X", "in", "carried-value");

        List<InOutValueDto> merged = ScriptDispatchDelegate.mergeNodeInputs(
                "X", "W", List.of(carried), List.of(), null);

        assertTrue(merged.isEmpty(), "carried rows only apply on the ref's minted node");
    }

    @Test
    void operatorOverride_foldsLastAndWins_evenWhenNotTheMintedNode() {
        // The retried failed node X re-dispatches with the ref minted for its predecessor W —
        // the operator's edited list (stored under run+X) must still reach X and WIN.
        InOutValueDto operatorRow = wire("W", "prompt", "X", "in", "operator-value");
        InOutValueDto upstreamRow = wire("W", "prompt", "X", "in", "upstream-value");

        List<InOutValueDto> merged = ScriptDispatchDelegate.mergeNodeInputs(
                "X", "W", List.of(), List.of(upstreamRow), List.of(operatorRow));

        assertEquals(1, merged.size());
        assertEquals("operator-value", merged.get(0).getValue(), "operator edit wins the overlap");
    }

    @Test
    void operatorOverride_newlyAddedParameters_landAsNewKeys() {
        InOutValueDto edited = wire("W", "prompt", "X", "in", "operator-value");
        InOutValueDto added = wire("W", "extra", "X", "in2", "added-value");
        InOutValueDto upstream = wire("W", "prompt", "X", "in", "upstream-value");

        List<InOutValueDto> merged = ScriptDispatchDelegate.mergeNodeInputs(
                "X", "W", List.of(), List.of(upstream), List.of(edited, added));

        assertEquals(2, merged.size(), "edited overlap collapses to one row; the added param is a new key");
        assertTrue(merged.stream().anyMatch(dto -> "operator-value".equals(dto.getValue())));
        assertTrue(merged.stream().anyMatch(dto -> "added-value".equals(dto.getValue())));
    }

    @Test
    void operatorOverride_emptyList_clearsEveryInput() {
        // The operator really cleared the node's inbound list: carried AND upstream must go.
        InOutValueDto upstream = wire("W", "prompt", "X", "in", "upstream-value");

        List<InOutValueDto> merged = ScriptDispatchDelegate.mergeNodeInputs(
                "X", "W", List.of(), List.of(upstream), List.of());

        assertTrue(merged.isEmpty(), "an explicit empty override wins over every other source");
    }

    @Test
    void operatorOverride_rowsAddressedToAnotherNode_stayFiltered() {
        // Defence in depth: even an operator override never leaks a row addressed to a different node.
        InOutValueDto foreign = wire("W", "prompt", "Y", "in", "foreign-value");

        List<InOutValueDto> merged = ScriptDispatchDelegate.mergeNodeInputs(
                "X", "W", List.of(), List.of(), List.of(foreign));

        assertTrue(merged.isEmpty());
    }

    @Test
    void operatorOverride_rows_areMarked_operatorSupplied() {
        // The transient flag is what the inbound port-schema filter keys off to exempt exactly the
        // operator's rows — carried/upstream rows must stay unmarked (they keep schema filtering).
        InOutValueDto operatorRow = wire("W", "prompt", "X", "in", "operator-value");
        InOutValueDto upstreamRow = wire("W", "extra", "X", "in2", "upstream-value");
        InOutValueDto carriedRow = wire("W", "carried", "X", "in3", "carried-value");

        List<InOutValueDto> merged = ScriptDispatchDelegate.mergeNodeInputs(
                "X", "X", List.of(carriedRow), List.of(upstreamRow), List.of(operatorRow));

        assertEquals(3, merged.size());
        assertTrue(flagOf(merged, "operator-value"), "override rows are marked operator-supplied");
        assertFalse(flagOf(merged, "upstream-value"), "upstream rows keep schema filtering");
        assertFalse(flagOf(merged, "carried-value"), "carried rows keep schema filtering");
    }

    private static boolean flagOf(List<InOutValueDto> rows, Object value) {
        return rows.stream()
                .filter(dto -> value.equals(dto.getValue()))
                .findFirst().orElseThrow()
                .isOperatorOverride();
    }
}
