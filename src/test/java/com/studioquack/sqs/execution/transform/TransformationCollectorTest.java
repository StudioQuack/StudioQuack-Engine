package com.studioquack.sqs.execution.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Collector contract: missing language ⇒ jolt (today's path); non-jolt / legacy script ⇒
 * kind=script only (omit jolt). No sandbox exec here.
 */
class TransformationCollectorTest {

    private final TransformationCollector collector =
            new TransformationCollector(new JoltTransformService(new ObjectMapper()));

    private ExecutionContext ctxWithEdge(EdgeDto edge) {
        NodeDto up = TransformFixtures.simpleNode("up", null, "result");
        NodeDto cur = TransformFixtures.simpleNode("cur", "prompt", "out");
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId("cur");
        ctx.setCurrentNode(cur);
        ctx.setNodes(new ArrayList<>(List.of(up, cur)));
        ctx.setEdges(new ArrayList<>(List.of(edge)));
        // Collector reads ctx.getCurrentNode(); resolve it the same way the pipeline does.
        ctx.createCurrentNode();
        return ctx;
    }

    private EdgeTransformDto mapping() {
        return TransformFixtures.transform("result", "prompt", "cur", "schema", null);
    }

    @Test
    void missingLanguage_withTransforms_emitsJoltOnly() {
        EdgeDto edge = TransformFixtures.edge("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(mapping())));

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxWithEdge(edge));

        assertEquals(1, specs.size());
        assertEquals("jolt", specs.get(0).getKind());
        assertTrue(specs.get(0).getJoltSpec() != null && !specs.get(0).getJoltSpec().isEmpty());
    }

    @Test
    void transformLanguagePython_withScript_omitsJolt() {
        EdgeDto edge = TransformFixtures.edgeWithScript("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(mapping())), "return payload", "javascript");
        edge.getData().setTransformLanguage("python");

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxWithEdge(edge));

        assertEquals(1, specs.size());
        assertEquals("script", specs.get(0).getKind());
        assertEquals("python", specs.get(0).getLanguage());
        assertEquals("return payload", specs.get(0).getScript());
    }

    @Test
    void legacyScriptWithoutTransformLanguage_emitsScriptOnly() {
        EdgeDto edge = TransformFixtures.edgeWithScript("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(mapping())), "return x", "typescript");

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxWithEdge(edge));

        assertEquals(1, specs.size());
        assertEquals("script", specs.get(0).getKind());
        assertEquals("typescript", specs.get(0).getLanguage());
    }

    @Test
    void legacyScriptWithoutLanguage_defaultsJavascript() {
        EdgeDto edge = TransformFixtures.edgeWithScript("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(mapping())), "return x", null);

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxWithEdge(edge));

        assertEquals(1, specs.size());
        assertEquals("script", specs.get(0).getKind());
        assertEquals("javascript", specs.get(0).getLanguage());
    }

    @Test
    void explicitJolt_withLeftoverScript_emitsJoltOnly() {
        // FE retains script body across tab switches; active language wins.
        EdgeDto edge = TransformFixtures.edgeWithScript("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(mapping())), "return leftover", "python");
        edge.getData().setTransformLanguage("jolt");
        edge.getData().setJoltNotation("prompt = result");

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxWithEdge(edge));

        assertEquals(1, specs.size());
        assertEquals("jolt", specs.get(0).getKind());
        // joltNotation is FE cache only — never copied onto TransformationSpecDto.
        assertEquals(null, specs.get(0).getScript());
    }

    @Test
    void joltNotation_doesNotAffectCollection() {
        EdgeDto edge = TransformFixtures.edge("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(mapping())));
        edge.getData().setJoltNotation("prompt = result");

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxWithEdge(edge));

        assertEquals(1, specs.size());
        assertEquals("jolt", specs.get(0).getKind());
    }

    @Test
    void foreignPinnedSource_emitsOwnSpecGroup() {
        // DuckTape Add-Node: a row pinned to node "pinned" rides the up→cur edge. It must become
        // its own spec so the pre-bake applies it to the pinned node's delivered inputList entry,
        // not to the primary wire's value.
        EdgeTransformDto pinned = TransformFixtures.transform("a", "prompt", "cur", "schema", null);
        pinned.setSourceNodeId("pinned");
        pinned.setSourcePortId("out_a");
        EdgeDto edge = TransformFixtures.edge("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(mapping(), pinned)));

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxWithEdge(edge));

        assertEquals(2, specs.size(), "one spec per source-node group");
        assertEquals("up", specs.get(0).getSourceNodeId(), "edge-source group first");
        assertEquals("result", specs.get(0).getSourcePortShortCode(),
                "primary group keeps the edge source handle short code");
        assertEquals("pinned", specs.get(1).getSourceNodeId());
        assertNull(specs.get(1).getSourcePortShortCode(),
                "foreign group matches the delivered entry by node alone (any of its ports)");
        assertEquals("prompt", specs.get(1).getTargetPortShortCode());
        assertEquals("input", specs.get(1).getDirection());
    }

    /**
     * A + gateway + B with both hops. Existing {@link #ctxWithEdge} only has up+cur —
     * do not change that helper; gateway tests need the A→gw edge so the producer walk
     * can stamp A's handle, not gw's.
     */
    private ExecutionContext ctxGatewayGraph(EdgeDto gwToB, NodeDto... extraNodes) {
        NodeDto a = TransformFixtures.simpleNode("A", null, "result");
        NodeDto gw = TransformFixtures.gatewayNode("gw");
        NodeDto b = TransformFixtures.simpleNode("B", "prompt", "out");
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId("B");
        ctx.setCurrentNode(b);
        List<NodeDto> nodes = new ArrayList<>(List.of(a, gw, b));
        for (NodeDto extra : extraNodes) {
            nodes.add(extra);
        }
        ctx.setNodes(nodes);
        ctx.setEdges(new ArrayList<>(List.of(
                TransformFixtures.edge("A", "gw", "OUTPUT-result", "INPUT-in", null),
                gwToB)));
        ctx.createCurrentNode();
        return ctx;
    }

    /** Gateway primary row (blank source) must stamp the real producer A / result, not gw / out. */
    @Test
    void gatewayPrimarySpec_stampedWithProducerNotGateway() {
        EdgeTransformDto primary = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        EdgeDto gwToB = TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(primary)));

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxGatewayGraph(gwToB));

        assertEquals(1, specs.size());
        assertEquals("jolt", specs.get(0).getKind());
        assertEquals("A", specs.get(0).getSourceNodeId());
        assertEquals("result", specs.get(0).getSourcePortShortCode());
        assertEquals("prompt", specs.get(0).getTargetPortShortCode());
    }

    /** Pinned C on a gateway edge stays its own group with a null source port. */
    @Test
    void gatewayPinnedGroup_stillCAndNullPort() {
        EdgeTransformDto primary = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        EdgeTransformDto pinned = TransformFixtures.transform("a", "prompt", "B", "schema", null);
        pinned.setSourceNodeId("C");
        EdgeDto gwToB = TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(primary, pinned)));
        NodeDto c = TransformFixtures.simpleNode("C", null, "out");

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxGatewayGraph(gwToB, c));

        assertEquals(2, specs.size());
        TransformationSpecDto producer = specBySource(specs, "A");
        TransformationSpecDto pinnedSpec = specBySource(specs, "C");
        assertNotNull(producer);
        assertEquals("result", producer.getSourcePortShortCode());
        assertNotNull(pinnedSpec);
        assertNull(pinnedSpec.getSourcePortShortCode());
    }

    /** Non-gateway up→cur primary stamp is unchanged (edge source + handle short code). */
    @Test
    void nonGatewayEdge_primarySpecUnchanged() {
        EdgeDto edge = TransformFixtures.edge("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(mapping())));

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxWithEdge(edge));

        assertEquals(1, specs.size());
        assertEquals("up", specs.get(0).getSourceNodeId());
        assertEquals("result", specs.get(0).getSourcePortShortCode());
    }

    /**
     * Routing-carrier rows ({@code condition} / {@code conditionExpression}) must not be fed
     * to {@code convertJoltSpec}. Only the data row emits a spec.
     */
    @Test
    void routingCarrierRows_omittedFromDataJolt() {
        EdgeTransformDto data = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        EdgeTransformDto condExpr = TransformFixtures.transform("conditionExpression", "unused", "B", "schema", null);
        EdgeTransformDto cond = TransformFixtures.transform("condition", "unused", "B", "schema", null);
        EdgeDto gwToB = TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(data, condExpr, cond)));

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxGatewayGraph(gwToB));

        assertEquals(1, specs.size(), "only the data row becomes a JOLT spec");
        assertFalse(shiftSpecHasSource(specs.get(0).getJoltSpec(), "condition"));
        assertFalse(shiftSpecHasSource(specs.get(0).getJoltSpec(), "conditionExpression"));
    }

    /** Carrier-only gw→B edge emits no data JOLT spec. */
    @Test
    void routingCarrierRows_only_emitZeroSpecs() {
        EdgeTransformDto condExpr = TransformFixtures.transform("conditionExpression", "unused", "B", "schema", null);
        EdgeTransformDto cond = TransformFixtures.transform("condition", "unused", "B", "schema", null);
        EdgeDto gwToB = TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(condExpr, cond)));

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxGatewayGraph(gwToB));

        assertEquals(0, specs.size());
    }

    /** Script lane on a gateway edge is re-keyed to the producer the same way JOLT is. */
    @Test
    void gatewayScriptSpec_rekeyedToProducer() {
        EdgeDto gwToB = TransformFixtures.edgeWithScript("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(), "return payload", "javascript");
        gwToB.getData().setTransformLanguage("javascript");

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxGatewayGraph(gwToB));

        assertEquals(1, specs.size());
        assertEquals("script", specs.get(0).getKind());
        assertEquals("A", specs.get(0).getSourceNodeId());
        assertEquals("result", specs.get(0).getSourcePortShortCode());
    }

    /**
     * Blank-source primary + a row already pinned to A must merge into one spec for A —
     * not a second A group.
     */
    @Test
    void gatewayPrimaryMergedWithPinnedSameProducer_oneSpec() {
        EdgeTransformDto primary = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        EdgeTransformDto pinnedA = TransformFixtures.transform("extra", "prompt", "B", "schema", null);
        pinnedA.setSourceNodeId("A");
        EdgeDto gwToB = TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(primary, pinnedA)));

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxGatewayGraph(gwToB));

        assertEquals(1, specs.size());
        assertEquals("A", specs.get(0).getSourceNodeId());
    }

    /**
     * DENY: a gateway's advisory routing row never becomes a data shift. {@code condition} /
     * {@code conditionExpression} are read by GatewayConditionCarriers to pick a branch, so
     * shifting one would publish the expression text as a mapped field.
     */
    @Test
    void gatewayRoutingCarrierRows_areOmittedFromTheDataSpec() {
        EdgeTransformDto condition = TransformFixtures.transform("condition", "prompt", "B", "schema", null);
        EdgeTransformDto legacy = TransformFixtures.transform("conditionExpression", "prompt", "B", "schema", null);
        EdgeTransformDto data = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        EdgeDto gwToB = TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(condition, legacy, data)));

        List<TransformationSpecDto> specs = collector.collectForCurrentNode(ctxGatewayGraph(gwToB));

        assertEquals(1, specs.size());
        TransformationSpecDto spec = specBySource(specs, "A");
        assertNotNull(spec, "the real data row still emits, keyed on the producer");
        assertTrue(shiftSpecHasSource(spec.getJoltSpec(), "result"));
        assertFalse(shiftSpecHasSource(spec.getJoltSpec(), "condition"));
        assertFalse(shiftSpecHasSource(spec.getJoltSpec(), "conditionExpression"));
    }

    private static TransformationSpecDto specBySource(List<TransformationSpecDto> specs, String sourceNodeId) {
        for (TransformationSpecDto spec : specs) {
            if (sourceNodeId.equals(spec.getSourceNodeId())) {
                return spec;
            }
        }
        return null;
    }

    /** True when a shift op's spec map has {@code sourceKey} as a top-level shift source. */
    private static boolean shiftSpecHasSource(List<Object> joltSpec, String sourceKey) {
        if (joltSpec == null) {
            return false;
        }
        for (Object op : joltSpec) {
            if (!(op instanceof Map<?, ?> map) || !"shift".equals(map.get("operation"))) {
                continue;
            }
            Object spec = map.get("spec");
            if (spec instanceof Map<?, ?> specMap && specMap.containsKey(sourceKey)) {
                return true;
            }
        }
        return false;
    }
}
