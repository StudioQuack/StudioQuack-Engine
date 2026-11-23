package com.studioquack.sqs.execution.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.SchemaDto;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.store.SubPiInvocation;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.ForeignEdgeInputResolver;
import com.studioquack.sqs.execution.transform.GenerateScriptService;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import com.studioquack.sqs.status.log.SandboxLogIoRedactor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The 2-phase inbound pipeline invariants (Mapping Invariants 1, 2, 3, 4, 10, 12, 14) exercised
 * against {@link DefaultTransformPipeline} with realistic node/edge fixtures.
 */
class MappingInvariantsPipelineTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JoltTransformService jolt = new JoltTransformService(mapper);
    // A real spiller over a mocked store: these fixtures are all far below the inline threshold, so it
    // must pass every value through untouched — which is itself part of the mapping invariant.
    private final DefaultTransformPipeline pipeline = new DefaultTransformPipeline(
            new ConnectionBuilder(jolt), new TransformationCollector(jolt), jolt, mapper,
            new com.studioquack.sqs.files.ParameterSpiller(
                    org.mockito.Mockito.mock(com.studioquack.sqs.files.ClientFileStore.class)));
    private final GenerateScriptService generateScript =
            new GenerateScriptService(mapper, language -> Optional.empty());

    /** cur ← up: an inbound edge shifting {@code result → prompt}, with one inbound value {result:x}. */
    private ExecutionContext inboundCtx(Object inboundValue) {
        NodeDto up = TransformFixtures.simpleNode("up", null, "result");
        NodeDto cur = TransformFixtures.simpleNode("cur", "prompt", "out");
        EdgeTransformDto t = TransformFixtures.transform("result", "prompt", "cur", "schema", null);
        EdgeDto e = TransformFixtures.edge("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(t)));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId("cur");
        ctx.setNodes(new ArrayList<>(List.of(up, cur)));
        ctx.setEdges(new ArrayList<>(List.of(e)));
        List<InOutValueDto> inbound = new ArrayList<>();
        inbound.add(TransformFixtures.input("up", "result", "cur", "prompt", inboundValue));
        ctx.setNextNodeInputList(inbound);
        return ctx;
    }

    /** Invariant 1: specs are collected while the graph is present, then the graph is cleared. */
    @Test
    void invariant1_precollect_happens_before_edge_clear() {
        ExecutionContext ctx = inboundCtx(TransformFixtures.map("result", "x"));
        pipeline.prepareInbound(ctx);

        assertNull(ctx.getEdges(), "edges must be cleared after prepare");
        assertNull(ctx.getNodes(), "nodes must be cleared after prepare");
        assertNotNull(ctx.getInboundTransformSpecs(), "specs must have been collected");
        assertFalse(ctx.getInboundTransformSpecs().isEmpty(),
                "non-empty specs prove collection ran BEFORE the edges were nulled");
    }

    /** Invariant 2: inbound JOLT is pre-baked onto inputList BEFORE the __SQ_CONTEXT__ bake. */
    @Test
    void invariant2_inbound_jolt_prebaked_before_context_bake() throws Exception {
        ExecutionContext ctx = inboundCtx(TransformFixtures.map("result", "x"));
        pipeline.prepareInbound(ctx);

        assertEquals(Map.of("prompt", "x"), ctx.getCurrentNode().getInputList().get(0).getValue(),
                "inputList value must be pre-baked (result→prompt) in place");

        String b64 = generateScript.bakeContextBase64(ctx);
        String json = new String(Base64.getDecoder().decode(b64));
        Map<?, ?> decoded = mapper.readValue(json, Map.class);
        Map<?, ?> currentNode = (Map<?, ?>) decoded.get("currentNode");
        List<?> inputList = (List<?>) currentNode.get("inputList");
        Object value = ((Map<?, ?>) inputList.get(0)).get("value");
        assertEquals(Map.of("prompt", "x"), value, "baked __SQ_CONTEXT__ must reflect the pre-baked value");
    }

    /** Invariant 3: remotely applied spec is stripped from the daemon directive by IdentityHashMap identity. */
    @Test
    void invariant3_applied_spec_stripped_by_identity_not_equals() {
        ExecutionContext ctx = inboundCtx(TransformFixtures.map("result", "x"));
        PreparedInbound prepared = pipeline.prepareInbound(ctx);

        assertEquals(1, prepared.beAppliedSpecs().size());
        assertTrue(prepared.daemonDirectives().isEmpty(), "applied spec must not be re-shipped to the sandbox");
        assertFalse(prepared.inputVariables().containsKey(TransformDirectiveApplier.DIRECTIVE_KEY));

        TransformationSpecDto applied = prepared.beAppliedSpecs().iterator().next();
        TransformationSpecDto valueEqualClone = TransformationSpecDto.builder()
                .kind(applied.getKind()).joltSpec(applied.getJoltSpec())
                .sourceNodeId(applied.getSourceNodeId()).targetNodeId(applied.getTargetNodeId())
                .sourcePortShortCode(applied.getSourcePortShortCode())
                .targetPortShortCode(applied.getTargetPortShortCode())
                .direction(applied.getDirection()).build();

        assertEquals(applied, valueEqualClone, "clone is value-equal (Lombok equals)");
        assertTrue(prepared.beAppliedSpecs().contains(applied), "identity hit on the exact applied instance");
        assertFalse(prepared.beAppliedSpecs().contains(valueEqualClone),
                "identity MISS on a value-equal clone — proves IdentityHashMap identity, not equals");
    }

    /** Invariant 4: an inbound JOLT with no matching inputList entry is NOT put on the wire. */
    @Test
    void invariant4_unmatched_inbound_jolt_not_emitted() {
        ExecutionContext ctx = inboundCtx(TransformFixtures.map("result", "x"));
        ctx.setNextNodeInputList(new ArrayList<>()); // node has NO inbound values (e.g. agent/root)
        PreparedInbound prepared = pipeline.prepareInbound(ctx);

        assertTrue(prepared.daemonDirectives().isEmpty(), "no matching input → drop the spec");
        assertFalse(prepared.inputVariables().containsKey(TransformDirectiveApplier.DIRECTIVE_KEY),
                "an unmatched inbound JOLT on the wire would trigger the daemon whole-ctx wipe");
    }

    /** Invariant 12: inputList flattens to a port map keyed targetPortValue||portValue, after pre-bake. */
    @Test
    void invariant12_port_flatten_prefers_targetPortValue() {
        ExecutionContext ctx = inboundCtx(TransformFixtures.map("result", "x"));
        // second entry with only a portValue → fallback key.
        ctx.getNextNodeInputList().add(TransformFixtures.input("up2", "foo", "cur", null, 42));
        PreparedInbound prepared = pipeline.prepareInbound(ctx);

        assertTrue(prepared.inputVariables().containsKey("prompt"), "targetPortValue wins as the port key");
        assertFalse(prepared.inputVariables().containsKey("result"), "source portValue must NOT be the key");
        assertTrue(prepared.inputVariables().containsKey("foo"), "fallback to portValue when targetPortValue is null");
        assertEquals(Map.of("prompt", "x"), prepared.inputVariables().get("prompt"),
                "flatten happens AFTER pre-bake (value is transformed)");
    }

    /** Invariant 10: function-port request+response lanes are emitted once at the caller node. */
    @Test
    void invariant10_function_port_transforms_single_owner_emit() {
        NodeDto cur = TransformFixtures.sandboxNode("cur", "prompt", "out", null);
        cur.getData().getPorts().add(TransformFixtures.functionPort("model",
                List.of(TransformFixtures.schema("x", "string")),
                List.of(TransformFixtures.schema("c", "string"))));
        NodeDto sub = TransformFixtures.simpleNode("sub", "main_input", "main_output");

        EdgeTransformDto req = TransformFixtures.transform("a", "x", "sub", "schema", null);
        EdgeTransformDto resp = TransformFixtures.transform("t", "c", "cur", "outputSchema", null);
        EdgeDto fn = TransformFixtures.edge("cur", "sub", "FUNCTION-model", "INPUT-main_input",
                new ArrayList<>(List.of(req, resp)));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-fn");
        ctx.setCurrentNodeId("cur");
        ctx.setNodes(new ArrayList<>(List.of(cur, sub)));
        ctx.setEdges(new ArrayList<>(List.of(fn)));
        ctx.setNextNodeInputList(new ArrayList<>());

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        assertTrue(prepared.functionPortTransforms().containsKey("model"), "port 'model' lanes collected");
        var lanes = prepared.functionPortTransforms().get("model");
        assertFalse(lanes.getRequest().isEmpty(), "request lane present");
        assertFalse(lanes.getResponse().isEmpty(), "response lane present");
        assertTrue(prepared.inputVariables().containsKey("__sq_function_port_transforms__"),
                "the single-owner directive is emitted exactly once, here at the caller");
    }

    /**
     * AI Agent {@code tools} response lane: MCP Server {@code status / data / http_status} mapped
     * onto the agent's {@code response} wrapper. Applied the way {@code HelperExecuteService}
     * applies it, the lane hands the agent all three fields under the wrapper.
     */
    @Test
    void function_port_response_wrapper_carries_every_mapped_field() {
        NodeDto agent = TransformFixtures.sandboxNode("agent", "main_input", "main_output", null);
        agent.getData().getPorts().add(TransformFixtures.functionPort("tools",
                List.of(TransformFixtures.schema("request", "wrapper")),
                List.of(TransformFixtures.schema("response", "wrapper"))));
        NodeDto mcp = TransformFixtures.simpleNode("mcp", "input", "output");
        List<EdgeTransformDto> rows = new ArrayList<>();
        for (String field : List.of("status", "data", "http_status")) {
            rows.add(TransformFixtures.transform(field, "response", "agent", "outputSchema", null));
        }
        EdgeDto fn = TransformFixtures.edge("agent", "mcp", "FUNCTION-tools", "INPUT-input", rows);

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-tools");
        ctx.setCurrentNodeId("agent");
        ctx.setNodes(new ArrayList<>(List.of(agent, mcp)));
        ctx.setEdges(new ArrayList<>(List.of(fn)));
        ctx.setNextNodeInputList(new ArrayList<>());

        List<TransformationSpecDto> response = pipeline.prepareInbound(ctx).functionPortTransforms()
                .get("tools").getResponse();
        Map<String, Object> data = TransformFixtures.map("tools", List.of(TransformFixtures.map("name", "ask")));
        Object toAgent = new TransformDirectiveApplier(jolt).applyToValue(response,
                TransformFixtures.map("status", "ok", "data", data, "http_status", 200)).value();

        assertEquals(TransformFixtures.map("response",
                TransformFixtures.map("status", "ok", "data", data, "http_status", 200)), toAgent);
    }

    /**
     * DuckTape Add-Node end-to-end: a pinned earlier node's delivered message is shaped by its
     * OWN JOLT group and lands on the port as a second list message, alongside the primary wire's.
     */
    @Test
    void addNode_foreignSource_messageShapedAlongsidePrimary() {
        NodeDto up = TransformFixtures.simpleNode("up", null, "result");
        NodeDto pinned = TransformFixtures.simpleNode("pinned", null, "out");
        NodeDto cur = TransformFixtures.simpleNode("cur", "prompt", "out");
        EdgeTransformDto primary = TransformFixtures.transform("result", "prompt", "cur", "schema", null);
        EdgeTransformDto foreign = TransformFixtures.transform("a", "prompt", "cur", "schema", null);
        foreign.setSourceNodeId("pinned");
        foreign.setSourcePortId("out_out");
        EdgeDto e = TransformFixtures.edge("up", "cur", "OUTPUT-result", "INPUT-prompt",
                new ArrayList<>(List.of(primary, foreign)));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId("cur");
        ctx.setNodes(new ArrayList<>(List.of(up, pinned, cur)));
        ctx.setEdges(new ArrayList<>(List.of(e)));
        List<InOutValueDto> inbound = new ArrayList<>();
        inbound.add(TransformFixtures.input("up", "result", "cur", "prompt", TransformFixtures.map("result", "x")));
        // The message ForeignEdgeInputResolver delivers for the pinned node (re-addressed clone).
        inbound.add(TransformFixtures.input("pinned", "out", "cur", "prompt", TransformFixtures.map("a", "A", "b", "B")));
        ctx.setNextNodeInputList(inbound);

        PreparedInbound prepared = pipeline.prepareInbound(ctx);

        List<InOutValueDto> inputs = ctx.getCurrentNode().getInputList();
        assertEquals(2, inputs.size(), "both messages ride the port's input list");
        assertEquals(Map.of("prompt", "x"), inputs.get(0).getValue(),
                "primary wire shaped by its own source group");
        assertEquals(Map.of("prompt", "A"), inputs.get(1).getValue(),
                "pinned node's message shaped by its own source group (a→prompt, b dropped)");
        assertEquals(2, prepared.beAppliedSpecs().size(), "both source groups applied pre-bake");
        assertTrue(prepared.daemonDirectives().isEmpty(), "nothing re-shipped to the sandbox");
    }

    /**
     * Function-port Add-Node: D lives only in child executions (parent has no D outputs).
     * Resolver list-wraps two invocation payloads; prepareInbound must JOLT {@code name → prompt}
     * over that list root — not leave a bare list of maps.
     */
    @Test
    void addNode_functionPortForeignSource_twoInvocations_joltCollectsNames() {
        NodeDto caller = TransformFixtures.simpleNode("A", null, "out");
        NodeDto consumer = TransformFixtures.simpleNode("B", "prompt", "out");
        NodeDto child = TransformFixtures.simpleNode("D", null, "d");

        // Only the pinned row — no primary A payload — so the shaped value is unambiguous.
        EdgeTransformDto pinned = TransformFixtures.transform("name", "prompt", "B", "schema", null);
        pinned.setSourceNodeId("D");
        pinned.setSourcePortId("out_d");
        pinned.setTargetPortId("in_prompt");
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-prompt",
                new ArrayList<>(List.of(pinned)));
        List<EdgeDto> edges = new ArrayList<>(List.of(edge));

        FunctionPortStoreStub store = new FunctionPortStoreStub()
                .withInvocation("exec-1", new SubPiInvocation("child-1", "A", "D"))
                .withInvocation("exec-1", new SubPiInvocation("child-2", "A", "D"))
                .withOutputs("child-1", "D", producedByD(TransformFixtures.map("name", "name1")))
                .withOutputs("child-2", "D", producedByD(TransformFixtures.map("name", "name2")));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId("B");
        ctx.setNodes(new ArrayList<>(List.of(caller, consumer, child)));
        ctx.setEdges(edges);

        ProcessContextRef ref = ProcessContextRef.builder().executionId("exec-1").build();
        // Same sequence as ScriptDispatchDelegate: resolve, merge onto nextNodeInputList, then prepare.
        List<InOutValueDto> foreign = ForeignEdgeInputResolver.resolve(store, ref, "B", edges, null, null);
        ctx.setNextNodeInputList(mergeInbound(ctx.getNextNodeInputList(), foreign));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);

        List<InOutValueDto> inputs = ctx.getCurrentNode().getInputList();
        assertEquals(1, inputs.size(), "D's list-wrapped message is the only inbound");
        assertEquals("D", inputs.get(0).getNodeId());
        assertFalse(inputs.get(0).getValue() instanceof List,
                "shift must have run — do not leave a bare list of maps");
        assertEquals(Map.of("prompt", List.of("name1", "name2")), inputs.get(0).getValue(),
                "name → prompt over two invocations collects in order");
        assertEquals(Map.of("prompt", List.of("name1", "name2")), prepared.inputVariables().get("prompt"));
        assertEquals(1, prepared.beAppliedSpecs().size(), "D's source group applied pre-bake");
        assertTrue(prepared.daemonDirectives().isEmpty(), "nothing re-shipped to the sandbox");
    }

    /** Invariant 14: finalizeOutputs does port fan-out + schema filter ONLY — never outbound JOLT. */
    @Test
    void invariant14_finalize_outputs_filters_schema_but_applies_no_jolt() {
        // Output port "out" schema allows only "a"; an outbound spec (that WOULD rename a→renamed) is present.
        List<SchemaDto> outSchema = List.of(TransformFixtures.schema("a", "integer"));
        NodeDto cur = TransformFixtures.sandboxNode("cur", "prompt", "out", outSchema);
        cur.setSpecListForNodeOutput(new ArrayList<>(List.of(
                new com.studioquack.common.dto.flow.SpecListForNodeOutputDto(
                        "cur", TransformFixtures.shiftSpec("a", "renamed")))));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNodeId("cur");
        ctx.setCurrentNode(cur);

        Map<String, Object> sandboxOutputs = TransformFixtures.map(
                "out", TransformFixtures.map("a", 1, "b", 2),
                "$stdout", "logs", "exitCode", 0);
        pipeline.finalizeOutputs(ctx, sandboxOutputs);

        List<InOutValueDto> outputs = cur.getOutputList();
        assertEquals(1, outputs.size(), "metadata keys ($stdout/exitCode) are not fanned out");
        Object value = outputs.get(0).getValue();
        assertEquals(Map.of("a", 1), value,
                "schema filter drops 'b'; NO outbound JOLT ran (would have renamed a→renamed)");
    }

    /**
     * output→input: producer fan-out carries source-shaped unfilteredValue. After JOLT
     * {@code a → x} the logged input is the mapped payload; source keys are not unknown.
     */
    @Test
    void inboundLog_postJoltValue_notProducerUnfiltered() {
        NodeDto producer = TransformFixtures.simpleNode("producer", null, "out");
        NodeDto consumer = TransformFixtures.sandboxNode("consumer", "in", "out", null);
        com.studioquack.common.dto.flow.PortDto inPort = consumer.getData().findPortByPortName("in");
        inPort.setType(PortType.INPUT);
        inPort.setSchema(List.of(
                TransformFixtures.schema("x", "string"),
                TransformFixtures.schema("y", "string"),
                TransformFixtures.schema("z", "string")));

        EdgeTransformDto t = TransformFixtures.transform("a", "x", "consumer", "schema", null);
        EdgeDto e = TransformFixtures.edge("producer", "consumer", "OUTPUT-out", "INPUT-in",
                new ArrayList<>(List.of(t)));

        Map<String, Object> source = TransformFixtures.map("a", "from-a", "b", "from-b");
        InOutValueDto inbound = TransformFixtures.input("producer", "out", "consumer", "in", source);
        inbound.setUnfilteredValue(TransformFixtures.map("a", "from-a", "b", "from-b"));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-io");
        ctx.setCurrentNodeId("consumer");
        ctx.setNodes(new ArrayList<>(List.of(producer, consumer)));
        ctx.setEdges(new ArrayList<>(List.of(e)));
        ctx.setNextNodeInputList(new ArrayList<>(List.of(inbound)));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        InOutValueDto logged = ctx.getCurrentNode().getInputList().get(0);
        assertEquals(Map.of("x", "from-a"), logged.getValue());
        assertTrue(ctx.getDroppedInputParameters() == null || ctx.getDroppedInputParameters().isEmpty(),
                "JOLT-consumed source keys are not unknown parameters");
        assertEquals(Map.of("x", "from-a"), prepared.inputVariables().get("in"));

        Object redacted = SandboxLogIoRedactor.redactInput(mapper, ctx.getCurrentNode().getInputList());
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) ((List<?>) redacted).get(0);
        assertEquals(Map.of("x", "from-a"), row.get("value"));
        assertFalse(((Map<?, ?>) row.get("value")).containsKey("a"));
        assertFalse(((Map<?, ?>) row.get("value")).containsKey("b"));
        assertFalse(row.containsKey("unfilteredValue"),
                "the producer envelope is dropped on the logged row, not shipped beside value");
        assertNotNull(logged.getUnfilteredValue(),
                "the live inbound envelope survives — DoclingDelegate.wireValue recovers file payloads from it");
    }

    /**
     * Gateway look-through: A's {@code {result:x}} rides a gw→B PRIMARY {@code result→prompt} row
     * (blank sourceNodeId). After prepare + port-schema filter the entry is {@code {prompt:x}} and
     * exactly one spec applied.
     */
    @Test
    void gatewayPrimaryMapping_lookThroughEntry_mapsAndSurvivesPortSchemaFilter() {
        NodeDto a = TransformFixtures.simpleNode("A", null, "result");
        NodeDto gw = TransformFixtures.gatewayNode("gw");
        NodeDto b = consumerBWithPromptSchema();
        EdgeTransformDto primary = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        ExecutionContext ctx = gatewayInboundCtx(List.of(a, gw, b), List.of(
                TransformFixtures.edge("A", "gw", "OUTPUT-result", "INPUT-in", null),
                TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                        new ArrayList<>(List.of(primary)))),
                List.of(TransformFixtures.lookThrough("A", "result", "B", "prompt",
                        TransformFixtures.map("result", "x"))));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        assertEquals(Map.of("prompt", "x"), ctx.getCurrentNode().getInputList().get(0).getValue());
        assertEquals(1, prepared.beAppliedSpecs().size());
        assertEquals(Map.of("prompt", "x"), prepared.inputVariables().get("prompt"));
    }

    /**
     * Primary look-through (A) and pinned C each keep their own JOLT group. Specs must not
     * cross-apply: A's message is not shaped by C's row and C's message is not shaped by A's.
     */
    @Test
    void gatewayPrimaryPlusPinnedC_bothMessagesShaped_specsDoNotCrossApply() {
        NodeDto a = TransformFixtures.simpleNode("A", null, "result");
        NodeDto c = TransformFixtures.simpleNode("C", null, "out");
        NodeDto gw = TransformFixtures.gatewayNode("gw");
        NodeDto b = consumerBWithPromptSchema();
        EdgeTransformDto primary = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        EdgeTransformDto pinned = TransformFixtures.transform("a", "prompt", "B", "schema", null);
        pinned.setSourceNodeId("C");
        ExecutionContext ctx = gatewayInboundCtx(List.of(a, c, gw, b), List.of(
                TransformFixtures.edge("A", "gw", "OUTPUT-result", "INPUT-in", null),
                TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                        new ArrayList<>(List.of(primary, pinned)))),
                List.of(
                        TransformFixtures.lookThrough("A", "result", "B", "prompt",
                                TransformFixtures.map("result", "x")),
                        TransformFixtures.input("C", "out", "B", "prompt",
                                TransformFixtures.map("a", "from-c"))));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        InOutValueDto aEntry = entryByNodeId(ctx, "A");
        InOutValueDto cEntry = entryByNodeId(ctx, "C");
        assertEquals(Map.of("prompt", "x"), aEntry.getValue());
        assertEquals(Map.of("prompt", "from-c"), cEntry.getValue());
        assertEquals(2, prepared.beAppliedSpecs().size());

        assertNotEquals(Map.of("prompt", "from-c"), aEntry.getValue());
        assertFalse(((Map<?, ?>) aEntry.getValue()).containsKey("a"));
        assertNotEquals(Map.of("prompt", "x"), cEntry.getValue());
        assertFalse(((Map<?, ?>) cEntry.getValue()).containsKey("result"));

        for (TransformationSpecDto spec : prepared.beAppliedSpecs()) {
            if ("C".equals(spec.getSourceNodeId())) {
                assertNotEquals(aEntry.getNodeId(), spec.getSourceNodeId());
            }
            if ("A".equals(spec.getSourceNodeId())) {
                assertNotEquals(cEntry.getNodeId(), spec.getSourceNodeId());
            }
        }
    }

    /**
     * A → gw1 → gw2 → B: the look-through producer is A. The gw2→B primary row must stamp
     * and match A's entry, not a gateway id.
     */
    @Test
    void gatewayTransitiveTwoGateways_resolvesToA() {
        NodeDto a = TransformFixtures.simpleNode("A", null, "result");
        NodeDto gw1 = TransformFixtures.gatewayNode("gw1");
        NodeDto gw2 = TransformFixtures.gatewayNode("gw2");
        NodeDto b = consumerBWithPromptSchema();
        EdgeTransformDto primary = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        ExecutionContext ctx = gatewayInboundCtx(List.of(a, gw1, gw2, b), List.of(
                TransformFixtures.edge("A", "gw1", "OUTPUT-result", "INPUT-in", null),
                TransformFixtures.edge("gw1", "gw2", "OUTPUT-out", "INPUT-in", null),
                TransformFixtures.edge("gw2", "B", "OUTPUT-out", "INPUT-prompt",
                        new ArrayList<>(List.of(primary)))),
                List.of(TransformFixtures.lookThrough("A", "result", "B", "prompt",
                        TransformFixtures.map("result", "x"))));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        assertEquals(Map.of("prompt", "x"), ctx.getCurrentNode().getInputList().get(0).getValue());
        assertEquals(1, prepared.beAppliedSpecs().size());
        assertEquals("A", prepared.beAppliedSpecs().iterator().next().getSourceNodeId());
    }

    /**
     * Fan-in: A1 and A2 both hop gw→B. Each look-through entry is shaped by its own producer
     * spec — A1 is not "two" and A2 is not "one".
     */
    @Test
    void gatewayFanIn_eachProducerSpecMatchesOnlyOwnEntry() {
        NodeDto a1 = TransformFixtures.simpleNode("A1", null, "result");
        NodeDto a2 = TransformFixtures.simpleNode("A2", null, "result");
        NodeDto gw = TransformFixtures.gatewayNode("gw");
        NodeDto b = consumerBWithPromptSchema();
        EdgeTransformDto primary = TransformFixtures.transform("result", "prompt", "B", "schema", null);
        ExecutionContext ctx = gatewayInboundCtx(List.of(a1, a2, gw, b), List.of(
                TransformFixtures.edge("A1", "gw", "OUTPUT-result", "INPUT-in", null),
                TransformFixtures.edge("A2", "gw", "OUTPUT-result", "INPUT-in", null),
                TransformFixtures.edge("gw", "B", "OUTPUT-out", "INPUT-prompt",
                        new ArrayList<>(List.of(primary)))),
                List.of(
                        TransformFixtures.lookThrough("A1", "result", "B", "prompt",
                                TransformFixtures.map("result", "one")),
                        TransformFixtures.lookThrough("A2", "result", "B", "prompt",
                                TransformFixtures.map("result", "two"))));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        InOutValueDto a1Entry = entryByNodeId(ctx, "A1");
        InOutValueDto a2Entry = entryByNodeId(ctx, "A2");
        assertEquals(Map.of("prompt", "one"), a1Entry.getValue());
        assertEquals(Map.of("prompt", "two"), a2Entry.getValue());
        assertEquals(2, prepared.beAppliedSpecs().size());
        Set<String> sources = new HashSet<>();
        for (TransformationSpecDto spec : prepared.beAppliedSpecs()) {
            sources.add(spec.getSourceNodeId());
        }
        assertEquals(Set.of("A1", "A2"), sources);

        assertNotEquals(Map.of("prompt", "two"), a1Entry.getValue());
        assertNotEquals(Map.of("prompt", "one"), a2Entry.getValue());
    }

    /**
     * B's prompt port is INPUT with {@code prompt:string} so {@code filterInboundAgainstPortSchemas}
     * prunes unmapped producer keys. An empty schema is fail-open.
     */
    private static NodeDto consumerBWithPromptSchema() {
        NodeDto b = TransformFixtures.simpleNode("B", "prompt", "out");
        com.studioquack.common.dto.flow.PortDto prompt = b.getData().findPortByPortName("prompt");
        prompt.setType(PortType.INPUT);
        prompt.setSchema(List.of(TransformFixtures.schema("prompt", "string")));
        return b;
    }

    private static ExecutionContext gatewayInboundCtx(List<NodeDto> nodes, List<EdgeDto> edges,
                                                      List<InOutValueDto> inbound) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-gw");
        ctx.setCurrentNodeId("B");
        ctx.setNodes(new ArrayList<>(nodes));
        ctx.setEdges(new ArrayList<>(edges));
        ctx.setNextNodeInputList(new ArrayList<>(inbound));
        return ctx;
    }

    private static InOutValueDto entryByNodeId(ExecutionContext ctx, String nodeId) {
        for (InOutValueDto in : ctx.getCurrentNode().getInputList()) {
            if (nodeId.equals(in.getNodeId())) {
                return in;
            }
        }
        throw new AssertionError("no inputList entry for nodeId=" + nodeId);
    }

    /** Existing inbound first, then foreign — the merge ScriptDispatchDelegate applies. */
    private static List<InOutValueDto> mergeInbound(List<InOutValueDto> existing, List<InOutValueDto> foreign) {
        List<InOutValueDto> merged = new ArrayList<>();
        if (existing != null) {
            merged.addAll(existing);
        }
        if (foreign != null) {
            merged.addAll(foreign);
        }
        return merged;
    }

    /** D's fanned-out port must be {@code out_d} so the pinned row matches. */
    private static InOutValueDto producedByD(Object value) {
        InOutValueDto dto = new InOutValueDto();
        dto.setNodeId("D");
        dto.setPortId("out_d");
        dto.setPortValue("d");
        dto.setValue(value);
        return dto;
    }

    /**
     * Per-execution output + invocation index. Parent {@code exec-1} has no D outputs so the
     * resolver falls through to the function-port children.
     */
    private static final class FunctionPortStoreStub implements ExecutionContextStore {
        private final Map<String, Map<String, List<InOutValueDto>>> outputsByExec = new LinkedHashMap<>();
        private final Map<String, List<SubPiInvocation>> invocationsByParent = new LinkedHashMap<>();

        FunctionPortStoreStub withOutputs(String executionId, String nodeId, InOutValueDto output) {
            outputsByExec.computeIfAbsent(executionId, k -> new LinkedHashMap<>())
                    .put(nodeId, List.of(output));
            return this;
        }

        FunctionPortStoreStub withInvocation(String parentExecutionId, SubPiInvocation invocation) {
            invocationsByParent.computeIfAbsent(parentExecutionId, k -> new ArrayList<>()).add(invocation);
            return this;
        }

        @Override
        public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId) {
            Map<String, List<InOutValueDto>> byNode = outputsByExec.get(executionId);
            return byNode == null ? Map.of() : byNode;
        }

        @Override
        public List<SubPiInvocation> subPiInvocationsOf(String parentExecutionId) {
            return invocationsByParent.getOrDefault(parentExecutionId, List.of());
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
}
