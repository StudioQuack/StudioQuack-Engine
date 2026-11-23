package com.studioquack.sqs.execution.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.SchemaDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DefaultTransformPipeline#filterInboundAgainstPortSchemas} — inbound port-schema pruning
 * after JOLT / edge-transform pre-steps.
 */
class InboundPortSchemaFilterTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JoltTransformService jolt = new JoltTransformService(mapper);
    private final DefaultTransformPipeline pipeline = new DefaultTransformPipeline(
            new ConnectionBuilder(jolt), new TransformationCollector(jolt), jolt, mapper,
            new com.studioquack.sqs.files.ParameterSpiller(
                    org.mockito.Mockito.mock(com.studioquack.sqs.files.ClientFileStore.class)));

    /** Consumer node with a {@code prompt} input port schema declaring only {@code prompt}. */
    private static NodeDto consumerWithPromptSchema() {
        SchemaDto promptField = TransformFixtures.schema("prompt", "string");
        PortDto in = new PortDto();
        in.setId("in_prompt");
        in.setName("prompt");
        in.setValue("prompt");
        in.setType(PortType.INPUT);
        in.setSchema(List.of(promptField));
        TemplateDto template = new TemplateDto();
        template.setBpmnType("custom.sandbox");
        template.setPorts(new ArrayList<>(List.of(in)));
        NodeDto node = new NodeDto();
        node.setId("agentB");
        node.setData(template);
        return node;
    }

    private ExecutionContext ctxWithInbound(Object value) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId("agentB");
        ctx.setNodes(new ArrayList<>(List.of(consumerWithPromptSchema())));
        ctx.setNextNodeInputList(new ArrayList<>(List.of(
                TransformFixtures.input("agentA", "result", "agentB", "prompt", value))));
        ctx.createCurrentNode();
        return ctx;
    }

    private PreparedInbound preparedFromInputList(ExecutionContext ctx) {
        Map<String, Object> vars = new LinkedHashMap<>();
        for (InOutValueDto in : ctx.getCurrentNode().getInputList()) {
            vars.put("prompt", in.getValue());
        }
        return new PreparedInbound(vars, List.of(), Map.of(), java.util.Set.of());
    }

    /** (a) Post-JOLT sibling keys outside the target port schema are dropped; mapped prompt kept. */
    @Test
    void prunesExtraSiblingKeys_afterJoltStyleMapping() {
        ExecutionContext ctx = ctxWithInbound(TransformFixtures.map("prompt", "hello", "voices", List.of("x")));
        PreparedInbound prepared = preparedFromInputList(ctx);

        PreparedInbound filtered = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        assertEquals(Map.of("prompt", "hello"), ctx.getCurrentNode().getInputList().get(0).getValue());
        assertEquals(Map.of("prompt", "hello"), filtered.inputVariables().get("prompt"));
        assertEquals(List.of("prompt.voices"), ctx.getDroppedInputParameters());
    }

    /** (b) Empty schema port passes through verbatim. */
    @Test
    void emptySchema_passesThroughVerbatim() {
        NodeDto node = TransformFixtures.simpleNode("cur", "prompt", "out");
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNodeId("cur");
        ctx.setNodes(new ArrayList<>(List.of(node)));
        Map<String, Object> wide = TransformFixtures.map("prompt", "x", "voices", 1);
        ctx.setNextNodeInputList(new ArrayList<>(List.of(
                TransformFixtures.input("up", "result", "cur", "prompt", wide))));
        ctx.createCurrentNode();
        PreparedInbound prepared = new PreparedInbound(Map.of("prompt", wide), List.of(), Map.of(), java.util.Set.of());

        PreparedInbound filtered = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        assertEquals(wide, ctx.getCurrentNode().getInputList().get(0).getValue());
        assertEquals(wide, filtered.inputVariables().get("prompt"));
        assertTrue(ctx.getDroppedInputParameters().isEmpty());
    }

    /** (c) Non-Map value passes through. */
    @Test
    void nonMapValue_passesThrough() {
        ExecutionContext ctx = ctxWithInbound("plain-string");
        PreparedInbound prepared = preparedFromInputList(ctx);

        PreparedInbound filtered = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        assertEquals("plain-string", ctx.getCurrentNode().getInputList().get(0).getValue());
        assertEquals("plain-string", filtered.inputVariables().get("prompt"));
        assertTrue(ctx.getDroppedInputParameters().isEmpty());
    }

    /** (d) Nothing dropped → empty list on context; dropped names sorted when present. */
    @Test
    void droppedNames_emptyWhenNothingDropped_sortedWhenPresent() {
        ExecutionContext ctx = ctxWithInbound(Map.of("prompt", "only"));
        pipeline.filterInboundAgainstPortSchemas(ctx, preparedFromInputList(ctx));
        assertTrue(ctx.getDroppedInputParameters().isEmpty());

        ExecutionContext ctx2 = ctxWithInbound(TransformFixtures.map("prompt", "a", "zebra", 1, "alpha", 2));
        pipeline.filterInboundAgainstPortSchemas(ctx2, preparedFromInputList(ctx2));
        assertEquals(List.of("prompt.alpha", "prompt.zebra"), ctx2.getDroppedInputParameters());
    }

    /** (e) Nested schema children recurse — nested extras stripped inside the allowed envelope key. */
    @Test
    void nestedSchema_recursesIntoChildren() {
        SchemaDto nestedAllowed = TransformFixtures.schema("body", "string");
        SchemaDto envelope = TransformFixtures.schema("payload", "object");
        envelope.setChildren(List.of(nestedAllowed));
        PortDto in = new PortDto();
        in.setId("in_msg");
        in.setName("msg");
        in.setValue("msg");
        in.setType(PortType.INPUT);
        in.setSchema(List.of(envelope));
        TemplateDto template = new TemplateDto();
        template.setPorts(new ArrayList<>(List.of(in)));
        NodeDto node = new NodeDto();
        node.setId("n1");
        node.setData(template);

        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("body", "text");
        inner.put("debug", "strip-me");
        Map<String, Object> value = new LinkedHashMap<>(Map.of("payload", inner));

        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(node);
        node.setInputList(new ArrayList<>(List.of(
                TransformFixtures.input("up", "out", "n1", "msg", value))));
        PreparedInbound prepared = new PreparedInbound(Map.of("msg", value), List.of(), Map.of(), java.util.Set.of());

        pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        Object filteredValue = ctx.getCurrentNode().getInputList().get(0).getValue();
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) ((Map<?, ?>) filteredValue).get("payload");
        assertEquals(Map.of("body", "text"), payload);
        assertTrue(ctx.getDroppedInputParameters().isEmpty(),
                "only nested keys were dropped — top-level port map keys unchanged");
    }

    /** (g): an operator-override row is exempt — every field passes, nothing reported. */
    @Test
    void operatorOverrideRow_passesVerbatim_andIsNeverReported() {
        ExecutionContext ctx = ctxWithInbound(TransformFixtures.map("prompt", "hello", "voices", List.of("x")));
        ctx.getCurrentNode().getInputList().get(0).setOperatorOverride(true);
        PreparedInbound prepared = preparedFromInputList(ctx);

        PreparedInbound filtered = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        assertEquals(TransformFixtures.map("prompt", "hello", "voices", List.of("x")),
                ctx.getCurrentNode().getInputList().get(0).getValue(),
                "operator-added fields reach the node unstripped");
        assertEquals(TransformFixtures.map("prompt", "hello", "voices", List.of("x")),
                filtered.inputVariables().get("prompt"),
                "operator-added fields reach the port map (and the sandbox-log input)");
        assertTrue(ctx.getDroppedInputParameters().isEmpty(),
                "operator-supplied fields are never reported as unknown parameters");
    }

    /** (h) …while an unflagged sibling row on the SAME node keeps today's filtering + reporting. */
    @Test
    void operatorRow_exemptWhileSiblingUpstreamRow_isStillFiltered() {
        NodeDto node = consumerWithPromptSchema();
        PortDto cfg = new PortDto();
        cfg.setId("in_cfg");
        cfg.setName("cfg");
        cfg.setValue("cfg");
        cfg.setType(PortType.INPUT);
        cfg.setSchema(List.of(TransformFixtures.schema("model", "string")));
        node.getData().getPorts().add(cfg);

        InOutValueDto operatorRow = TransformFixtures.input("op", "out", "agentB", "prompt",
                TransformFixtures.map("prompt", "hi", "voices", 1));
        operatorRow.setOperatorOverride(true);
        InOutValueDto upstreamRow = TransformFixtures.input("up", "out", "agentB", "cfg",
                TransformFixtures.map("model", "gpt", "debug", 1));
        node.setInputList(new ArrayList<>(List.of(operatorRow, upstreamRow)));
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNode(node);

        pipeline.filterInboundAgainstPortSchemas(ctx,
                new PreparedInbound(Map.of(), List.of(), Map.of(), java.util.Set.of()));

        assertEquals(TransformFixtures.map("prompt", "hi", "voices", 1), operatorRow.getValue(),
                "the operator row keeps its added field");
        assertEquals(Map.of("model", "gpt"), upstreamRow.getValue(),
                "the non-operator row is still pruned to the declared schema");
        assertEquals(List.of("cfg.debug"), ctx.getDroppedInputParameters(),
                "only the non-operator row's dropped field is reported");
    }

    /**
     * (f) Simulates post-edge-transform-pre-step shape: extra keys present after a script hop,
     * filter invoked on the prepared inbound (same call site as ScriptDispatchDelegate).
     */
    @Test
    void postEdgeTransformOutput_prunedWhenFilterRunsAfterEdgeStep() {
        ExecutionContext ctx = ctxWithInbound(TransformFixtures.map("prompt", "from-script", "trace", "drop"));
        Map<String, Object> inputVars = new LinkedHashMap<>(Map.of("prompt",
                ctx.getCurrentNode().getInputList().get(0).getValue()));
        inputVars.put(TransformDirectiveApplier.DIRECTIVE_KEY, List.of());
        PreparedInbound postEdge = new PreparedInbound(inputVars, List.of(), Map.of(), java.util.Set.of());

        PreparedInbound filtered = pipeline.filterInboundAgainstPortSchemas(ctx, postEdge);

        assertEquals(Map.of("prompt", "from-script"), filtered.inputVariables().get("prompt"));
        assertNull(filtered.inputVariables().get("trace"));
        assertEquals(List.of("prompt.trace"), ctx.getDroppedInputParameters());
        assertTrue(filtered.inputVariables().containsKey(TransformDirectiveApplier.DIRECTIVE_KEY));
    }
}
