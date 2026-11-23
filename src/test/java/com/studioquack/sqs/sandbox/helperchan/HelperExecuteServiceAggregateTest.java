package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ConnectionDetailDto;
import com.studioquack.common.dto.flow.ConnectionDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.SchemaDto;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.pipeline.DefaultTransformPipeline;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Provider return-path harvest: a designer wire {@code llm_output → model} stamps
 * {@code targetPortValue=model}. The aggregator must keep that value (count=1), not drop
 * it because the target is neither empty nor {@code end_input}.
 */
class HelperExecuteServiceAggregateTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JoltTransformService jolt = new JoltTransformService(mapper);
    private final DefaultTransformPipeline pipeline = new DefaultTransformPipeline(
            new ConnectionBuilder(jolt), new TransformationCollector(jolt), jolt, mapper,
            new com.studioquack.sqs.files.ParameterSpiller(
                    org.mockito.Mockito.mock(com.studioquack.sqs.files.ClientFileStore.class)));

    @Test
    void wiredLlmOutputToModelIsKept() {
        InOutValueDto dto = value("model", "provider-text");

        assertEquals(List.of("provider-text"),
                HelperExecuteService.filterEndInputValues(List.of(dto), "model"));

        List<Object> aggregated = newService(mock(ExecutionContextStore.class))
                .aggregateSubPiResult(Map.of("provider_output_1", List.of(dto)),
                        "child-wfe", "provider-node", "model");
        assertEquals(1, aggregated.size());
        assertEquals("provider-text", aggregated.get(0));
    }

    @Test
    void unwiredEmptyAndEndInputStillKept() {
        List<InOutValueDto> outputs = List.of(
                value(null, "null-target"),
                value("", "empty-target"),
                value("end_input", "end-input-target"));

        assertEquals(List.of("null-target", "empty-target", "end-input-target"),
                HelperExecuteService.filterEndInputValues(outputs, "model"));
        assertEquals(List.of("null-target", "empty-target", "end-input-target"),
                HelperExecuteService.filterEndInputValues(outputs, null));
    }

    @Test
    void multipleWiresKeepInvokedPortOnly() {
        List<InOutValueDto> outputs = List.of(
                value("model", "first"),
                value("model", "second"),
                value("thoughts", "dropped"));

        assertEquals(List.of("first", "second"),
                HelperExecuteService.filterEndInputValues(outputs, "model"));
    }

    @Test
    void invokedPortMatchIsCaseInsensitive() {
        assertEquals(List.of("kept"),
                HelperExecuteService.filterEndInputValues(List.of(value("Model", "kept")), "model"));
        assertEquals(List.of("kept"),
                HelperExecuteService.filterEndInputValues(List.of(value("model", "kept")), "Model"));
    }

    @Test
    void blankInvokedPortKeepsOnlyLegacyTargets() {
        List<InOutValueDto> outputs = List.of(
                value("model", "wired"),
                value("", "unwired"),
                value("end_input", "end"));

        assertEquals(List.of("unwired", "end"),
                HelperExecuteService.filterEndInputValues(outputs, ""));
        assertEquals(List.of("unwired", "end"),
                HelperExecuteService.filterEndInputValues(outputs, "   "));
        assertEquals(List.of("unwired", "end"),
                HelperExecuteService.filterEndInputValues(outputs, null));
    }

    @Test
    void aggregateFallsBackToContextStoreWhenRawVarsEmpty() {
        InOutValueDto dto = value("model", "from-store");
        ExecutionContextStore store = mock(ExecutionContextStore.class);
        when(store.collectOutputsByNode("child-wfe"))
                .thenReturn(Map.of("provider-node", List.of(dto)));

        List<Object> aggregated = newService(store)
                .aggregateSubPiResult(Map.of("unrelated", 1), "child-wfe", "provider-node", "model");
        assertEquals(List.of("from-store"), aggregated);
    }

    /**
     * Live defect: stale deployed {@code llm_output} schema (shortCodes without
     * {@code result}/{@code output_files}) schema-filters the sandbox envelope to {@code {}},
     * and fan-out stamps {@code targetPortValue} as the internal end-sub port — not the
     * parent function port {@code model}. Harvest must still return the raw image envelope.
     */
    @Test
    void staleSchemaInternalEndWireHarvestsRawLlmOutput() {
        List<SchemaDto> staleSchema = List.of(TransformFixtures.schema("content", "string"));
        NodeDto provider = TransformFixtures.sandboxNode(
                "b74b8fe5", "llm_input", "llm_output", staleSchema);
        wireToInternalEnd(provider, "llm_output", "end_sub_b74b8fe5", "end_sub_in");

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("child-wfe");
        ctx.setCurrentNodeId("b74b8fe5");
        ctx.setCurrentNode(provider);

        Map<String, Object> generatedFile = TransformFixtures.map(
                "name", "generated_a173a786.png",
                "mime_type", "image/png",
                "kind", "image",
                "data", "aaa");
        Map<String, Object> llmOutput = TransformFixtures.map(
                "result", TransformFixtures.map("output_files", List.of(generatedFile)));
        pipeline.finalizeOutputs(ctx, TransformFixtures.map("llm_output", llmOutput));

        List<InOutValueDto> persisted = provider.getOutputList();
        assertEquals(1, persisted.size(), "one fan-out row onto the internal end-sub wire");
        assertEquals("llm_output", persisted.get(0).getPortValue());
        assertEquals("end_sub_in", persisted.get(0).getTargetPortValue(),
                "sub-PI wire is the end-sub port, not the parent function port");
        assertEquals(Map.of(), persisted.get(0).getValue(),
                "stale schema strips result/output_files — wire value becomes {}");

        ExecutionContextStore store = mock(ExecutionContextStore.class);
        when(store.collectOutputsByNode("child-wfe"))
                .thenReturn(Map.of("b74b8fe5", persisted));

        // Production completion vars are REF-only (no output_* keys). Same filtered rows
        // in the store is the live harvest input that currently yields count=0.
        List<Object> fromStore = newService(store)
                .aggregateSubPiResult(Map.of("studioquackRef", "ref"),
                        "child-wfe", "b74b8fe5", "model");
        assertHarvestedGeneratedImage(fromStore);

        // Same rows on an output_* engine key (convert succeeds, target filter still drops).
        List<Object> fromVars = newService(mock(ExecutionContextStore.class))
                .aggregateSubPiResult(Map.of("provider_output_1", persisted),
                        "child-wfe", "b74b8fe5", "model");
        assertHarvestedGeneratedImage(fromVars);
    }

    private static void assertHarvestedGeneratedImage(List<Object> aggregated) {
        assertEquals(1, aggregated.size(), "function-return harvest must not stay at count=0");
        assertInstanceOf(Map.class, aggregated.get(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> harvested = (Map<String, Object>) aggregated.get(0);
        assertTrue(harvested.containsKey("result"), "raw llm_output envelope, not schema-stripped {}");
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) harvested.get("result");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = (List<Map<String, Object>>) result.get("output_files");
        assertEquals("generated_a173a786.png", files.get(0).get("name"));
    }

    private static HelperExecuteService newService(ExecutionContextStore store) {
        return new HelperExecuteService(
                new ObjectMapper(), mock(SqsEngine.class), mock(SubProcessCompletionRegistry.class),
                mock(RefOnlyVarBuilder.class), mock(TransformDirectiveApplier.class),
                store,
                new com.studioquack.sqs.channel.wg.OverlayPeerDirectory(),
                new com.studioquack.sqs.channel.wg.mesh.MeshIdentity());
    }

    @Test
    void nonEmptyStrippedValueStillPrefersUnfilteredFiles() {
        InOutValueDto dto = value("model", Map.of("result", Map.of("content", "Generated 1 image(s).")));
        dto.setUnfilteredValue(Map.of("result", Map.of(
                "content", "Generated 1 image(s).",
                "output_files", List.of(Map.of("name", "generated.png")))));

        List<Object> harvested = HelperExecuteService.filterEndInputValues(List.of(dto), "model");
        assertEquals(1, harvested.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) ((Map<String, Object>) harvested.get(0)).get("result");
        assertEquals(1, ((List<?>) result.get("output_files")).size());
    }

    @Test
    void emptyRawVarWithoutUnfilteredFallsBackToStore() {
        InOutValueDto emptyOnWire = value("end_sub_in", Map.of());
        InOutValueDto stored = value("end_sub_in", Map.of());
        stored.setUnfilteredValue(Map.of("result", Map.of(
                "output_files", List.of(Map.of("name", "generated.png")))));

        ExecutionContextStore store = mock(ExecutionContextStore.class);
        when(store.collectOutputsByNode("child-wfe"))
                .thenReturn(Map.of("provider-node", List.of(stored)));

        List<Object> aggregated = newService(store)
                .aggregateSubPiResult(Map.of("provider_output_1", List.of(emptyOnWire)),
                        "child-wfe", "provider-node", "model");
        assertEquals(1, aggregated.size(), "empty rawVar {} must not block the store fallback");
        @SuppressWarnings("unchecked")
        Map<String, Object> harvested = (Map<String, Object>) aggregated.get(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) harvested.get("result");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = (List<Map<String, Object>>) result.get("output_files");
        assertEquals("generated.png", files.get(0).get("name"));
    }

    private static InOutValueDto value(String targetPortValue, Object value) {
        InOutValueDto dto = new InOutValueDto();
        dto.setTargetPortValue(targetPortValue);
        dto.setValue(value);
        return dto;
    }

    /** Realistic sub-PI fan-out: provider output port → synthetic end-sub input, not {@code model}. */
    private static void wireToInternalEnd(NodeDto node, String sourcePort,
                                         String targetNodeId, String targetPort) {
        PortDto target = TransformFixtures.dataPort("in_" + targetPort, targetPort, null);
        ConnectionDetailDto detail = new ConnectionDetailDto();
        detail.setNodeId(targetNodeId);
        detail.setPortDto(target);
        ConnectionDto conn = new ConnectionDto();
        conn.setPortValue(sourcePort);
        conn.setPortName(sourcePort);
        conn.setConnectionDetailList(List.of(detail));
        node.setConnectionList(List.of(conn));
    }
}
