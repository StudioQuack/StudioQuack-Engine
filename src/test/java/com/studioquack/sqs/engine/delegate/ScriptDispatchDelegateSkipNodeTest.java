package com.studioquack.sqs.engine.delegate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.pipeline.DefaultTransformPipeline;
import com.studioquack.sqs.execution.pipeline.TransformPipeline;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.store.OperatorInputOverrideStore;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.files.ParameterSpiller;
import com.studioquack.sqs.runtime.DispatchExecutionRegistry;
import com.studioquack.sqs.runtime.DispatchResultCoordinator;
import com.studioquack.sqs.runtime.GatewayRouteResolver;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Operator "Skip &amp; Continue" short-circuit in {@link ScriptDispatchDelegate}: a run held at
 * {@code hold_<nodeId>} is requeued with {@code __sq_skip_node} pinned on the instance, and the
 * re-dispatch consumes the pin instead of running a sandbox. An authored {@code SQ_SKIP} error
 * boundary gets the typed failure (same routing as a raised BpmnError).
 */
class ScriptDispatchDelegateSkipNodeTest {

    private static final String WFE = "wfe-1";
    private static final String PI = "pi-1";
    private static final String NODE = "n1";
    private static final String DISPATCH = "disp-skip";

    private DispatchResultCoordinator coordinator;
    private GatewayRouteResolver routeResolver;
    private RefOnlyVarBuilder varBuilder;
    private CapturingLogCallback logs;
    private SqsEngine engine;
    private DispatchExecutionRegistry registry;
    private ScriptDispatchDelegate delegate;

    @BeforeEach
    void setUp() {
        coordinator = mock(DispatchResultCoordinator.class);
        routeResolver = mock(GatewayRouteResolver.class);
        varBuilder = mock(RefOnlyVarBuilder.class);
        logs = new CapturingLogCallback();
        engine = mock(SqsEngine.class);
        registry = new DispatchExecutionRegistry(); // in-memory-only test constructor
        registry.register(DISPATCH, "exec-1", PI, WFE, NODE, "wait_" + NODE, null);
        delegate = new ScriptDispatchDelegate(
                null, null, null, null, null, null,
                varBuilder,
                null,
                registry,
                coordinator,
                routeResolver,
                null,
                logs,
                null, null, null, null, null, null, null, null, null, null,
                new MockEnvironment(),
                null,
                null,
                null,
                null,
                providerOf(engine));
    }

    @Test
    void consumeSkipNodePin_consumes_only_a_pin_naming_this_node() {
        when(engine.getProcessVariable(PI, EngineVariableNames.SKIP_NODE)).thenReturn(NODE);

        assertTrue(delegate.consumeSkipNodePin(DISPATCH, NODE));
        // Consumed FIRST (removed before the short-circuit runs) so a crash leaves no stale pin.
        verify(engine).removeVariables(PI, Set.of(EngineVariableNames.SKIP_NODE));

        // A pin naming ANOTHER node (or absent) is left untouched and does not trigger the skip.
        when(engine.getProcessVariable(PI, EngineVariableNames.SKIP_NODE)).thenReturn("other");
        assertFalse(delegate.consumeSkipNodePin(DISPATCH, NODE));
        assertFalse(delegate.consumeSkipNodePin("unknown-dispatch", NODE));
    }

    @Test
    void skippedNode_without_skip_boundary_continues_with_no_outputs() {
        Map<String, Object> engineVars = Map.of(EngineVariableNames.STUDIOQUACK_REF, "ref-1");
        Map<String, Object> routeTokens = Map.of("route__g1", "YES");
        when(varBuilder.toEngineVariables(any())).thenReturn(engineVars);
        when(routeResolver.resolveRouteTokens(any(), any(), eq(NODE), any(), any()))
                .thenReturn(routeTokens);

        delegate.handleSkippedNode(DISPATCH, NODE, ref(), ctx(), graphNodes(false), graphEdges());

        verify(coordinator).deliverSuccess(DISPATCH, engineVars, routeTokens);
        verify(coordinator, never()).deliverFailure(anyString(), anyString(), anyString());
        assertEquals("SKIPPED", logs.lastStatus);
        assertEquals("NONE", logs.lastKind);
        assertTrue(logs.lastError.contains("continued with no outputs"), logs.lastError);
    }

    @Test
    void skippedNode_with_operator_override_reports_the_edited_inputs() {
        // Repair: the operator applied apply-inputs on the held node and then chose Skip & Continue
        // — no sandbox runs, but the SKIPPED row must show the edited parameters.
        com.studioquack.sqs.execution.store.OperatorInputOverrideStore overrideStore =
                mock(com.studioquack.sqs.execution.store.OperatorInputOverrideStore.class);
        com.studioquack.common.dto.flow.InOutValueDto edited =
                new com.studioquack.common.dto.flow.InOutValueDto();
        edited.setNodeId("W");
        edited.setPortValue("prompt");
        edited.setTargetNodeId(NODE);
        edited.setValue("operator-value");
        when(overrideStore.loadOverride(WFE, NODE))
                .thenReturn(java.util.Optional.of(List.of(edited)));
        delegate.setOperatorInputOverrideStore(overrideStore);

        delegate.handleSkippedNode(DISPATCH, NODE, ref(), ctx(), graphNodes(false), graphEdges());

        assertEquals("SKIPPED", logs.lastStatus);
        assertTrue(logs.lastInput instanceof List<?> list && list.size() == 1
                        && list.get(0) instanceof com.studioquack.common.dto.flow.InOutValueDto dto
                        && "operator-value".equals(dto.getValue()),
                "the SKIPPED row must carry the operator-edited inputs, not a blank IO");
    }

    @Test
    void skippedNode_without_override_keeps_empty_io() {
        delegate.handleSkippedNode(DISPATCH, NODE, ref(), ctx(), graphNodes(false), graphEdges());

        assertEquals("SKIPPED", logs.lastStatus);
        org.junit.jupiter.api.Assertions.assertNull(logs.lastInput,
                "no override → the SKIPPED row stays honestly IO-less");
    }

    @Test
    void skippedNode_with_output_override_fans_out_persists_and_routes_with_them() {
        // DuckTales output edit + Skip & Continue: the operator's outputs become the skipped
        // node's outputs — persisted so downstream loadInputsTargetingNode sees them, carried
        // into route-token resolution, and shown on the SKIPPED row.
        OperatorInputOverrideStore overrideStore = mock(OperatorInputOverrideStore.class);
        InOutValueDto editedOut = new InOutValueDto();
        editedOut.setPortValue("result");
        editedOut.setValue("operator-output");
        when(overrideStore.loadOutputOverride(WFE, NODE)).thenReturn(Optional.of(List.of(editedOut)));

        ExecutionContextStore contextStore = mock(ExecutionContextStore.class);
        ScriptDispatchDelegate d = delegateWithOutputs(contextStore, realPipeline());
        d.setOperatorInputOverrideStore(overrideStore);

        Map<String, Object> engineVars = Map.of(EngineVariableNames.STUDIOQUACK_REF, "ref-1");
        Map<String, Object> routeTokens = Map.of("route__g1", "YES");
        when(varBuilder.toEngineVariables(any())).thenReturn(engineVars);
        when(routeResolver.resolveRouteTokens(any(), any(), eq(NODE), any(), any()))
                .thenReturn(routeTokens);

        ProcessContextRef ref = ref();
        d.handleSkippedNode(DISPATCH, NODE, ref, ctx(), outputGraphNodes(), outputGraphEdges());

        // Persisted exactly like the normal success branch — fanned out and addressed to n2.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<InOutValueDto>> persisted = ArgumentCaptor.forClass(List.class);
        verify(contextStore).saveOutputs(eq(ref), persisted.capture());
        assertEquals(1, persisted.getValue().size());
        assertEquals("n2", persisted.getValue().get(0).getTargetNodeId());
        assertEquals("operator-output", persisted.getValue().get(0).getValue());

        // Route tokens resolve from the fanned-out outputs, not an empty list.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<InOutValueDto>> routed = ArgumentCaptor.forClass(List.class);
        verify(routeResolver).resolveRouteTokens(any(), any(), eq(NODE), routed.capture(), any());
        assertEquals(1, routed.getValue().size());
        assertEquals("operator-output", routed.getValue().get(0).getValue());

        verify(coordinator).deliverSuccess(DISPATCH, engineVars, routeTokens);
        assertEquals("SKIPPED", logs.lastStatus);
        assertTrue(logs.lastError.contains("operator-edited outputs"), logs.lastError);
        assertTrue(logs.lastOutput instanceof List<?> list && list.size() == 1
                        && list.get(0) instanceof InOutValueDto dto
                        && "operator-output".equals(dto.getValue()),
                "the SKIPPED row must show the operator-edited outputs");
    }

    @Test
    void rerun_hands_on_the_operator_edited_output_ports_over_what_the_node_produced() {
        // DuckTales output edit + Retry / Replay: the node runs, but an edited port's saved
        // message replaces what it produced for that port; unedited ports keep the node's values.
        OperatorInputOverrideStore overrideStore = mock(OperatorInputOverrideStore.class);
        InOutValueDto editedOut = new InOutValueDto();
        editedOut.setPortValue("result");
        editedOut.setValue(Map.of("aaaaaa", "operator"));
        when(overrideStore.loadOutputOverride(WFE, NODE)).thenReturn(Optional.of(List.of(editedOut)));
        delegate.setOperatorInputOverrideStore(overrideStore);
        ExecutionContext ctx = ctx();

        Map<String, Object> handedOn = delegate.withOperatorOutputOverride(ref(), ctx, NODE,
                Map.of("result", Map.of("status", "produced"), "meta", "kept"));

        assertEquals(Map.of("aaaaaa", "operator"), handedOn.get("result"));
        assertEquals("kept", handedOn.get("meta"));
        assertEquals(Set.of("result"), ctx.getOperatorOverrideOutputPorts());
    }

    @Test
    void rerun_without_an_output_override_hands_on_the_produced_outputs_unchanged() {
        OperatorInputOverrideStore overrideStore = mock(OperatorInputOverrideStore.class);
        delegate.setOperatorInputOverrideStore(overrideStore);
        Map<String, Object> produced = Map.of("result", "produced");
        ExecutionContext ctx = ctx();

        assertTrue(produced == delegate.withOperatorOutputOverride(ref(), ctx, NODE, produced));
        org.junit.jupiter.api.Assertions.assertNull(ctx.getOperatorOverrideOutputPorts());
    }

    @Test
    void skippedNode_with_inputs_only_override_keeps_no_outputs() {
        // An INPUT override alone changes nothing on the output lane: no persistence, no fan-out,
        // and the SKIPPED row still reports "no outputs".
        OperatorInputOverrideStore overrideStore = mock(OperatorInputOverrideStore.class);
        InOutValueDto edited = new InOutValueDto();
        edited.setPortValue("prompt");
        edited.setValue("operator-value");
        when(overrideStore.loadOverride(WFE, NODE)).thenReturn(Optional.of(List.of(edited)));
        // loadOutputOverride stays unstubbed → Mockito hands back an empty Optional.

        ExecutionContextStore contextStore = mock(ExecutionContextStore.class);
        ScriptDispatchDelegate d = delegateWithOutputs(contextStore, realPipeline());
        d.setOperatorInputOverrideStore(overrideStore);
        when(varBuilder.toEngineVariables(any())).thenReturn(Map.of());

        d.handleSkippedNode(DISPATCH, NODE, ref(), ctx(), graphNodes(false), graphEdges());

        verify(contextStore, never()).saveOutputs(any(), any());
        assertEquals("SKIPPED", logs.lastStatus);
        assertTrue(logs.lastError.contains("continued with no outputs"), logs.lastError);
    }

    @Test
    void skippedNode_with_authored_skip_boundary_routes_the_typed_failure() {
        delegate.handleSkippedNode(DISPATCH, NODE, ref(), ctx(), graphNodes(true), graphEdges());

        verify(coordinator).deliverFailure(DISPATCH, EngineVariableNames.SKIP_ERROR_CODE,
                "skipped by operator");
        verify(coordinator, never()).deliverSuccess(anyString(), anyMap(), anyMap());
        assertEquals("SKIPPED", logs.lastStatus);
        assertTrue(logs.lastError.contains("routed to the step's skip handler"), logs.lastError);
    }

    @Test
    void skippedNode_with_authored_skip_boundary_also_fans_out_operator_outputs() {
        // The authored SQ_SKIP branch used to return BEFORE the output-override fan-out, so the
        // step's skip handler never saw the operator's outputs. The fan-out + persist now run
        // BEFORE the SQ_SKIP failure is delivered; the branch's routing is unchanged.
        OperatorInputOverrideStore overrideStore = mock(OperatorInputOverrideStore.class);
        InOutValueDto editedOut = new InOutValueDto();
        editedOut.setPortValue("result");
        editedOut.setValue("operator-output");
        when(overrideStore.loadOutputOverride(WFE, NODE)).thenReturn(Optional.of(List.of(editedOut)));

        ExecutionContextStore contextStore = mock(ExecutionContextStore.class);
        ScriptDispatchDelegate d = delegateWithOutputs(contextStore, realPipeline());
        d.setOperatorInputOverrideStore(overrideStore);

        ProcessContextRef ref = ref();
        d.handleSkippedNode(DISPATCH, NODE, ref, ctx(), outputGraphNodesWithSkipBoundary(),
                outputGraphEdges());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<InOutValueDto>> persisted = ArgumentCaptor.forClass(List.class);
        verify(contextStore).saveOutputs(eq(ref), persisted.capture());
        assertEquals(1, persisted.getValue().size());
        assertEquals("n2", persisted.getValue().get(0).getTargetNodeId());
        assertEquals("operator-output", persisted.getValue().get(0).getValue(),
                "the skip handler's dispatch reads the operator outputs from the sealed row");

        verify(coordinator).deliverFailure(DISPATCH, EngineVariableNames.SKIP_ERROR_CODE,
                "skipped by operator");
        verify(coordinator, never()).deliverSuccess(anyString(), anyMap(), anyMap());
        assertEquals("SKIPPED", logs.lastStatus);
        assertTrue(logs.lastError.contains("routed to the step's skip handler"), logs.lastError);
    }

    @Test
    void skippedNode_output_override_keeps_operator_fields_outside_port_schema() {
        // DuckTales "Add item" on an OUTPUT field: the added key is not in the port's declared
        // schema, but it is operator-supplied — it must fan out downstream and into the record,
        // never be stripped by the output schema filter.
        OperatorInputOverrideStore overrideStore = mock(OperatorInputOverrideStore.class);
        InOutValueDto editedOut = new InOutValueDto();
        editedOut.setPortValue("result");
        editedOut.setValue(Map.of("summary", "s", "operator_extra", "kept"));
        when(overrideStore.loadOutputOverride(WFE, NODE)).thenReturn(Optional.of(List.of(editedOut)));

        ExecutionContextStore contextStore = mock(ExecutionContextStore.class);
        ScriptDispatchDelegate d = delegateWithOutputs(contextStore, realPipeline());
        d.setOperatorInputOverrideStore(overrideStore);
        when(varBuilder.toEngineVariables(any())).thenReturn(Map.of());
        when(routeResolver.resolveRouteTokens(any(), any(), eq(NODE), any(), any()))
                .thenReturn(Map.of());

        // n1's OUTPUT "result" declares ONLY {summary} — operator_extra is outside the schema.
        List<NodeDto> nodes = List.of(
                TransformFixtures.sandboxNode(NODE, "in", "result",
                        List.of(TransformFixtures.schema("summary", "string"))),
                TransformFixtures.simpleNode("n2", "in", null));
        ProcessContextRef ref = ref();
        d.handleSkippedNode(DISPATCH, NODE, ref, ctx(), nodes, outputGraphEdges());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<InOutValueDto>> persisted = ArgumentCaptor.forClass(List.class);
        verify(contextStore).saveOutputs(eq(ref), persisted.capture());
        assertEquals(Map.of("summary", "s", "operator_extra", "kept"),
                persisted.getValue().get(0).getValue(),
                "operator-added output fields survive the port-schema filter");
    }

    /**
     * A delegate wired like {@link #setUp()} but with a real output pipeline + context store, so
     * the skip-with-output-override path runs the production fan-out instead of a null lane.
     */
    private ScriptDispatchDelegate delegateWithOutputs(ExecutionContextStore contextStore,
                                                       TransformPipeline pipeline) {
        ScriptDispatchDelegate d = new ScriptDispatchDelegate(
                pipeline, null, null, null, null, null,
                varBuilder,
                contextStore,
                registry,
                coordinator,
                routeResolver,
                null,
                logs,
                null, null, null, null, null, null, null, null, null, null,
                new MockEnvironment(),
                null,
                null,
                null,
                null,
                providerOf(engine));
        d.setConnectionBuilder(new ConnectionBuilder(null));
        return d;
    }

    /** The production output pipeline; the spiller is stubbed to pass values through untouched. */
    private static TransformPipeline realPipeline() {
        ParameterSpiller spiller = mock(ParameterSpiller.class);
        when(spiller.spillIfOversized(any(), any(), any())).thenAnswer(inv -> inv.getArgument(0));
        return new DefaultTransformPipeline(new ConnectionBuilder(null), null, null,
                new ObjectMapper(), spiller);
    }

    /** Node {@code n1} with OUTPUT port "result" wired to consumer {@code n2}'s INPUT "in". */
    private static List<NodeDto> outputGraphNodes() {
        return List.of(TransformFixtures.simpleNode(NODE, "in", "result"),
                TransformFixtures.simpleNode("n2", "in", null));
    }

    /** {@link #outputGraphNodes()} plus an authored {@code SQ_SKIP} error boundary on n1. */
    private static List<NodeDto> outputGraphNodesWithSkipBoundary() {
        NodeDto node = TransformFixtures.simpleNode(NODE, "in", "result");
        node.getData().setBoundaryEventConfigs(
                List.of(BoundaryEventConfig.error(EngineVariableNames.SKIP_ERROR_CODE)));
        return List.of(node, TransformFixtures.simpleNode("n2", "in", null));
    }

    private static List<EdgeDto> outputGraphEdges() {
        return List.of(TransformFixtures.edge(NODE, "n2", "OUTPUT-result", "INPUT-in", null));
    }

    private static ProcessContextRef ref() {
        return ProcessContextRef.builder()
                .workflowExecutionId(WFE)
                .currentNodeRefId("ref-1")
                .envId("env-1")
                .build();
    }

    private static ExecutionContext ctx() {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(WFE);
        return ctx;
    }

    /** Node {@code n1} → gateway {@code g1}; the node authors an SQ_SKIP boundary only when asked. */
    private static List<NodeDto> graphNodes(boolean withSkipBoundary) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType("script");
        if (withSkipBoundary) {
            // ERROR config carrying the reserved SQ_SKIP code (the config-map factory).
            template.setBoundaryEventConfigs(
                    List.of(BoundaryEventConfig.error(EngineVariableNames.SKIP_ERROR_CODE)));
        }
        NodeDto node = new NodeDto();
        node.setId(NODE);
        node.setData(template);
        NodeDto gateway = new NodeDto();
        gateway.setId("g1");
        TemplateDto gatewayTemplate = new TemplateDto();
        gatewayTemplate.setBpmnType("exclusive.gateway");
        gateway.setData(gatewayTemplate);
        return List.of(node, gateway);
    }

    private static List<EdgeDto> graphEdges() {
        EdgeDto edge = new EdgeDto();
        edge.setId("e1");
        edge.setSource(NODE);
        edge.setTarget("g1");
        return List.of(edge);
    }

    private static <T> ObjectProvider<T> providerOf(T value) {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                return value;
            }

            @Override
            public T getIfAvailable() {
                return value;
            }

            @Override
            public Stream<T> stream() {
                return value == null ? Stream.empty() : Stream.of(value);
            }
        };
    }

    /** Captures the last boundary-log row so the SKIPPED status and message are assertable. */
    private static final class CapturingLogCallback implements SandboxLogCallback {
        String lastStatus;
        String lastError;
        String lastKind;
        Object lastInput;
        Object lastOutput;

        @Override
        public void report(SandboxLogRecord record) {
            lastStatus = record.status();
            lastError = record.errorMessage();
            lastKind = record.failureKind();
            lastInput = record.input();
            lastOutput = record.output();
        }

        @Override
        public void reportStarted(SandboxStartRecord record) {
            // unused
        }
    }
}
