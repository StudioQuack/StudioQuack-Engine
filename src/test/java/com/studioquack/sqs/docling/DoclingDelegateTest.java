package com.studioquack.sqs.docling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ConnectionDetailDto;
import com.studioquack.common.dto.flow.ConnectionDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.SchemaDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.store.OperatorInputOverrideStore;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.pipeline.DefaultTransformPipeline;
import com.studioquack.sqs.execution.pipeline.TransformPipeline;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import com.studioquack.sqs.runtime.DispatchExecutionRegistry;
import com.studioquack.sqs.runtime.DispatchResultCoordinator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.activiti.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Docling v2 — {@link DoclingDelegate} unit test. Proves the async/durable/REF-ONLY contract that
 * must be preserved across the backend swap
 */
class DoclingDelegateTest {

    private ExecutionContextStore contextStore;
    private RefOnlyVarBuilder refOnlyVarBuilder;
    private DispatchResultCoordinator resultCoordinator;
    private DispatchExecutionRegistry registry;
    private ObjectMapper mapper;
    private ConnectionBuilder connectionBuilder;
    private JoltTransformService joltTransformService;
    private com.studioquack.sqs.execution.transform.TransformationCollector transformationCollector;
    private com.studioquack.sqs.execution.transform.EdgeTransformPreStep edgeTransformPreStep;
    private com.studioquack.sqs.status.SandboxLogCallback sandboxLogCallback;
    private com.studioquack.sqs.files.ClientFileStore clientFileStore;
    private DoclingDelegate delegate;

    @BeforeEach
    void setUp() {
        sandboxLogCallback = mock(com.studioquack.sqs.status.SandboxLogCallback.class);
        clientFileStore = mock(com.studioquack.sqs.files.ClientFileStore.class);
        contextStore = mock(ExecutionContextStore.class);
        refOnlyVarBuilder = mock(RefOnlyVarBuilder.class);
        resultCoordinator = mock(DispatchResultCoordinator.class);
        registry = new DispatchExecutionRegistry(); // in-memory (no durability / engine)
        mapper = new ObjectMapper();
        // No-op by default (no nodes/edges on the test ExecutionContext) — the "wired output" test
        // below overrides this to actually populate the current node's connectionList.
        connectionBuilder = mock(ConnectionBuilder.class);
        joltTransformService = mock(JoltTransformService.class);
        // No inbound specs by default — the inbound-JOLT leg is exercised in DoclingDelegateRealJoltTest.
        transformationCollector = mock(com.studioquack.sqs.execution.transform.TransformationCollector.class);
        edgeTransformPreStep = mock(com.studioquack.sqs.execution.transform.EdgeTransformPreStep.class);
    }

    @AfterEach
    void tearDown() {
        if (delegate != null) {
            delegate.shutdown();
        }
    }

    private ProcessContextRef ref() {
        ProcessContextRef ref = new ProcessContextRef();
        ref.setEnvId("env-1");
        ref.setCurrentNodeRefId("nr-1");
        return ref;
    }

    private DelegateExecution execution(ProcessContextRef ref) {
        DelegateExecution ex = mock(DelegateExecution.class);
        when(ex.getCurrentActivityId()).thenReturn("node_dc1");
        when(ex.getProcessInstanceBusinessKey()).thenReturn("bk-1");
        when(ex.getProcessInstanceId()).thenReturn("pi-1");
        when(ex.getId()).thenReturn("exec-1");
        when(ex.getVariable(EngineVariableNames.STUDIOQUACK_REF)).thenReturn(ref);
        return ex;
    }

    private DoclingDelegate delegate(DoclingBackend backend) {
        return delegate(backend, true);
    }

    /** @param doclingEnabled the per-environment toggle the delegate now gates on */
    private DoclingDelegate delegate(DoclingBackend backend, boolean doclingEnabled) {
        return delegate(backend, doclingEnabled, null);
    }

    private DoclingDelegate delegate(DoclingBackend backend, boolean doclingEnabled,
                                     CrossEnvDoclingBackend crossEnvBackend) {
        var entitlements = org.mockito.Mockito.mock(
                com.studioquack.sqs.entitlement.EntitlementsHolder.class);
        org.mockito.Mockito.when(entitlements.doclingEnabled()).thenReturn(doclingEnabled);
        DoclingDelegate d = new DoclingDelegate(contextStore, refOnlyVarBuilder, registry,
                resultCoordinator, backend, entitlements, mapper, connectionBuilder, joltTransformService,
                transformationCollector, edgeTransformPreStep,
                transformPipeline(),
                clientFileStore,
                sandboxLogCallback, null, null, null, null, crossEnvBackend, null, 900);
        d.init(); // @PostConstruct is not invoked outside Spring — build the dispatch pool ourselves
        return d;
    }

    private TransformPipeline transformPipeline() {
        return new DefaultTransformPipeline(
                connectionBuilder, transformationCollector, joltTransformService, mapper,
                new com.studioquack.sqs.files.ParameterSpiller(
                        org.mockito.Mockito.mock(com.studioquack.sqs.files.ClientFileStore.class)));
    }

    /** Variant of {@link #delegate(DoclingBackend)} wired with the engine facade the skip pin needs. */
    private DoclingDelegate delegateWithSkipPin(DoclingBackend backend, SqsEngine engine) {
        var entitlements = org.mockito.Mockito.mock(
                com.studioquack.sqs.entitlement.EntitlementsHolder.class);
        org.mockito.Mockito.when(entitlements.doclingEnabled()).thenReturn(true);
        DoclingDelegate d = new DoclingDelegate(contextStore, refOnlyVarBuilder, registry,
                resultCoordinator, backend, entitlements, mapper, connectionBuilder, joltTransformService,
                transformationCollector, edgeTransformPreStep,
                transformPipeline(),
                clientFileStore,
                sandboxLogCallback, null, null, null, null, null, providerOf(engine), 900);
        d.init(); // same PostConstruct stand-in as the main factory
        return d;
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

    /** A dc1 node carrying a {@code target_env} configuration parameter in its deployed graph data. */
    private NodeDto doclingNodeWithTargetEnv(String targetEnvValue) {
        NodeDto node = new NodeDto();
        node.setId("dc1");
        com.studioquack.common.dto.flow.TemplateDto data =
                new com.studioquack.common.dto.flow.TemplateDto();
        com.studioquack.common.dto.flow.ParameterDto param =
                new com.studioquack.common.dto.flow.ParameterDto();
        param.setShortCode("target_env");
        param.setValue(targetEnvValue);
        data.setConfigurationParameters(List.of(param));
        node.setData(data);
        return node;
    }

    /**
     * Expected ContentBlock for a successful Docling {@code texts} publish: {@code data} carries the
     * PLAIN converted text (the resolved content), never base64.
     */
    private static Map<String, Object> markdownContentBlock(String markdown, String name) {
        return Map.of(
                "kind", "document",
                "mime_type", "text/markdown",
                "source", "text",
                "name", name,
                "data", markdown);
    }

    /**
     * Expected file item for the {@code files} publish: base64 under {@code data}. The test
     * {@code ClientFileStore} mock seals no handle, so the block stays {@code source=base64}.
     */
    private static Map<String, Object> filesContentBlock(String markdown, String name) {
        return Map.of(
                "kind", "document",
                "mime_type", "text/markdown",
                "source", "base64",
                "name", name,
                "data", Base64.getEncoder().encodeToString(markdown.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * A Docling node on an environment with the toggle off must FAIL, loudly and by name.
     * Converting anyway would ignore an explicit operator choice; passing the document through
     * unconverted would produce silently wrong output, which is worse than either.
     */
    @Test
    void disabled_environment_fails_the_node_with_a_named_error() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        // Would throw if reached — proves the gate short-circuits before any conversion is attempted.
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new AssertionError("the backend must not be called when Docling is disabled");
        };
        delegate = delegate(backend, false);

        delegate.execute(execution(ref));

        ArgumentCaptor<String> code = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
        verify(resultCoordinator, timeout(2000)).deliverFailure(any(), code.capture(), message.capture());
        org.junit.jupiter.api.Assertions.assertEquals("DOCLING_DISABLED", code.getValue());
        org.junit.jupiter.api.Assertions.assertTrue(
                message.getValue().contains("Engine Settings"),
                "the message must name where to enable it: " + message.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    void converts_seals_ref_only_and_resumes() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto in = new InOutValueDto();
        in.setValue("raw document text");
        in.setPortValue("mydoc.txt");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(in));
        Map<String, Object> refOnlyVars = Map.of(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME, ref);
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(refOnlyVars);

        // In-test backend: returns fixed markdown (self-hosted forwarded is covered separately).
        DoclingBackend backend = (bytes, filename, mimeType, r) -> "# Converted Markdown";
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        // REF-ONLY seal: the converted markdown lands in the store, not on the engine.
        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        InOutValueDto sealed = outCaptor.getValue().get(0);
        assertEquals("dc1", sealed.getNodeId(), "output is addressed to the docling node");
        assertEquals(List.of(markdownContentBlock("# Converted Markdown", "mydoc.md")), sealed.getValue(),
                "converted markdown is sealed as a document ContentBlock named from the input file");

        // Resume the parked ReceiveTask with the REF-ONLY carry; no failure, no gateway routing.
        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), eq(refOnlyVars), eq(Map.of()));
        verify(resultCoordinator, never()).deliverFailure(anyString(), anyString(), anyString());
    }

    /**
     * Reproduces the reported break: a Docling node wired to a downstream node must have its
     * converted output actually reach it. {@code loadInputsTargetingNode} (the real store) filters
     * strictly on {@code targetNodeId}, so the sealed {@link InOutValueDto} must carry the downstream
     * node/port — the pre-fix flat shape (no {@code targetNodeId}) would never match there.
     */
    @Test
    @SuppressWarnings("unchecked")
    void converts_routes_output_to_the_wired_downstream_node_and_port() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        ctx.setEdges(new ArrayList<>(List.of(new com.studioquack.common.dto.flow.EdgeDto())));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        // The node must actually receive a document: with no inbound value the delegate fails closed
        // before it can route anything, so an empty input list tested nothing about routing.
        InOutValueDto pastedText = new InOutValueDto();
        pastedText.setPortValue("text");
        pastedText.setValue("a pasted document");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(pastedText));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        // Simulate what the real ConnectionBuilder computes for a node wired to a downstream
        // "summarize" node's "in" port: one OUTPUT connection detail.
        doAnswer((InvocationOnMock invocation) -> {
            NodeDto currentNode = invocation.getArgument(0);
            ConnectionDto conn = new ConnectionDto();
            conn.setPortType(PortType.OUTPUT);
            conn.setPortValue("docling_output");
            PortDto targetPort = new PortDto();
            targetPort.setId("port-in-1");
            targetPort.setValue("docling_input");
            ConnectionDetailDto detail = new ConnectionDetailDto();
            detail.setNodeId("summarize1");
            detail.setPortDto(targetPort);
            conn.getConnectionDetailList().add(detail);
            currentNode.setConnectionList(List.of(conn));
            return null;
        }).when(connectionBuilder).buildConnections(any(), any(), any());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> "# Converted Markdown";
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        InOutValueDto sealed = outCaptor.getValue().get(0);
        assertEquals("dc1", sealed.getNodeId(), "output is addressed FROM the docling node");
        assertEquals("summarize1", sealed.getTargetNodeId(), "output is routed TO the wired downstream node");
        assertEquals("port-in-1", sealed.getTargetPortId(), "output is routed to the wired downstream port");
        assertEquals(Map.of(
                        "texts", List.of(markdownContentBlock("# Converted Markdown", "text.md")),
                        "files", List.of(filesContentBlock("# Converted Markdown", "text.md"))),
                sealed.getValue(),
                "connection path stays source-shaped {texts:[…], files:[…]} — never guesses target schema");
    }

    /**
     * Retry / Replay of a converter whose output the operator edited (apply-inputs OUTPUT
     * override): the conversion runs, but the saved outputs are what is sealed and handed on —
     * the same values Skip &amp; Continue would hand on.
     */
    @Test
    @SuppressWarnings("unchecked")
    void rerun_converter_hands_on_the_operator_edited_outputs() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        ctx.setEdges(new ArrayList<>(List.of(new com.studioquack.common.dto.flow.EdgeDto())));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto pastedText = new InOutValueDto();
        pastedText.setPortValue("text");
        pastedText.setValue("a pasted document");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(pastedText));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());
        doAnswer((InvocationOnMock invocation) -> {
            NodeDto currentNode = invocation.getArgument(0);
            ConnectionDto conn = new ConnectionDto();
            conn.setPortType(PortType.OUTPUT);
            conn.setPortValue("docling_output");
            PortDto targetPort = new PortDto();
            targetPort.setId("port-in-1");
            targetPort.setValue("docling_input");
            ConnectionDetailDto detail = new ConnectionDetailDto();
            detail.setNodeId("summarize1");
            detail.setPortDto(targetPort);
            conn.getConnectionDetailList().add(detail);
            currentNode.setConnectionList(List.of(conn));
            return null;
        }).when(connectionBuilder).buildConnections(any(), any(), any());

        OperatorInputOverrideStore overrideStore = mock(OperatorInputOverrideStore.class);
        InOutValueDto editedOut = new InOutValueDto();
        editedOut.setPortValue("docling_output");
        editedOut.setValue("operator-output");
        when(overrideStore.loadOutputOverride("bk-1", "dc1")).thenReturn(Optional.of(List.of(editedOut)));

        delegate = delegate((bytes, filename, mimeType, r) -> "# Converted Markdown");
        delegate.setOperatorInputOverrideStore(overrideStore);
        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(1, outCaptor.getValue().size());
        assertEquals("summarize1", outCaptor.getValue().get(0).getTargetNodeId());
        assertEquals("operator-output", outCaptor.getValue().get(0).getValue(),
                "the operator's saved outputs replace the conversion on a re-run");
        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
    }

    /**
     * Operator "Skip &amp; Continue" on a converter node with an AUTHORED {@code SQ_SKIP} error
     * boundary: the operator's apply-inputs OUTPUT override is fanned out + persisted BEFORE the
     * typed failure is delivered, so the step's own skip handler reads those outputs from
     * {@code loadInputsTargetingNode} like any other successor. No conversion runs.
     */
    @Test
    @SuppressWarnings("unchecked")
    void authored_skip_boundary_fans_out_operator_outputs_before_the_typed_failure() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        com.studioquack.common.dto.flow.TemplateDto template =
                new com.studioquack.common.dto.flow.TemplateDto();
        template.setBoundaryEventConfigs(List.of(
                com.studioquack.common.dto.flow.BoundaryEventConfig.error(
                        EngineVariableNames.SKIP_ERROR_CODE)));
        template.setPorts(new ArrayList<>()); // no declared ports → no output schema filter
        doclingNode.setData(template);
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));

        // The skip-step command pinned __sq_skip_node=dc1 on the instance and requeued the hold.
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.getProcessVariable("pi-1", EngineVariableNames.SKIP_NODE)).thenReturn("dc1");

        OperatorInputOverrideStore overrideStore = mock(OperatorInputOverrideStore.class);
        InOutValueDto editedOut = new InOutValueDto();
        editedOut.setPortValue("docling_output");
        editedOut.setValue("operator-output");
        when(overrideStore.loadOutputOverride("bk-1", "dc1"))
                .thenReturn(Optional.of(List.of(editedOut)));

        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new AssertionError("operator skip: no conversion may run");
        };
        delegate = delegateWithSkipPin(backend, engine);
        delegate.setOperatorInputOverrideStore(overrideStore);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals("operator-output", outCaptor.getValue().get(0).getValue(),
                "the operator's outputs are sealed onto the row before the SQ_SKIP failure");
        verify(resultCoordinator, timeout(3_000)).deliverFailure(anyString(),
                eq(EngineVariableNames.SKIP_ERROR_CODE), eq("skipped by operator"));
        verify(resultCoordinator, never()).deliverSuccess(anyString(), anyMap(), anyMap());
    }

    /**
     * Authored edge {@code texts → prompt}: producer publishes SOURCE-shaped {@code {texts:[md]}}
     * and still routes to the consumer port. TargetPath / target schema fields (e.g. {@code system})
     * are not invented here — inbound prepareInbound owns the JOLT shift.
     */
    @Test
    @SuppressWarnings("unchecked")
    void converts_authored_edge_publishes_source_shaped_texts_routed_to_target() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        EdgeTransformDto edge = new EdgeTransformDto();
        edge.setSourceNodeId("dc1");
        edge.setSourcePortId("out-1");
        edge.setSourcePortShortCode("texts");
        edge.setSourcePath("texts");
        edge.setTargetNodeId("agent1");
        edge.setTargetPortId("port-prompt-1");
        edge.setTargetPortShortCode("prompt");
        edge.setTargetPath("prompt");
        doclingNode.setEdgeTransformListForCurrentNode(List.of(edge));
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        ctx.setEdges(new ArrayList<>(List.of(new com.studioquack.common.dto.flow.EdgeDto())));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto pastedText = new InOutValueDto();
        pastedText.setPortValue("text");
        pastedText.setValue("a pasted document");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(pastedText));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        // Target port declares TWO fields — producer must not invent either under the sealed value.
        doAnswer((InvocationOnMock invocation) -> {
            NodeDto currentNode = invocation.getArgument(0);
            ConnectionDto conn = new ConnectionDto();
            conn.setPortType(PortType.OUTPUT);
            conn.setPortValue("docling_output");
            PortDto targetPort = new PortDto();
            targetPort.setId("port-prompt-1");
            targetPort.setValue("agent_input");
            SchemaDto promptField = new SchemaDto();
            promptField.setShortCode("prompt");
            promptField.setType("string");
            SchemaDto otherField = new SchemaDto();
            otherField.setShortCode("system");
            otherField.setType("string");
            targetPort.setInputSchema(List.of(promptField, otherField));
            ConnectionDetailDto detail = new ConnectionDetailDto();
            detail.setNodeId("agent1");
            detail.setPortDto(targetPort);
            conn.getConnectionDetailList().add(detail);
            currentNode.setConnectionList(List.of(conn));
            return null;
        }).when(connectionBuilder).buildConnections(any(), any(), any());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> "# Converted Markdown";
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        InOutValueDto sealed = outCaptor.getValue().get(0);
        assertEquals("agent1", sealed.getTargetNodeId());
        assertEquals("port-prompt-1", sealed.getTargetPortId());
        assertEquals(Map.of("texts", List.of(markdownContentBlock("# Converted Markdown", "text.md"))),
                sealed.getValue(),
                "producer publishes source-shaped {texts:[ContentBlock]}; consumer owns JOLT");
        @SuppressWarnings("unchecked")
        Map<String, Object> value = assertInstanceOf(Map.class, sealed.getValue());
        assertFalse(value.containsKey("system"),
                "second declared field on the target port must not be invented by Docling");
        assertFalse(value.containsKey("prompt"),
                "targetPath must not be pre-shifted by the producer");
    }

    /**
     * Authored edge with targetPath still seals SOURCE-shaped — producer never applies JOLT.
     */
    @Test
    @SuppressWarnings("unchecked")
    void converts_authored_edge_publishes_source_shaped() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        EdgeTransformDto edge = new EdgeTransformDto();
        edge.setSourceNodeId("dc1");
        edge.setSourcePortId("out-1");
        edge.setSourcePortShortCode("texts");
        edge.setSourcePath("texts");
        edge.setTargetNodeId("agent1");
        edge.setTargetPortId("port-prompt-1");
        edge.setTargetPortShortCode("prompt");
        edge.setTargetPath("prompt");
        doclingNode.setEdgeTransformListForCurrentNode(List.of(edge));
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        ctx.setEdges(new ArrayList<>(List.of(new com.studioquack.common.dto.flow.EdgeDto())));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto pastedText = new InOutValueDto();
        pastedText.setPortValue("text");
        pastedText.setValue("a pasted document");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(pastedText));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        doAnswer((InvocationOnMock invocation) -> {
            NodeDto currentNode = invocation.getArgument(0);
            ConnectionDto conn = new ConnectionDto();
            conn.setPortType(PortType.OUTPUT);
            conn.setPortValue("docling_output");
            PortDto targetPort = new PortDto();
            targetPort.setId("port-prompt-1");
            targetPort.setValue("agent_input");
            SchemaDto promptField = new SchemaDto();
            promptField.setShortCode("prompt");
            promptField.setType("string");
            targetPort.setInputSchema(List.of(promptField));
            ConnectionDetailDto detail = new ConnectionDetailDto();
            detail.setNodeId("agent1");
            detail.setPortDto(targetPort);
            conn.getConnectionDetailList().add(detail);
            currentNode.setConnectionList(List.of(conn));
            return null;
        }).when(connectionBuilder).buildConnections(any(), any(), any());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> "# Converted Markdown";
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        InOutValueDto sealed = outCaptor.getValue().get(0);
        assertEquals("dc1", sealed.getNodeId());
        assertEquals("agent1", sealed.getTargetNodeId());
        assertEquals("port-prompt-1", sealed.getTargetPortId());
        assertEquals(Map.of("texts", List.of(markdownContentBlock("# Converted Markdown", "text.md"))),
                sealed.getValue(),
                "source-shaped publish regardless of authored targetPath");
    }

    /**
     * No authored transform (connection path only): stay under source {@code texts}; INFO names both ports.
     * Target PortDto schema fields must not be used for payload keys.
     */
    @Test
    @SuppressWarnings("unchecked")
    void converts_no_authored_transform_stays_under_source_and_logs_both_ports() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        ctx.setEdges(new ArrayList<>(List.of(new com.studioquack.common.dto.flow.EdgeDto())));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto pastedText = new InOutValueDto();
        pastedText.setPortValue("text");
        pastedText.setValue("a pasted document");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(pastedText));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        doAnswer((InvocationOnMock invocation) -> {
            NodeDto currentNode = invocation.getArgument(0);
            ConnectionDto conn = new ConnectionDto();
            conn.setPortType(PortType.OUTPUT);
            conn.setPortName("Docling Output");
            conn.setPortValue("docling_output");
            PortDto targetPort = new PortDto();
            targetPort.setId("port-prompt-1");
            targetPort.setName("Agent Input");
            targetPort.setValue("agent_input");
            SchemaDto promptField = new SchemaDto();
            promptField.setShortCode("prompt");
            promptField.setType("string");
            targetPort.setInputSchema(List.of(promptField));
            ConnectionDetailDto detail = new ConnectionDetailDto();
            detail.setNodeId("agent1");
            detail.setPortDto(targetPort);
            conn.getConnectionDetailList().add(detail);
            currentNode.setConnectionList(List.of(conn));
            return null;
        }).when(connectionBuilder).buildConnections(any(), any(), any());

        Logger logger = (Logger) LoggerFactory.getLogger(DoclingDelegate.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        Level previous = logger.getLevel();
        logger.setLevel(Level.INFO);
        try {
            DoclingBackend backend = (bytes, filename, mimeType, r) -> "# Converted Markdown";
            delegate = delegate(backend);

            delegate.execute(execution(ref));

            ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
            verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
            InOutValueDto sealed = outCaptor.getValue().get(0);
            assertEquals("agent1", sealed.getTargetNodeId());
            assertEquals("port-prompt-1", sealed.getTargetPortId());
            assertEquals(Map.of(
                            "texts", List.of(markdownContentBlock("# Converted Markdown", "text.md")),
                            "files", List.of(filesContentBlock("# Converted Markdown", "text.md"))),
                    sealed.getValue(),
                    "no authored transform → source attributes only; never target schema shortCode");

            assertTrue(appender.list.stream().anyMatch(e ->
                            e.getLevel() == Level.INFO
                                    && e.getFormattedMessage().contains("noAuthoredMapping")
                                    && e.getFormattedMessage().contains("Docling Output")
                                    && e.getFormattedMessage().contains("docling_output")
                                    && e.getFormattedMessage().contains("Agent Input")
                                    && e.getFormattedMessage().contains("agent_input")
                                    && e.getFormattedMessage().contains("port-prompt-1")),
                    "INFO must name both source and target ports for diagnosis");
        } finally {
            logger.detachAppender(appender);
            appender.stop();
            logger.setLevel(previous);
        }
    }

    @Test
    void conversion_failure_routes_to_error_gate_without_throwing() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new RuntimeException("docling backend down");
        };
        delegate = delegate(backend);

        // The engine-thread execute() must never throw (a throw would dead-letter the async job).
        assertDoesNotThrow(() -> delegate.execute(execution(ref)));

        verify(resultCoordinator, timeout(3_000)).deliverFailure(anyString(), eq("DOCLING_FAILED"), anyString());
        verify(resultCoordinator, never()).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(contextStore, never()).saveOutputs(any(), any());
    }

    /**
     * A non-Simple (script) DuckTape transform on the inbound edge runs as a transformation
     * SANDBOX before conversion: the delegate hands the script spec to EdgeTransformPreStep and
     * converts only the merged (transformed) inputList value.
     */
    @Test
    void script_edge_transform_runs_the_transform_sandbox_before_conversion() throws Exception {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));

        InOutValueDto wire = new InOutValueDto();
        wire.setNodeId("duck-start");
        wire.setPortValue("msg");
        wire.setTargetNodeId("dc1");
        wire.setTargetPortValue("input");
        wire.setValue(Map.of("msg", "raw wire value"));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(wire));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        com.studioquack.common.dto.flow.TransformationSpecDto scriptSpec =
                com.studioquack.common.dto.flow.TransformationSpecDto.builder()
                        .kind("script").language("python").script("target.document = source.msg")
                        .sourceNodeId("duck-start").targetNodeId("dc1")
                        .sourcePortShortCode("msg").targetPortShortCode("input")
                        .direction("input").build();
        when(transformationCollector.collectForCurrentNode(any())).thenReturn(List.of(scriptSpec));

        // The real pre-step merges the hop result onto the matching inputList entry in place.
        doAnswer((InvocationOnMock invocation) -> {
            com.studioquack.sqs.execution.pipeline.PreparedInbound prepared = invocation.getArgument(1);
            wire.setValue(Map.of("document", "transformed in sandbox"));
            return new com.studioquack.sqs.execution.transform.EdgeTransformPreStep.Result(
                    prepared, List.of(), null, null);
        }).when(edgeTransformPreStep).run(any(), any(), any(), anyLong(), anyString());

        java.util.concurrent.atomic.AtomicReference<byte[]> convertedBytes =
                new java.util.concurrent.atomic.AtomicReference<>();
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            convertedBytes.set(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return "# Converted";
        };
        delegate = delegate(backend);
        delegate.execute(execution(ref));

        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), any());
        verify(edgeTransformPreStep, timeout(3_000)).run(any(), any(), any(), anyLong(), anyString());
        Map<?, ?> received = new ObjectMapper().readValue(convertedBytes.get(), Map.class);
        assertEquals("transformed in sandbox", received.get("document"),
                "Docling must convert the transform-sandbox result, not the raw wire value");
    }

    /**
     * A failed inbound transform sandbox HARD-fails the node with the edge-transform code —
     * the converter must never run on the untransformed payload.
     */
    @Test
    void script_edge_transform_failure_fails_the_node_without_converting() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));

        InOutValueDto wire = new InOutValueDto();
        wire.setNodeId("duck-start");
        wire.setPortValue("msg");
        wire.setTargetNodeId("dc1");
        wire.setTargetPortValue("input");
        wire.setValue(Map.of("msg", "raw wire value"));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(wire));

        com.studioquack.common.dto.flow.TransformationSpecDto scriptSpec =
                com.studioquack.common.dto.flow.TransformationSpecDto.builder()
                        .kind("script").language("python").script("target.document = source.msg")
                        .sourceNodeId("duck-start").targetNodeId("dc1")
                        .sourcePortShortCode("msg").targetPortShortCode("input")
                        .direction("input").build();
        when(transformationCollector.collectForCurrentNode(any())).thenReturn(List.of(scriptSpec));
        when(edgeTransformPreStep.run(any(), any(), any(), anyLong(), anyString()))
                .thenReturn(new com.studioquack.sqs.execution.transform.EdgeTransformPreStep.Result(
                        com.studioquack.sqs.execution.pipeline.PreparedInbound.empty(), List.of(),
                        com.studioquack.sqs.execution.transform.EdgeTransformPreStep.ERROR_FAILED,
                        "edge-transform duck-start→dc1: failed"));

        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new AssertionError("the converter must not run when the transform sandbox failed");
        };
        delegate = delegate(backend);
        delegate.execute(execution(ref));

        verify(resultCoordinator, timeout(3_000)).deliverFailure(anyString(),
                eq(com.studioquack.sqs.execution.transform.EdgeTransformPreStep.ERROR_FAILED), anyString());
        verify(resultCoordinator, never()).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(contextStore, never()).saveOutputs(any(), any());
    }

    /**
     * The producer's persistOutputs and this node's loadInputsTargetingNode run on different threads,
     * so a parallel fan-out can read the store a beat before the producer's commit is visible. The
     * delegate must retry briefly instead of failing closed on an empty first read.
     */
    @Test
    void inbound_rows_arriving_after_a_raced_first_read_are_used() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto in = new InOutValueDto();
        in.setValue("raw document text");
        in.setPortValue("mydoc.txt");
        // First two reads see nothing (producer still committing); the third sees the row.
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1")))
                .thenReturn(List.of())
                .thenReturn(List.of())
                .thenReturn(List.of(in));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());
        delegate = delegate((bytes, filename, mimeType, r) -> "# Converted");

        delegate.execute(execution(ref));

        // The conversion must have run on the retried input, not failed closed on the empty reads.
        verify(contextStore, timeout(5_000)).saveOutputs(eq(ref), any());
        verify(resultCoordinator, timeout(5_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(resultCoordinator, never()).deliverFailure(anyString(), anyString(), anyString());
    }

    /**
     * A failed output seal must NOT complete the run: downstream nodes would start with missing
     * input while the node shows COMPLETED — the silent loss the IO mirror exists to expose.
     * The node fails loudly (ENGINE) instead.
     */
    @Test
    void seal_failure_fails_the_node_loudly_instead_of_completing_empty_handed() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto in = new InOutValueDto();
        in.setValue("raw document text");
        in.setPortValue("mydoc.txt");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(in));
        org.mockito.Mockito.doThrow(new RuntimeException("store write failed"))
                .when(contextStore).saveOutputs(eq(ref), any());
        delegate = delegate((bytes, filename, mimeType, r) -> "# Converted Markdown");

        assertDoesNotThrow(() -> delegate.execute(execution(ref)));

        verify(resultCoordinator, timeout(3_000)).deliverFailure(anyString(), eq("DOCLING_FAILED"),
                org.mockito.ArgumentMatchers.contains("could not be sealed"));
        verify(resultCoordinator, never()).deliverSuccess(anyString(), anyMap(), anyMap());
    }

    /**
     * The step has to be visible to the backend exactly like a sandbox script step: a STARTED marker so
     * the node goes RUNNING, then a terminal COMPLETED row. Without these the run finished with no
     * record that the converter ever ran.
     */
    @Test
    void successful_conversion_reports_started_then_completed_to_the_backend() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto in = new InOutValueDto();
        in.setValue("raw document text");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(in));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());
        delegate = delegate((bytes, filename, mimeType, r) -> "# Converted");

        delegate.execute(execution(ref));

        verify(sandboxLogCallback, timeout(3_000)).reportStarted(
                any(com.studioquack.sqs.status.SandboxLogCallback.SandboxStartRecord.class));
        ArgumentCaptor<com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord> terminal =
                ArgumentCaptor.forClass(com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, timeout(3_000)).report(terminal.capture());
        assertEquals("COMPLETED", terminal.getValue().status());
        assertEquals("NONE", terminal.getValue().failureKind());
        assertEquals("docling", terminal.getValue().language());
    }

    /**
     * A side-car that dies mid-conversion (host memory pressure — the reported case) must reach the
     * backend as a FAILED step, not just a daemon log line. The failure is classified as an
     * infrastructure fault so it is not read as "the document was bad".
     */
    @Test
    void conversion_failure_reports_a_failed_step_to_the_backend() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto in = new InOutValueDto();
        in.setValue("raw document text");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(in));
        delegate = delegate((bytes, filename, mimeType, r) -> {
            throw new RuntimeException("Unexpected end of file from server");
        });

        delegate.execute(execution(ref));

        ArgumentCaptor<com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord> terminal =
                ArgumentCaptor.forClass(com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, timeout(3_000)).report(terminal.capture());
        assertEquals("FAILED", terminal.getValue().status());
        assertEquals("INFRA", terminal.getValue().failureKind(),
                "no runtime diagnosis available in this test → generic infrastructure fault");
        org.junit.jupiter.api.Assertions.assertTrue(
                terminal.getValue().errorMessage().contains("RuntimeException"),
                "only the exception class travels — a Docling error body can echo document content");
    }

    /**
     * The reported break: a Docling node whose {@code files} parameter is left EMPTY must not
     * convert anything. The node now skips the conversion and completes with an empty output so the
     * run flows on.
     */
    @Test
    @SuppressWarnings("unchecked")
    void empty_files_parameter_skips_conversion_and_completes() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto files = new InOutValueDto();
        files.setPortValue("files");
        files.setValue(null); // parameter left blank — no file supplied
        InOutValueDto prompt = new InOutValueDto();
        prompt.setPortValue("prompt");
        prompt.setValue("summarize this please");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(files, prompt));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new AssertionError("the converter must not run when the files parameter is empty");
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(Map.of("texts", List.of(), "files", List.of()),
                outCaptor.getValue().get(0).getValue(),
                "a skipped conversion seals schema-shaped empties ({texts:[], files:[]}) — never a "
                        + "raw string a downstream JOLT would flatten into texts.data[/files.name[ keys");
        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(resultCoordinator, never()).deliverFailure(anyString(), anyString(), anyString());

        ArgumentCaptor<com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord> terminal =
                ArgumentCaptor.forClass(com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, timeout(3_000)).report(terminal.capture());
        assertEquals("COMPLETED", terminal.getValue().status());
        assertTrue(terminal.getValue().stdout().contains("no file input"),
                "the step log must say the conversion was skipped, not that a file was converted");
    }

    /**
     * Same contract when the empty parameter arrives as an empty list instead of null:
     * nothing to convert → skip, complete, never touch the converter.
     */
    @Test
    void empty_files_list_skips_conversion_and_completes() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto files = new InOutValueDto();
        files.setPortValue("files");
        files.setValue(List.of());
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(files));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new AssertionError("the converter must not run when the files parameter is empty");
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(resultCoordinator, never()).deliverFailure(anyString(), anyString(), anyString());
    }

    /**
     * Mystery M3: Duckbot-start fans out on port {@code output} as {@code {files: []}}, not on a
     * port named files/document/file. That wrapper is still "no document" — skip, never stringify
     * the JSON as inline text (that used to start the sidecar and convert 393 chars).
     */
    @Test
    @SuppressWarnings("unchecked")
    void empty_files_wrapper_on_output_port_skips_conversion() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("output");
        inbound.setValue(Map.of("files", List.of()));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new AssertionError("sidecar must not start for {files:[]} on port=output");
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(Map.of("texts", List.of(), "files", List.of()),
                outCaptor.getValue().get(0).getValue(),
                "empty file wrapper seals schema-shaped empties — never converted JSON");
        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(resultCoordinator, never()).deliverFailure(anyString(), anyString(), anyString());

        ArgumentCaptor<com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord> terminal =
                ArgumentCaptor.forClass(com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, timeout(3_000)).report(terminal.capture());
        assertEquals("COMPLETED", terminal.getValue().status());
        assertTrue(terminal.getValue().stdout().contains("no file input"),
                "skip must keep the existing COMPLETED mirror text");
    }

    /**
     * A file object with blank {@code data} and no {@code sq-file:} handle is an empty container,
     * even when wrapped as {@code {files:[{name:"",data:""}]}} on a non-files port.
     */
    @Test
    @SuppressWarnings("unchecked")
    void blank_named_file_object_skips_conversion() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("output");
        inbound.setValue(Map.of("files", List.of(Map.of("name", "", "data", ""))));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new AssertionError("blank {name,data} is not a document — do not convert");
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(Map.of("texts", List.of(), "files", List.of()),
                outCaptor.getValue().get(0).getValue(),
                "blank file object skips with schema-shaped empties");
        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(resultCoordinator, never()).deliverFailure(anyString(), anyString(), anyString());

        ArgumentCaptor<com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord> terminal =
                ArgumentCaptor.forClass(com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, timeout(3_000)).report(terminal.capture());
        assertEquals("COMPLETED", terminal.getValue().status());
        assertTrue(terminal.getValue().stdout().contains("no file input"));
    }

    /**
     * A real {@code {files:[{name, data}]}} wrapper with non-blank base64 still converts.
     */
    @Test
    @SuppressWarnings("unchecked")
    void real_files_wrapper_still_converts() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("output");
        String data = Base64.getEncoder().encodeToString("hello docling".getBytes(StandardCharsets.UTF_8));
        inbound.setValue(Map.of("files", List.of(Map.of("name", "note.txt", "data", data))));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        AtomicBoolean converted = new AtomicBoolean(false);
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            converted.set(true);
            return "# from-file";
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        assertTrue(converted.get(), "non-blank base64 inside {files:[…]} must reach convert()");
        assertEquals(List.of(markdownContentBlock("# from-file", "note.md")),
                outCaptor.getValue().get(0).getValue(),
                "named input file note.txt yields note.md on the sealed ContentBlock");
    }

    /**
     * DuckBot voice → Docling: the start node's {@code voices} port publishes the clip as a
     * files-shaped ContentBlock list. Wired straight into Docling's {@code files} input with no
     * authored mapping, the row arrives source-shaped ({@code {voices:[…]}} on port voices,
     * targetPort files) — the deep search must still resolve the clip as the conversion document.
     */
    @Test
    void duckbot_voices_wire_into_files_port_converts_the_clip() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("voices");
        inbound.setTargetPortValue("files");
        // EBML magic + DocType element: the pre-submit AV guard mirrors filetype's Webm
        // matcher, which requires both within the sniffed window.
        byte[] clip = new byte[]{0x1A, 0x45, (byte) 0xDF, (byte) 0xA3, 0x42, (byte) 0x82,
                (byte) 0x84, 'w', 'e', 'b', 'm', 0x18, 0x53, (byte) 0x80, 0x67, 0x01};
        String data = Base64.getEncoder().encodeToString(clip);
        inbound.setValue(Map.of("voices", List.of(Map.of(
                "kind", "audio",
                "mime_type", "audio/webm",
                "source", "sq-file",
                "name", "user prompt by voice command.webm",
                "data", data))));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        AtomicReference<byte[]> seenBytes = new AtomicReference<>();
        AtomicReference<String> seenName = new AtomicReference<>();
        AtomicReference<String> seenMime = new AtomicReference<>();
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            seenBytes.set(bytes);
            seenName.set(filename);
            seenMime.set(mimeType);
            return "# transcript";
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        assertArrayEquals(clip, seenBytes.get(),
                "the voices ContentBlock must resolve as the conversion document");
        assertEquals("user prompt by voice command.webm", seenName.get());
        assertEquals("audio/webm", seenMime.get());
    }

    /**
     * Pasted plain text is not file-shaped — the inline-text fallback must still convert it.
     */
    @Test
    void plain_text_string_still_converts() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("output");
        inbound.setValue("pasted document text");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        AtomicBoolean converted = new AtomicBoolean(false);
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            converted.set(true);
            return "# from-text";
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        assertTrue(converted.get(), "a non-file-shaped string must still take the inline-text path");
    }

    /**
     * Duckbot-start fan-out row: {@code {msg, files:[]}} on port {@code output}. The {@code files}
     * key makes it file-shaped even though {@code msg} is a sibling string — never stringify the
     * whole map as a document.
     */
    @Test
    @SuppressWarnings("unchecked")
    void duckbot_start_fanout_empty_files_skips_conversion() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("output");
        inbound.setValue(Map.of("msg", "hello", "files", List.of()));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            throw new AssertionError("Duckbot {msg, files:[]} must skip — do not convert the wrapper");
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(Map.of("texts", List.of(), "files", List.of()),
                outCaptor.getValue().get(0).getValue(),
                "skip stays schema-shaped even on the duckbot fan-out wrapper");
        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(resultCoordinator, never()).deliverFailure(anyString(), anyString(), anyString());

        ArgumentCaptor<com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord> terminal =
                ArgumentCaptor.forClass(com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, timeout(3_000)).report(terminal.capture());
        assertEquals("COMPLETED", terminal.getValue().status());
        assertTrue(terminal.getValue().stdout().contains("no file input"));
    }

    /**
     * Skip must happen before any backend method — {@code convert} is what calls
     * {@code DoclingContainer.ensureRunning()} (sidecar startup). A mock that throws on every
     * invocation proves the skip path never touches the sidecar.
     */
    @Test
    @SuppressWarnings("unchecked")
    void empty_file_container_never_touches_backend() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("output");
        inbound.setValue(Map.of("files", List.of()));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        // Throws on ANY interface method (convert is the only one; it is also the ensure trigger).
        DoclingBackend backend = mock(DoclingBackend.class, invocation -> {
            throw new AssertionError("backend." + invocation.getMethod().getName()
                    + " must not run on an empty file container");
        });
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(Map.of("texts", List.of(), "files", List.of()),
                outCaptor.getValue().get(0).getValue(),
                "empty container skips with schema-shaped empties");
        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        // convert() declares Exception — wrap so the never() check compiles.
        assertDoesNotThrow(() -> verify(backend, never()).convert(any(), any(), any(), any()));
    }

    /** A disabled environment is an operator-config failure and must still show up as a failed step. */
    @Test
    void disabled_environment_reports_a_failed_step_to_the_backend() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        delegate = delegate((bytes, filename, mimeType, r) -> "unreachable", false);

        delegate.execute(execution(ref));

        ArgumentCaptor<com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord> terminal =
                ArgumentCaptor.forClass(com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, timeout(3_000)).report(terminal.capture());
        assertEquals("FAILED", terminal.getValue().status());
        verify(sandboxLogCallback, never()).reportStarted(any());
    }

    /**
     * Cross-env routing: a node pinned to a sibling engine ({@code target_env=env-2}) converts via
     * the {@link CrossEnvDoclingBackend} — and the LOCAL entitlement gate does not apply, because
     * what gates the conversion is the TARGET's entitlement + its inbound Network Access Control
     * rule. The local backend is never touched.
     */
    @Test
    void remote_target_converts_via_the_cross_env_backend_and_skips_the_local_entitlement_gate()
            throws Exception {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setNodes(new ArrayList<>(List.of(doclingNodeWithTargetEnv("env-2"))));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto pastedText = new InOutValueDto();
        pastedText.setPortValue("text");
        pastedText.setValue("a pasted document");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(pastedText));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        CrossEnvDoclingBackend crossEnv = mock(CrossEnvDoclingBackend.class);
        when(crossEnv.convert(eq("env-2"), any(), any(), any(), any())).thenReturn("# Remote Markdown");
        DoclingBackend local = mock(DoclingBackend.class);
        // Local entitlement OFF — the remote route must still run.
        delegate = delegate(local, false, crossEnv);

        delegate.execute(execution(ref));

        verify(resultCoordinator, timeout(3_000)).deliverSuccess(anyString(), anyMap(), anyMap());
        verify(crossEnv).convert(eq("env-2"), any(), any(), any(), eq("md"));
        verify(local, never()).convert(any(), any(), any(), any(), any());
    }

    /** {@code target_env=host} is the explicit local selection: the entitlement gate applies. */
    @Test
    void host_target_keeps_the_local_entitlement_gate() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setNodes(new ArrayList<>(List.of(doclingNodeWithTargetEnv("host"))));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));

        CrossEnvDoclingBackend crossEnv = mock(CrossEnvDoclingBackend.class);
        delegate = delegate((bytes, filename, mimeType, r) -> "unreachable", false, crossEnv);

        delegate.execute(execution(ref));

        verify(resultCoordinator, timeout(3_000)).deliverFailure(anyString(), eq("DOCLING_DISABLED"), anyString());
        verify(crossEnv, never()).convert(any(), any(), any(), any(), any());
    }

    /** A node pinned at the engine it is deployed to is local — no overlay round-trip to itself. */
    @Test
    void self_target_is_treated_as_local() {
        ProcessContextRef ref = ref(); // env-1
        ExecutionContext ctx = new ExecutionContext();
        ctx.setNodes(new ArrayList<>(List.of(doclingNodeWithTargetEnv("env-1"))));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));

        CrossEnvDoclingBackend crossEnv = mock(CrossEnvDoclingBackend.class);
        delegate = delegate((bytes, filename, mimeType, r) -> "unreachable", false, crossEnv);

        delegate.execute(execution(ref));

        verify(resultCoordinator, timeout(3_000)).deliverFailure(anyString(), eq("DOCLING_DISABLED"), anyString());
        verify(crossEnv, never()).convert(any(), any(), any(), any(), any());
    }

    /** A cross-env failure surfaces as {@code DOCLING_FAILED} with the route named — never a throw. */
    @Test
    void remote_target_failure_is_routed_to_the_error_gate() throws Exception {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setNodes(new ArrayList<>(List.of(doclingNodeWithTargetEnv("env-2"))));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto pastedText = new InOutValueDto();
        pastedText.setPortValue("text");
        pastedText.setValue("a pasted document");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(pastedText));

        CrossEnvDoclingBackend crossEnv = mock(CrossEnvDoclingBackend.class);
        when(crossEnv.convert(eq("env-2"), any(), any(), any(), any()))
                .thenThrow(new IllegalStateException("engine env-2 does not allow this engine on port 5001"));
        delegate = delegate(mock(DoclingBackend.class), true, crossEnv);

        delegate.execute(execution(ref));

        ArgumentCaptor<String> error = ArgumentCaptor.forClass(String.class);
        verify(resultCoordinator, timeout(3_000))
                .deliverFailure(anyString(), eq("DOCLING_FAILED"), error.capture());
        assertTrue(error.getValue().contains("env-2"),
                "the failure names the target engine so the operator knows where to add the rule: "
                        + error.getValue());
    }

    /** A dc1 node carrying an {@code output_format} configuration parameter in its graph data. */
    private NodeDto doclingNodeWithOutputFormat(String outputFormat) {
        NodeDto node = new NodeDto();
        node.setId("dc1");
        com.studioquack.common.dto.flow.TemplateDto data =
                new com.studioquack.common.dto.flow.TemplateDto();
        com.studioquack.common.dto.flow.ParameterDto param =
                new com.studioquack.common.dto.flow.ParameterDto();
        param.setShortCode("output_format");
        param.setValue(outputFormat);
        data.setConfigurationParameters(List.of(param));
        node.setData(data);
        return node;
    }

    /**
     * {@code output_format=text} asks the sidecar for {@code to_formats=text} and publishes a
     * {@code .txt}/{@code text/plain} item whose {@code data} is the PLAIN converted text.
     */
    @Test
    @SuppressWarnings("unchecked")
    void output_format_text_requests_text_and_publishes_plain_txt() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setNodes(new ArrayList<>(List.of(doclingNodeWithOutputFormat("text"))));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("mydoc.txt");
        inbound.setValue("raw document text");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        AtomicReference<String> seenFormats = new AtomicReference<>();
        DoclingBackend backend = new DoclingBackend() {
            @Override
            public String convert(byte[] bytes, String filename, String mimeType, ProcessContextRef r) {
                return "plain text body";
            }

            @Override
            public String convert(byte[] bytes, String filename, String mimeType, ProcessContextRef r,
                                  String outputFormat) {
                seenFormats.set(outputFormat);
                return "plain text body";
            }
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        // saveOutputs happens AFTER convert on the async dispatch thread — observing it first
        // guarantees seenFormats is already published.
        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals("text", seenFormats.get(),
                "output_format=text must reach the sidecar as to_formats=text");
        assertEquals(List.of(Map.of(
                        "kind", "document",
                        "mime_type", "text/plain",
                        "source", "text",
                        "name", "mydoc.txt",
                        "data", "plain text body")),
                outCaptor.getValue().get(0).getValue(),
                "text format publishes a .txt text/plain item with the plain content under data");
    }

    /**
     * The sidecar answers a JSON envelope ({@code ConvertDocumentResponse}) — the delegate must
     * unwrap {@code md_content} so {@code texts.data} is the plain markdown, not base64 JSON.
     */
    @Test
    @SuppressWarnings("unchecked")
    void docling_json_envelope_is_unwrapped_to_plain_markdown() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("report.pdf");
        inbound.setValue("raw pdf bytes as text");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        DoclingBackend backend = (bytes, filename, mimeType, r) ->
                "{\"status\":\"success\",\"document\":{\"md_content\":\"# Hello Docling\"}}";
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(List.of(markdownContentBlock("# Hello Docling", "report.md")),
                outCaptor.getValue().get(0).getValue(),
                "the JSON envelope must be unwrapped to the plain md_content");
    }

    /**
     * The {@code files} output seals the converted content into the run-scoped store and publishes
     * the handle: {@code source=sq-file} + {@code fileId}, with base64 kept for inline consumers.
     */
    @Test
    @SuppressWarnings("unchecked")
    void files_output_is_sealed_into_the_run_store_and_carries_the_handle() {
        ProcessContextRef ref = ref();
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        EdgeTransformDto edge = new EdgeTransformDto();
        edge.setSourceNodeId("dc1");
        edge.setSourcePortId("out-files");
        edge.setSourcePortShortCode("files");
        edge.setSourcePath("files");
        edge.setTargetNodeId("sandbox1");
        edge.setTargetPortId("port-files-1");
        edge.setTargetPortShortCode("files");
        edge.setTargetPath("files");
        doclingNode.setEdgeTransformListForCurrentNode(List.of(edge));
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        InOutValueDto pastedText = new InOutValueDto();
        pastedText.setPortValue("text");
        pastedText.setValue("a pasted document");
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(pastedText));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());
        // The run id IS the process business key ("bk-1" in the execution() stub above).
        when(clientFileStore.store(eq(com.studioquack.sqs.files.ClientFileStore.SCOPE_RUN), eq("bk-1"),
                eq("text.md"), eq("text/markdown"), any()))
                .thenReturn("sq-file://run/bk-1/abc123");

        DoclingBackend backend = (bytes, filename, mimeType, r) -> "# Converted Markdown";
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(Map.of("files", List.of(Map.of(
                        "kind", "document",
                        "mime_type", "text/markdown",
                        "source", "sq-file",
                        "name", "text.md",
                        "fileId", "sq-file://run/bk-1/abc123",
                        "data", Base64.getEncoder()
                                .encodeToString("# Converted Markdown".getBytes(StandardCharsets.UTF_8))))),
                outCaptor.getValue().get(0).getValue(),
                "files publishes the store handle (sq-file + fileId) alongside the base64 data");
    }

    /**
     * A .webm-named upload whose bytes are NOT a webm container (here: a JSON text payload, the
     * mis-wired-port case) never reaches the side-car: docling's extension fallback has no
     * audio/video branches, so upstream admission depends on content-sniffing alone and would
     * answer the opaque "File format not allowed". The delegate fails the node locally, naming
     * what actually arrived.
     */
    @Test
    void webm_named_text_payload_fails_before_submit_naming_what_arrived() {
        ProcessContextRef ref = ref();
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(new ExecutionContext()));
        InOutValueDto inbound = new InOutValueDto();
        inbound.setPortValue("voices");
        inbound.setTargetPortValue("files");
        String data = Base64.getEncoder()
                .encodeToString("{\"files\":[]}".getBytes(StandardCharsets.UTF_8));
        inbound.setValue(Map.of("voices", List.of(Map.of(
                "kind", "audio",
                "mime_type", "audio/webm",
                "source", "sq-file",
                "name", "voice-1787827940565.webm",
                "data", data))));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(inbound));

        AtomicBoolean convertCalled = new AtomicBoolean(false);
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            convertCalled.set(true);
            return "# should never happen";
        };
        delegate = delegate(backend);

        delegate.execute(execution(ref));

        ArgumentCaptor<String> failure = ArgumentCaptor.forClass(String.class);
        verify(resultCoordinator, timeout(3_000)).deliverFailure(anyString(), eq("DOCLING_FAILED"),
                failure.capture());
        assertFalse(convertCalled.get(), "a non-webm payload must never be submitted to Docling");
        assertTrue(failure.getValue().contains("voice-1787827940565.webm"),
                "the file is named: " + failure.getValue());
        assertTrue(failure.getValue().contains("not webm content"),
                "the mismatch is named: " + failure.getValue());
        assertTrue(failure.getValue().contains("printable text"),
                "the JSON payload is identified as text: " + failure.getValue());
    }

    @Test
    void av_container_mismatch_unit_matrix() {
        // EBML magic + DocType element — what filetype's Webm matcher requires.
        byte[] ebml = {0x1A, 0x45, (byte) 0xDF, (byte) 0xA3, 0x42, (byte) 0x82, (byte) 0x84,
                'w', 'e', 'b', 'm', 0x18, 0x53, (byte) 0x80, 0x67, 0x01};
        byte[] ebmlMkv = {0x1A, 0x45, (byte) 0xDF, (byte) 0xA3, 0x42, (byte) 0x82, (byte) 0x88,
                'm', 'a', 't', 'r', 'o', 's', 'k', 'a', 0x18, 0x53, (byte) 0x80, 0x67, 0x01};
        byte[] ftyp = {0, 0, 0, 0x18, 'f', 't', 'y', 'p', 'i', 's', 'o', 'm'};
        byte[] riffWave = {'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'A', 'V', 'E'};
        byte[] id3 = {'I', 'D', '3', 4, 0, 0, 0, 0, 0, 0, 0, 0};
        byte[] text = "{\"files\":[]}".getBytes(StandardCharsets.UTF_8);

        // Real container bytes pass for every guarded extension family.
        assertNull(DoclingDelegate.avContainerMismatch("voice.webm", ebml));
        assertNull(DoclingDelegate.avContainerMismatch("clip.mkv", ebmlMkv));
        assertNull(DoclingDelegate.avContainerMismatch("clip.mp4", ftyp));
        assertNull(DoclingDelegate.avContainerMismatch("voice.m4a", ftyp));
        assertNull(DoclingDelegate.avContainerMismatch("voice.wav", riffWave));
        assertNull(DoclingDelegate.avContainerMismatch("song.mp3", id3));

        // Wrong content for the name is caught, with the evidence in the message.
        String mismatch = DoclingDelegate.avContainerMismatch("voice.webm", text);
        assertNotNull(mismatch);
        assertTrue(mismatch.contains("not webm content"), mismatch);
        assertTrue(mismatch.contains("printable text"), mismatch);
        assertNotNull(DoclingDelegate.avContainerMismatch("clip.mp4", text));
        assertNotNull(DoclingDelegate.avContainerMismatch("song.mp3", text));

        // An EBML header stub WITHOUT the DocType element still fails — filetype needs both,
        // so docling would reject this even though the magic is right.
        byte[] stub = {0x1A, 0x45, (byte) 0xDF, (byte) 0xA3, 0, 0, 0, 0, 0, 0, 0, 0};
        assertNotNull(DoclingDelegate.avContainerMismatch("voice.webm", stub));

        // Too small to sniff is caught too.
        String tiny = DoclingDelegate.avContainerMismatch("voice.webm", new byte[]{0x1A, 0x45});
        assertNotNull(tiny);
        assertTrue(tiny.contains("too small"), tiny);

        // Non-audio/video names are not guarded (docling's extension fallback covers them).
        assertNull(DoclingDelegate.avContainerMismatch("doc.pdf", text));
        assertNull(DoclingDelegate.avContainerMismatch("no-extension", text));
        assertNull(DoclingDelegate.avContainerMismatch(null, text));
    }
}
