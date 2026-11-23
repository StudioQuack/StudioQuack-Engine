package com.studioquack.sqs.docling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ConnectionDetailDto;
import com.studioquack.common.dto.flow.ConnectionDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.pipeline.DefaultTransformPipeline;
import com.studioquack.sqs.execution.pipeline.TransformPipeline;
import com.studioquack.sqs.runtime.DispatchExecutionRegistry;
import com.studioquack.sqs.runtime.DispatchResultCoordinator;
import org.activiti.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies Docling seals SOURCE-shaped output even when {@link EdgeTransformDto} carries {@code
 * targetPath} / {@code targetDataType}. Producer JOLT is intentionally not applied — inbound {@code
 * TransformPipeline.prepareInbound} on the consumer owns the mapping.
 */
class DoclingDelegateRealJoltTest {

    private static final String MD = "# Converted Markdown";

    private ExecutionContextStore contextStore;
    private RefOnlyVarBuilder refOnlyVarBuilder;
    private DispatchResultCoordinator resultCoordinator;
    private DispatchExecutionRegistry registry;
    private ObjectMapper mapper;
    private ConnectionBuilder connectionBuilder;
    /** Real service — injected but must not shape sealed output. */
    private JoltTransformService joltTransformService;
    /** Real collector on the real JOLT service — the inbound leg IS under test here. */
    private com.studioquack.sqs.execution.transform.TransformationCollector transformationCollector;
    private com.studioquack.sqs.status.SandboxLogCallback sandboxLogCallback;
    private DoclingDelegate delegate;

    @BeforeEach
    void setUp() {
        sandboxLogCallback = mock(com.studioquack.sqs.status.SandboxLogCallback.class);
        contextStore = mock(ExecutionContextStore.class);
        refOnlyVarBuilder = mock(RefOnlyVarBuilder.class);
        resultCoordinator = mock(DispatchResultCoordinator.class);
        registry = new DispatchExecutionRegistry();
        mapper = new ObjectMapper();
        connectionBuilder = mock(ConnectionBuilder.class);
        joltTransformService = new JoltTransformService(new ObjectMapper());
        transformationCollector = new com.studioquack.sqs.execution.transform.TransformationCollector(
                joltTransformService);
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
        return delegate(backend, org.mockito.Mockito.mock(com.studioquack.sqs.files.ClientFileStore.class));
    }

    private DoclingDelegate delegate(DoclingBackend backend,
                                     com.studioquack.sqs.files.ClientFileStore fileStore) {
        var entitlements = org.mockito.Mockito.mock(
                com.studioquack.sqs.entitlement.EntitlementsHolder.class);
        org.mockito.Mockito.when(entitlements.doclingEnabled()).thenReturn(true);
        DoclingDelegate d = new DoclingDelegate(contextStore, refOnlyVarBuilder, registry,
                resultCoordinator, backend, entitlements, mapper, connectionBuilder, joltTransformService,
                transformationCollector,
                org.mockito.Mockito.mock(com.studioquack.sqs.execution.transform.EdgeTransformPreStep.class),
                transformPipeline(),
                fileStore,
                sandboxLogCallback, null, null, null, null, null, null, 900);
        d.init();
        return d;
    }

    private TransformPipeline transformPipeline() {
        return new DefaultTransformPipeline(
                connectionBuilder, transformationCollector, joltTransformService, mapper,
                new com.studioquack.sqs.files.ParameterSpiller(
                        org.mockito.Mockito.mock(com.studioquack.sqs.files.ClientFileStore.class)));
    }

    /** Authored outbound edge texts → targetPath, with OUTPUT connection stub for buildConnections. */
    private EdgeTransformDto textsEdge(String targetPath, String targetDataType) {
        EdgeTransformDto edge = new EdgeTransformDto();
        edge.setSourceNodeId("dc1");
        edge.setSourcePortId("out-1");
        edge.setSourcePortShortCode("texts");
        edge.setSourcePath("texts");
        edge.setTargetNodeId("agent1");
        edge.setTargetPortId("port-A-1");
        edge.setTargetPortShortCode("A");
        edge.setTargetPath(targetPath);
        if (targetDataType != null) {
            edge.setTargetDataType(targetDataType);
        }
        return edge;
    }

    private void stubWiredContext(ProcessContextRef ref, List<EdgeTransformDto> edges) {
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        doclingNode.setEdgeTransformListForCurrentNode(edges);
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
            targetPort.setId("port-A-1");
            targetPort.setValue("agent_input");
            ConnectionDetailDto detail = new ConnectionDetailDto();
            detail.setNodeId("agent1");
            detail.setPortDto(targetPort);
            conn.getConnectionDetailList().add(detail);
            currentNode.setConnectionList(List.of(conn));
            return null;
        }).when(connectionBuilder).buildConnections(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> executeAndCaptureSealedValue(EdgeTransformDto edge) {
        ProcessContextRef ref = ref();
        stubWiredContext(ref, List.of(edge));
        DoclingBackend backend = (bytes, filename, mimeType, r) -> MD;
        delegate = delegate(backend);
        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        InOutValueDto sealed = outCaptor.getValue().get(0);
        assertEquals("agent1", sealed.getTargetNodeId());
        assertEquals("port-A-1", sealed.getTargetPortId());
        return assertInstanceOf(Map.class, sealed.getValue());
    }

    /** Expected ContentBlock for a successful Docling {@code texts} publish (plain-text data). */
    private static Map<String, Object> markdownContentBlock(String markdown, String name) {
        return Map.of(
                "kind", "document",
                "mime_type", "text/markdown",
                "source", "text",
                "name", name,
                "data", markdown);
    }

    private static void assertSourceShapedTexts(Map<String, Object> value) {
        assertEquals(List.of(markdownContentBlock(MD, "text.md")), value.get("texts"),
                "producer must seal source-shaped {texts:[ContentBlock]}");
        assertFalse(value.containsKey("A"),
                "targetPath must not be pre-shifted by Docling");
    }

    /** Flat texts → A on the edge: sealed value stays under texts, not A. */
    @Test
    void publishes_source_shaped_texts_despite_targetPath_A() {
        Map<String, Object> value = executeAndCaptureSealedValue(textsEdge("A", null));
        assertSourceShapedTexts(value);
    }

    /** Nested texts → A.b on the edge: still source-shaped at top level. */
    @Test
    void publishes_source_shaped_texts_despite_nested_targetPath_A_b() {
        Map<String, Object> value = executeAndCaptureSealedValue(textsEdge("A.b", null));
        assertSourceShapedTexts(value);
        assertFalse(value.containsKey("A"), "must not nest under A either");
    }

    /** list targetDataType on the edge must not coerce or rename the sealed payload. */
    @Test
    void publishes_source_shaped_texts_despite_list_targetDataType() {
        Map<String, Object> value = executeAndCaptureSealedValue(textsEdge("A", "list"));
        assertSourceShapedTexts(value);
    }

    /**
     * The exact reported flow: a docling → agent edge mapping {@code texts.data → prompt}. The
     * producer seals schema-shaped {@code {texts:[ContentBlock]}}; the consumer's inbound shift —
     * built from the SAME row by {@code convertJoltSpec} — must navigate the list-valued {@code
     * texts} root and deliver the plain text to {@code prompt}.
     */
    @Test
    void outbound_texts_data_leaf_maps_to_prompt_through_the_real_inbound_shift() {
        EdgeTransformDto edge = textsEdge("prompt", "string");
        edge.setSourcePath("texts.data");
        Map<String, Object> sealed = executeAndCaptureSealedValue(edge);
        assertSourceShapedTexts(sealed);

        List<Object> inboundSpec = joltTransformService.convertJoltSpec(List.of(edge),
                Map.of("prompt", "string"));
        Object transformed = joltTransformService.transform(sealed, inboundSpec);
        assertEquals(Map.of("prompt", MD), transformed,
                "texts.data → prompt must deliver the converted text, never the raw texts param");
    }

    /** string targetDataType on the edge must not stringify or rename the sealed payload. */
    @Test
    void publishes_source_shaped_texts_despite_string_targetDataType() {
        Map<String, Object> value = executeAndCaptureSealedValue(textsEdge("A", "string"));
        assertSourceShapedTexts(value);
        assertFalse(value.containsValue("[\"# Converted Markdown\"]"),
                "must not JSON-stringify the list under a target key");
    }

    /**
     * Two EdgeTransformDto rows on the SAME wire (same source/target ports) emit ONE InOutValueDto.
     */
    @Test
    @SuppressWarnings("unchecked")
    void groupByWire_two_rows_same_wire_emits_one_dto() {
        ProcessContextRef ref = ref();
        EdgeTransformDto row1 = textsEdge("prompt", null);
        EdgeTransformDto row2 = textsEdge("prompt", null);
        // Same wire identity; second row reuses texts sourcePath (Docling has one output attribute).
        stubWiredContext(ref, List.of(row1, row2));
        DoclingBackend backend = (bytes, filename, mimeType, r) -> MD;
        delegate = delegate(backend);
        delegate.execute(execution(ref));

        ArgumentCaptor<List<InOutValueDto>> outCaptor = ArgumentCaptor.forClass(List.class);
        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), outCaptor.capture());
        assertEquals(1, outCaptor.getValue().size(),
                "two attribute rows on one wire must publish a single DTO");
        Map<String, Object> value = assertInstanceOf(Map.class, outCaptor.getValue().get(0).getValue());
        assertSourceShapedTexts(value);
    }

    /**
     * Inbound leg of "JOLT before AND after Docling": an authored shift on the Duckbot→Docling
     * edge runs BEFORE conversion, so the converter receives the remapped document — never the
     * raw wire payload. The real {@code TransformationCollector} + {@code JoltTransformService}
     * build and apply the spec exactly as a sandbox node's prepareInbound would.
     */
    @Test
    void inbound_jolt_shapes_the_document_before_conversion() throws Exception {
        ProcessContextRef ref = ref();

        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));

        // Inbound edge duck-start → dc1 with an authored msg → document shift on the edge data.
        EdgeTransformDto inboundRow = new EdgeTransformDto();
        inboundRow.setSourceNodeId("duck-start");
        inboundRow.setSourcePortShortCode("msg");
        inboundRow.setSourcePath("msg");
        inboundRow.setTargetNodeId("dc1");
        inboundRow.setTargetPortShortCode("input");
        inboundRow.setTargetPath("document");
        com.studioquack.common.dto.flow.EdgeDataDto edgeData =
                new com.studioquack.common.dto.flow.EdgeDataDto();
        edgeData.setEdgeTransforms(List.of(inboundRow));
        com.studioquack.common.dto.flow.EdgeDto inboundEdge =
                new com.studioquack.common.dto.flow.EdgeDto();
        inboundEdge.setSource("duck-start");
        inboundEdge.setTarget("dc1");
        inboundEdge.setSourceHandle("out-msg");
        inboundEdge.setTargetHandle("in-input");
        inboundEdge.setData(edgeData);
        ctx.setEdges(new ArrayList<>(List.of(inboundEdge)));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));

        // The raw wire value carries the Duckbot message under its SOURCE name.
        InOutValueDto wire = new InOutValueDto();
        wire.setNodeId("duck-start");
        wire.setPortValue("msg");
        wire.setTargetNodeId("dc1");
        wire.setTargetPortValue("input");
        wire.setValue(Map.of("msg", "the user prompt"));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of(wire));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        java.util.concurrent.atomic.AtomicReference<byte[]> convertedBytes =
                new java.util.concurrent.atomic.AtomicReference<>();
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            convertedBytes.set(bytes);
            return MD;
        };
        delegate = delegate(backend);
        delegate.execute(execution(ref));

        verify(contextStore, timeout(3_000)).saveOutputs(eq(ref), any());
        Map<?, ?> received = new ObjectMapper().readValue(convertedBytes.get(), Map.class);
        assertEquals("the user prompt", received.get("document"),
                "inbound edge JOLT must shift msg → document BEFORE Docling converts");
        assertFalse(received.containsKey("msg"), "the raw wire key must not survive the inbound shift");
    }

    /**
     * A leaf-mapped outbound row ({@code texts.data → A}) must still publish the ROOT attribute:
     * the payload keys on {@code texts}, never the literal dotted path. The consumer's inbound
     * shift — built from the same row — navigates {@code texts[*].data} off the root, so a
     * literal {@code "texts.data"} key is one its spec never looks for (and it renders as a
     * garbage flattened key in the run logs).
     */
    @Test
    void publishes_root_attribute_when_sourcePath_is_a_leaf() {
        EdgeTransformDto edge = textsEdge("A", null);
        edge.setSourcePath("texts.data");
        Map<String, Object> value = executeAndCaptureSealedValue(edge);
        assertSourceShapedTexts(value);
        assertFalse(value.containsKey("texts.data"),
                "a dotted leaf sourcePath must not become a literal payload key");
    }

    /**
     * The parallel-gateway fan-out topology: start --msg--> gw, start --voices--> gw, and
     * gw --voices→files (row pinned to the start)--> docling. The start's outputs target the
     * GATEWAY, so Docling's strict {@code loadInputsTargetingNode} finds nothing; the voices
     * entry rides in via {@code ForeignEdgeInputResolver}, the inbound JOLT renames
     * voices→files, the REF strip drops the inline base64 (handle kept), and the converter must
     * still receive the clip's bytes by resolving the {@code sq-file:} handle.
     */
    @Test
    void gateway_fanout_voices_reach_docling_as_files() {
        gatewayFanoutConvertsVoice("dc1");
    }

    /**
     * Same topology, but the pinned row carries NO targetNodeId (the FE's gateway-predecessor
     * tile rows can persist without one). ForeignEdgeInputResolver tolerates the blank and
     * delivers the payload; the collector must not drop the row's mapping then — otherwise the
     * value arrives keyed {@code voices} and is never renamed to {@code files}.
     */
    @Test
    void gateway_fanout_voices_row_without_targetNodeId_still_maps() {
        gatewayFanoutConvertsVoice(null);
    }

    private void gatewayFanoutConvertsVoice(String rowTargetNodeId) {
        ProcessContextRef ref = ref();
        ref.setExecutionId("exec-gw");

        // EBML magic + DocType element: the delegate's pre-submit AV guard mirrors filetype's
        // Webm matcher, which requires both within the sniffed window.
        byte[] clip = new byte[]{0x1A, 0x45, (byte) 0xDF, (byte) 0xA3, 0x42, (byte) 0x82,
                (byte) 0x84, 'w', 'e', 'b', 'm', 0x18, 0x53, (byte) 0x80, 0x67, 0x01};
        String handle = "sq-file://run/exec-gw/clip";
        com.studioquack.sqs.files.ClientFileStore fileStore =
                mock(com.studioquack.sqs.files.ClientFileStore.class);
        when(fileStore.resolve(eq(handle))).thenReturn(Optional.of(
                new com.studioquack.sqs.files.ClientFileStore.StoredFile(
                        "clip", "clip.webm", "audio/webm", clip.length, clip)));

        // 1) Real start-node publish: two authored wires to the gateway (msg + voices ports).
        com.studioquack.sqs.engine.delegate.DuckbotBridge bridge =
                new com.studioquack.sqs.engine.delegate.DuckbotBridge(output -> List.of(),
                        mock(com.studioquack.sqs.conversation.ConversationStore.class),
                        fileStore, new ObjectMapper());
        EdgeTransformDto msgWire = new EdgeTransformDto();
        msgWire.setSourceNodeId("duck-start");
        msgWire.setTargetNodeId("gw");
        msgWire.setSourcePortId("port-out");
        msgWire.setSourcePortShortCode("output");
        msgWire.setTargetPortId("port-gw-in");
        msgWire.setTargetPortShortCode("input");
        msgWire.setSourcePath("msg");
        msgWire.setTargetPath("msg");
        EdgeTransformDto voicesWire = new EdgeTransformDto();
        voicesWire.setSourceNodeId("duck-start");
        voicesWire.setTargetNodeId("gw");
        voicesWire.setSourcePortId("port-voices");
        voicesWire.setSourcePortShortCode("voices");
        voicesWire.setTargetPortId("port-gw-in");
        voicesWire.setTargetPortShortCode("input");
        voicesWire.setSourcePath("voices");
        voicesWire.setTargetPath("voices");
        NodeDto start = new NodeDto();
        start.setId("duck-start");
        com.studioquack.common.dto.flow.TemplateDto startTemplate =
                new com.studioquack.common.dto.flow.TemplateDto();
        startTemplate.setCategory("conversion");
        startTemplate.setNodeType("start");
        start.setData(startTemplate);
        start.setEdgeTransformListForCurrentNode(List.of(msgWire, voicesWire));
        ExecutionContext startCtx = new ExecutionContext();
        startCtx.setCurrentNode(start);
        com.studioquack.common.dto.flow.AgenticInfo ai =
                new com.studioquack.common.dto.flow.AgenticInfo("conv-1", "agent", "gordun mu", true);
        com.studioquack.common.dto.flow.ConversationFileRefDto voice =
                new com.studioquack.common.dto.flow.ConversationFileRefDto();
        voice.setFileId(handle);
        voice.setName("clip.webm");
        voice.setMimeType("audio/webm");
        voice.setKind("audio");
        ai.setVoice(voice);
        startCtx.setAgenticInfo(ai);
        bridge.applyStart(startCtx);
        List<InOutValueDto> startOutputs = start.getOutputList();
        assertEquals(2, startOutputs.size(), "one DTO per wire (msg + voices), both to the gateway");

        // 2) Docling dispatch: nothing targets dc1 directly — the voices entry rides foreign.
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        EdgeTransformDto gwRow = new EdgeTransformDto();
        gwRow.setSourceNodeId("duck-start");
        gwRow.setSourcePortId("port-voices");
        gwRow.setSourcePortShortCode("voices");
        gwRow.setSourcePath("voices");
        gwRow.setTargetNodeId(rowTargetNodeId);
        gwRow.setTargetPortId("port-files");
        gwRow.setTargetPortShortCode("files");
        gwRow.setTargetPath("files");
        com.studioquack.common.dto.flow.EdgeDataDto gwData =
                new com.studioquack.common.dto.flow.EdgeDataDto();
        gwData.setEdgeTransforms(List.of(gwRow));
        com.studioquack.common.dto.flow.EdgeDto gwEdge =
                new com.studioquack.common.dto.flow.EdgeDto();
        gwEdge.setSource("gw");
        gwEdge.setTarget("dc1");
        gwEdge.setSourceHandle("output-output");
        gwEdge.setTargetHandle("input-files");
        gwEdge.setData(gwData);
        ctx.setEdges(new ArrayList<>(List.of(gwEdge)));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(List.of());
        when(contextStore.collectOutputsByNode(eq("exec-gw"),
                eq(java.util.Set.of("duck-start"))))
                .thenReturn(Map.of("duck-start", startOutputs));
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        java.util.concurrent.atomic.AtomicReference<byte[]> convertedBytes =
                new java.util.concurrent.atomic.AtomicReference<>();
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            convertedBytes.set(bytes);
            return MD;
        };
        delegate = delegate(backend, fileStore);
        delegate.execute(execution(ref));

        verify(contextStore, timeout(5_000)).saveOutputs(eq(ref), any());
        org.junit.jupiter.api.Assertions.assertArrayEquals(clip, convertedBytes.get(),
                "the voice clip must reach Docling through the gateway fan-out: foreign delivery "
                        + "→ inbound voices→files shift → sq-file handle resolution");

        // The terminal log echoes the POST-transform inputList: the delivered entry must have
        // been renamed voices → files by the inbound shift, never left on the raw wire key.
        ArgumentCaptor<com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord> logCaptor =
                ArgumentCaptor.forClass(
                        com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback, timeout(3_000).atLeastOnce()).report(logCaptor.capture());
        com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord finished = logCaptor
                .getAllValues().stream()
                .filter(r -> "COMPLETED".equals(r.status()) && r.input() != null)
                .findFirst().orElseThrow(() -> new AssertionError("no COMPLETED log with input echo"));
        List<?> echoEntries = assertInstanceOf(List.class, finished.input());
        Map<?, ?> echoEntry = echoEntries.stream()
                .filter(e -> e instanceof Map<?, ?> m && "files".equals(m.get("targetPortValue")))
                .map(e -> (Map<?, ?>) e).findFirst()
                .orElseThrow(() -> new AssertionError("no input entry addressed to the files port"));
        Map<?, ?> echoValue = assertInstanceOf(Map.class, echoEntry.get("value"));
        assertTrue(echoValue.containsKey("files"),
                "the inbound shift must rename voices → files before conversion");
        assertFalse(echoValue.containsKey("voices"),
                "the raw voices key must not survive the inbound shift");
    }

    /**
     * The direct start→Docling topology from the 2026-08-26 run log (no canvas gateway — the BPMN
     * generator inserts the parallel gateways).
     */
    @Test
    void direct_edge_voices_mapped_to_files_leaf_still_converts() {
        ProcessContextRef ref = ref();
        ref.setExecutionId("exec-direct");

        // EBML magic + DocType element: the delegate's pre-submit AV guard mirrors filetype's
        // Webm matcher, which requires both within the sniffed window.
        byte[] clip = new byte[]{0x1A, 0x45, (byte) 0xDF, (byte) 0xA3, 0x42, (byte) 0x82,
                (byte) 0x84, 'w', 'e', 'b', 'm', 0x18, 0x53, (byte) 0x80, 0x67, 0x01};
        String handle = "sq-file://run/exec-direct/clip";
        com.studioquack.sqs.files.ClientFileStore fileStore =
                mock(com.studioquack.sqs.files.ClientFileStore.class);
        when(fileStore.resolve(eq(handle))).thenReturn(Optional.of(
                new com.studioquack.sqs.files.ClientFileStore.StoredFile(
                        "clip", "clip.webm", "audio/webm", clip.length, clip)));

        // 1) Real start publish: ONE direct wire to Docling carrying the logged row set.
        com.studioquack.sqs.engine.delegate.DuckbotBridge bridge =
                new com.studioquack.sqs.engine.delegate.DuckbotBridge(output -> List.of(),
                        mock(com.studioquack.sqs.conversation.ConversationStore.class),
                        fileStore, new ObjectMapper());
        List<EdgeTransformDto> rows = new ArrayList<>();
        rows.add(directLeafRow("files.name", "files.name"));
        rows.add(directLeafRow("files.source", "files.source"));
        rows.add(directLeafRow("voices", "files.data"));
        rows.add(directLeafRow("files.kind", "files.kind"));
        rows.add(directLeafRow("files.data", "files.data"));
        rows.add(directLeafRow("files.mime_type", "files.mime_type"));
        NodeDto start = new NodeDto();
        start.setId("duck-start");
        com.studioquack.common.dto.flow.TemplateDto startTemplate =
                new com.studioquack.common.dto.flow.TemplateDto();
        startTemplate.setCategory("conversion");
        startTemplate.setNodeType("start");
        start.setData(startTemplate);
        start.setEdgeTransformListForCurrentNode(rows);
        ExecutionContext startCtx = new ExecutionContext();
        startCtx.setCurrentNode(start);
        com.studioquack.common.dto.flow.AgenticInfo ai =
                new com.studioquack.common.dto.flow.AgenticInfo("conv-1", "agent", "gordun mu", true);
        com.studioquack.common.dto.flow.ConversationFileRefDto voice =
                new com.studioquack.common.dto.flow.ConversationFileRefDto();
        voice.setFileId(handle);
        voice.setName("clip.webm");
        voice.setMimeType("audio/webm");
        voice.setKind("audio");
        ai.setVoice(voice);
        startCtx.setAgenticInfo(ai);
        bridge.applyStart(startCtx);
        List<InOutValueDto> startOutputs = start.getOutputList();
        assertEquals(1, startOutputs.size(), "one wire → one DTO keyed by the row sourcePaths");

        // 2) Docling dispatch: direct delivery + the same rows drive the inbound spec.
        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        com.studioquack.common.dto.flow.EdgeDataDto edgeData =
                new com.studioquack.common.dto.flow.EdgeDataDto();
        edgeData.setEdgeTransforms(rows);
        com.studioquack.common.dto.flow.EdgeDto edge =
                new com.studioquack.common.dto.flow.EdgeDto();
        edge.setSource("duck-start");
        edge.setTarget("dc1");
        edge.setSourceHandle("output-output");
        edge.setTargetHandle("input-docling_input");
        edge.setData(edgeData);
        ctx.setEdges(new ArrayList<>(List.of(edge)));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(startOutputs);
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        java.util.concurrent.atomic.AtomicReference<byte[]> convertedBytes =
                new java.util.concurrent.atomic.AtomicReference<>();
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            convertedBytes.set(bytes);
            return MD;
        };
        delegate = delegate(backend, fileStore);
        delegate.execute(execution(ref));

        verify(contextStore, timeout(5_000)).saveOutputs(eq(ref), any());
        org.junit.jupiter.api.Assertions.assertArrayEquals(clip, convertedBytes.get(),
                "a voices→files.data leaf row must not bury the clip: the toString coercion "
                        + "stringified the REF-only block into a JSON string, so resolveDocument "
                        + "found no handle and skipped the conversion");
    }

    /** One row of the logged direct-edge set: output → docling_input, string-typed leaf. */
    private static EdgeTransformDto directLeafRow(String sourcePath, String targetPath) {
        EdgeTransformDto row = new EdgeTransformDto();
        row.setSourceNodeId("duck-start");
        row.setSourcePortId("port-out");
        row.setSourcePortShortCode("output");
        row.setSourcePath(sourcePath);
        row.setTargetNodeId("dc1");
        row.setTargetPortId("port-files");
        row.setTargetPortShortCode("docling_input");
        row.setTargetPath(targetPath);
        row.setTargetDataType("string");
        return row;
    }

    /**
     * The 2026-08-26 15:06 run's exact wiring: {@code voices → files} on the docling input ROOT
     * (plus the autoconnect leaf rows). The start's value picker matched {@code "files"} on the
     * row's TARGET before looking at the source, so it published the EMPTY attachment list under
     * the {@code voices} key — the run log showed {@code "voices":[]} on the docling DTO while the
     * agent branch carried the clip, and Docling read {@code {files: []}} after the inbound shift
     * ("no file input — conversion skipped").
     */
    @Test
    void direct_edge_voices_to_files_root_publishes_the_clip() {
        ProcessContextRef ref = ref();
        ref.setExecutionId("exec-root");

        // EBML magic + DocType element: the delegate's pre-submit AV guard mirrors filetype's
        // Webm matcher, which requires both within the sniffed window.
        byte[] clip = new byte[]{0x1A, 0x45, (byte) 0xDF, (byte) 0xA3, 0x42, (byte) 0x82,
                (byte) 0x84, 'w', 'e', 'b', 'm', 0x18, 0x53, (byte) 0x80, 0x67, 0x01};
        String handle = "sq-file://run/exec-root/clip";
        com.studioquack.sqs.files.ClientFileStore fileStore =
                mock(com.studioquack.sqs.files.ClientFileStore.class);
        when(fileStore.resolve(eq(handle))).thenReturn(Optional.of(
                new com.studioquack.sqs.files.ClientFileStore.StoredFile(
                        "clip", "clip.webm", "audio/webm", clip.length, clip)));

        com.studioquack.sqs.engine.delegate.DuckbotBridge bridge =
                new com.studioquack.sqs.engine.delegate.DuckbotBridge(output -> List.of(),
                        mock(com.studioquack.sqs.conversation.ConversationStore.class),
                        fileStore, new ObjectMapper());
        List<EdgeTransformDto> rows = new ArrayList<>();
        rows.add(directLeafRow("files.name", "files.name"));
        rows.add(directLeafRow("files.source", "files.source"));
        // The user's actual mapping: voices → files root (no string coercion on a file target).
        EdgeTransformDto voicesRow = directLeafRow("voices", "files");
        voicesRow.setTargetDataType(null);
        rows.add(voicesRow);
        rows.add(directLeafRow("files.kind", "files.kind"));
        rows.add(directLeafRow("files.data", "files.data"));
        rows.add(directLeafRow("files.mime_type", "files.mime_type"));
        NodeDto start = new NodeDto();
        start.setId("duck-start");
        com.studioquack.common.dto.flow.TemplateDto startTemplate =
                new com.studioquack.common.dto.flow.TemplateDto();
        startTemplate.setCategory("conversion");
        startTemplate.setNodeType("start");
        start.setData(startTemplate);
        start.setEdgeTransformListForCurrentNode(rows);
        ExecutionContext startCtx = new ExecutionContext();
        startCtx.setCurrentNode(start);
        com.studioquack.common.dto.flow.AgenticInfo ai =
                new com.studioquack.common.dto.flow.AgenticInfo("conv-1", "agent", "gordun mu", true);
        com.studioquack.common.dto.flow.ConversationFileRefDto voice =
                new com.studioquack.common.dto.flow.ConversationFileRefDto();
        voice.setFileId(handle);
        voice.setName("clip.webm");
        voice.setMimeType("audio/webm");
        voice.setKind("audio");
        ai.setVoice(voice);
        startCtx.setAgenticInfo(ai);
        bridge.applyStart(startCtx);
        List<InOutValueDto> startOutputs = start.getOutputList();
        assertEquals(1, startOutputs.size(), "one wire → one DTO");

        // The publish-level regression: voices must carry the clip, never the empty files list.
        @SuppressWarnings("unchecked")
        Map<String, Object> published = (Map<String, Object>) startOutputs.get(0).getValue();
        Object voicesValue = published.get("voices");
        assertInstanceOf(List.class, voicesValue, "voices key must be the voice list");
        assertFalse(((List<?>) voicesValue).isEmpty(),
                "voices → files must publish the voice clip; matching the TARGET name 'files' "
                        + "shipped the empty attachment list instead");

        ExecutionContext ctx = new ExecutionContext();
        NodeDto doclingNode = new NodeDto();
        doclingNode.setId("dc1");
        ctx.setNodes(new ArrayList<>(List.of(doclingNode)));
        com.studioquack.common.dto.flow.EdgeDataDto edgeData =
                new com.studioquack.common.dto.flow.EdgeDataDto();
        edgeData.setEdgeTransforms(rows);
        com.studioquack.common.dto.flow.EdgeDto edge =
                new com.studioquack.common.dto.flow.EdgeDto();
        edge.setSource("duck-start");
        edge.setTarget("dc1");
        edge.setSourceHandle("output-output");
        edge.setTargetHandle("input-docling_input");
        edge.setData(edgeData);
        ctx.setEdges(new ArrayList<>(List.of(edge)));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
        when(contextStore.loadInputsTargetingNode(eq(ref), eq("dc1"))).thenReturn(startOutputs);
        when(refOnlyVarBuilder.toEngineVariables(any())).thenReturn(Map.of());

        java.util.concurrent.atomic.AtomicReference<byte[]> convertedBytes =
                new java.util.concurrent.atomic.AtomicReference<>();
        DoclingBackend backend = (bytes, filename, mimeType, r) -> {
            convertedBytes.set(bytes);
            return MD;
        };
        delegate = delegate(backend, fileStore);
        delegate.execute(execution(ref));

        verify(contextStore, timeout(5_000)).saveOutputs(eq(ref), any());
        org.junit.jupiter.api.Assertions.assertArrayEquals(clip, convertedBytes.get(),
                "voices → files (root) must deliver the clip through the inbound shift");
    }
}
