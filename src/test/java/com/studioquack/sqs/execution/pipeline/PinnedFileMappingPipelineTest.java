package com.studioquack.sqs.execution.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.ForeignEdgeInputResolver;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import com.studioquack.sqs.files.ClientFileStore;
import com.studioquack.sqs.files.ContentBlockHydrator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * DuckTape Add-Node / whole-block file mapping vs the {@code sq-file:} handle. Leaf-mapping {@code
 * name}/{@code data}/{@code mime_type}/{@code kind}/{@code source} never copies {@code fileId}.
 */
class PinnedFileMappingPipelineTest {

    /** Handle-less deny fixture — short base64, never logged. */
    private static final String INLINE_B64 = "Ynl0ZXM=";

    private final ObjectMapper mapper = new ObjectMapper();
    private final JoltTransformService jolt = new JoltTransformService(mapper);
    private final DefaultTransformPipeline pipeline = new DefaultTransformPipeline(
            new ConnectionBuilder(jolt), new TransformationCollector(jolt), jolt, mapper,
            new com.studioquack.sqs.files.ParameterSpiller(mock(ClientFileStore.class)));

    /**
     * DEFECT: pinned Add-Node leaf rows rebuild {@code inputFiles} without {@code fileId}.
     * Metadata survives; hydrate cannot fill {@code data}. Must fail on current code.
     */
    @Test
    void pinnedLeaf_fileHandleSurvivesSoHydrateCanMaterialiseBytes() {
        NodeDto a = TransformFixtures.simpleNode("A", null, "out");
        NodeDto c = TransformFixtures.simpleNode("C", null, "out");
        NodeDto b = consumerBWithFilePort();

        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-file",
                new ArrayList<>(pinnedFileLeaves()));
        List<EdgeDto> edges = new ArrayList<>(List.of(edge));

        Map<String, Object> sealed = TransformFixtures.fileContentBlock(
                "report.pdf", "application/pdf", "sq-file:pinned-1");
        StubStore store = new StubStore().withOutputs("C", List.of(producedByC(sealed)));

        ExecutionContext ctx = consumerCtx(List.of(a, c, b), edges);
        ProcessContextRef ref = ProcessContextRef.builder().executionId("exec-1").build();
        List<InOutValueDto> foreign = ForeignEdgeInputResolver.resolve(store, ref, "B", edges, null, null);
        ctx.setNextNodeInputList(mergeInbound(ctx.getNextNodeInputList(), foreign));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        Map<String, Object> inputFiles = requireInputFilesMap(prepared.inputVariables().get("file"));
        assertEquals("report.pdf", inputFiles.get("name"), "leaf-mapped name must survive");
        assertEquals("application/pdf", inputFiles.get("mime_type"), "leaf-mapped mime_type must survive");

        byte[] bytes = "pdf".getBytes(StandardCharsets.UTF_8);
        ClientFileStore files = mock(ClientFileStore.class);
        when(files.resolve(eq("sq-file:pinned-1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("pinned-1", "report.pdf", "application/pdf",
                        bytes.length, bytes)));
        new ContentBlockHydrator(files).hydrate(inputFiles);
        Object data = inputFiles.get("data");

        assertAll(
                () -> assertEquals("sq-file:pinned-1", inputFiles.get("fileId"),
                        "pinned leaf mapping must keep the sq-file: handle — today JOLT drops unmapped fileId"),
                () -> assertTrue(ClientFileStore.isRef(inputFiles.get("fileId")),
                        "handle must remain a store ref so hydrate can resolve it"),
                () -> assertTrue(data instanceof String s && !s.isBlank(),
                        "hydrate must fill data from the surviving handle — today data stays empty"));
    }

    /**
     * WORKING PATH: whole-block list ({@code files → inputFiles}) fail-opens in the schema
     * filter (non-Map). Pin this so a handle-preserving fix cannot regress the real wire.
     */
    @Test
    void realWire_wholeBlockList_keepsHandleAndHydrates() {
        NodeDto a = TransformFixtures.simpleNode("A", null, "out");
        NodeDto b = consumerBWithFilePort();
        EdgeTransformDto row = TransformFixtures.transform("files", "inputFiles", "B", "schema", null);
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-file",
                new ArrayList<>(List.of(row)));

        Map<String, Object> block = TransformFixtures.fileContentBlock(
                "wired.pdf", "application/pdf", "sq-file:wire-1");
        ExecutionContext ctx = consumerCtx(List.of(a, b), List.of(edge));
        ctx.setNextNodeInputList(new ArrayList<>(List.of(
                TransformFixtures.input("A", "out", "B", "file",
                        TransformFixtures.map("files", List.of(block))))));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        Map<String, Object> inputFiles = requireInputFilesBlockFromList(prepared.inputVariables().get("file"));
        assertEquals("sq-file:wire-1", inputFiles.get("fileId"),
                "list-shaped whole-block mapping must keep fileId (schema filter fail-opens on List)");

        byte[] bytes = "wire".getBytes(StandardCharsets.UTF_8);
        ClientFileStore files = mock(ClientFileStore.class);
        when(files.resolve(eq("sq-file:wire-1"))).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("wire-1", "wired.pdf", "application/pdf",
                        bytes.length, bytes)));
        new ContentBlockHydrator(files).hydrate(inputFiles);
        Object data = inputFiles.get("data");
        assertTrue(data instanceof String s && !s.isBlank(),
                "hydrate fills data when the list-shaped handle survived");
    }

    /**
     * DENY: handle-less {@code source=sq-file} plus inline base64 is the only copy.
     * FileRefSanitizer must not treat the bare source token as a ref and strip data to empty.
     */
    @Test
    void deny_handlelessInlineBytes_notStrippedToEmptiness() {
        NodeDto a = TransformFixtures.simpleNode("A", null, "out");
        NodeDto c = TransformFixtures.simpleNode("C", null, "out");
        NodeDto b = consumerBWithFilePort();

        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-file",
                new ArrayList<>(pinnedFileLeaves()));
        List<EdgeDto> edges = new ArrayList<>(List.of(edge));

        Map<String, Object> handleless = TransformFixtures.fileContentBlock("clip.webm", "audio/webm", null);
        handleless.put("data", INLINE_B64);
        StubStore store = new StubStore().withOutputs("C", List.of(producedByC(handleless)));

        ExecutionContext ctx = consumerCtx(List.of(a, c, b), edges);
        ProcessContextRef ref = ProcessContextRef.builder().executionId("exec-1").build();
        ctx.setNextNodeInputList(mergeInbound(ctx.getNextNodeInputList(),
                ForeignEdgeInputResolver.resolve(store, ref, "B", edges, null, null)));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        Map<String, Object> inputFiles = requireInputFilesMap(prepared.inputVariables().get("file"));
        assertEquals(INLINE_B64, inputFiles.get("data"),
                "handle-less source=sq-file must keep inline data — it is the only copy");
        assertFalse(ClientFileStore.isRef(inputFiles.get("fileId")),
                "fileId may be absent; a bare source token is not a store handle");
    }

    /**
     * DEFECT (sibling of pinned leaf): whole-block MAP {@code doc → inputFiles} still has
     * {@code fileId} after JOLT, but PortSchemaFilter drops it because schema children omit
     * {@code fileId}. Must fail on current code.
     */
    @Test
    void wholeBlockMap_keepsHandleDespiteSchemaOmittingFileId() {
        NodeDto a = TransformFixtures.simpleNode("A", null, "out");
        NodeDto b = consumerBWithFilePort();
        EdgeTransformDto row = TransformFixtures.transform("doc", "inputFiles", "B", "schema", null);
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-file",
                new ArrayList<>(List.of(row)));

        Map<String, Object> block = TransformFixtures.fileContentBlock(
                "block.pdf", "application/pdf", "sq-file:block-1");
        ExecutionContext ctx = consumerCtx(List.of(a, b), List.of(edge));
        ctx.setNextNodeInputList(new ArrayList<>(List.of(
                TransformFixtures.input("A", "out", "B", "file",
                        TransformFixtures.map("doc", block)))));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        Map<String, Object> inputFiles = requireInputFilesMap(prepared.inputVariables().get("file"));
        assertEquals("sq-file:block-1", inputFiles.get("fileId"),
                "map-shaped whole-block must keep fileId even though schema children omit it");
    }

    /**
     * DEFECT (JOLT-only): leaf shift of name/data/mime never copies {@code fileId}.
     * Sanitizer strips inline data when a handle is present; the handle itself must still
     * ride through. Must fail on current code (fileId gone after shift).
     */
    @Test
    void joltLeaf_fileIdRidesThroughWhenDataIsSanitized() {
        Map<String, Object> input = TransformFixtures.fileContentBlock(
                "report.pdf", "application/pdf", "sq-file:jolt-1");
        input.put("data", INLINE_B64);

        List<EdgeTransformDto> rows = List.of(
                TransformFixtures.transform("name", "name", "B", "schema", null),
                TransformFixtures.transform("data", "data", "B", "schema", null),
                TransformFixtures.transform("mime_type", "mime_type", "B", "schema", null));

        @SuppressWarnings("unchecked")
        Map<String, Object> got = (Map<String, Object>) jolt.transform(input, jolt.convertJoltSpec(rows));

        assertEquals("report.pdf", got.get("name"));
        assertEquals("application/pdf", got.get("mime_type"));
        assertFalse(got.containsKey("data") || (got.get("data") instanceof String s && !s.isBlank()),
                "sanitizer strips inline data when a handle is present");
        assertEquals("sq-file:jolt-1", got.get("fileId"),
                "leaf shift must keep fileId — today unmapped fileId is dropped");
    }

    /** B's INPUT port is {@code file} so handle {@code INPUT-file} matches {@code findPortByPortName}. */
    private static NodeDto consumerBWithFilePort() {
        NodeDto b = TransformFixtures.simpleNode("B", "file", "out");
        PortDto file = b.getData().findPortByPortName("file");
        file.setType(PortType.INPUT);
        file.setSchema(List.of(TransformFixtures.inputFilesFileSchema()));
        return b;
    }

    /** DuckTape leaf rows pinned to C — never maps {@code fileId}. */
    private static List<EdgeTransformDto> pinnedFileLeaves() {
        return List.of(
                pinnedLeaf("name", "inputFiles.name"),
                pinnedLeaf("data", "inputFiles.data"),
                pinnedLeaf("mime_type", "inputFiles.mime_type"),
                pinnedLeaf("kind", "inputFiles.kind"),
                pinnedLeaf("source", "inputFiles.source"));
    }

    private static EdgeTransformDto pinnedLeaf(String sourcePath, String targetPath) {
        EdgeTransformDto t = TransformFixtures.transform(sourcePath, targetPath, "B", "schema", null);
        t.setSourceNodeId("C");
        t.setSourcePortId("out_out");
        t.setTargetPortId("in_file");
        return t;
    }

    private static ExecutionContext consumerCtx(List<NodeDto> nodes, List<EdgeDto> edges) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId("B");
        ctx.setNodes(new ArrayList<>(nodes));
        ctx.setEdges(new ArrayList<>(edges));
        ctx.setNextNodeInputList(new ArrayList<>());
        return ctx;
    }

    /** C's fanned-out port must be {@code out_out} so the pinned row matches. */
    private static InOutValueDto producedByC(Object value) {
        InOutValueDto dto = new InOutValueDto();
        dto.setNodeId("C");
        dto.setPortId("out_out");
        dto.setPortValue("out");
        dto.setValue(value);
        return dto;
    }

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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> requireInputFilesMap(Object portValue) {
        assertInstanceOf(Map.class, portValue, "consumer file port value must be a map");
        Object inputFiles = ((Map<String, Object>) portValue).get("inputFiles");
        assertInstanceOf(Map.class, inputFiles, "inputFiles must be a ContentBlock map");
        return (Map<String, Object>) inputFiles;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> requireInputFilesBlockFromList(Object portValue) {
        assertInstanceOf(Map.class, portValue, "consumer file port value must be a map");
        Object inputFiles = ((Map<String, Object>) portValue).get("inputFiles");
        assertInstanceOf(List.class, inputFiles, "whole-block list mapping yields a list");
        List<?> list = (List<?>) inputFiles;
        assertFalse(list.isEmpty(), "inputFiles list must contain the ContentBlock");
        assertInstanceOf(Map.class, list.get(0));
        return (Map<String, Object>) list.get(0);
    }

    /** Minimal store: serves prepared outputs per node (same shape as ForeignEdgeInputResolverTest). */
    private static final class StubStore implements ExecutionContextStore {
        private final Map<String, List<InOutValueDto>> byNode = new LinkedHashMap<>();

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
            Map<String, List<InOutValueDto>> out = new LinkedHashMap<>();
            for (String id : nodeIds) {
                if (byNode.containsKey(id)) {
                    out.put(id, byNode.get(id));
                }
            }
            return out;
        }

        @Override
        public ProcessContextRef snapshot(com.studioquack.sqs.execution.context.ExecutionContext ctx, Long jobKey) {
            return null;
        }

        @Override
        public Optional<com.studioquack.sqs.execution.context.ExecutionContext> loadByRef(ProcessContextRef ref) {
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
