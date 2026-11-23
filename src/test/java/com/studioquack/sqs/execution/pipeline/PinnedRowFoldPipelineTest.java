package com.studioquack.sqs.execution.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.ForeignEdgeInputResolver;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import com.studioquack.sqs.files.ClientFileStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

/**
 * Connection lines decide the message count; attribute mappings never do. A port fed by N
 * connection lines carries N messages.
 */
class PinnedRowFoldPipelineTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JoltTransformService jolt = new JoltTransformService(mapper);
    private final DefaultTransformPipeline pipeline = new DefaultTransformPipeline(
            new ConnectionBuilder(jolt), new TransformationCollector(jolt), jolt, mapper,
            new com.studioquack.sqs.files.ParameterSpiller(mock(ClientFileStore.class)));

    @Test
    void primaryWireAndPinnedSource_bothLandOnTheOneInputObject() {
        NodeDto a = TransformFixtures.simpleNode("A", null, "out");
        NodeDto c = TransformFixtures.simpleNode("C", null, "out");
        NodeDto b = TransformFixtures.simpleNode("B", "input", "out");

        // A maps title → input.title (primary wire); C is pinned on the A→B screen and maps
        // file → input.attachment.
        EdgeTransformDto fromA = TransformFixtures.transform("title", "title", "B", "schema", null);
        EdgeTransformDto fromC = TransformFixtures.transform("file", "attachment", "B", "schema", null);
        fromC.setSourceNodeId("C");
        fromC.setSourcePortId("out_out");
        fromC.setTargetPortId("in_input");
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-input",
                new ArrayList<>(List.of(fromA, fromC)));

        InOutValueDto wireA = TransformFixtures.input("A", "out", "B", "input",
                TransformFixtures.map("title", "Defect #1"));
        InOutValueDto pinnedC = TransformFixtures.input("C", "out", "B", "input",
                TransformFixtures.map("file", TransformFixtures.map("name", "shot.png", "fileId", "sq-file:c-1")));
        pinnedC.setPortId("out_out");
        pinnedC.setTargetPortId("in_input");

        List<InOutValueDto> merged = new ArrayList<>(List.of(wireA, pinnedC));
        ExecutionContext ctx = consumerCtx(List.of(a, c, b), List.of(edge), merged);
        ctx.setPinnedInputCarriers(carriers(merged, pinnedC, "A"));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);

        Object input = prepared.inputVariables().get("input");
        assertInstanceOf(Map.class, input, "the consuming port carries one object");
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = (Map<String, Object>) input;
        assertEquals("Defect #1", obj.get("title"), "the primary wire's field is kept");
        assertInstanceOf(Map.class, obj.get("attachment"), "the pinned source's field is present too");
        assertEquals(1, ctx.getCurrentNode().getInputList().size(),
                "the pinned row folded into the wire's row, so the SDK's inputs() sees the same object");
    }

    @Test
    void pinnedFileParam_isRenamedByTheScreensJoltAndLandsOnTheTargetField_noUnknownParameter() {
        // Real case: A (Duckbot start, {msg, files}) → B.input; a tile C pinned on that screen maps
        // its file-typed `files` onto B's file-typed `file`. B.input schema = {msg, file}.
        // Expected: one message {msg, file:<C's block>} and NO "Unknown parameters: input.files".
        NodeDto a = TransformFixtures.simpleNode("A", null, "out");
        NodeDto c = TransformFixtures.simpleNode("C", null, "out");
        NodeDto b = TransformFixtures.simpleNode("B", "input", "out");
        com.studioquack.common.dto.flow.PortDto input = b.getData().findPortByPortName("input");
        input.setType(com.studioquack.common.enums.flow.PortType.INPUT);
        com.studioquack.common.dto.flow.SchemaDto file = TransformFixtures.schema("file", "file");
        file.setChildren(TransformFixtures.fileSchemaChildren());
        input.setSchema(List.of(TransformFixtures.schema("msg", "string"), file));

        EdgeTransformDto msgRow = TransformFixtures.transform("msg", "msg", "B", "string", null);
        EdgeTransformDto fileRow = TransformFixtures.transform("files", "file", "B", "file", "list");
        fileRow.setSourceNodeId("C");
        fileRow.setSourcePortId("out_out");
        fileRow.setTargetPortId("in_input");
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-input",
                new ArrayList<>(List.of(msgRow, fileRow)));

        Map<String, Object> block = TransformFixtures.fileContentBlock("shot.png", "image/png", "sq-file:c-1");
        InOutValueDto wireA = TransformFixtures.input("A", "out", "B", "input",
                TransformFixtures.map("msg", "check this", "files", new ArrayList<>()));
        InOutValueDto pinnedC = TransformFixtures.input("C", "out", "B", "input",
                TransformFixtures.map("files", new ArrayList<>(List.of(block))));
        pinnedC.setPortId("out_out");
        pinnedC.setTargetPortId("in_input");
        List<InOutValueDto> merged = new ArrayList<>(List.of(wireA, pinnedC));
        ExecutionContext ctx = consumerCtx(List.of(a, c, b), List.of(edge), merged);
        ctx.setPinnedInputCarriers(carriers(merged, pinnedC, "A"));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        assertEquals(1, ctx.getCurrentNode().getInputList().size(), "one message on B.input");
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = (Map<String, Object>) prepared.inputVariables().get("input");
        assertEquals("check this", obj.get("msg"));
        Object landed = obj.get("file");
        Object first = landed instanceof List<?> l && !l.isEmpty() ? l.get(0) : landed;
        assertInstanceOf(Map.class, first, "C's file block landed on B's `file`, got: " + landed);
        assertEquals("shot.png", ((Map<?, ?>) first).get("name"));
        assertEquals(List.of(), ctx.getDroppedInputParameters() == null ? List.of() : ctx.getDroppedInputParameters(),
                "nothing outside B.input's schema may survive to be reported as unknown");
    }

    @Test
    void authoredScreen_wireWithNoRowsContributesNothingRaw_noUnknownParameter() {
        // Same screen, but the author connected ONLY the pinned tile (C.files → B.file) and left A's
        // attributes unconnected. The screen defines the message: A's raw {msg, files} must not pass
        // through beside C's mapped field — that raw `files` is what surfaced as
        // "Unknown parameters: input.files".
        NodeDto a = TransformFixtures.simpleNode("A", null, "out");
        NodeDto c = TransformFixtures.simpleNode("C", null, "out");
        NodeDto b = TransformFixtures.simpleNode("B", "input", "out");
        com.studioquack.common.dto.flow.PortDto input = b.getData().findPortByPortName("input");
        input.setType(com.studioquack.common.enums.flow.PortType.INPUT);
        com.studioquack.common.dto.flow.SchemaDto file = TransformFixtures.schema("file", "file");
        file.setChildren(TransformFixtures.fileSchemaChildren());
        input.setSchema(List.of(TransformFixtures.schema("msg", "string"), file));

        EdgeTransformDto fileRow = TransformFixtures.transform("files", "file", "B", "file", "list");
        fileRow.setSourceNodeId("C");
        fileRow.setSourcePortId("out_out");
        fileRow.setTargetPortId("in_input");
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-input",
                new ArrayList<>(List.of(fileRow)));

        Map<String, Object> block = TransformFixtures.fileContentBlock("shot.png", "image/png", "sq-file:c-1");
        InOutValueDto wireA = TransformFixtures.input("A", "out", "B", "input",
                TransformFixtures.map("msg", "check this", "files", new ArrayList<>()));
        InOutValueDto pinnedC = TransformFixtures.input("C", "out", "B", "input",
                TransformFixtures.map("files", new ArrayList<>(List.of(block))));
        pinnedC.setPortId("out_out");
        pinnedC.setTargetPortId("in_input");
        List<InOutValueDto> merged = new ArrayList<>(List.of(wireA, pinnedC));
        ExecutionContext ctx = consumerCtx(List.of(a, c, b), List.of(edge), merged);
        ctx.setPinnedInputCarriers(carriers(merged, pinnedC, "A"));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);
        prepared = pipeline.filterInboundAgainstPortSchemas(ctx, prepared);

        assertEquals(1, ctx.getCurrentNode().getInputList().size(), "one message on B.input");
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = (Map<String, Object>) prepared.inputVariables().get("input");
        Object landed = obj.get("file");
        Object first = landed instanceof List<?> l && !l.isEmpty() ? l.get(0) : landed;
        assertInstanceOf(Map.class, first, "C's file block landed on B's `file`, got: " + landed);
        assertNull(obj.get("msg"), "an unconnected attribute of the wire does not flow on an authored screen");
        assertEquals(List.of(), ctx.getDroppedInputParameters() == null ? List.of() : ctx.getDroppedInputParameters(),
                "the wire's raw `files` must not reach the filter — that was the reported unknown parameter");
    }

    @Test
    void twoLinesIntoOnePort_pinnedTileEnrichesItsOwnLineOnly() {
        // A→C and B→C are two connection lines into C.input: two messages, always. A tile D pinned
        // on the B→C screen belongs to B's message — never A's, never a third message.
        NodeDto c = TransformFixtures.simpleNode("C", "input", "out");
        EdgeDto ac = TransformFixtures.edge("A", "C", "OUTPUT-out", "INPUT-input", new ArrayList<>());
        EdgeDto bc = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-input", new ArrayList<>());

        InOutValueDto wireA = TransformFixtures.input("A", "out", "C", "input", TransformFixtures.map("from", "A"));
        InOutValueDto wireB = TransformFixtures.input("B", "out", "C", "input", TransformFixtures.map("from", "B"));
        InOutValueDto pinnedD = TransformFixtures.input("D", "out", "C", "input", TransformFixtures.map("extra", "d"));
        pinnedD.setPortId("out_out");
        List<InOutValueDto> merged = new ArrayList<>(List.of(wireA, wireB, pinnedD));
        ExecutionContext ctx = consumerCtxFor("C", List.of(c), List.of(ac, bc), merged);
        ctx.setPinnedInputCarriers(carriers(merged, pinnedD, "B"));

        pipeline.prepareInbound(ctx);

        List<InOutValueDto> rows = ctx.getCurrentNode().getInputList();
        assertEquals(2, rows.size(), "two connection lines → two messages, the tile is not a third");
        assertSame(wireA, rows.get(0));
        assertSame(wireB, rows.get(1));
        assertNull(((Map<?, ?>) wireA.getValue()).get("extra"), "A's message is untouched");
        assertEquals("d", ((Map<?, ?>) wireB.getValue()).get("extra"), "D enriched B's message");
    }

    @Test
    void pinnedTileWhoseOwnLineDeliveredNothing_becomesThatLinesMessage() {
        // A→C fired, B→C did not (B was skipped); a tile D pinned on the B→C screen still resolved.
        // D is the B→C line's message — not merged into A's, and only one message per line.
        NodeDto c = TransformFixtures.simpleNode("C", "input", "out");
        EdgeDto ac = TransformFixtures.edge("A", "C", "OUTPUT-out", "INPUT-input", new ArrayList<>());
        EdgeDto bc = TransformFixtures.edge("B", "C", "OUTPUT-out", "INPUT-input", new ArrayList<>());

        InOutValueDto wireA = TransformFixtures.input("A", "out", "C", "input", TransformFixtures.map("from", "A"));
        InOutValueDto pinnedD = TransformFixtures.input("D", "out", "C", "input", TransformFixtures.map("x", 1));
        pinnedD.setPortId("out_out");
        InOutValueDto pinnedE = TransformFixtures.input("E", "out", "C", "input", TransformFixtures.map("y", 2));
        pinnedE.setPortId("out_out");
        List<InOutValueDto> merged = new ArrayList<>(List.of(wireA, pinnedD, pinnedE));
        ExecutionContext ctx = consumerCtxFor("C", List.of(c), List.of(ac, bc), merged);
        Map<String, String> carriers = new LinkedHashMap<>();
        carriers.put(ForeignEdgeInputResolver.bindingKey(pinnedD), "B");
        carriers.put(ForeignEdgeInputResolver.bindingKey(pinnedE), "B");
        ctx.setPinnedInputCarriers(carriers);

        pipeline.prepareInbound(ctx);

        List<InOutValueDto> rows = ctx.getCurrentNode().getInputList();
        assertEquals(2, rows.size(), "A's message plus ONE message for the B→C line");
        assertNull(((Map<?, ?>) wireA.getValue()).get("x"), "A's message is untouched");
        Map<?, ?> lineB = (Map<?, ?>) rows.get(1).getValue();
        assertEquals(1, lineB.get("x"));
        assertEquals(2, lineB.get("y"), "the second tile on the same line folded into the first");
    }

    @Test
    void gatewayLine_pinnedTileFoldsIntoTheLookThroughWire() {
        // A→gw→C: the wire row on C carries the real producer A with gateway=true; a tile pinned on
        // the gw→C screen has carrier gw and must land on that look-through row.
        NodeDto c = TransformFixtures.simpleNode("C", "input", "out");
        EdgeDto gwc = TransformFixtures.edge("gw", "C", "OUTPUT-out", "INPUT-input", new ArrayList<>());
        InOutValueDto viaGw = TransformFixtures.lookThrough("A", "out", "C", "input", TransformFixtures.map("from", "A"));
        InOutValueDto pinnedD = TransformFixtures.input("D", "out", "C", "input", TransformFixtures.map("extra", "d"));
        pinnedD.setPortId("out_out");
        List<InOutValueDto> merged = new ArrayList<>(List.of(viaGw, pinnedD));
        ExecutionContext ctx = consumerCtxFor("C", List.of(c), List.of(gwc), merged);
        ctx.setPinnedInputCarriers(carriers(merged, pinnedD, "gw"));

        pipeline.prepareInbound(ctx);

        assertEquals(1, ctx.getCurrentNode().getInputList().size());
        assertEquals("d", ((Map<?, ?>) viaGw.getValue()).get("extra"));
    }

    @Test
    void genuineMultiWirePort_isNeverFolded() {
        NodeDto b = TransformFixtures.simpleNode("B", "input", "out");
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-input", new ArrayList<>());
        InOutValueDto wireA = TransformFixtures.input("A", "out", "B", "input", TransformFixtures.map("role", "user"));
        InOutValueDto wireX = TransformFixtures.input("X", "out", "B", "input", TransformFixtures.map("role", "system"));
        List<InOutValueDto> merged = new ArrayList<>(List.of(wireA, wireX));
        ExecutionContext ctx = consumerCtx(List.of(b), List.of(edge), merged);
        ctx.setPinnedInputCarriers(Map.of());

        pipeline.prepareInbound(ctx);

        assertEquals(2, ctx.getCurrentNode().getInputList().size(), "unpinned rows are untouched");
    }

    @Test
    void pinnedListJoinsTheWiresListOnTheSamePort_oneMessage() {
        NodeDto b = TransformFixtures.simpleNode("B", "files", "out");
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-files", new ArrayList<>());
        InOutValueDto wireA = TransformFixtures.input("A", "out", "B", "files",
                new ArrayList<>(List.of(TransformFixtures.map("name", "a.png"))));
        InOutValueDto pinnedC = TransformFixtures.input("C", "out", "B", "files",
                new ArrayList<>(List.of(TransformFixtures.map("name", "c.pdf"))));
        pinnedC.setPortId("out_out");
        List<InOutValueDto> merged = new ArrayList<>(List.of(wireA, pinnedC));
        ExecutionContext ctx = consumerCtx(List.of(b), List.of(edge), merged);
        ctx.setPinnedInputCarriers(carriers(merged, pinnedC, "A"));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);

        assertEquals(1, ctx.getCurrentNode().getInputList().size(), "one message on the port");
        List<?> files = (List<?>) prepared.inputVariables().get("files");
        assertEquals(2, files.size(), "the wire's file, then the pinned file");
        assertEquals("a.png", ((Map<?, ?>) files.get(0)).get("name"));
        assertEquals("c.pdf", ((Map<?, ?>) files.get(1)).get("name"));
    }

    @Test
    void pinnedValueOfAnotherShape_isDroppedNotSentAsASecondMessage() {
        NodeDto b = TransformFixtures.simpleNode("B", "input", "out");
        EdgeDto edge = TransformFixtures.edge("A", "B", "OUTPUT-out", "INPUT-input", new ArrayList<>());
        InOutValueDto wireA = TransformFixtures.input("A", "out", "B", "input", TransformFixtures.map("title", "t"));
        InOutValueDto pinnedC = TransformFixtures.input("C", "out", "B", "input", List.of("a", "b"));
        pinnedC.setPortId("out_out");
        List<InOutValueDto> merged = new ArrayList<>(List.of(wireA, pinnedC));
        ExecutionContext ctx = consumerCtx(List.of(b), List.of(edge), merged);
        ctx.setPinnedInputCarriers(carriers(merged, pinnedC, "A"));

        PreparedInbound prepared = pipeline.prepareInbound(ctx);

        assertEquals(1, ctx.getCurrentNode().getInputList().size(),
                "never a second message on the port, whatever the pinned shape");
        Map<?, ?> obj = (Map<?, ?>) prepared.inputVariables().get("input");
        assertEquals("t", obj.get("title"), "the wire's object is left exactly as produced");
        assertNull(obj.get("0"), "a list cannot be spliced into an object");
    }

    /** {@code bindingKey → carrier} the dispatcher hands over, for one pinned row. */
    private static Map<String, String> carriers(List<InOutValueDto> merged, InOutValueDto pinned, String carrier) {
        Map<InOutValueDto, String> byRow = new java.util.IdentityHashMap<>();
        byRow.put(pinned, carrier);
        return ForeignEdgeInputResolver.pinnedCarriers(merged,
                new ForeignEdgeInputResolver.Resolution(List.of(pinned), byRow));
    }

    private static ExecutionContext consumerCtx(List<NodeDto> nodes, List<EdgeDto> edges, List<InOutValueDto> inputs) {
        return consumerCtxFor("B", nodes, edges, inputs);
    }

    private static ExecutionContext consumerCtxFor(String nodeId, List<NodeDto> nodes, List<EdgeDto> edges,
                                                   List<InOutValueDto> inputs) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("exec-1");
        ctx.setCurrentNodeId(nodeId);
        ctx.setNodes(new ArrayList<>(nodes));
        ctx.setEdges(new ArrayList<>(edges));
        ctx.setNextNodeInputList(inputs);
        return ctx;
    }
}
