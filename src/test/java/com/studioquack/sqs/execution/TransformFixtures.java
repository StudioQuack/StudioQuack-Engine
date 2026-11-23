package com.studioquack.sqs.execution;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.SchemaDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.enums.flow.PortType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Node / edge / input fixtures mirroring the {@code BpmnGenerationServiceTest} shapes
 * (custom.sandbox nodes, {@code OUTPUT-*}/{@code INPUT-*}/{@code FUNCTION-*} handles) so the
 * transform tests exercise realistic canvas wiring rather than toy maps.
 */
public final class TransformFixtures {

    private TransformFixtures() {
    }

    /** A sandbox node with a template + one input and one output data port carrying an optional schema. */
    public static NodeDto sandboxNode(String id, String inPortValue, String outPortValue, List<SchemaDto> outSchema) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType("custom.sandbox");
        template.setJobType("custom-script-executor");
        template.setNodeType("serviceTask");
        template.setLabel(id);
        List<PortDto> ports = new ArrayList<>();
        if (inPortValue != null) {
            ports.add(dataPort("in_" + inPortValue, inPortValue, null));
        }
        if (outPortValue != null) {
            ports.add(dataPort("out_" + outPortValue, outPortValue, outSchema));
        }
        template.setPorts(ports);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    public static NodeDto simpleNode(String id, String inPortValue, String outPortValue) {
        return sandboxNode(id, inPortValue, outPortValue, null);
    }

    /**
     * Exclusive gateway with dummy {@code INPUT-in} / {@code OUTPUT-out} handles so look-through
     * graphs can wire A→gw→B the same way the canvas does.
     */
    public static NodeDto gatewayNode(String id) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType("exclusive.gateway");
        template.setNodeType("gateway");
        template.setLabel(id);
        List<PortDto> ports = new ArrayList<>();
        PortDto in = dataPort("in_in", "in", null);
        in.setType(PortType.INPUT);
        ports.add(in);
        ports.add(dataPort("out_out", "out", null));
        template.setPorts(ports);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    public static PortDto dataPort(String id, String value, List<SchemaDto> schema) {
        PortDto port = new PortDto();
        port.setId(id);
        port.setName(value);
        port.setValue(value);
        port.setType(PortType.OUTPUT);
        port.setSchema(schema);
        return port;
    }

    /** A function port carrying request (inputSchema) + response (outputSchema) shapes. */
    public static PortDto functionPort(String value, List<SchemaDto> inputSchema, List<SchemaDto> outputSchema) {
        PortDto port = new PortDto();
        port.setId("fn_" + value);
        port.setName(value);
        port.setValue(value);
        port.setType(PortType.FUNCTION);
        port.setInputSchema(inputSchema);
        port.setOutputSchema(outputSchema);
        return port;
    }

    public static SchemaDto schema(String shortCode, String type) {
        SchemaDto s = new SchemaDto();
        s.setShortCode(shortCode);
        s.setType(type);
        return s;
    }

    /** Edge with handles + optional inbound JOLT transforms. */
    public static EdgeDto edge(String source, String target, String sourceHandle, String targetHandle,
                        List<EdgeTransformDto> transforms) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        edge.setSourceHandle(sourceHandle);
        edge.setTargetHandle(targetHandle);
        EdgeDataDto data = new EdgeDataDto();
        data.setEdgeTransforms(transforms);
        edge.setData(data);
        return edge;
    }

    /** Edge additionally carrying a Ducktales inbound source script. */
    public static EdgeDto edgeWithScript(String source, String target, String sourceHandle, String targetHandle,
                                  List<EdgeTransformDto> transforms, String scriptBody, String scriptLang) {
        EdgeDto edge = edge(source, target, sourceHandle, targetHandle, transforms);
        edge.getData().setScriptContentInput(scriptBody);
        edge.getData().setScriptLanguageInput(scriptLang);
        return edge;
    }

    public static EdgeTransformDto transform(String sourcePath, String targetPath, String targetNodeId,
                                      String targetSchemaType, String targetDataType) {
        EdgeTransformDto t = new EdgeTransformDto();
        t.setSourcePath(sourcePath);
        t.setTargetPath(targetPath);
        t.setTargetNodeId(targetNodeId);
        t.setTargetSchemaType(targetSchemaType);
        t.setTargetDataType(targetDataType);
        return t;
    }

    /** One inbound wire entry as it lands on {@code currentNode.inputList}. */
    public static InOutValueDto input(String sourceNodeId, String sourcePortValue, String targetNodeId,
                               String targetPortValue, Object value) {
        InOutValueDto in = new InOutValueDto();
        in.setNodeId(sourceNodeId);
        in.setPortValue(sourcePortValue);
        in.setTargetNodeId(targetNodeId);
        in.setTargetPortValue(targetPortValue);
        in.setValue(value);
        return in;
    }

    /** Look-through inbound: producer identity + {@code gateway=true} (the hop was a gateway). */
    public static InOutValueDto lookThrough(String producerId, String producerPort, String consumerId,
                                     String consumerPort, Object value) {
        InOutValueDto in = input(producerId, producerPort, consumerId, consumerPort, value);
        in.setGateway(true);
        return in;
    }

    public static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    /**
     * File ContentBlock as sealed on the transform path: metadata + optional {@code fileId}
     * handle. {@code data} is omitted so callers that need inline bytes add it themselves —
     * a sealed hop must not carry base64 beside a handle.
     */
    public static Map<String, Object> fileContentBlock(String name, String mimeType, String fileId) {
        Map<String, Object> block = map(
                "kind", "document",
                "mime_type", mimeType,
                "source", "sq-file",
                "name", name);
        if (fileId != null) {
            block.put("fileId", fileId);
        }
        return block;
    }

    /**
     * AI Agent {@code inputFiles} file-schema children. Deliberately omits {@code fileId} —
     * that is the canvas-declared shape and the reason PortSchemaFilter can drop the handle
     * from a map-shaped value.
     */
    public static List<SchemaDto> fileSchemaChildren() {
        return List.of(
                schema("kind", "string"),
                schema("mime_type", "string"),
                schema("source", "string"),
                schema("data", "string"),
                schema("name", "string"));
    }

    /** Single {@code inputFiles} field, type {@code file}, children {@link #fileSchemaChildren()}. */
    public static SchemaDto inputFilesFileSchema() {
        SchemaDto field = schema("inputFiles", "file");
        field.setChildren(fileSchemaChildren());
        return field;
    }

    /** BV-shaped single shift op: {@code {operation:shift, spec:{src:[tgt]}}}. */
    public static List<Object> shiftSpec(String src, String tgt) {
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("operation", "shift");
        Map<String, Object> spec = new LinkedHashMap<>();
        List<Object> targets = new ArrayList<>();
        targets.add(tgt);
        spec.put(src, targets);
        op.put("spec", spec);
        List<Object> out = new ArrayList<>();
        out.add(op);
        return out;
    }
}
