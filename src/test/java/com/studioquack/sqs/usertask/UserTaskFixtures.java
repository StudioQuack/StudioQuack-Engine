package com.studioquack.sqs.usertask;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.SchemaDto;
import com.studioquack.common.enums.flow.PortType;

import java.util.ArrayList;
import java.util.List;

/**
 * Graph fixtures shaped like the designer saves them: ports as the {@code user.task} template
 * declares them, and edges wired port-to-port with the designer's {@code output-<port>} /
 * {@code input-<port>} handles — the shape the pipeline's fan-out resolves downstream wires from.
 */
public final class UserTaskFixtures {

    private UserTaskFixtures() {
    }

    /** Adds the template's Task Input (values wrapper) and Task Output (decision) ports. */
    public static NodeDto withUserTaskPorts(NodeDto node) {
        addPort(node, port(node.getId() + "-in", UserTaskIo.TASK_INPUT_PORT, PortType.INPUT,
                schema(UserTaskIo.VALUES_FIELD, "wrapper")));
        addPort(node, port(node.getId() + "-out", UserTaskIo.TASK_OUTPUT_PORT, PortType.OUTPUT,
                schema(UserTaskIo.DECISION_FIELD, "string")));
        return node;
    }

    /** Adds a plain input port named {@code name} (a gateway's or end event's wire target). */
    public static NodeDto withInputPort(NodeDto node, String name) {
        addPort(node, port(node.getId() + "-" + name, name, PortType.INPUT));
        return node;
    }

    /** An edge wired from {@code sourcePort} of {@code source} to {@code targetPort} of {@code target}. */
    public static EdgeDto wire(String source, String sourcePort, String target, String targetPort) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        edge.setSourceHandle("output-" + sourcePort);
        edge.setTargetHandle("input-" + targetPort);
        return edge;
    }

    private static void addPort(NodeDto node, PortDto port) {
        List<PortDto> ports = node.getData().getPorts() == null
                ? new ArrayList<>() : new ArrayList<>(node.getData().getPorts());
        ports.add(port);
        node.getData().setPorts(ports);
    }

    private static PortDto port(String id, String name, PortType type, SchemaDto... schema) {
        PortDto port = new PortDto();
        port.setId(id);
        port.setName(name);
        port.setValue(name);
        port.setType(type);
        port.setSchema(new ArrayList<>(List.of(schema)));
        port.setInputSchema(new ArrayList<>());
        port.setOutputSchema(new ArrayList<>());
        return port;
    }

    private static SchemaDto schema(String shortCode, String type) {
        SchemaDto schema = new SchemaDto();
        schema.setShortCode(shortCode);
        schema.setLabel(shortCode);
        schema.setType(type);
        schema.setChildren(new ArrayList<>());
        return schema;
    }
}
