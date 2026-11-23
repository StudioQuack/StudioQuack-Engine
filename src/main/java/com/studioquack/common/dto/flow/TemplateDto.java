package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.annotation.Id;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemplateDto implements Serializable, Cloneable {
    @Id
    private String id;
    /**
     * studioquackt.start
     */
    private String bpmnType;
    /**
     * Canonical BPMN visual/task kind for designer rendering (e.g. service.task).
     */
    private String bpmnVisualType;
    /**
     * Per-instance, per-workflow-unique short code used for DuckTape addressing.
     */
    private String nodeShortCode;
    /**
     * Standard, Function, Variable-Provider
     */
    private String nodeType;
    private String version;
    private String label; // mutable. Name given when the node instance is created; may be updated later
    private String name; // immutable. Name from when the custom node was first created
    private String description;
    private String icon;
    private List<String> tags;
    private String category;
    private String author;
    private String documentation;
    private List<PortDto> ports;
    private List<ParameterDto> configurationParameters = new ArrayList<>();
    private boolean published;
    private boolean custom;
    private String jobType;
    private ExecutionRequirementsDto executionRequirementsDto;
    private String rootId;
    private String ownerId;
    private TemplateScriptDto templateScript;
    private int connectionGroupId;
    private String backgroundColor;
    private boolean agenticNode;
    /**
     * When true the node accepts live stream-IN: graph-routed {@code emit-partial} frames
     * are delivered into a running invocation over {@code POST …/invoke/stream} instead of
     * waiting for the assembled upstream value. Absent/false keeps completion-time fan-out.
     */
    private Boolean streamThrough;
    /**
     * Boundary-event alternative configurations for this node.
     * Provides the same behavior without a native boundary event on the graph.
     */
    private List<BoundaryEventConfig> boundaryEventConfigs;

    public ParameterDto findParameterById(String id) {
        return this.configurationParameters.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getId().equals(id))
                .findFirst()
                .orElse(null);
    }

    public String findParameterValueById(String id) {
        return this.configurationParameters.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getId().equals(id))
                .map(ParameterDto::getValue)
                .map(TemplateDto::stringifyParameterValue)
                .findFirst()
                .orElse(StringUtils.EMPTY);
    }

    public ParameterDto findParameterByShortCode(String shortCode) {
        return this.configurationParameters.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getShortCode().equals(shortCode))
                .findFirst()
                .orElse(null);
    }

    public String findParameterValueByShortCode(String shortCode) {
        return this.configurationParameters.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getShortCode().equals(shortCode))
                .map(ParameterDto::getValue)
                .map(TemplateDto::stringifyParameterValue)
                .filter(StringUtils::isNotEmpty)
                .findFirst()
                .orElse(StringUtils.EMPTY);
    }

    public ParameterDto findParameterByType(String type) {
        return this.configurationParameters.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getType().equals(type))
                .findFirst()
                .orElse(null);
    }

    public String findParameterValueByType(String type) {
        return this.configurationParameters.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getType().equals(type))
                .map(ParameterDto::getValue)
                .map(TemplateDto::stringifyParameterValue)
                .findFirst()
                .orElse(StringUtils.EMPTY);
    }

    /**
     * Turns a parameter value into the string callers expect.
     * Structured values are written as JSON so secret-ref placeholders stay parseable.
     */
    public static String stringifyParameterValue(Object raw) {
        if (raw == null) return StringUtils.EMPTY;
        if (raw instanceof CharSequence) return raw.toString();
        if (raw instanceof Number || raw instanceof Boolean
                || raw instanceof Character) {
            return raw.toString();
        }
        // Structured values go out as JSON so secret-ref placeholders stay parseable.
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(raw);
        } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
            // Fall back to toString when JSON serialization fails.
            return raw.toString();
        }
    }

    public PortDto findPortByPortName(String portName) {
        if (this.ports == null) {
            return null;
        }
        return this.ports.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getName().equals(portName))
                .findFirst()
                .orElse(null);
    }

    public PortDto findPortByPortValue(String portValue) {
        if (this.ports == null) {
            return null;
        }
        return this.ports.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getValue().equals(portValue))
                .findFirst()
                .orElse(null);
    }

    public PortDto findPortByPortId(String portId) {
        if (this.ports == null) {
            return null;
        }
        return this.ports.stream()
                .filter(Objects::nonNull)
                .filter(p -> p.getId().equals(portId))
                .findFirst()
                .orElse(null);
    }

    @Override
    public TemplateDto clone() {
        try {
            return (TemplateDto) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
