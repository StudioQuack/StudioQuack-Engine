package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EdgeDataDto implements Serializable {
    private String scriptLanguageInput;
    private String scriptContentInput;
    private String scriptLanguageOutput;
    private String scriptContentOutput;

    /**
     * Active transform language for this edge ("jolt" | "python" | "javascript" | …).
     * Missing/null ⇒ daemon treats as "jolt". Non-jolt ⇒ runtime emits kind=script only
     * and omits kind=jolt (script wins).
     */
    private String transformLanguage;

    /**
     * Optional FE cache of the JOLT-tab authored text. Not required on the daemon hot path;
     * when language is jolt the daemon rebuilds the spec from {@link #edgeTransforms}.
     */
    private String joltNotation;

    private List<EdgeTransformDto> edgeTransforms;
    private List<SchemaItemDto> schemaItems;

    /**
     * Boundary event id when this edge leaves a boundary event.
     * The value EQUALS the host node's {@code TemplateDto.boundaryEventConfigs[].id}
     * (the bare boundary-event id), not the {@code sourceHandle} string.
     * The boundary's target node is this edge's {@code target}, and BPMN generation
     * resolves the target from there — {@code BoundaryEventConfig.targetNodeId} is transient,
     * so it is not persisted.
     */
    private String boundaryEventId;

    /**
     * Tab-state information used by the frontend.
     */
    private List<String> initializedTabs;

    /**
     * BPMN edge kind: {@code SEQUENCE} | {@code MESSAGE} | {@code ASSOCIATION}.
     * Absent/null ⇒ SEQUENCE.
     */
    private String flowType;

    /**
     * Typed gateway-branch condition expression (exclusive/inclusive gateway outbound).
     */
    private String condition;

    /**
     * Explicit default-flow flag for a gateway branch (at most one per gateway source).
     */
    private Boolean isDefaultFlow;
}
