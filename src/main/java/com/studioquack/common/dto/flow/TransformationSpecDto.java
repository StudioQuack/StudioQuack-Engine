package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Edge data-transformation directive applied before a node runs.
 * {@code jolt} is a declarative spec list; {@code script} is a sandbox snippet that returns the mutated context.
 * Secret references are resolved after the transform.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransformationSpecDto implements Serializable {

    /** Discriminator: {@code "jolt"} or {@code "script"}. Never null on the wire. */
    private String kind;

    /** JOLT spec list ({@code operation} and {@code spec}). Set when {@link #kind} is {@code jolt}. */
    private List<Object> joltSpec;

    /**
     * Inline user script body. Populated when {@link #kind} = "script".
     * Comes from {@code EdgeDataDto.scriptContentInput} /
     * {@code scriptContentOutput} depending on direction.
     */
    private String script;

    /**
     * Script language hint for the daemon (e.g. {@code "python"},
     * {@code "javascript"}). Mirrors the values surfaced by the
     * Ducktales Add Source Script editor.
     */
    private String language;

    /**
     * Source node id this transformation applies to (the node that
     * produced the upstream output).
     */
    private String sourceNodeId;

    /**
     * Target node id this transformation lands on (almost always
     * the {@code currentNode} the daemon is about to execute).
     */
    private String targetNodeId;

    /**
     * Source port short-code — when the same edge feeds multiple
     * inputs the daemon uses this to disambiguate which output
     * value the spec applies to.
     */
    private String sourcePortShortCode;

    /** Target port short-code, mirror of {@link #sourcePortShortCode}. */
    private String targetPortShortCode;

    /**
     * Direction discriminator — {@code "input"} when the spec
     * transforms upstream output into the current node's input,
     * {@code "output"} when it transforms the current node's
     * output before downstream nodes consume it. Lets the daemon
     * decide whether to apply the spec pre-script or post-script.
     */
    private String direction;
}
