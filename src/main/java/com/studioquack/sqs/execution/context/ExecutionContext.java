package com.studioquack.sqs.execution.context;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.studioquack.common.dto.flow.AgenticInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.FunctionPortTransformsDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.SpecListForNodeOutputDto;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sealed execution context for one node dispatch.
 * The transform pipeline mutates it in memory; only an encrypted handle is stored on the engine.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutionContext implements Serializable {

    // ---- Core identifiers -------------------------------------------------
    private String workflowExecutionId;
    private String workflowId;
    private int workflowVersion;
    private String workflowName;
    private String userId;
    /** Target environment id — matches {@code ProcessContextRef.envId} / {@code ExecutionContext.envId}. */
    private String envId;
    /**
 * Set by the control plane only on a Create Node trial run: the node's custom image runs in a
 * temporary container started for its job and removed after it, never in a stateful instance
 * or a warm slot. False on canvas runs and deployed runs.
 */
    private boolean temporaryContainer;
    /**
 * Set by the control plane only alongside {@link #temporaryContainer} for an inject-mode trial: the
 * build-context id of the workspace staged on this engine, copied into the temporary
 * container's bespoke folder before the node runs.
 */
    private String stagedWorkspaceId;

    /**
 * Duckbot / agentic conversation context, preserved from the control plane seedContext so the daemon's
 * Duckbot start/end bridges can (a) inject the user prompt as the start node's output and
 * (b) route the workflow's final answer back to the conversation. Previously dropped by this
 * client context (unknown JSON was ignored), which broke the Duckbot start→workflow input and
 * the end→conversation-screen output for client-orchestrated runs.
 */
    private AgenticInfo agenticInfo;

    // ---- Graph (present only until clearNodeAndEdges()) -------------------
    private List<NodeDto> nodes;
    private List<EdgeDto> edges;

    /**
 * Workflow global variables sealed at run start from the deploy artifact.
 * Addressed by {@code $(wf.key)} in {@code ExpressionResolver}. Plaintext design-time
 * configuration — never secrets.
 */
    private Map<String, Object> workflowVariables;

    /**
 * Project variables sealed at run start from the deploy artifact.
 * Addressed by {@code $(project.key)} in {@code ExpressionResolver}. Plaintext
 * design-time configuration — never secrets.
 */
    private Map<String, Object> projectVariables;

    // ---- Per-dispatch state ----------------------------------------------
    private String jobKey;
    private String currentNodeId;
    private NodeDto currentNode;
    private List<InOutValueDto> nextNodeInputList = new ArrayList<>();
    private List<SpecListForNodeOutputDto> specListForNodeOutput;

    /**
     * Per-edge inbound transform specs (JOLT + user-authored script) targeting
     * {@link #currentNode}, pre-collected <b>before</b> {@link #clearNodeAndEdges()}.
     *
     * <p>{@code @JsonIgnore} — a strictly in-process hand-off between the pipeline's
     * pre-collect phase and its pre-bake phase. It must NEVER be baked into the sandbox
     * {@code __SQ_CONTEXT__} blob or persisted to the context store (Mapping Invariant 6:
     * the {@code __sq_transformation__} directive is stripped from the context the user
     * script sees).
     */
    @JsonIgnore
    private List<TransformationSpecDto> inboundTransformSpecs;

    /**
     * Per-function-port transform directives (request + response lanes) the
     * {@code studioquack.execute()} route consumes. Same {@code @JsonIgnore} rationale as
     * {@link #inboundTransformSpecs}; the single-owner rule (Mapping Invariant 10) means
     * these are emitted onto the wire exactly once, at the caller node.
     */
    @JsonIgnore
    private Map<String, FunctionPortTransformsDto> functionPortTransforms;

    /**
     * Top-level inbound parameter names dropped by {@code filterInboundAgainstPortSchemas}
     * because they were outside the target port's declared schema — recorded as
     * {@code portShortCode.paramName}, sorted. Strictly in-process; never baked into the
     * sandbox {@code __SQ_CONTEXT__} blob.
     */
    @JsonIgnore
    private List<String> droppedInputParameters;

    /**
     * {@code bindingKey → carrier} for the {@code inputList} rows the resolver delivered for
     * DuckTape Add-Node (pinned) sources and Workflow/Project Globals. The carrier is the source
     * node of the connection line whose DuckTape screen the tile was pinned on. Right after the
     * JOLT pre-bake the pipeline folds each such row into THAT line's message (the wire row from
     * the carrier), so a port fed by N connection lines still carries exactly N messages and a
     * pinned tile only enriches the line it belongs to. Without the fold the port map and the
     * helper SDK's {@code inputs()} both keep only the last row on a port and silently drop the
     * other source's fields. Strictly in-process; never baked into the sandbox
     * {@code __SQ_CONTEXT__} blob or persisted.
     */
    @JsonIgnore
    private Map<String, String> pinnedInputCarriers;

    /**
 * Output port names whose value came from the operator's {@code apply-inputs} OUTPUT override
 * (Skip &amp; Continue fan-out,). Exactly those ports are exempt from the output
 * port-schema filter for that fan-out: an output field the operator added must fan out to
 * downstream nodes and the record, never be stripped against the port's declared schema.
 * Strictly in-process; never baked into the sandbox {@code __SQ_CONTEXT__} blob or persisted.
 */
    @JsonIgnore
    private Set<String> operatorOverrideOutputPorts;

    /**
     * Resolves {@link #currentNode} from {@link #currentNodeId} within {@link #nodes} and
     * moves the pending {@link #nextNodeInputList} onto the node's {@code inputList} — the
     * canonical home the inbound JOLT pre-bake and the helper SDK read from. Idempotent:
     * a no-op once {@code currentNode} is already resolved.
     */
    public void createCurrentNode() {
        if (currentNode != null) {
            return;
        }
        if (CollectionUtils.isEmpty(nodes) || StringUtils.isEmpty(currentNodeId)) {
            return;
        }
        nodes.stream()
                .filter(node -> currentNodeId.equals(node.getId()))
                .findFirst()
                .ifPresent(this::setCurrentNode);
        if (currentNode != null) {
            currentNode.setInputList(nextNodeInputList);
            nextNodeInputList = null;
        }
    }

    /**
 * Drops the graph after the connection maps + inbound transform specs have been
 * collected. Mirrors the control plane contract so the pre-collect-before-clear ordering
 * (Mapping Invariant 1) is enforced by construction.
 */
    public void clearNodeAndEdges() {
        this.nodes = null;
        this.edges = null;
    }

    @JsonIgnore
    public String findCurrentNodeLanguage() {
        return currentNode == null ? null : currentNode.findCurrentNodeLanguage();
    }

    @JsonIgnore
    public String findCurrentNodeScript() {
        return currentNode == null ? null : currentNode.findCurrentNodeScript();
    }

    @JsonIgnore
    public boolean hasInboundSpecs() {
        return CollectionUtils.isNotEmpty(inboundTransformSpecs);
    }

    @JsonIgnore
    public String getCurrentNodeIdSafe() {
        return currentNode != null ? currentNode.getId() : currentNodeId;
    }
}
