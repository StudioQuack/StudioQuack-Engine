package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.SelectOptionParser;
import com.studioquack.common.dto.flow.SelectOptionRole;

import java.util.List;

/**
 * Graph used to generate and deploy BPMN: id, name, version, nodes, and edges.
 *
 * @param workflowId stable workflow id — becomes the main process key {@code Process_<workflowId>}
 * @param workflowName human-readable name (folded into the graph signature + process name)
 * @param workflowVersion designer version string (used only in the deployment/resource names)
 * @param nodes the workflow nodes
 * @param edges the workflow edges
 * @param draftToken non-null ONLY for a Designer Run: isolates this throwaway deployment into
 * its own BPMN namespace so it cannot collide with the version the user
 * deployed to the engine. See {@link #namespaceSuffix}.
 */
public record WorkflowGraph(
        String workflowId,
        String workflowName,
        String workflowVersion,
        List<NodeDto> nodes,
        List<EdgeDto> edges,
        String draftToken) {

    /** A steady, published deployment — no draft namespace. */
    public WorkflowGraph(String workflowId, String workflowName, String workflowVersion,
                         List<NodeDto> nodes, List<EdgeDto> edges) {
        this(workflowId, workflowName, workflowVersion, nodes, edges, null);
    }

    /**
     * Suffix appended to EVERY BPMN process key this graph produces, empty for a published deployment.
     *
     * <p>A Designer Run redeploys on every click while the user edits, so without a namespace of its
     * own it lands as a newer version of the same {@code Process_<workflowId>} the user already
     * deployed — and Activiti's "start the latest version" then hands that unfinished draft to webhook,
     * API and Duckbot triggers too. The sub-process keys need it just as much: they are keyed by node
     * id, which the draft shares with the published graph, so {@code studioquack.execute()} on a
     * published run would otherwise reach into the draft's sub-process.
     */
    public String namespaceSuffix() {
        return draftToken == null || draftToken.isBlank() ? "" : DRAFT_MARKER + draftToken;
    }

    /** True when this deployment is a throwaway Designer Run rather than a published version. */
    public boolean isDraft() {
        return !namespaceSuffix().isEmpty();
    }

    /** Separates a workflow id from its draft token inside a process key. */
    public static final String DRAFT_MARKER = "__draft_";

    /**
     * The draft namespace carried by a process key, or {@code ""} for a published one. Lets the
     * runtime keep a sub-process call inside the same namespace as the run that made it.
     */
    public static String namespaceSuffixOf(String processKey) {
        if (processKey == null) {
            return "";
        }
        int marker = processKey.indexOf(DRAFT_MARKER);
        return marker < 0 ? "" : processKey.substring(marker);
    }

    /** {@code Process_<wfId>[__draft_<token>]} → {@code <wfId>}; anything else → {@code null}. */
    public static String workflowIdOf(String processKey) {
        if (processKey == null || !processKey.startsWith("Process_")) {
            return null;
        }
        String rest = processKey.substring("Process_".length());
        int draft = rest.indexOf(DRAFT_MARKER);
        if (draft >= 0) {
            rest = rest.substring(0, draft);
        }
        return rest.isBlank() ? null : rest;
    }

    /** The main process key Activiti deploys/starts for this graph. */
    public String processKey() {
        return "Process_" + workflowId + namespaceSuffix();
    }

    /** True when the graph carries a structural start node (mirrors the control plane {@code hasStartEvent} guard). */
    public boolean hasStartEvent() {
        if (nodes == null) {
            return false;
        }
        return nodes.stream().anyMatch(n -> n.getData() != null
                && ("start".equalsIgnoreCase(n.getData().getNodeType())
                        || SelectOptionParser.resolveActiveRole(n.getData()) == SelectOptionRole.LISTEN));
    }
}
