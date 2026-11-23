package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.WorkflowGraph;

import java.util.List;
import java.util.Map;

/**
 * Wire payload of a BE-pushed {@code deploy} command. The control plane ships the full workflow graph + the
 * node templates/scripts snapshot so the client can compile + deploy the BPMN into its embedded engine and run
 * the environment self-contained, offline.
 *
 * @param workflowId stable workflow id — the main process key becomes {@code Process_<workflowId>}
 * @param version designer version string (used in deployment/resource names + the snapshot key)
 * @param name human-readable workflow name (folded into the graph signature + process name)
 * @param nodes the workflow nodes (each carries its {@link TemplateDto} in {@code data})
 * @param edges the workflow edges
 * @param templates optional explicit templates/scripts snapshot; when absent the snapshot is derived from
 * {@code nodes[].data} so a slim BE payload still self-provisions
 * @param graphSignature BE-computed {@code bpmn-signature:v7} (advisory — the client recomputes it, the
 * recomputed value is authoritative for the deploy-reuse guard)
 * @param ephemeral this deploy backs a Designer Run, so the resulting Activiti deployment is throwaway and
 * gets reaped once no run needs it. Designer runs re-push the graph on every click of Run; without this flag
 * every iteration would leave a permanent deployment behind
 * @param draftToken the per-run token that isolates a Designer Run into its own BPMN namespace, so an
 * in-progress draft can never become the "latest version" that webhook, API and Duckbot triggers of the
 * published workflow pick up
 * @param projectShortCode owning project's short code, so a webhook can resolve {@code workflowId}. Optional;
 * absent when the project has no short code.
 * @param projectVersion CORE — {@code ProjectVersionEntity.versionNumber} this deploy was cut from (the
 * "active/deployed version" a webhook caller may omit and default to).
 * @param workflowShortCode CORE — this workflow's short code, unique within {@code projectShortCode}.
 * @param globalParams workflow-scoped global parameters sealed into the run context ({@code $(wf.…)}).
 * Optional; absent on older BE payloads.
 * @param projectVariables project-scoped variables sealed into the run context ({@code $(project.…)}).
 * Optional; absent on older BE payloads.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DeployCommandPayload(
        String workflowId,
        String version,
        String name,
        List<NodeDto> nodes,
        List<EdgeDto> edges,
        List<TemplateDto> templates,
        String graphSignature,
        boolean redeploy,
        boolean ephemeral,
        String draftToken,
        String projectShortCode,
        Integer projectVersion,
        String workflowShortCode,
        Map<String, Object> globalParams,
        Map<String, Object> projectVariables) {

    /**
     * A deploy with no draft namespace (a published version, or an older BE that predates the field).
     */
    public DeployCommandPayload(String workflowId, String version, String name, List<NodeDto> nodes,
                                List<EdgeDto> edges, List<TemplateDto> templates, String graphSignature,
                                boolean redeploy, boolean ephemeral) {
        this(workflowId, version, name, nodes, edges, templates, graphSignature, redeploy, ephemeral,
                null, null, null, null, null, null);
    }

    /**
     * A deploy with a draft namespace but no webhook-addressing triple (an older BE payload).
     */
    public DeployCommandPayload(String workflowId, String version, String name, List<NodeDto> nodes,
                                List<EdgeDto> edges, List<TemplateDto> templates, String graphSignature,
                                boolean redeploy, boolean ephemeral, String draftToken) {
        this(workflowId, version, name, nodes, edges, templates, graphSignature, redeploy, ephemeral,
                draftToken, null, null, null, null, null);
    }

    /**
     * Adapts this payload to the E1 {@link WorkflowGraph} the deploy/start orchestration consumes.
     */
    public WorkflowGraph toGraph() {
        return new WorkflowGraph(workflowId, name, version, nodes, edges, draftToken);
    }

    /**
     * True when the payload carries the minimum needed to compile + deploy a BPMN graph.
     */
    public boolean isDeployable() {
        return workflowId != null && !workflowId.isBlank()
                && nodes != null && !nodes.isEmpty();
    }
}
