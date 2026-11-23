package com.studioquack.sqs.command.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * The client-side deployment cache. One row per workflow holds the last {@code DeploymentInfo} the
 * {@code deploy} command produced — {@code bpmnProcessId}, pinned {@code processDefinitionId}, engine
 * {@code version}, and the {@code bpmn-signature:v7} {@link #graphSignature}.
 */
@Entity
@Table(name = "deployed_definition", indexes = {
        @Index(name = "idx_deployed_def_process", columnList = "bpmnProcessId")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeployedDefinitionEntity {

    /**
     * Workflow id — the cache key (one latest definition per workflow on a single-tenant daemon).
     */
    @Id
    @Column(name = "workflow_id", length = 128, nullable = false)
    private String workflowId;

    /**
     * Last designer version string deployed for this workflow.
     */
    @Column(name = "workflow_version", length = 64)
    private String workflowVersion;

    /**
     * BPMN process key ({@code Process_<workflowId>}).
     */
    @Column(name = "bpmn_process_id", length = 191)
    private String bpmnProcessId;

    /**
     * Pinned deployed definition id (Activiti {@code String} id) for a Designer Run.
     */
    @Column(name = "process_definition_id", length = 191)
    private String processDefinitionId;

    /**
     * Engine definition version at last deploy.
     */
    @Column(name = "definition_version")
    private int definitionVersion;

    /**
     * {@code bpmn-signature:v7} of the graph at last deploy — the deploy-reuse comparison key.
     */
    @Column(name = "graph_signature", length = 128)
    private String graphSignature;

    /**
     * The deployed workflow graph (nodes) as JSON, persisted at deploy so a REF-only {@code start-run} (which no
     * longer carries the graph) can hydrate it locally. Deploy is the authoritative source of topology; the run
     * only supplies trigger params.
     */
    @Column(name = "nodes_json", columnDefinition = "text")
    private String nodesJson;

    /**
     * The deployed workflow graph (edges) as JSON — see {@link #nodesJson}.
     */
    @Column(name = "edges_json", columnDefinition = "text")
    private String edgesJson;

    /**
     * Workflow-scoped global parameters ({@code $(wf.…)}) as JSON, persisted at deploy on the same row as the
     * graph. Survives restart and is sealed into {@code seedContext} at REF-only start-run.
     */
    @Column(name = "global_params_json", columnDefinition = "text")
    private String globalParamsJson;

    /**
     * Project-scoped variables ({@code $(project.…)}) as JSON — see {@link #globalParamsJson}.
     */
    @Column(name = "project_variables_json", columnDefinition = "text")
    private String projectVariablesJson;

    /**
     * Project and workflow short codes so a webhook can resolve this deployed workflow. Null when the workflow has
     * no short code.
     */
    @Column(name = "project_short_code", length = 64)
    private String projectShortCode;

    /**
     * {@code ProjectVersionEntity.versionNumber} at deploy time — see {@link #projectShortCode}.
     */
    @Column(name = "project_version")
    private Integer projectVersion;

    /**
     * Workflow short code, unique within the owning project — see {@link #projectShortCode}.
     */
    @Column(name = "workflow_short_code", length = 64)
    private String workflowShortCode;

    @Column(name = "cdate")
    private OffsetDateTime cdate;

    @Column(name = "udate")
    private OffsetDateTime udate;
}
