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
 * One node template snapshot persisted on {@code deploy}, keyed by {@code (workflowId, version)}. Copying the
 * templates the workflow uses onto the local store lets the environment run self-contained, offline: the
 * engine + the script generator resolve everything the graph needs from local state, never a BE round-trip.
 */
@Entity
@Table(name = "deployed_template", indexes = {
        // "everything deployed for this workflow version" — the offline self-provisioning read.
        @Index(name = "idx_deployed_template_wf", columnList = "workflowId, workflowVersion"),
        // "which deploys referenced this template" — audit / GC when a version is retired.
        @Index(name = "idx_deployed_template_tpl", columnList = "templateId")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeployedTemplateEntity {

    /**
     * Deterministic snapshot key: {@code workflowId|version|templateKey}. Upsert on re-deploy.
     */
    @Id
    @Column(name = "id", length = 400, nullable = false)
    private String id;

    /**
     * Workflow this snapshot belongs to.
     */
    @Column(name = "workflow_id", length = 128)
    private String workflowId;

    /**
     * Designer version string this snapshot was captured at.
     */
    @Column(name = "workflow_version", length = 64)
    private String workflowVersion;

    /**
     * Source template id ({@code TemplateDto.id}); may be null for inline/ad-hoc nodes.
     */
    @Column(name = "template_id", length = 128)
    private String templateId;

    /**
     * A node id that used this template (traceability; a template may be used by several nodes).
     */
    @Column(name = "node_id", length = 128)
    private String nodeId;

    /**
     * Template display name/label.
     */
    @Column(name = "name", length = 256)
    private String name;

    /**
     * Template category (e.g. {@code conversion}, {@code application}); promoted for local queries.
     */
    @Column(name = "category", length = 128)
    private String category;

    /**
     * Designer node role ({@code start} / {@code end} / {@code serviceTask} / …).
     */
    @Column(name = "node_type", length = 64)
    private String nodeType;

    /**
     * Sandbox job type ({@code custom-script-executor}, {@code function-script-executor}, …).
     */
    @Column(name = "job_type", length = 128)
    private String jobType;

    /**
     * Full {@code TemplateDto} serialised as JSON text — the complete offline snapshot.
     */
    @Column(name = "template_json", columnDefinition = "text")
    private String templateJson;

    @Column(name = "cdate")
    private OffsetDateTime cdate;

    @Column(name = "udate")
    private OffsetDateTime udate;
}
