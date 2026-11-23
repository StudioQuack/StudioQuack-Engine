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
 * One node script snapshot persisted on {@code deploy}, keyed by {@code (workflowId, version)}. Split from
 * {@link DeployedTemplateEntity} because a template's executable body ({@code TemplateScriptDto}: language +
 * source) is the part the in-process sandbox actually runs, and storing it as its own row keeps the
 * (potentially large) script text out of template metadata scans and lets the language be indexed for
 * warm-pool planning.
 */
@Entity
@Table(name = "deployed_script", indexes = {
        @Index(name = "idx_deployed_script_wf", columnList = "workflowId, workflowVersion"),
        @Index(name = "idx_deployed_script_tpl", columnList = "templateId"),
        @Index(name = "idx_deployed_script_lang", columnList = "language")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeployedScriptEntity {

    /**
     * Deterministic key: {@code workflowId|version|templateKey|language}. Upsert on re-deploy.
     */
    @Id
    @Column(name = "id", length = 464, nullable = false)
    private String id;

    /**
     * Workflow this script snapshot belongs to.
     */
    @Column(name = "workflow_id", length = 128)
    private String workflowId;

    /**
     * Designer version string this snapshot was captured at.
     */
    @Column(name = "workflow_version", length = 64)
    private String workflowVersion;

    /**
     * Source template id owning this script ({@code TemplateDto.id}); may be null for inline nodes.
     */
    @Column(name = "template_id", length = 128)
    private String templateId;

    /**
     * A node id that used this script (traceability).
     */
    @Column(name = "node_id", length = 128)
    private String nodeId;

    /**
     * Script language wire value (e.g. {@code python}, {@code node}, {@code bash}).
     */
    @Column(name = "language", length = 32)
    private String language;

    /**
     * The script source text (offline-executable body). {@code columnDefinition = "text"} — never plain
     * {@code @Lob}: Postgres maps {@code @Lob} to {@code oid}, and every access then needs the Large Object API
     * inside an explicit transaction ("Unable to access lob stream").
     */
    @Column(name = "script", columnDefinition = "text")
    private String script;

    @Column(name = "cdate")
    private OffsetDateTime cdate;

    @Column(name = "udate")
    private OffsetDateTime udate;
}
