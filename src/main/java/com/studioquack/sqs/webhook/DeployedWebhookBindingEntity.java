package com.studioquack.sqs.webhook;

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
 * Deploy-time index mapping a bound {@code $webhookKeyRef} to an active workflow listener,
 * intermediate message catch, send-and-wait reply catch, or ingress-node message boundary.
 */
@Entity
@Table(name = "deployed_webhook_bindings", indexes = {
        @Index(name = "idx_dwb_webhook_key_ref", columnList = "webhook_key_ref"),
        @Index(name = "idx_dwb_wf_ns", columnList = "workflow_id, namespace_key")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeployedWebhookBindingEntity {

    @Id
    @Column(name = "binding_id", length = 191, nullable = false)
    private String bindingId;

    @Column(name = "webhook_key_ref", length = 64, nullable = false)
    private String webhookKeyRef;

    @Column(name = "workflow_id", length = 128, nullable = false)
    private String workflowId;

    /** Empty string for published deployments; {@code __draft_<token>} for draft namespaces. */
    @Column(name = "namespace_key", length = 128, nullable = false)
    private String namespaceKey;

    @Column(name = "process_definition_id", length = 191)
    private String processDefinitionId;

    /** Designer node id (or host activity node id when {@link #boundaryId} is non-null). */
    @Column(name = "node_id", length = 128, nullable = false)
    private String nodeId;

    /** Nullable; populated for {@code BOUNDARY_MESSAGE} bindings. */
    @Column(name = "boundary_id", length = 128)
    private String boundaryId;

    /** {@code MESSAGE_START} | {@code MESSAGE_CATCH} | {@code SEND_AND_WAIT} | {@code BOUNDARY_MESSAGE}. */
    @Column(name = "binding_kind", length = 32, nullable = false)
    private String bindingKind;

    /** Deterministic Activiti BPMN message name ({@code __sq_hook_...} or boundary message name). */
    @Column(name = "message_name", length = 256, nullable = false)
    private String messageName;

    /** Template id used to deduplicate shared-key parser invocations. */
    @Column(name = "template_id", length = 128)
    private String templateId;

    /** SHA-256 of canonical parser configuration (templateId + role + parameter handles). */
    @Column(name = "params_hash", length = 64)
    private String paramsHash;

    /** JSON snapshot of the bound node's {@code TemplateDto} (including script + {@code $secretRef} handles). */
    @Column(name = "target_node_json", columnDefinition = "text")
    private String targetNodeJson;

    @Column(name = "cdate")
    private OffsetDateTime cdate;

    @Column(name = "udate")
    private OffsetDateTime udate;
}
