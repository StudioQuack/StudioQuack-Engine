package com.studioquack.sqs.webhook;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/** JPA repository for deploy-time {@link DeployedWebhookBindingEntity} records. */
public interface DeployedWebhookBindingRepository extends JpaRepository<DeployedWebhookBindingEntity, String> {

    List<DeployedWebhookBindingEntity> findByWebhookKeyRef(String webhookKeyRef);

    List<DeployedWebhookBindingEntity> findByWorkflowIdAndNamespaceKey(String workflowId, String namespaceKey);

    List<DeployedWebhookBindingEntity> findByWorkflowId(String workflowId);

    long deleteByWorkflowIdAndNamespaceKey(String workflowId, String namespaceKey);

    long deleteByWorkflowId(String workflowId);
}
