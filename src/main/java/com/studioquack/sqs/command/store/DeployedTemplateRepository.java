package com.studioquack.sqs.command.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data JPA repository for the {@code deployed_template} snapshot on the encrypted local database. Reads
 * are keyed by {@code (workflowId, version)} — the offline self-provisioning lookup the script generator +
 * monitoring use to run from the local store.
 */
@Repository
public interface DeployedTemplateRepository extends JpaRepository<DeployedTemplateEntity, String> {

    List<DeployedTemplateEntity> findByWorkflowIdAndWorkflowVersion(String workflowId, String workflowVersion);

    long countByWorkflowIdAndWorkflowVersion(String workflowId, String workflowVersion);

    /**
     * {@code undeploy} — drops EVERY version's snapshot rows for the workflow.
     */
    long deleteByWorkflowId(String workflowId);
}
