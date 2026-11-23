package com.studioquack.sqs.command.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data JPA repository for the {@code deployed_script} snapshot on the encrypted local database. The
 * executable node bodies the in-process sandbox runs, addressed by {@code (workflowId, version)} so a run
 * resolves its scripts locally, offline.
 */
@Repository
public interface DeployedScriptRepository extends JpaRepository<DeployedScriptEntity, String> {

    List<DeployedScriptEntity> findByWorkflowIdAndWorkflowVersion(String workflowId, String workflowVersion);

    long countByWorkflowIdAndWorkflowVersion(String workflowId, String workflowVersion);

    /**
     * {@code undeploy} — drops EVERY version's snapshot rows for the workflow.
     */
    long deleteByWorkflowId(String workflowId);
}
