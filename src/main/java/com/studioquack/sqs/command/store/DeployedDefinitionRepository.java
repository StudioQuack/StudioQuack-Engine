package com.studioquack.sqs.command.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data JPA repository for the {@code deployed_definition} deployment cache on the encrypted local
 * database. Keyed by {@code workflowId}; supplies the durable "prior" {@code DeploymentInfo} the deploy
 * command's {@code bpmn-signature:v7} reuse guard compares against (survives daemon restarts).
 */
@Repository
public interface DeployedDefinitionRepository extends JpaRepository<DeployedDefinitionEntity, String> {

    /**
     * Call.activity short-code targeting — every deployed row carrying this workflow short code, across projects
     * and project versions. The resolver ({@code CallActivityShortCodeResolver}) prefers the caller's own
     * project/version and fails the deploy loudly when the code stays ambiguous.
     */
    List<DeployedDefinitionEntity> findByWorkflowShortCode(String workflowShortCode);
}
