package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.files.ClientFileStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles the BE-pushed {@code undeploy} command — the missing second half of the deployment lifecycle. Until
 * this handler existed, undeploying from the FE only dropped the backend's deployment row: the daemon kept the
 * Activiti definition, so its message-start subscriptions stayed live and blocked any other workflow from
 * subscribing the same message name (the "already subscribed by another deployed workflow" guard fired against
 * a deployment the user believed was gone).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UndeployCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "undeploy";

    private final ObjectMapper objectMapper;
    private final SqsEngine sqsEngine;
    private final DeployedArtifactStore artifactStore;
    private final ClientFileStore clientFileStore;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        UndeployCommandPayload payload;
        try {
            payload = objectMapper.readValue(command.payload(), UndeployCommandPayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("undeploy: malformed payload: " + message(parse));
        }
        if (payload == null || payload.workflowId() == null || payload.workflowId().isBlank()) {
            return DaemonCommandOutcome.failed("undeploy: payload missing workflowId");
        }
        String workflowId = payload.workflowId();

        try {
            DeploymentInfo prior = artifactStore.findPriorDefinition(workflowId);
            String deploymentId = prior == null ? null : sqsEngine.deploymentIdOf(prior.getProcessDefinitionId());

            if (deploymentId != null) {
                long running = sqsEngine.runningInstanceCount(deploymentId);
                if (running > 0 && !payload.force()) {
                    return DaemonCommandOutcome.failed("undeploy refused: workflow " + workflowId
                            + " has " + running + " running instance(s) — cancel them first or retry with force");
                }
                if (!sqsEngine.deleteDeployment(deploymentId)) {
                    return DaemonCommandOutcome.failed("undeploy: engine refused to delete deployment "
                            + deploymentId + " (workflow " + workflowId + ")");
                }
            }

            long[] removed = artifactStore.deleteArtifacts(workflowId);
            long files = clientFileStore.deleteScope(ClientFileStore.SCOPE_DEPLOY, workflowId);

            UndeployResult result = new UndeployResult(workflowId, deploymentId, deploymentId != null,
                    removed[0] > 0, removed[1], removed[2], files);
            log.info("undeploy.done workflowId={} deploymentId={} engineDeleted={} templatesRemoved={} scriptsRemoved={} filesRemoved={}",
                    workflowId, deploymentId, deploymentId != null, removed[1], removed[2], files);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("undeploy.failed workflowId={}: {}", workflowId, ex.toString());
            return DaemonCommandOutcome.failed("undeploy failed: " + message(ex));
        }
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Wire payload: the workflow to undeploy; {@code force} cascades over live instances.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record UndeployCommandPayload(String workflowId, boolean force) {
    }

    /**
     * Success output shipped to the BE: what the undeploy actually removed.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record UndeployResult(String workflowId, String deploymentId, boolean engineDeploymentDeleted,
                          boolean definitionCacheCleared, long templatesRemoved, long scriptsRemoved,
                          long filesRemoved) {
    }
}
