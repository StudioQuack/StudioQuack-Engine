package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles the BE-pushed {@code deploy} command: compile the workflow graph to BPMN and deploy it into the
 * embedded engine, then snapshot the node templates/scripts locally so the environment runs self-contained.
 * Mapping onto the engine: {@link DeployCommandPayload#toGraph()} → {@link WorkflowDeploymentService#deploy}
 * which calls {@code BpmnGenerationService.generate} + {@code SqsEngine.deploy} (main + every function-port
 * sub-process in ONE deployment).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeployCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "deploy";

    private final ObjectMapper objectMapper;
    private final WorkflowDeploymentService workflowDeploymentService;
    private final DeployedArtifactStore artifactStore;
    private final com.studioquack.sqs.engine.EphemeralDeploymentReaper ephemeralDeploymentReaper;
    private final com.studioquack.sqs.files.DeployFileIngest deployFileIngest;
    private final CallActivityShortCodeResolver callActivityShortCodeResolver;

    /** Null in slice tests that construct this handler by hand. */
    private com.studioquack.sqs.webhook.DeployedWebhookBindingStore webhookBindingStore;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setWebhookBindingStore(com.studioquack.sqs.webhook.DeployedWebhookBindingStore webhookBindingStore) {
        this.webhookBindingStore = webhookBindingStore;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        DeployCommandPayload payload;
        try {
            payload = objectMapper.readValue(command.payload(), DeployCommandPayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("deploy: malformed payload: " + message(parse));
        }
        if (payload == null || !payload.isDeployable()) {
            return DaemonCommandOutcome.failed("deploy: payload missing workflowId/nodes");
        }

        try {
            // Move any embedded file content into the daemon's own sealed store BEFORE the graph is
            // persisted, so base64 never lands in the artifact store or in a later execution context.
            deployFileIngest.ingest(payload.workflowId(), payload.nodes());

            // Call.activity short-code targeting: stamp a literal processId into every node that
            // addresses its target by workflowShortCode, while the payload still carries the
            // enclosing deploy's (projectShortCode, projectVersion) disambiguation context.
            // Before signature computation so the resolved target is what the reuse guard hashes.
            callActivityShortCodeResolver.resolveDeployPayload(payload);

            // V6 reuse guard: pass the durable prior so an unchanged graph reuses its version.
            // A redeploy (operator re-deploys a project version) forces recompilation + a fresh engine
            // version even when the graph is byte-identical, so "Redeploy" always reloads the daemon.
            WorkflowGraph graphForDeploy = payload.toGraph();
            if (!payload.ephemeral() && webhookBindingStore != null) {
                webhookBindingStore.validateStartCap(
                        payload.workflowId(), graphForDeploy.namespaceSuffix(), payload.nodes());
            }

            DeploymentInfo prior = artifactStore.findPriorDefinition(payload.workflowId());
            DeploymentInfo info = workflowDeploymentService.deploy(
                    graphForDeploy, prior, payload.redeploy(), payload.ephemeral());
            if (payload.redeploy()) {
                log.info("deploy.redeploy workflowId={} version={} — forced reload of the deployed definition",
                        payload.workflowId(), payload.version());
            }

            // Persist the deployment cache + the templates/scripts snapshot (self-contained env).
            // A Designer Run writes under its own draft key, never the workflow's: its deployment is
            // deleted once the run ends, so pointing the published reuse cache at it would leave that
            // cache naming a definition the reaper is about to remove — and would hand the published
            // workflow's next REF-only run the unfinished draft graph to hydrate from.


            // The reaper is about to delete the definition it names.
            WorkflowGraph deployedGraph = payload.toGraph();
            String artifactKey = payload.workflowId() + deployedGraph.namespaceSuffix();
            if (!payload.ephemeral() || deployedGraph.isDraft()) {
                artifactStore.saveDefinition(artifactKey, payload.version(), info);
            }


            artifactStore.saveDeployedGraph(artifactKey, payload.nodes(), payload.edges());


            artifactStore.saveDeployVariables(artifactKey, payload.globalParams(), payload.projectVariables());
            int templatesSnapshotted = artifactStore.saveTemplateSnapshot(payload);
            // Persist the project and workflow short codes so a webhook can resolve this row. Only for


            if (!payload.ephemeral()) {
                artifactStore.saveWebhookAddress(artifactKey, payload.projectShortCode(),
                        payload.projectVersion(), payload.workflowShortCode());
                if (webhookBindingStore != null) {
                    webhookBindingStore.replaceForWorkflow(
                            payload.workflowId(), deployedGraph.namespaceSuffix(),
                            info.getProcessDefinitionId(), payload.nodes());
                }
            }

            DeployResult result = new DeployResult(
                    info.getProcessDefinitionId(), info.getVersion(), info.getBpmnProcessId(),
                    info.getGraphSignature(), templatesSnapshotted);
            log.info("deploy.done workflowId={} processDefinitionId={} version={} templatesSnapshotted={}",
                    payload.workflowId(), info.getProcessDefinitionId(), info.getVersion(), templatesSnapshotted);
            if (payload.ephemeral()) {
                // Clear this workflow's PREVIOUS designer deployments now that a newer one supersedes
                // the one just created is inside the reaper's grace window, so it is safe.
                ephemeralDeploymentReaper.reapAfterDeploy();
            }
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("deploy.failed workflowId={}: {}", payload.workflowId(), ex.toString());
            return DaemonCommandOutcome.failed("deploy failed: " + message(ex));
        }
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Success output shipped to the BE: the pinned {@code processDefinitionId} + engine {@code version} (the
     * contract the task requires), enriched with the process key, the {@code v9} signature, and how many templates
     * were snapshotted.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record DeployResult(String processDefinitionId, int version, String processKey,
                        String graphSignature, int templatesSnapshotted) {
    }
}
