package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.sqs.engine.BpmnGenerationService.GeneratedBpmn;
import com.studioquack.sqs.engine.runtime.BeRuntimeEpoch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.CallActivity;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.engine.repository.Deployment;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Deploys a workflow graph and starts a process instance for it.
 * An unchanged graph reuses the existing deployment. The business key is the workflow execution id.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WorkflowDeploymentService {

    private final BpmnGenerationService bpmnGenerationService;
    private final SqsEngine sqsEngine;
    private final BeRuntimeEpoch beRuntimeEpoch;

    /**
 * Deploy the workflow's BPMN (main + every function-port sub-process) in ONE deployment, unless
 * the graph is byte-for-byte unchanged AND a matching definition already exists in the engine —
 * then reuse it. The cache key is {@code bpmn-signature:v9}.
 *
 * @param graph the workflow graph
 * @param prior the previously stored {@link DeploymentInfo} (or {@code null} on first deploy)
 * @param forceRedeploy skip the cache and always deploy (mirrors the FE {@code anyChangeOnWorkflow})
 * @return the {@link DeploymentInfo} to start with (the reused {@code prior}, or a freshly deployed one)
 */
    public DeploymentInfo deploy(WorkflowGraph graph, DeploymentInfo prior, boolean forceRedeploy) {
        return deploy(graph, prior, forceRedeploy, false);
    }

    /**
     * As {@link #deploy(WorkflowGraph, DeploymentInfo, boolean)}, but for a Designer Run, whose
     * deployment is throwaway: stamped with {@link SqsEngine#EPHEMERAL_CATEGORY} and deleted by
     * {@link EphemeralDeploymentReaper} as soon as its run finishes.
     *
     * <p><b>An ephemeral deploy NEVER reuses a prior deployment.</b> A designer run has no steady
     * deployment record — it deploys the draft, runs it, and the deployment is then deleted — so
     * "unchanged graph, therefore reuse" is not a valid inference here. Two ways it broke:
     * <ul>
     *   <li>The prior deployment recorded in the artifact store has already been reaped, so reuse
     *       returns a dangling {@code processDefinitionId}.</li>
     *   <li>Worse, when the workflow ALSO has a published deployment, {@code latestDefinitionExists}
     *       is satisfied by THAT one, so an unchanged designer run silently reuses it and executes the
     *       published graph instead of the draft on the canvas — a wrong-version run, not an error.</li>
     * </ul>
     * Deploying unconditionally costs one BPMN compile per designer run and removes the whole class.
     */
    public DeploymentInfo deploy(WorkflowGraph graph, DeploymentInfo prior, boolean forceRedeploy,
                                 boolean ephemeral) {
        String processKey = graph.processKey();
        String currentSignature = bpmnGenerationService.computeGraphSignature(
                graph.workflowName(), graph.nodes(), graph.edges());
        boolean missing = prior == null;
        boolean signatureMatches = !missing
                && currentSignature != null
                && currentSignature.equals(prior.getGraphSignature());

        // Reuse only when the graph is unchanged AND a definition is ACTUALLY deployed in the engine
        // (a stored DeploymentInfo alone does not prove that — e.g. after a fresh daemon install),
        // AND this is not a Designer Run (see above: its deployment does not survive the run).
        if (!forceRedeploy && !ephemeral && signatureMatches && sqsEngine.latestDefinitionExists(processKey)) {
            log.debug("bpmn.deploy.skipped reason=reuse-latest signature={}", abbreviate(currentSignature));
            return prior;
        }

        GeneratedBpmn generated = bpmnGenerationService.generate(
                graph.workflowId(), graph.workflowName(), graph.nodes(), graph.edges(),
                graph.namespaceSuffix());

        String mainResourceName = String.format("%s-v%s-%s.bpmn",
                graph.workflowId(), graph.workflowVersion(), UUID.randomUUID().toString().substring(0, 6));
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(mainResourceName, generated.mainXml());
        if (generated.subProcessXmls() != null) {
            for (Map.Entry<String, String> entry : generated.subProcessXmls().entrySet()) {
                String subResourceName = String.format("%s-%s-v%s-%s.bpmn",
                        graph.workflowId(), entry.getKey(), graph.workflowVersion(),
                        UUID.randomUUID().toString().substring(0, 6));
                resources.put(subResourceName, entry.getValue());
            }
        }

        // The draft token is part of the name so a Designer Run is identifiable in `wg`-style
        // listings and in the reaper's log, rather than looking like another published version.
        String deploymentName = graph.workflowId() + "-v" + graph.workflowVersion()
                + graph.namespaceSuffix();
        String category = ephemeral ? SqsEngine.EPHEMERAL_CATEGORY : null;
        Deployment deployment = sqsEngine.deploy(deploymentName, category, resources);
        try {
            ProcessDefinition definition = sqsEngine.findDeployedDefinition(deployment.getId(), processKey);
            assertCallActivityTargetsResolve(deployment.getId());

            DeploymentInfo info = new DeploymentInfo();
            info.setBpmnProcessId(processKey);
            info.setProcessDefinitionId(definition.getId());   // pin exact version for a Designer Run
            info.setVersion(definition.getVersion());
            info.setGraphSignature(currentSignature);          // stamp so the next run can cache-hit
            log.info("activiti.deploy.done bpmnProcessId={} version={} definitionId={} resources={} ephemeral={}",
                    info.getBpmnProcessId(), info.getVersion(), info.getProcessDefinitionId(),
                    resources.size(), ephemeral);
            return info;
        } catch (RuntimeException postDeployFailure) {
            // Activiti registers message-start subscriptions AT deploy time, so a post-deploy
            // validation failure must not leave the definition behind: a leaked (draft) deployment
            // keeps its subscription, blocks the published deploy via the duplicate-message guard,
            // and answers inbound messages with a graph whose call.activity target may not exist.
            sqsEngine.deleteDeployment(deployment.getId());
            throw postDeployFailure;
        }
    }

    /**
     * Start a process instance for a deployed workflow. {@code businessKey == workflowExecutionId} so
     * the status listener and monitoring can correlate the run. A Designer Run pins the exact deployed
     * version; a trigger uses the latest.
     *
     * @param graph              the workflow graph (used only for the {@code hasStartEvent} guard)
     * @param info               the {@link DeploymentInfo} from {@link #deploy}
     * @param workflowExecutionId the stable execution id — becomes the businessKey
     * @param refOnlyVariables   REF-ONLY start variables ({@code studioquackRef} + non-PHI tokens); may be null
     * @param pinVersion         {@code true} = pin the exact definition (Designer Run); {@code false} = latest
     * @return the started {@link ProcessInstance}, or {@code null} when the graph has no start event
     */
    public ProcessInstance startProcessInstance(WorkflowGraph graph, DeploymentInfo info,
                                                String workflowExecutionId,
                                                Map<String, Object> refOnlyVariables,
                                                boolean pinVersion) {
        if (!graph.hasStartEvent()) {
            log.info("activiti.process_instance.create.skipped reason=no_start_event workflowExecutionId={}",
                    workflowExecutionId);
            return null;
        }
        // The daemon never serialises the fat graph into the engine (REF-ONLY): building a fresh var
        // map here IS the single-tenant analogue of the control plane's clearNodeAndEdges pre-start.
        Map<String, Object> variables = new HashMap<>();
        if (refOnlyVariables != null) {
            variables.putAll(refOnlyVariables);
        }
        // Stamp the daemon startup epoch so an orphan activation from a previous boot can be rejected.
        variables.put(BeRuntimeEpoch.VARIABLE_NAME, beRuntimeEpoch.current());

        boolean pin = pinVersion && StringUtils.isNotBlank(info.getProcessDefinitionId());
        ProcessInstance instance = pin
                ? sqsEngine.startByDefinitionId(info.getProcessDefinitionId(), workflowExecutionId, variables)
                : sqsEngine.startLatest(info.getBpmnProcessId(), workflowExecutionId, variables);
        info.setProcessInstanceId(instance.getId());
        log.info("activiti.process_instance.create.completed processInstanceId={} versionMode={} bpmnProcessId={}",
                instance.getId(), pin ? "pinned" : "latest", info.getBpmnProcessId());
        return instance;
    }

    /**
 * Convenience mirror of the control plane {@code handleBpmnDeploymentAndExecution}: deploy (or reuse)
 * then start a single instance. Designer Run pins the just-deployed version.
 */
    public ProcessInstance deployAndStart(WorkflowGraph graph, DeploymentInfo prior,
                                          String workflowExecutionId,
                                          Map<String, Object> refOnlyVariables,
                                          boolean inDesign) {
        DeploymentInfo info = deploy(graph, prior, inDesign);
        return startProcessInstance(graph, info, workflowExecutionId, refOnlyVariables, inDesign);
    }

    private String abbreviate(String s) {
        if (s == null) {
            return "null";
        }
        return s.length() <= 12 ? s : s.substring(0, 12);
    }

    /**
     * Loud deploy failure for {@code call.activity} targets: every literal {@code calledElement}
     * minted by {@code ActivitiCallActivityStrategy} must resolve to a process definition deployed
     * on this daemon. Without the check a typo'd or not-yet-deployed target deploys
     * "successfully" and the run only fails when the token reaches the node. {@code ${…}}
     * expression targets resolve at runtime and are skipped.
     */
    private void assertCallActivityTargetsResolve(String deploymentId) {
        List<String> unresolved = new ArrayList<>();
        for (ProcessDefinition def : sqsEngine.findDeployedDefinitions(deploymentId)) {
            BpmnModel model = sqsEngine.readDeployedModel(def.getId());
            if (model == null) {
                continue;
            }
            for (org.activiti.bpmn.model.Process p : model.getProcesses()) {
                for (FlowElement element : p.getFlowElements()) {
                    if (element instanceof CallActivity callActivity
                            && StringUtils.isNotBlank(callActivity.getCalledElement())
                            && !sqsEngine.latestDefinitionExists(callActivity.getCalledElement())) {
                        unresolved.add(callActivity.getId() + " -> " + callActivity.getCalledElement());
                    }
                }
            }
        }
        if (!unresolved.isEmpty()) {
            throw new IllegalStateException(
                    "deploy failed: call.activity target(s) not deployed on this daemon: " + unresolved
                            + " — deploy the target workflow first (a Designer Run calls the same draft"
                            + " namespace, never the published process)");
        }
    }
}
