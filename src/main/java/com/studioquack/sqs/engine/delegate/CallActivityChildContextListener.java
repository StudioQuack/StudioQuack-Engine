package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Seeds a {@code call.activity} child's OWN execution context at its (none) start event. The
 * generator attaches this listener to every none-start {@code StartEvent}; it no-ops unless the
 * process instance has a super execution — i.e. it was started by a {@code CallActivity}.
 *
 * <p><b>Why.</b> The child inherits the parent's variables, including the {@code studioquackRef}
 * handle — which points at the PARENT's sealed context (the parent's graph). The child's first
 * sandbox node would then resolve its definition (script language, template) against the wrong
 * graph and fail with {@code DISPATCH_UNRESOLVED}. This listener replaces the inherited ref with
 * a fresh context hydrated from the CHILD's deployed graph (+ its deploy-time variable scopes),
 * keyed by the child's own businessKey (minted {@code sub_<nanoId>} or DuckTape-mapped — see
 * {@link CallActivityIoListener}).
 *
 * <p>Function-port sub-PIs ({@code studioquack.execute()}) are NOT affected: they start without a
 * super execution, so the check below no-ops them. Fully guarded — a seeding hiccup is logged and
 * the run keeps the inherited ref (the pre-fix behaviour), never rolls back the child start.
 */
@Slf4j
@Component("callActivityChildContextListener")
public class CallActivityChildContextListener implements ExecutionListener {

    private static final String PROCESS_KEY_PREFIX = "Process_";
    private static final String NODE_ID_PREFIX = "node_";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<DeployedArtifactStore> artifactStoreProvider;
    private final ObjectProvider<ExecutionContextStore> contextStoreProvider;

    /** This daemon's own environment id, stamped onto the seeded context (sandbox jobs read it). */
    @Value("${sqs.channel.envId:}")
    private String ownEnvId;

    public CallActivityChildContextListener(ObjectProvider<SqsEngine> sqsEngineProvider,
                                            ObjectProvider<DeployedArtifactStore> artifactStoreProvider,
                                            ObjectProvider<ExecutionContextStore> contextStoreProvider) {
        this.sqsEngineProvider = sqsEngineProvider;
        this.artifactStoreProvider = artifactStoreProvider;
        this.contextStoreProvider = contextStoreProvider;
    }

    @Override
    public void notify(DelegateExecution execution) {
        if (!ExecutionListener.EVENTNAME_START.equals(execution.getEventName())) {
            return;
        }
        try {
            // The start event runs on a FLOW execution whose parent chain tops out at the sub-PI
            // root; the superExecutionId (→ the parent's parked call-activity execution) lives on
            // the PI root, so walk up before concluding this is not a call-activity child.
            DelegateExecution piExecution = execution;
            while (piExecution != null && !piExecution.isProcessInstanceType()) {
                piExecution = piExecution.getParent();
            }
            if (piExecution == null || StringUtils.isBlank(piExecution.getSuperExecutionId())) {
                return;
            }
            String businessKey = execution.getProcessInstanceBusinessKey();
            if (StringUtils.isBlank(businessKey)) {
                log.warn("callActivity.child.seed skipped: blank child businessKey "
                        + "(stale parent graph pinning ''? redeploy the parent)");
                return;
            }
            SqsEngine engine = sqsEngineProvider.getIfAvailable();
            DeployedArtifactStore artifactStore = artifactStoreProvider.getIfAvailable();
            ExecutionContextStore contextStore = contextStoreProvider.getIfAvailable();
            if (engine == null || artifactStore == null || contextStore == null) {
                return;
            }
            BpmnModel model = engine.readDeployedModel(execution.getProcessDefinitionId());
            if (model == null || model.getMainProcess() == null) {
                return;
            }
            String processKey = model.getMainProcess().getId();
            String workflowId = workflowIdFromProcessKey(processKey);
            if (workflowId == null) {
                return;
            }
            ExecutionContext seedCtx = new ExecutionContext();
            seedCtx.setWorkflowExecutionId(businessKey);
            seedCtx.setWorkflowId(workflowId);
            seedCtx.setCurrentNodeId(StringUtils.removeStart(
                    StringUtils.defaultString(execution.getCurrentActivityId()), NODE_ID_PREFIX));
            seedCtx.setEnvId(ownEnvId);
            String artifactKey = workflowId + WorkflowGraph.namespaceSuffixOf(processKey);
            artifactStore.loadDeployedGraph(artifactKey).ifPresentOrElse(graph -> {
                seedCtx.setNodes(graph.nodes());
                seedCtx.setEdges(graph.edges());
            }, () -> log.warn("callActivity.child.seed-no-graph workflowId={} — the deploy graph row "
                    + "is missing; the child's first dispatchable node will park", workflowId));
            artifactStore.loadDeployVariables(artifactKey).ifPresent(scopes -> {
                seedCtx.setWorkflowVariables(scopes.globalParams());
                seedCtx.setProjectVariables(scopes.projectVariables());
            });
            ProcessContextRef ref = contextStore.snapshot(seedCtx, null);
            // PI-scope set: overrides the inherited parent ref for the whole child instance.
            execution.setVariable(EngineVariableNames.STUDIOQUACK_REF, ref);
            log.info("callActivity.child.seeded workflowId={} businessKey={} nodes={}",
                    workflowId, businessKey,
                    seedCtx.getNodes() == null ? 0 : seedCtx.getNodes().size());
        } catch (Exception ex) {
            log.warn("callActivity.child.seed failed: {}", ex.toString());
        }
    }

    /** {@code Process_<wfId>[__draft_<token>]} → {@code <wfId>}; anything else → null. */
    private static String workflowIdFromProcessKey(String processKey) {
        if (processKey == null || !processKey.startsWith(PROCESS_KEY_PREFIX)) {
            return null;
        }
        String id = processKey.substring(PROCESS_KEY_PREFIX.length());
        int draft = id.indexOf(WorkflowGraph.DRAFT_MARKER);
        return draft >= 0 ? id.substring(0, draft) : id;
    }
}
