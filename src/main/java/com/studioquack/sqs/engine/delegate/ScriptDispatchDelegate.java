package com.studioquack.sqs.engine.delegate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.ExecutionRequirementsDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.expression.ConfigParameterExpressionResolver;
import com.studioquack.sqs.engine.expression.ExpressionResolver;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import org.activiti.bpmn.model.BpmnModel;
import org.apache.commons.lang3.StringUtils;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.pipeline.PreparedInbound;
import com.studioquack.sqs.execution.pipeline.TransformPipeline;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.store.OperatorInputOverrideStore;
import com.studioquack.sqs.execution.transform.EdgeTransformPreStep;
import com.studioquack.sqs.execution.transform.ForeignEdgeInputResolver;
import com.studioquack.sqs.execution.transform.GenerateScriptService;
import com.studioquack.sqs.runtime.CancelledRunGuard;
import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import com.studioquack.sqs.runtime.DispatchExecutionRegistry;
import com.studioquack.sqs.runtime.DispatchResultCoordinator;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder;
import com.studioquack.sqs.runtime.GatewayRouteResolver;
import com.studioquack.sqs.sandbox.SandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxImageResolution;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.queue.DurableJobQueue;
import com.studioquack.sqs.sandbox.queue.JobState;
import com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import com.studioquack.sqs.sandbox.stateful.StatefulSandboxExecution;
import com.studioquack.sqs.status.RunLogModeRegistry;
import com.studioquack.sqs.status.SandboxLogCallback;
import com.studioquack.sqs.status.log.SandboxLogIoRedactor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The in-process sandbox dispatch delegate. Bean name {@code scriptDispatchDelegate} matches the
 * {@code ${scriptDispatchDelegate}} expression the generator emits on every async sandbox
 * ServiceTask ({@code node_<nodeId>}).
 *
 * <p>The hosting ServiceTask is
 * {@code activiti:async}, so {@link #execute} runs on the engine's async-executor thread. It:
 * <ol>
 * <li>captures the routing ids (incl. the candidate parked execution id — for a linear flow the
 * ServiceTask execution IS the one that parks at {@code wait_<node>});</li>
 * <li><b>durably registers</b> the wait-state via {@link DispatchExecutionRegistry} — this commits
 * atomically with the engine transaction that advances the token to the ReceiveTask, so the
 * registration is guaranteed present before any result can resume it;</li>
 * <li>hands the heavy sandbox work to a virtual-thread-per-task executor and returns (freeing
 * the engine thread).</li>
 * </ol>
 * The background task runs the 2-phase pipeline + sandbox, computes any gateway route tokens, then
 * delivers the result through {@link DispatchResultCoordinator} — a bounded retry that waits until
 * the token is actually parked, so a fast-returning script can never see "execution not found". A
 * {@code kill -9} between park and resume is recovered on the next boot (hydrate + re-deliver).
 *
 * <p>This delegate never throws (a throw would dead-letter the async job); dispatch failures are
 * routed through the error gate via {@link DispatchResultCoordinator#deliverFailure}.
 */
@Slf4j
@Component("scriptDispatchDelegate")
public class ScriptDispatchDelegate implements JavaDelegate {

    private static final String NODE_ACTIVITY_PREFIX = "node_";
    private static final String WAIT_ACTIVITY_PREFIX = "wait_";
    /** Typed error code used when the node context cannot be dispatched (unresolvable). */
    private static final String ERROR_DISPATCH_UNRESOLVED = "DISPATCH_UNRESOLVED";
    /** Typed error code when a $secretRef about to be resolved was not bound on this node at deploy. */
    private static final String ERROR_SECRET_BINDING_MISMATCH = "Error_SecretBindingMismatch";

    private final TransformPipeline transformPipeline;
    private final GenerateScriptService generateScriptService;
    private final EdgeTransformPreStep edgeTransformPreStep;
    private final SandboxExecutionPort sandbox;
    private final StatefulInstanceManager statefulInstances;
    private final StatefulSandboxExecution statefulSandbox;
    private final RefOnlyVarBuilder refOnlyVarBuilder;
    private final ExecutionContextStore contextStore;
    private final DispatchExecutionRegistry dispatchRegistry;
    private final DispatchResultCoordinator resultCoordinator;
    private final GatewayRouteResolver routeResolver;
    private final DurableJobQueue jobQueue;
    private final SandboxLogCallback sandboxLogCallback;
    private final DuckbotBridge duckbotBridge;
    private final SecretResolver secretResolver;
    /** {@code $(…)} substitution in configuration-parameter strings (before secrets). */
    private final ExpressionResolver expressionResolver;
    private final com.studioquack.sqs.sandbox.secrets.DeployedSecretBindingGuard secretBindingGuard;
    private final com.studioquack.sqs.channel.TamperEventReporter tamperEventReporter;
    private final com.studioquack.sqs.files.SandboxFileMaterializer sandboxFileMaterializer;
    private final com.studioquack.sqs.files.ContentBlockHydrator contentBlockHydrator;
    private final ObjectMapper objectMapper;
    private final DispatchCancellationRegistry dispatchCancellationRegistry;
    private final CancelledRunGuard cancelledRunGuard;
    private final Environment environment;
    /**
 * (item 0) — live overlay-egress readiness, read when a node's sandbox log is mirrored so
 * a container that ran on a DEGRADED host carries that fact into the run log. Never null in
 * production (the manager is a plain {@code @Component}); the manual-construction test seam may
 * pass a stub.
 */
    private final com.studioquack.sqs.sandbox.egress.SandboxEgressManager sandboxEgressManager;
    /** Optional — same ObjectProvider pattern as GatewayRouteResolver / BpmnNodeLogListener. */
    private final ObjectProvider<GatewayDecisionRecorder> decisionRecorder;
    /**
     * Sub-PI log-mode inheritance probe (nullable — a bare slice test wires no registry). A
     * function-port child starts via {@code HelperExecuteService} on a thread whose Activiti
     * listener order vs the register call is not guaranteed; when PROCESS_STARTED already
     * defaulted the child to BATCH, the dispatch corrects the entry to the parent's mode so the
     * provider node's per-node rows mirror live (STREAM) instead of being buffered for a batch
     * flush that races the parent's terminal status.
     */
    private final ObjectProvider<RunLogModeRegistry> runLogModeRegistryProvider;
    /** Lazy sub-PI link probe (same ObjectProvider cycle-avoidance as the log callbacks). */
    private final ObjectProvider<com.studioquack.sqs.runtime.SubProcessCompletionRegistry>
            subProcessRegistryProvider;
    /** Resolves the BPMN process key → deploy artifact key for secret-binding guard lookups. */
    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    /**
     * Operator input overrides ({@code apply-inputs}), keyed by (run, node) — field-injected so the
     * manual-construction test seam keeps its signature; {@code null} in bare slice tests, where no
     * override exists anyway.
     */
    private OperatorInputOverrideStore operatorInputOverrideStore;
    /**
     * Wire fan-out for the skip-with-output-override path (builds the skipped node's connection
     * list so {@code finalizeOutputs} can address the operator's outputs to downstream targets).
     * Field-injected so the manual-construction test seam keeps its signature; {@code null} in
     * bare slice tests, where the fan-out degrades to unaddressed seed rows.
     */
    private com.studioquack.sqs.execution.transform.ConnectionBuilder connectionBuilder;
    /**
     * Where an inject-mode trial's workspace is staged before the run. Field-injected so the
     * manual-construction test seam keeps its signature; {@code null} in bare slice tests, where
     * such a trial fails with "workspace is not on the engine".
     */
    private com.studioquack.sqs.sandbox.oci.OciBuildContextStore buildContextStore;

    private final AtomicLong invocationCount = new AtomicLong();
    private ExecutorService dispatchPool;

    /** Test seam — recorder absent. */
    public ScriptDispatchDelegate(TransformPipeline transformPipeline,
                                  GenerateScriptService generateScriptService,
                                  EdgeTransformPreStep edgeTransformPreStep,
                                  SandboxExecutionPort sandbox,
                                  StatefulInstanceManager statefulInstances,
                                  StatefulSandboxExecution statefulSandbox,
                                  RefOnlyVarBuilder refOnlyVarBuilder,
                                  ExecutionContextStore contextStore,
                                  DispatchExecutionRegistry dispatchRegistry,
                                  DispatchResultCoordinator resultCoordinator,
                                  GatewayRouteResolver routeResolver,
                                  DurableJobQueue jobQueue,
                                  SandboxLogCallback sandboxLogCallback,
                                  DuckbotBridge duckbotBridge,
                                  SecretResolver secretResolver,
                                  ExpressionResolver expressionResolver,
                                  com.studioquack.sqs.sandbox.secrets.DeployedSecretBindingGuard secretBindingGuard,
                                  com.studioquack.sqs.channel.TamperEventReporter tamperEventReporter,
                                  com.studioquack.sqs.files.SandboxFileMaterializer sandboxFileMaterializer,
                                  com.studioquack.sqs.files.ContentBlockHydrator contentBlockHydrator,
                                  ObjectMapper objectMapper,
                                  DispatchCancellationRegistry dispatchCancellationRegistry,
                                  CancelledRunGuard cancelledRunGuard,
                                  Environment environment,
                                  com.studioquack.sqs.sandbox.egress.SandboxEgressManager sandboxEgressManager) {
        this(transformPipeline, generateScriptService, edgeTransformPreStep, sandbox,
                statefulInstances, statefulSandbox, refOnlyVarBuilder, contextStore,
                dispatchRegistry, resultCoordinator, routeResolver, jobQueue, sandboxLogCallback,
                duckbotBridge, secretResolver, expressionResolver, secretBindingGuard,
                tamperEventReporter, sandboxFileMaterializer, contentBlockHydrator, objectMapper,
                dispatchCancellationRegistry, cancelledRunGuard, environment, sandboxEgressManager,
                null, null, null, null);
    }

    @Autowired
    public ScriptDispatchDelegate(TransformPipeline transformPipeline,
                                  GenerateScriptService generateScriptService,
                                  EdgeTransformPreStep edgeTransformPreStep,
                                  SandboxExecutionPort sandbox,
                                  StatefulInstanceManager statefulInstances,
                                  StatefulSandboxExecution statefulSandbox,
                                  RefOnlyVarBuilder refOnlyVarBuilder,
                                  ExecutionContextStore contextStore,
                                  DispatchExecutionRegistry dispatchRegistry,
                                  DispatchResultCoordinator resultCoordinator,
                                  GatewayRouteResolver routeResolver,
                                  DurableJobQueue jobQueue,
                                  SandboxLogCallback sandboxLogCallback,
                                  DuckbotBridge duckbotBridge,
                                  SecretResolver secretResolver,
                                  ExpressionResolver expressionResolver,
                                  com.studioquack.sqs.sandbox.secrets.DeployedSecretBindingGuard secretBindingGuard,
                                  com.studioquack.sqs.channel.TamperEventReporter tamperEventReporter,
                                  com.studioquack.sqs.files.SandboxFileMaterializer sandboxFileMaterializer,
                                  com.studioquack.sqs.files.ContentBlockHydrator contentBlockHydrator,
                                  ObjectMapper objectMapper,
                                  DispatchCancellationRegistry dispatchCancellationRegistry,
                                  CancelledRunGuard cancelledRunGuard,
                                  Environment environment,
                                  com.studioquack.sqs.sandbox.egress.SandboxEgressManager sandboxEgressManager,
                                  ObjectProvider<GatewayDecisionRecorder> decisionRecorder,
                                  ObjectProvider<RunLogModeRegistry> runLogModeRegistryProvider,
                                  ObjectProvider<com.studioquack.sqs.runtime.SubProcessCompletionRegistry>
                                          subProcessRegistryProvider,
                                  ObjectProvider<SqsEngine> sqsEngineProvider) {
        this.transformPipeline = transformPipeline;
        this.generateScriptService = generateScriptService;
        this.edgeTransformPreStep = edgeTransformPreStep;
        this.sandbox = sandbox;
        this.statefulInstances = statefulInstances;
        this.statefulSandbox = statefulSandbox;
        this.refOnlyVarBuilder = refOnlyVarBuilder;
        this.contextStore = contextStore;
        this.dispatchRegistry = dispatchRegistry;
        this.resultCoordinator = resultCoordinator;
        this.routeResolver = routeResolver;
        this.jobQueue = jobQueue;
        this.sandboxLogCallback = sandboxLogCallback;
        this.duckbotBridge = duckbotBridge;
        this.secretResolver = secretResolver;
        this.expressionResolver = expressionResolver;
        this.secretBindingGuard = secretBindingGuard;
        this.tamperEventReporter = tamperEventReporter;
        this.sandboxFileMaterializer = sandboxFileMaterializer;
        this.contentBlockHydrator = contentBlockHydrator;
        this.objectMapper = objectMapper;
        this.dispatchCancellationRegistry = dispatchCancellationRegistry;
        this.cancelledRunGuard = cancelledRunGuard;
        this.environment = environment;
        this.sandboxEgressManager = sandboxEgressManager;
        this.decisionRecorder = decisionRecorder;
        this.runLogModeRegistryProvider = runLogModeRegistryProvider;
        this.subProcessRegistryProvider = subProcessRegistryProvider;
        this.sqsEngineProvider = sqsEngineProvider;
    }

    /** Optional override store — absent in slice tests (package-visible for the test seam). */
    @Autowired(required = false)
    void setOperatorInputOverrideStore(OperatorInputOverrideStore operatorInputOverrideStore) {
        this.operatorInputOverrideStore = operatorInputOverrideStore;
    }

    /** Optional connection builder — absent in slice tests (package-visible for the test seam). */
    @Autowired(required = false)
    void setConnectionBuilder(com.studioquack.sqs.execution.transform.ConnectionBuilder connectionBuilder) {
        this.connectionBuilder = connectionBuilder;
    }

    /** Optional build-context store — absent in slice tests (package-visible for the test seam). */
    @Autowired(required = false)
    void setBuildContextStore(com.studioquack.sqs.sandbox.oci.OciBuildContextStore buildContextStore) {
        this.buildContextStore = buildContextStore;
    }

    @PostConstruct
    void init() {
        // Virtual thread per dispatch: a dispatch is almost entirely blocking I/O (podman exec,
        // in-container app, helper round-trips), and nested runs (an agent node waiting on its
        // model/tool sub-PIs, which dispatch through THIS SAME executor) turn any fixed-size pool
        // into a starvation deadlock once in-flight dispatches reach the pool size — the
        // "2 workflows stuck, third crawling, SANDBOX_DEADLINE" incident. There is deliberately
        // NO cap here; real bounds live on the resources a dispatch actually consumes (the
        // per-instance stateful admission gate, the warm-pool size, the helper deadlines).
        dispatchPool = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("sandbox-dispatch-", 0).factory());
    }

    @PreDestroy
    void shutdown() {
        if (dispatchPool != null) {
            dispatchPool.shutdownNow();
        }
    }

    @Override
    public void execute(DelegateExecution execution) {
        invocationCount.incrementAndGet();
        String activityId = execution.getCurrentActivityId();
        String nodeId = stripPrefix(activityId);
        String businessKey = execution.getProcessInstanceBusinessKey();
        String processInstanceId = execution.getProcessInstanceId();
        String candidateExecutionId = execution.getId();
        String waitActivityId = WAIT_ACTIVITY_PREFIX + nodeId;
        ProcessContextRef ref = readRef(execution);

        // No dispatchable context (bare start ref / no stored node) → park like the stub. Never throw:
        // a throw would dead-letter the async job. No wait-state is registered (nothing to resume).
        if (ref == null || ref.getCurrentNodeRefId() == null) {
            log.info("scriptDispatchDelegate: no dispatchable context for node={} (ref={}); parking the "
                    + "ReceiveTask (no durable wait-state registered).", nodeId,
                    ref == null ? "<null>" : "no-node-ref");
            return;
        }

        // Durably register the wait-state BEFORE handing off — this commits with the engine tx that
        // parks the token, so the registration always precedes any resume (closes the race).
        String dispatchId = UUID.randomUUID().toString();
        dispatchRegistry.register(dispatchId, candidateExecutionId, processInstanceId, businessKey,
                nodeId, waitActivityId, ref.getEnvId());
        boolean retryAttempt = consumeRetryNodePin(execution, nodeId);
        log.info("scriptDispatchDelegate.dispatch node={} dispatchId={} businessKey={} retry={} (async → sandbox)",
                nodeId, dispatchId, businessKey, retryAttempt);
        // Bounded, self-cleaning cancel registry: begin tracking BEFORE submit (so a cancel racing the
        // submission itself still finds the entry), attach the pool Future once submit returns it, and
        // ALWAYS end tracking in a finally around the task itself — regardless of how it exits — so an
        // entry never outlives the dispatch it describes (the registry can never grow unbounded).
        dispatchCancellationRegistry.beginTracking(processInstanceId, dispatchId, nodeId);
        String processDefinitionId = execution.getProcessDefinitionId();
        Future<?> future = dispatchPool.submit(() -> {
            try {
                runDispatch(dispatchId, nodeId, ref, processDefinitionId, retryAttempt);
            } finally {
                dispatchCancellationRegistry.endTracking(dispatchId);
            }
        });
        dispatchCancellationRegistry.attachFuture(dispatchId, future);
    }

    /** Consumes the one-shot {@code __sq_retry_<nodeId>} pin placed by {@code RetryIncidentCommandHandler}. */
    private static boolean consumeRetryNodePin(DelegateExecution execution, String nodeId) {
        if (execution == null || nodeId == null || nodeId.isBlank()) {
            return false;
        }
        String varName = EngineVariableNames.retryNodeVariableName(nodeId);
        Object val = execution.getVariable(varName);
        if (val == null) {
            return false;
        }
        execution.removeVariable(varName);
        return Boolean.TRUE.equals(val) || "true".equalsIgnoreCase(String.valueOf(val));
    }

    /** The heavy lifting — runs off the engine thread; delivers the result to the parked ReceiveTask. */
    private void runDispatch(String dispatchId, String nodeId, ProcessContextRef ref,
                             String processDefinitionId) {
        runDispatch(dispatchId, nodeId, ref, processDefinitionId, false);
    }

    private void runDispatch(String dispatchId, String nodeId, ProcessContextRef ref,
                             String processDefinitionId, boolean retryAttempt) {
        // Hoisted so the outer catch can name a downstream gateway on a resume-time routing miss.
        List<NodeDto> graphNodes = List.of();
        List<EdgeDto> graphEdges = List.of();
        try {
            ExecutionContext ctx = contextStore.loadByRef(ref).orElse(null);
            if (ctx == null) {
                log.warn("scriptDispatchDelegate: context vanished for node={} ref={}; failing to error end.",
                        nodeId, ref.getCurrentNodeRefId());
                emitTerminalFailureLog(ref.getWorkflowExecutionId(), nodeId, null,
                        SandboxJobResponse.FailureKind.ENGINE, "context unavailable", retryAttempt);
                resultCoordinator.deliverFailure(dispatchId, ERROR_DISPATCH_UNRESOLVED, "context unavailable",
                        SandboxJobResponse.FailureKind.ENGINE.name(), null);
                return;
            }
            // Capture the graph BEFORE prepareInbound clears it — needed to resolve gateway routing
            // AND to carry forward onto the next node's ref (see the success branch below).
            graphNodes = ctx.getNodes() == null ? List.of() : new ArrayList<>(ctx.getNodes());
            graphEdges = ctx.getEdges() == null ? List.of() : new ArrayList<>(ctx.getEdges());

            // The node the ref was MINTED for (from the loaded blob). For a FUNCTION-port sub-process
            // this is the entry node the daemon addressed its studioquack.execute() payload to. Capture
            // it — and the carried entry payload — BEFORE overwriting currentNodeId below.
            String refNodeId = ctx.getCurrentNodeId();
            // The function-entry payload the daemon stamped onto the ref's context blob
            // (nextNodeInputList); a sub-process's first node receives its inputs THIS way.
            List<InOutValueDto> carriedEntryInputs = ctx.getNextNodeInputList();

            ctx.setCurrentNodeId(nodeId);
            ctx.setCurrentNode(null);

            // Operator "Skip & Continue" on a run held at hold_<nodeId>: the skip-step command
            // pinned __sq_skip_node on the instance and requeued the hold, which loops the token
            // back here. Consume the pin and short-circuit BEFORE any input/transform work — no
            // sandbox runs, no outputs persist. An authored SQ_SKIP boundary gets the typed
            // failure the error gate would have raised; anything else continues along the node's
            // normal outbound path as if it completed with no outputs.
            if (consumeSkipNodePin(dispatchId, nodeId)) {
                handleSkippedNode(dispatchId, nodeId, ref, ctx, graphNodes, graphEdges);
                return;
            }

            // Multi-node I/O: inbound values come from TWO sources that must be MERGED, not one
            // overwriting the other:
            //   (a) upstream node outputs persisted for this execution (loadInputsTargetingNode) —
            //       sealed per-ref with targetNodeId by the output fan-out;
            //   (b) the function-entry payload carried on the ref blob (above), honoured ONLY for the
            //       node the ref was minted for so later sub-process nodes don't re-consume it.
            // Blindly overwriting nextNodeInputList with (a) silently dropped (b), so a FUNCTION-port
            // sub-process's first sandbox (e.g. the `model` provider reading `llm_input`) booted with no
            // input ("No input received on '<port>' port"). Ported from the control plane ScriptDispatchDelegate
            // fix for parity. The top-level trigger node has no upstream, so its seedContext
            // nextNodeInputList still flows through unchanged.
            List<InOutValueDto> upstreamInputs = contextStore.loadInputsTargetingNode(ref, nodeId);
            // repair: the operator's apply-inputs override for (run, node) folds LAST, so it
            // wins on every re-run path (retry, replay — the key survives both; an empty override
            // means the operator cleared the inbound list). null when no override was stored.
            List<InOutValueDto> operatorOverride = loadOperatorOverride(ref, ctx, nodeId);
            List<InOutValueDto> mergedInputs = mergeNodeInputs(nodeId, refNodeId, carriedEntryInputs,
                    upstreamInputs, operatorOverride);
            // DuckTape Add-Node: a mapping row pinned on an inbound edge can name an earlier node
            // with NO wire into this one. Re-address that node's persisted outputs onto this node's
            // input list so its message rides along with the triggering predecessor's — the same
            // list-of-messages contract a real multi-connection port already has.
            // The sealed context's global variable maps ride along so rows pinned from the
            // virtual "Workflow/Project Globals" cards materialize without a store scan.
            ForeignEdgeInputResolver.Resolution foreign = ForeignEdgeInputResolver.resolveWithCarriers(
                    contextStore, ref, nodeId, graphEdges,
                    ctx.getWorkflowVariables(), ctx.getProjectVariables());
            List<InOutValueDto> foreignInputs = foreign.rows();
            mergedInputs = mergeForeignInputs(mergedInputs, foreignInputs);
            // Tell the pipeline which rows are pinned and which connection line each belongs to, so
            // it folds every pinned row into THAT line's message after the JOLT pre-bake. Connection
            // lines decide the message count; the port map and the helper SDK's inputs() keep ONE
            // value per port, so an extra row there meant the last one silently won.
            ctx.setPinnedInputCarriers(ForeignEdgeInputResolver.pinnedCarriers(mergedInputs, foreign));
            ctx.setNextNodeInputList(mergedInputs);
            boolean isEntryNode = nodeId != null && nodeId.equalsIgnoreCase(refNodeId);
            PreparedInbound prepared = transformPipeline.prepareInbound(ctx);

            // Duckbot boundary nodes carry no sandbox script: the start node injects the user prompt
            // as its output; the end node routes the final answer back to the conversation. Both then
            // resume the parked token like a normal successful node (no sandbox dispatch, no log).
            if (duckbotBridge.isStart(ctx) || duckbotBridge.isEnd(ctx)) {
                log.debug("scriptDispatchDelegate.io node={} refNodeId={} isEntryNode={} carried={} upstream={} foreign={} merged={}",
                        nodeId, refNodeId, isEntryNode,
                        carriedEntryInputs == null ? 0 : carriedEntryInputs.size(),
                        upstreamInputs == null ? 0 : upstreamInputs.size(), foreignInputs.size(), mergedInputs.size());
                handleDuckbotBoundary(dispatchId, nodeId, ref, ctx, graphNodes, graphEdges);
                return;
            }

            log.info("scriptDispatchDelegate.io node={} refNodeId={} isEntryNode={} carried={} upstream={} foreign={} merged={}",
                    nodeId, refNodeId, isEntryNode,
                    carriedEntryInputs == null ? 0 : carriedEntryInputs.size(),
                    upstreamInputs == null ? 0 : upstreamInputs.size(), foreignInputs.size(), mergedInputs.size());

            SandboxLanguage language = SandboxLanguage.fromWire(ctx.findCurrentNodeLanguage());
            // an entrypoint (custom-image) node needs no script language: the image owns its
            // start argv, the script bake is skipped below and SQ_CONTEXT carries the context. Only
            // the SCRIPT path may fail "unresolved language" — a build-mode node (Script toggle off)
            // persists no templateScript.language at all and must still reach its image.
            ExecutionRequirementsDto gateReq = currentExecutionRequirements(ctx);
            boolean entrypointImageNode = SandboxImageResolution.isEntrypointImage(
                    gateReq == null ? null : gateReq.getOciImageRef(), language, environment::getProperty);
            if (language == null && !entrypointImageNode) {
                log.warn("scriptDispatchDelegate: unresolved language for node={}; failing to error end.", nodeId);
                emitTerminalFailureLog(ctx.getWorkflowExecutionId(), nodeId, currentNodeName(ctx),
                        SandboxJobResponse.FailureKind.INFRA, "unresolved language", retryAttempt);
                resultCoordinator.deliverFailure(dispatchId, ERROR_DISPATCH_UNRESOLVED, "unresolved language",
                        SandboxJobResponse.FailureKind.INFRA.name(), null);
                return;
            }

            // S6 / — script edge-transform pre-step (before secrets / file materialise).
            ExecutionRequirementsDto earlyReq = currentExecutionRequirements(ctx);
            long nodeWallMs = limitsFrom(earlyReq).wallMs();
            EdgeTransformPreStep.Result edgeStep =
                    edgeTransformPreStep.run(ctx, prepared, language, nodeWallMs, dispatchId);
            if (edgeStep.failed()) {
                emitTerminalFailureLog(ctx.getWorkflowExecutionId(), nodeId, currentNodeName(ctx),
                        SandboxJobResponse.FailureKind.SCRIPT, edgeStep.errorMessage(), retryAttempt);
                jobQueue.finish(dispatchId, JobState.FAILED, -1, "", "", edgeStep.errorMessage(), null);
                resultCoordinator.deliverFailure(dispatchId, edgeStep.errorCode(), edgeStep.errorMessage(),
                        SandboxJobResponse.FailureKind.SCRIPT.name(), null);
                return;
            }
            prepared = edgeStep.prepared();
            prepared = transformPipeline.filterInboundAgainstPortSchemas(ctx, prepared);

            // resolve $(…) in configuration-parameter strings BEFORE secrets ------------
            // prepareInbound cleared nodes; restore briefly so $(@shortCode…) can address the graph.
            // Config params are NOT copied into prepared.inputVariables() (port map only) — secrets
            // later walk currentNode + inputVariables, so mutating currentNode is the right lane.
            ctx.setNodes(graphNodes);
            resolveExpressionRefsInConfig(ctx);
            ctx.clearNodeAndEdges();

            // --- Deployed-binding guard (fail-closed, BEFORE any secret is read) -------------------
            // A $secretRef may resolve only when the deployed graph bound that exact ref on THIS
            // node. A context pointing a parameter at a foreign/stale ref (tampered seed, carried
            // helper-channel payload, leftover from a previous engine on this host) is rejected
            // here — never at the OS store — and reported to the control plane as an tamper event.
            try {
                secretBindingGuard.verifyNodeBindings(
                        deployArtifactKey(ctx.getWorkflowId(), processDefinitionId),
                        nodeId, ctx.getCurrentNode(), graphNodes, prepared.inputVariables());
            } catch (com.studioquack.sqs.sandbox.secrets.DeployedSecretBindingGuard.SecretBindingViolation v) {
                tamperEventReporter.report(
                        com.studioquack.sqs.sandbox.secrets.DeployedSecretBindingGuard.TAMPER_TYPE,
                        v.getMessage());
                log.warn("scriptDispatchDelegate: {} — failing node={} closed.", v.getMessage(), nodeId);
                emitTerminalFailureLog(ctx.getWorkflowExecutionId(), nodeId, currentNodeName(ctx),
                        SandboxJobResponse.FailureKind.ENGINE, v.getMessage(), retryAttempt);
                jobQueue.finish(dispatchId, JobState.FAILED, -1, "", "", v.getMessage(), null);
                resultCoordinator.deliverFailure(dispatchId, ERROR_SECRET_BINDING_MISMATCH, v.getMessage(),
                        SandboxJobResponse.FailureKind.ENGINE.name(), null);
                return;
            }

            // --- Just-in-time secret binding (secure inject) --------------------------------------
            // The persisted context, the engine `studioquackRef`, and the sandbox-log mirror all keep
            // ONLY the `$secretRef` handles (REF-ONLY + at-rest AES-GCM seal). At the last moment before
            // the container runs we materialise the sandbox-facing context as a PRIVATE per-dispatch
            // snapshot and replace every `$secretRef` in THIS node's subtree with the real value read
            // from the OS key manager. `ctx` itself is never mutated, so the resolved plaintext never
            // reaches the sealed execution_context store, the engine variables, or any log/audit.
            Map<String, Object> sandboxContext = toContextMap(ctx);              // graph-less, currentNode = handles
            Map<String, Object> sandboxInputVars = privateInputSnapshot(prepared.inputVariables());
            Set<String> secretLiterals = new LinkedHashSet<>();
            // Scope: the CURRENT node ONLY (never the `nodes` graph) so a node's container can never
            // read another node's resolved secret. Each target node resolves its own at its own dispatch.
            secretLiterals.addAll(secretResolver.resolveInline(sandboxContext.get("currentNode")));
            secretLiterals.addAll(secretResolver.resolveInline(sandboxInputVars));

            // ContentBlocks sealed as { fileId, … } (no multi-MB data) → fill data for the agent
            // script only on this private map. Must stay off the persisted ExecutionContext.
            contentBlockHydrator.hydrate(sandboxContext.get("currentNode"), sandboxInputVars);

            // Same seam, same scope as secrets: turn this node's `sq-file:` handles into real files and
            // swap each handle for the path it will occupy in the container. Must happen BEFORE the bake
            // below, because that is what freezes the value the script sees.
            List<SandboxJobRequest.AttachedFile> attachedFiles =
                    sandboxFileMaterializer.materialize(sandboxContext.get("currentNode"), sandboxInputVars);

            // Wire the OCI ref early so entrypoint-mode can skip the script bake below. Same lookup
            // the job builder reuses after the graph re-attach (executionRequirements live on currentNode).
            ExecutionRequirementsDto req = currentExecutionRequirements(ctx);
            boolean entrypointMode = SandboxImageResolution.isEntrypointImage(
                    req == null ? null : req.getOciImageRef(), language, environment::getProperty);

            // Bake: language-match script transforms → single combined source (1 acquire / 1 exec);
            // otherwise the normal node helper + user script (mismatch hops already merged above).
            // Entrypoint images skip the bake entirely — context rides SQ_CONTEXT instead.
            GenerateScriptService.BuildSourceResult built;
            String contextBase64 = null;
            if (entrypointMode) {
                // / — the image owns its entrypoint: no helper prepend, no script bake, no stdin
                // bootstrap. The context travels as SQ_CONTEXT — baked from the SAME sandboxContext map at
                // the SAME point (before the graph re-attach below) as the script path's __SQ_CONTEXT__.
                // A stored script, if one exists, is ignored here, never executed (and never deleted).
                if (edgeStep.useCombinedBake()) {
                    log.warn("scriptDispatchDelegate: node={} runs a custom entrypoint image — same-language "
                            + "edge-transform scripts are skipped (script-based execution is disabled for it)", nodeId);
                }
                contextBase64 = generateScriptService.bakeContextBase64(sandboxContext);
                built = new GenerateScriptService.BuildSourceResult("", contextBase64);
            } else {
                built = edgeStep.useCombinedBake()
                        ? generateScriptService.buildCombinedTransformAndNodeSource(
                                sandboxContext, ctx.findCurrentNodeLanguage(), ctx.findCurrentNodeScript(),
                                edgeStep.matchedBakes())
                        : generateScriptService.buildScriptSourceFromContext(
                                sandboxContext, ctx.findCurrentNodeLanguage(), ctx.findCurrentNodeScript());
            }

            // Null language is legal exactly in entrypoint mode (gate above); label the WAL row
            // "custom" so the queue's language column never carries null. The run id rides along
            // so list-jobs rows can key historic sandbox logs — the run they are persisted under.
            jobQueue.submit(dispatchId, language == null ? "custom" : language.wire(), built.source(),
                    logRunId(ctx.getWorkflowExecutionId()));
            jobQueue.leaseById(dispatchId);

            // Re-attach the graph as HANDLES (never resolved) for both the persisted ctx and the
            // sandbox-facing context. An agent node's studioquack.execute() carries the sandbox context to
            // the helper channel, which starts the FUNCTION-port target node (e.g. the `model` provider) as
            // a local sub-PI; that sub-PI resolves the TARGET node's definition (language/script) from
            // `nodes`. prepareInbound cleared the graph above, so without this the carried context has only
            // currentNode and an EMPTY nodes list → the provider sub-PI fails "unresolved language". Node
            // definitions are static config (not secrets); their own `$secretRef` handles stay UNRESOLVED
            // here and are bound only when that node is itself dispatched.
            ctx.setNodes(graphNodes);
            ctx.setEdges(graphEdges);
            sandboxContext.put("nodes", objectMapper.convertValue(graphNodes,
                    new TypeReference<List<Map<String, Object>>>() {}));
            sandboxContext.put("edges", objectMapper.convertValue(graphEdges,
                    new TypeReference<List<Map<String, Object>>>() {}));

            // Wire the OCI ref; copy the legacy stateful flag for stored-template compat.
            // Dispatch routing ignores the flag (image-based: live instance → warm → cold).
            // / — templateId is NodeDto.data.id (TemplateDto.id), NOT the React Flow node id.
            String templateId = currentTemplateId(ctx);
            String stagedWorkspaceId = ctx.isTemporaryContainer()
                    ? StringUtils.trimToNull(ctx.getStagedWorkspaceId()) : null;
            SandboxJobRequest job = SandboxJobRequest.builder(dispatchId, language, built.source())
                    .inputVariables(sandboxInputVars)
                    .executionContext(sandboxContext)
                    .envId(ctx.getEnvId())
                    .workflowExecutionId(ctx.getWorkflowExecutionId())
                    .temporaryContainer(ctx.isTemporaryContainer())
                    .stagedWorkspaceId(stagedWorkspaceId)
                    .currentNodeId(nodeId)
                    .templateId(templateId)
                    .bespokeDir(bespokeDirFor(templateId, req, stagedWorkspaceId != null))
                    .contextBase64(contextBase64)
                    .limits(limitsFrom(req))
                    .cpus(req == null ? 0 : req.getCpu())
                    .diskMb(req == null ? 0 : req.getDisc())
                    .ociImageRef(req == null ? null : req.getOciImageRef())
                    .stateful(req != null && req.isStateful())
                    .secretLiterals(secretLiterals)
                    .attachedFiles(attachedFiles)
                    .streaming(streamingRequested(ctx))
                    .build();

            // A function-port sub-PI child's PROCESS_STARTED can fire on the engine thread BEFORE
            // HelperExecuteService's register call returns (both race), and the listener then
            // defaults the child to BATCH — buffering the provider node's rows for a batch flush
            // that races the parent's terminal status. Re-assert the parent's mode here, when the link is durable.
            inheritParentLogModeIfSubPiChild(ctx);

            // Two-step live signal: tell the control plane this node's sandbox is STARTING (non-blocking) so the
            // execution animation shows it RUNNING; the terminal result is mirrored after dispatch.
            reportSandboxStarted(ctx, nodeId);

            // Cancel-instance marks the business key first, then reaps. If we lost the race
            // before a container was bound, do not start one — interrupting this thread during
            // the upcoming H2 persist would close the engine store.
            if (cancelledRunGuard != null
                    && cancelledRunGuard.isCancelled(ctx.getWorkflowExecutionId())) {
                log.info("scriptDispatchDelegate: run already cancelled; skipping sandbox node={} dispatchId={}",
                        nodeId, dispatchId);
                return;
            }
            if (dispatchCancellationRegistry.isCancelled(dispatchId)) {
                log.info("scriptDispatchDelegate: step already cancelled; skipping sandbox node={} dispatchId={}",
                        nodeId, dispatchId);
                return;
            }

            SandboxJobResponse response = dispatch(job);
            // Sandbox returned (ok or not): further work is engine/JPA — no longer interruptible.
            dispatchCancellationRegistry.markEnginePhase(dispatchId);
            // Boundary cancelled wait_<node> while the sandbox ran: do not resume a gone wait
            // and do not overwrite the CANCELLED sandbox-log BpmnNodeLogListener already emitted.
            if (dispatchCancellationRegistry.isCancelled(dispatchId)) {
                log.info("scriptDispatchDelegate: dispatch cancelled after sandbox; skipping resume/log "
                                + "node={} dispatchId={}",
                        nodeId, dispatchId);
                return;
            }

            // Only a successful node may materialise normal output and resume its outgoing sequence
            // flow. Failed-node diagnostics stay exclusively in the container log/error boundary path.
            if (response.ok()) {
                transformPipeline.finalizeOutputs(ctx,
                        withOperatorOutputOverride(ref, ctx, nodeId, response.outputVariables()));
                persistOutputs(ref, ctx);
            }

            jobQueue.finish(dispatchId, response.ok() ? JobState.SUCCEEDED : JobState.FAILED,
                    response.exitCode(), response.stdout(), response.stderr(),
                    response.errorMessage(), null);

            // Mirror this node's result into the control plane's sandbox_execution_logs so the Monitoring UI's
            // per-node "echoes"/log panels populate for client-orchestrated runs (best-effort).
            reportSandboxLog(ctx, nodeId, response, retryAttempt);
            // Language-match transforms ran inside B — still emit per-wire Duck Tales rows under
            // the synthetic edge-transform identity (no extra acquire).
            if (edgeStep.useCombinedBake()) {
                edgeTransformPreStep.reportMatchedTransformLogs(
                        ctx, edgeStep.matchedBakes(), language == null ? "custom" : language.wire(), response);
            }

            if (response.ok()) {
                // Carry the graph FORWARD onto the next node's ref. prepareInbound cleared nodes/edges
                // off ctx for THIS dispatch, but the next node reloads by ref and needs the full graph to
                // resolve its own currentNode + rebuild connections. Without this the ref chain becomes
                // graph-less after the first node and every downstream node fails to resolve ("unresolved
                // language" / no currentNode). Re-attaching keeps the chain self-describing end-to-end.
                // Also required BEFORE route-token resolution so typed conditions can resolve
                // $(@nodeShortCode…) against ctx.nodes.
                ctx.setNodes(graphNodes);
                ctx.setEdges(graphEdges);
                Map<String, Object> routeTokens = routeResolver.resolveRouteTokens(
                        graphNodes, graphEdges, nodeId, currentOutputs(ctx), ctx);
                Map<String, Object> engineVars = refOnlyVarBuilder.toEngineVariables(ctx);
                resultCoordinator.deliverSuccess(dispatchId, engineVars, routeTokens);
            } else {
                // Language-match transform prelude failures surface as B-job failures — remap codes.
                String failCode = response.errorCode();
                if (edgeStep.useCombinedBake()) {
                    String mapped = edgeTransformPreStep.mapCombinedFailureCode(response);
                    if (mapped != null) {
                        failCode = mapped;
                    }
                }
                resultCoordinator.deliverFailure(dispatchId, failCode, response.errorMessage(),
                        response.failureKind().name(), response.exitCode());
            }
        } catch (Exception e) {
            // Catches EVERYTHING between loading the ref and delivering the result — including a
            // classified system failure elevated by persistOutputs()/finalizeOutputs() below, and a lost
            // "terminal callback" (deliverSuccess/deliverFailure itself throwing after the sandbox
            // already ran). ENGINE-kind: neither the user's script nor the container/runtime.
            // A gateway routing miss (no route token / JUEL Unknown property) is named as such;
            // otherwise only the exception CLASS name is reported (never e.getMessage() / PHI).
            reportCaughtDispatchFailure(dispatchId, nodeId, ref, graphNodes, graphEdges, e, retryAttempt);
        }
    }

    /**
     * Image-based routing: every job with an OCI image ref resolves a LIVE
     * {@code (envId,imageRef)} instance (in-memory only — no Podman I/O on this hot path).
     * A match uses {@link StatefulSandboxExecution}. With no live instance the image is
     * cold-started as a stateful container on demand ({@link StatefulInstanceManager#start})
     * and the dispatch is served through it — a custom image's entrypoint is a serve-forever
     * listener, so the ephemeral one-shot path cannot run it (the exec would park until the
     * wall-clock SIGKILL, exit 137). A Create Node trial job is the exception: its image runs in
     * a temporary container started for that job alone and removed after it, so it neither looks
     * up nor starts an instance. The per-node {@code stateful} flag is a no-op for dispatch.
     */
    SandboxJobResponse dispatch(SandboxJobRequest job) {
        if (job.hasOciImageRef()) {
            // Resolve once on the dispatch worker, not while the workflow/template is prepared.
            String instanceEnv = instanceEnvFor(job);
            if (job.temporaryContainer() && !isBuiltinLibraryImage(job.ociImageRef())) {
                return dispatchToTemporaryContainer(job, instanceEnv);
            }
            StatefulInstance instance = statefulInstances
                    .resolveRunning(instanceEnv, job.ociImageRef()).orElse(null);
            if (instance == null) {
                instance = coldStartImageInstance(job, instanceEnv);
            }
            if (instance != null) {
                log.info("scriptDispatchDelegate.dispatch node={} → stateful instance={} container={} ({})",
                        job.currentNodeId(), instance.instanceId(), shortId(instance.containerId()),
                        instance.dispatchCapable() ? "parallel" : "fifo");
                return statefulSandbox.execute(job, instance);
            }
            return SandboxJobResponse.failure(-1, "", "",
                    Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    "image cold start failed for '" + job.ociImageRef() + "' — see daemon log",
                    0L, null, SandboxJobResponse.FailureKind.CONTAINER);
        }
        return sandbox.execute(job);
    }

    /**
     * Runs a Create Node trial job in a temporary container: started from the job's image for this
     * job alone and removed when it returns, on success, failure or a failed start alike. An
     * inject-mode trial's staged workspace is copied into the container first, and deleted from
     * the engine with it — a staged workspace serves exactly one run.
     */
    private SandboxJobResponse dispatchToTemporaryContainer(SandboxJobRequest job, String envId) {
        StatefulInstanceManager.StartResult started = null;
        try {
            started = statefulInstances.startTemporary(envId, job.ociImageRef());
            StatefulInstance container = statefulInstances.temporary(started.instanceId())
                    .filter(StatefulInstance::isRunning).orElse(null);
            if (container == null) {
                return temporaryContainerFailure("The temporary container for '" + job.ociImageRef() + "' did not start"
                        + (started.message() == null ? " — see the engine log" : ": " + started.message()),
                        started.containerId());
            }
            if (job.stagedWorkspaceId() != null) {
                String problem = installStagedWorkspace(job, container);
                if (problem != null) {
                    return temporaryContainerFailure(problem, container.containerId());
                }
            }
            log.info("scriptDispatchDelegate.dispatch node={} → temporary container={} container={}",
                    job.currentNodeId(), container.instanceId(), shortId(container.containerId()));
            return statefulSandbox.execute(job, container);
        } catch (RuntimeException e) {
            log.warn("scriptDispatchDelegate: temporary container for ref='{}' threw: {}",
                    job.ociImageRef(), e.toString());
            return temporaryContainerFailure(
                    "The temporary container for '" + job.ociImageRef() + "' failed — see the engine log", null);
        } finally {
            if (started != null) {
                statefulInstances.removeTemporary(started.instanceId());
            }
            if (job.stagedWorkspaceId() != null && buildContextStore != null) {
                buildContextStore.deleteContext(job.stagedWorkspaceId());
            }
        }
    }

    /**
     * Copies an inject-mode trial's staged workspace into the temporary container's bespoke
     * folder. Returns why it could not, or {@code null} once the files are in place.
     */
    private String installStagedWorkspace(SandboxJobRequest job, StatefulInstance container) {
        if (buildContextStore == null || !buildContextStore.contextExists(job.stagedWorkspaceId())) {
            return "The workspace staged for this run is not on the engine — run Build & Run again";
        }
        try {
            statefulInstances.installTemporaryFiles(container.instanceId(), job.templateId(),
                    buildContextStore.contextDir(job.stagedWorkspaceId()));
            return null;
        } catch (java.io.IOException | IllegalArgumentException e) {
            log.warn("scriptDispatchDelegate: staged workspace for node={} not copied: {}",
                    job.currentNodeId(), e.getMessage());
            return "The workspace could not be copied into the temporary container: " + e.getMessage();
        }
    }

    private static SandboxJobResponse temporaryContainerFailure(String reason, String containerId) {
        return SandboxJobResponse.failure(-1, "", reason, Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                reason, 0L, containerId, SandboxJobResponse.FailureKind.CONTAINER);
    }

    /**
     * On-demand cold start of the (env, imageRef) stateful instance. Returns the RUNNING
     * instance, or {@code null} when the start failed (already logged by the manager).
     * Concurrent dispatches for the same image serialize on the manager's per-key start lock;
     * the loser re-resolves the live instance the winner just started.
     */
    private StatefulInstance coldStartImageInstance(SandboxJobRequest job, String envId) {
        boolean library = isBuiltinLibraryImage(job.ociImageRef());
        log.info("scriptDispatchDelegate: no live instance for node={} (env={}, ref='{}') — cold start{}",
                job.currentNodeId(), envId, job.ociImageRef(), library ? " of the builtin library" : "");
        StatefulInstanceManager.StartResult res;
        try {
            if (library) {
                // Same identity BuiltinNodeLibraryManager starts: a generic cold start here gave
                // the library a second, randomly-named copy beside the system instance. No
                // resource limits, exactly like the system start.
                statefulInstances.reconcilePlatformSingleton(envId, job.ociImageRef(),
                        BuiltinNodeLibraryManager.INSTANCE_ID);
                res = statefulInstances.start(envId, job.ociImageRef(), true,
                        StatefulCaps.uncapped(),
                        BuiltinNodeLibraryManager.INSTANCE_ID, null, null, (Map<String, String>) null);
            } else {
                // Stateful user instances start uncapped (no CPU/RAM/disk/GPU limits).
                res = statefulInstances.start(envId, job.ociImageRef(), true, StatefulCaps.uncapped());
            }
        } catch (RuntimeException e) {
            log.warn("scriptDispatchDelegate: cold start threw for ref='{}': {}",
                    job.ociImageRef(), e.toString());
            return null;
        }
        if (res == null || !StatefulStatus.RUNNING.equals(res.status())) {
            log.warn("scriptDispatchDelegate: cold start ref='{}' → status={} msg={}",
                    job.ociImageRef(), res == null ? "null" : res.status(),
                    res == null ? null : res.message());
            return null;
        }
        return statefulInstances.resolveRunning(envId, job.ociImageRef()).orElse(null);
    }

    /**
     * The env a job's image instance is keyed under. The builtin library is one platform
     * instance registered under the daemon's own env id, so a job env that drifted from it
     * (re-enrolment, replaced engine) must still find that instance instead of starting another.
     */
    private String instanceEnvFor(SandboxJobRequest job) {
        if (environment != null && isBuiltinLibraryImage(job.ociImageRef())) {
            String daemonEnv = environment.getProperty("sqs.channel.env-id", "").trim();
            if (!daemonEnv.isEmpty()) {
                return daemonEnv;
            }
        }
        return job.envId();
    }

    private boolean isBuiltinLibraryImage(String ref) {
        String libraryRef = environment == null ? BuiltinNodeLibraryManager.IMAGE_REF
                : environment.getProperty("sqs.builtin-library.image", BuiltinNodeLibraryManager.IMAGE_REF);
        return ref != null && ref.equals(libraryRef);
    }

    /** The current node's execution requirements ({@code cpu/ram/disc} + {@code ociImageRef}/{@code stateful}), or null. */
    private ExecutionRequirementsDto currentExecutionRequirements(ExecutionContext ctx) {
        if (ctx.getCurrentNode() == null || ctx.getCurrentNode().getData() == null) {
            return null;
        }
        return ctx.getCurrentNode().getData().getExecutionRequirementsDto();
    }

    /**
 * / — the node TEMPLATE id ({@code NodeDto.data.id} / {@code TemplateDto.id}).
 * This is what the canvas carries as {@code node.data.id}; it is NOT the React Flow
 * instance id ({@link NodeDto#getId}) and NOT {@code currentNodeId}.
 */
    private String currentTemplateId(ExecutionContext ctx) {
        if (ctx == null || ctx.getCurrentNode() == null || ctx.getCurrentNode().getData() == null) {
            return null;
        }
        String id = ctx.getCurrentNode().getData().getId();
        return id == null || id.isBlank() ? null : id;
    }

    /**
     * The node's own streaming contract ({@code stream} canonical, {@code streaming} alias —
     * same precedence the node handlers use). SSE-streaming nodes legitimately
     * occupy a stateful instance for minutes while SSE deltas flow, so the stateful FIFO wait
     * must not apply the 30 s queue cap to them. Absent/unparseable → {@code false} (the 30 s
     * posture stays the default for every node that does not declare streaming).
     */
    private static boolean streamingRequested(ExecutionContext ctx) {
        if (ctx == null || ctx.getCurrentNode() == null || ctx.getCurrentNode().getData() == null) {
            return false;
        }
        java.util.List<com.studioquack.common.dto.flow.ParameterDto> params =
                ctx.getCurrentNode().getData().getConfigurationParameters();
        if (params == null || params.isEmpty()) {
            return false;
        }
        Object alias = null;
        for (com.studioquack.common.dto.flow.ParameterDto p : params) {
            if (p == null || p.getShortCode() == null) {
                continue;
            }
            if ("stream".equals(p.getShortCode())) {
                return truthy(p.getValue());
            }
            if ("streaming".equals(p.getShortCode())) {
                alias = p.getValue();
            }
        }
        return truthy(alias);
    }

    private static boolean truthy(Object v) {
        if (v instanceof Boolean b) {
            return b;
        }
        return v != null && "true".equalsIgnoreCase(String.valueOf(v).trim());
    }

    /**
 * / — {@code /data/sq-bespoke/<templateId>} when the template has uploaded
 * bespoke files, or when an inject-mode trial brings a staged workspace that is copied there;
 * {@code null} otherwise so the stateful invoke payload omits the key.
 */
    private static String bespokeDirFor(String templateId, ExecutionRequirementsDto req,
                                        boolean stagedWorkspace) {
        if (templateId == null || templateId.isBlank()) {
            return null;
        }
        boolean uploaded = req != null && req.getBespokeFiles() != null && !req.getBespokeFiles().isEmpty();
        return uploaded || stagedWorkspace ? "/data/sq-bespoke/" + templateId : null;
    }

    /** Maps the node's {@code ram} (MiB) into a sandbox RAM cap; other dimensions ride the request. */
    private SandboxLimits limitsFrom(ExecutionRequirementsDto req) {
        if (req == null || req.getRam() <= 0) {
            return SandboxLimits.unbounded();
        }
        // node-declared RAM cannot exceed the host ceiling. Unset RAM stays unbounded
        // here; stateful cold start applies the per-field defaults separately.
        int ram = req.getRam();
        int ceiling = com.studioquack.sqs.sandbox.oci.SandboxHostBudget.maxRamMb(maxRamMb());
        if (ram > ceiling) {
            log.warn("sandbox: node-declared RAM {} MiB exceeds the host ceiling {} MiB; clamping",
                    ram, ceiling);
            ram = ceiling;
        }
        return SandboxLimits.of(ram, 0L);
    }

    /** {@code sqs.sandbox.max-ram-mb}; {@code <= 0} derives 75% of host memory. */
    private int maxRamMb() {
        if (environment == null) {
            return 0;
        }
        return environment.getProperty("sqs.sandbox.max-ram-mb", Integer.class, 0);
    }

    private static String shortId(String id) {
        return id == null ? "" : id.substring(0, Math.min(12, id.length()));
    }

    /**
     * Resolves a Duckbot start/end boundary node WITHOUT a sandbox job: run its bridge (prompt
     * injection / conversation write-back), persist the resulting outputs, then resume the parked
     * ReceiveTask exactly like a successful sandbox node so the token advances. Never throws — a
     * boundary hiccup routes through the error gate rather than hanging the run.
     */
    private void handleDuckbotBoundary(String dispatchId, String nodeId, ProcessContextRef ref,
                                       ExecutionContext ctx, List<NodeDto> graphNodes, List<EdgeDto> graphEdges) {
        long startedNanos = System.nanoTime();
        try {
            if (duckbotBridge.isStart(ctx)) {
                duckbotBridge.applyStart(ctx);
            } else {
                duckbotBridge.applyEnd(ctx);
            }
            persistOutputs(ref, ctx);
            // Mirror the boundary node for AUDIT/LOGS before resuming: the start node's output port
            // carries the user message and any attached files, and the end node carries the final
            // answer, so a run's log trail is incomplete without them. Emitted TERMINAL-only (no
            // "started" half) on purpose — a boundary node does nothing observable to animate, and a
            // non-terminal marker would make it render as the live node.
            reportBoundaryLog(ctx, nodeId, "COMPLETED", null, startedNanos, SandboxJobResponse.FailureKind.NONE);
            // Restore graph before route tokens so typed $(@…) conditions can resolve shortCodes.
            ctx.setNodes(graphNodes);
            ctx.setEdges(graphEdges);
            Map<String, Object> routeTokens = routeResolver.resolveRouteTokens(
                    graphNodes, graphEdges, nodeId, currentOutputs(ctx), ctx);
            Map<String, Object> engineVars = refOnlyVarBuilder.toEngineVariables(ctx);
            resultCoordinator.deliverSuccess(dispatchId, engineVars, routeTokens);
        } catch (Exception e) {
            reportCaughtDuckbotFailure(dispatchId, nodeId, ref, ctx, graphNodes, graphEdges,
                    startedNanos, e);
        }
    }

    /**
     * Operator "Skip &amp; Continue" short-circuit for a held-then-requeued node (mirrors
     * {@link #handleDuckbotBoundary}: no sandbox, a boundary-style log row, then resume the parked
     * token). An authored {@code SQ_SKIP} error boundary on the node gets the typed failure the
     * error gate would have raised — the token routes to the step's own skip handler. Without one
     * the node is treated as completed: no sandbox job and route tokens resolve from the graph.
     * Outputs come from the operator's apply-inputs OUTPUT override when one exists (fanned out +
     * persisted like the normal success branch, so downstream reads them as if the node had
     * produced them) — on BOTH branches, so the authored skip handler sees them too; otherwise the
     * node completes with NO outputs. Never logs payload values —
     * the SKIPPED row's currentNode stays null (no IO) unless an operator input override exists
     * for the node, in which case the row mirrors exactly those edited parameters. Never throws.
     */
    void handleSkippedNode(String dispatchId, String nodeId, ProcessContextRef ref,
                           ExecutionContext ctx, List<NodeDto> graphNodes, List<EdgeDto> graphEdges) {
        long startedNanos = System.nanoTime();
        try {
            // repair: a SKIPPED row must still show the operator-edited parameters from
            // apply-inputs (the sandbox never ran, so nothing else would surface them). Only when
            // an override exists — otherwise the row keeps its honest no-IO shape.
            mirrorOperatorOverrideOnSkippedRow(ref, ctx, graphNodes, nodeId);
            if (ActivitiBoundaryEventHelper.hasAuthoredErrorBoundary(
                    findNode(graphNodes, nodeId), EngineVariableNames.SKIP_ERROR_CODE)) {
                // The authored skip handler must see the operator's output override too: fan out +
                // persist BEFORE delivering the SQ_SKIP failure, so the skip handler's dispatch
                // reads those outputs from loadInputsTargetingNode like any other successor.
                // Route tokens stay as today for this branch (the typed failure routes the token).
                applyOperatorOutputOverride(ref, ctx, graphNodes, graphEdges, nodeId);
                reportBoundaryLog(ctx, nodeId, "SKIPPED",
                        "skipped by operator — routed to the step's skip handler", startedNanos,
                        SandboxJobResponse.FailureKind.NONE);
                resultCoordinator.deliverFailure(dispatchId, EngineVariableNames.SKIP_ERROR_CODE,
                        "skipped by operator");
                return;
            }
            // repair (output overrides): the operator may have edited/added the FAILED node's
            // OUTPUT parameters via apply-inputs. Those become the skipped node's outputs — fanned
            // out and persisted exactly like the normal success branch (finalizeOutputs →
            // persistOutputs), so the next node(s) read them from loadInputsTargetingNode as if
            // the node had produced them. No override → no outputs (unchanged).
            boolean outputsApplied = applyOperatorOutputOverride(ref, ctx, graphNodes, graphEdges, nodeId);
            reportBoundaryLog(ctx, nodeId, "SKIPPED",
                    outputsApplied
                            ? "skipped by operator — continued with operator-edited outputs"
                            : "skipped by operator — continued with no outputs",
                    startedNanos,
                    SandboxJobResponse.FailureKind.NONE);
            // Restore graph before route tokens so typed $(@…) conditions can resolve shortCodes.
            ctx.setNodes(graphNodes);
            ctx.setEdges(graphEdges);
            Map<String, Object> routeTokens = routeResolver.resolveRouteTokens(
                    graphNodes, graphEdges, nodeId, currentOutputs(ctx), ctx);
            Map<String, Object> engineVars = refOnlyVarBuilder.toEngineVariables(ctx);
            resultCoordinator.deliverSuccess(dispatchId, engineVars, routeTokens);
        } catch (Exception e) {
            reportCaughtSkippedFailure(dispatchId, nodeId, ref, ctx, graphNodes, graphEdges,
                    startedNanos, e);
        }
    }

    /**
     * Fans out + persists the operator's apply-inputs OUTPUT override as the skipped node's
     * outputs (exactly like the normal success branch: finalizeOutputs → persistOutputs), so
     * downstream nodes read them from {@code loadInputsTargetingNode} as if the node had produced
     * them. The whole map is operator-supplied, so exactly those ports are exempted from the
     * output port-schema filter for this fan-out (transient ctx flag — never persisted).
     *
     * @return {@code true} when an override existed and was fanned out; {@code false} = no outputs.
     */
    private boolean applyOperatorOutputOverride(ProcessContextRef ref, ExecutionContext ctx,
                                                List<NodeDto> graphNodes, List<EdgeDto> graphEdges,
                                                String nodeId) {
        List<InOutValueDto> outputOverride = loadOperatorOutputOverride(ref, ctx, nodeId);
        if (outputOverride == null || outputOverride.isEmpty() || transformPipeline == null) {
            return false;
        }
        resolveSkippedNodeForOutputFanOut(ctx, graphNodes, graphEdges, nodeId);
        Map<String, Object> operatorOutputs = toPortValueMap(outputOverride);
        ctx.setOperatorOverrideOutputPorts(new LinkedHashSet<>(operatorOutputs.keySet()));
        transformPipeline.finalizeOutputs(ctx, operatorOutputs);
        persistOutputs(ref, ctx);
        return true;
    }

    /**
     * Reads and CONSUMES the operator skip pin ({@link EngineVariableNames#SKIP_NODE}) for this
     * dispatch's process instance: {@code true} only when the pin names exactly {@code nodeId}.
     * The pin is removed BEFORE the short-circuit runs (best-effort —
     * {@link SqsEngine#removeVariables} already swallows a stale-scope race) so a dispatch that
     * dies midway can never leave a stale pin behind to skip a LATER run of the same node.
     * Always {@code false} when no engine/registry is wired (slice tests).
     */
    boolean consumeSkipNodePin(String dispatchId, String nodeId) {
        SqsEngine engine = sqsEngineProvider == null ? null : sqsEngineProvider.getIfAvailable();
        if (engine == null || nodeId == null || dispatchRegistry == null) {
            return false;
        }
        String pid = dispatchRegistry.find(dispatchId)
                .map(DispatchExecutionRegistry.WaitState::processInstanceId)
                .orElse(null);
        if (pid == null) {
            return false;
        }
        Object pinned = engine.getProcessVariable(pid, EngineVariableNames.SKIP_NODE);
        if (!nodeId.equals(pinned)) {
            return false;
        }
        engine.removeVariables(pid, Set.of(EngineVariableNames.SKIP_NODE));
        log.info("scriptDispatchDelegate.skip node={} dispatchId={} pi={} — operator Skip & Continue "
                + "consumed; no sandbox runs", nodeId, dispatchId, pid);
        return true;
    }

    /**
     * Puts the operator's apply-inputs override for {@code (run, nodeId)} onto the node the
     * SKIPPED boundary-log row reports ({@code reportBoundaryLog} reads
     * {@code currentNode.inputList}), so the record shows the edited parameters. No-op when no
     * override was stored; the skip path never snapshots again, so the in-process mutation of the
     * graph node cannot leak into another node's dispatch.
     */
    private void mirrorOperatorOverrideOnSkippedRow(ProcessContextRef ref, ExecutionContext ctx,
                                                    List<NodeDto> graphNodes, String nodeId) {
        List<InOutValueDto> override = loadOperatorOverride(ref, ctx, nodeId);
        if (override == null || ctx == null) {
            return;
        }
        NodeDto node = findNode(graphNodes, nodeId);
        if (node == null) {
            node = new NodeDto();
            node.setId(nodeId);
        }
        node.setInputList(new ArrayList<>(override));
        ctx.setCurrentNode(node);
    }

    /**
     * Gives the skip path a fan-out-capable current node: resolves it from the graph snapshot
     * (the input mirror may already have) and builds its connection list from the captured
     * graph, so {@code finalizeOutputs}' per-wire fan-out addresses the operator's outputs to
     * their downstream targets. No-op when the graph cannot resolve the node — the pipeline then
     * leaves the output list untouched and the skip completes with no outputs.
     */
    private void resolveSkippedNodeForOutputFanOut(ExecutionContext ctx, List<NodeDto> graphNodes,
                                                   List<EdgeDto> graphEdges, String nodeId) {
        if (ctx == null) {
            return;
        }
        if (ctx.getCurrentNode() == null) {
            NodeDto node = findNode(graphNodes, nodeId);
            if (node == null) {
                return;
            }
            ctx.setCurrentNode(node);
        }
        // NodeDto initialises connectionList EMPTY (never null) — rebuild whenever unwired;
        // buildConnections replaces the list wholesale, so a re-build is idempotent.
        if (connectionBuilder != null
                && org.apache.commons.collections4.CollectionUtils.isEmpty(
                        ctx.getCurrentNode().getConnectionList())
                && graphNodes != null && graphEdges != null) {
            connectionBuilder.buildConnections(ctx.getCurrentNode(), graphNodes, graphEdges);
        }
    }

    /**
     * The node's produced outputs with the operator's saved apply-inputs OUTPUT edits laid over
     * them: an edited port's saved message replaces what the node produced for that port, so a
     * Retry / Replay hands downstream — and records on the attempt's log row — what the operator
     * saved, as Skip &amp; Continue does. Ports the operator did not edit keep the node's own
     * values. The edited ports are exempt from the output port-schema filter for this fan-out
     * (transient ctx flag), exactly like the skip branch.
     */
    Map<String, Object> withOperatorOutputOverride(ProcessContextRef ref, ExecutionContext ctx,
                                                   String nodeId, Map<String, Object> produced) {
        List<InOutValueDto> outputOverride = loadOperatorOutputOverride(ref, ctx, nodeId);
        if (outputOverride == null || outputOverride.isEmpty()) {
            return produced;
        }
        Map<String, Object> operatorOutputs = toPortValueMap(outputOverride);
        if (operatorOutputs.isEmpty()) {
            return produced;
        }
        Map<String, Object> merged = produced == null ? new LinkedHashMap<>() : new LinkedHashMap<>(produced);
        merged.putAll(operatorOutputs);
        ctx.setOperatorOverrideOutputPorts(new LinkedHashSet<>(operatorOutputs.keySet()));
        return merged;
    }

    /**
     * The operator's output override as the {@code portValue → value} map
     * {@code finalizeOutputs} consumes (same shape a sandbox's {@code outputVariables} has).
     * Rows without a port name cannot bind to a port and are dropped.
     */
    private static Map<String, Object> toPortValueMap(List<InOutValueDto> outputOverride) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (InOutValueDto row : outputOverride) {
            if (row == null) {
                continue;
            }
            String port = StringUtils.firstNonEmpty(row.getPortValue(), row.getTargetPortValue());
            if (StringUtils.isNotEmpty(port)) {
                out.put(port, row.getValue());
            }
        }
        return out;
    }

    /** handleSkippedNode catch: same honesty as {@link #reportCaughtDuckbotFailure}. */
    void reportCaughtSkippedFailure(String dispatchId, String nodeId, ProcessContextRef ref,
                                    ExecutionContext ctx, List<NodeDto> nodes, List<EdgeDto> edges,
                                    long startedNanos, Exception e) {
        String message = honestEngineFailureMessage(e, ref, nodeId, nodes, edges,
                "skip short-circuit error: ");
        log.warn("scriptDispatchDelegate.skip-error node={} dispatchId={}: {}",
                nodeId, dispatchId, e.toString());
        reportBoundaryLog(ctx, nodeId, "FAILED", message, startedNanos,
                SandboxJobResponse.FailureKind.ENGINE);
        deliverFailureBestEffort(dispatchId, message);
    }

    /** The graph node with {@code nodeId}, or {@code null} when the snapshot does not carry it. */
    private static NodeDto findNode(List<NodeDto> graphNodes, String nodeId) {
        if (graphNodes == null || nodeId == null) {
            return null;
        }
        for (NodeDto node : graphNodes) {
            if (node != null && nodeId.equals(node.getId())) {
                return node;
            }
        }
        return null;
    }

    /**
     * Test seam: {@link DispatchResultCoordinator#deliverSuccess} then the same catch as
     * {@link #runDispatch}. Used to reproduce a resume-time Activiti Unknown-property throw.
     */
    void resumeSuccessOrReportFailure(String dispatchId, String nodeId, ProcessContextRef ref,
                                      List<NodeDto> nodes, List<EdgeDto> edges,
                                      Map<String, Object> engineVars, Map<String, Object> routeTokens) {
        try {
            resultCoordinator.deliverSuccess(dispatchId, engineVars, routeTokens);
        } catch (Exception e) {
            reportCaughtDispatchFailure(dispatchId, nodeId, ref, nodes, edges, e);
        }
    }

    /**
     * Test seam: same honesty as {@link #handleDuckbotBoundary}'s catch when deliverSuccess throws.
     */
    void resumeDuckbotSuccessOrReportFailure(String dispatchId, String nodeId, ProcessContextRef ref,
                                             ExecutionContext ctx, List<NodeDto> nodes, List<EdgeDto> edges,
                                             Map<String, Object> engineVars, Map<String, Object> routeTokens) {
        long startedNanos = System.nanoTime();
        try {
            resultCoordinator.deliverSuccess(dispatchId, engineVars, routeTokens);
        } catch (Exception e) {
            reportCaughtDuckbotFailure(dispatchId, nodeId, ref, ctx, nodes, edges, startedNanos, e);
        }
    }

    /** runDispatch outer catch: honest ENGINE message on emitTerminalFailureLog + deliverFailure. */
    void reportCaughtDispatchFailure(String dispatchId, String nodeId, ProcessContextRef ref,
                                     List<NodeDto> nodes, List<EdgeDto> edges, Exception e) {
        reportCaughtDispatchFailure(dispatchId, nodeId, ref, nodes, edges, e, false);
    }

    void reportCaughtDispatchFailure(String dispatchId, String nodeId, ProcessContextRef ref,
                                     List<NodeDto> nodes, List<EdgeDto> edges, Exception e,
                                     boolean retryAttempt) {
        String message = honestEngineFailureMessage(e, ref, nodeId, nodes, edges, "dispatch error: ");
        log.warn("scriptDispatchDelegate.dispatch-error node={} dispatchId={}: {}",
                nodeId, dispatchId, e.toString());
        String wfeId = ref == null ? null : ref.getWorkflowExecutionId();
        emitTerminalFailureLog(wfeId, nodeId, null, SandboxJobResponse.FailureKind.ENGINE, message, retryAttempt);
        if (jobQueue != null) {
            jobQueue.finish(dispatchId, JobState.FAILED, -1, "", "", message, null);
        }
        deliverFailureBestEffort(dispatchId, message);
    }

    /** handleDuckbotBoundary catch: same honest message on reportBoundaryLog + deliverFailure. */
    void reportCaughtDuckbotFailure(String dispatchId, String nodeId, ProcessContextRef ref,
                                    ExecutionContext ctx, List<NodeDto> nodes, List<EdgeDto> edges,
                                    long startedNanos, Exception e) {
        String message = honestEngineFailureMessage(e, ref, nodeId, nodes, edges,
                "duckbot boundary error: ");
        log.warn("scriptDispatchDelegate.duckbot-boundary-error node={} dispatchId={}: {}",
                nodeId, dispatchId, e.toString());
        reportBoundaryLog(ctx, nodeId, "FAILED", message, startedNanos,
                SandboxJobResponse.FailureKind.ENGINE);
        deliverFailureBestEffort(dispatchId, message);
    }

    private String honestEngineFailureMessage(Exception e, ProcessContextRef ref, String nodeId,
                                              List<NodeDto> nodes, List<EdgeDto> edges,
                                              String genericPrefix) {
        String wfeId = ref == null ? null : ref.getWorkflowExecutionId();
        return GatewayRoutingFailureMessages.message(
                e, wfeId, nodeId, nodes, edges, recorderOrNull(), genericPrefix);
    }

    private GatewayDecisionRecorder recorderOrNull() {
        try {
            return decisionRecorder == null ? null : decisionRecorder.getIfAvailable();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private void deliverFailureBestEffort(String dispatchId, String message) {
        try {
            resultCoordinator.deliverFailure(dispatchId, SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    message, SandboxJobResponse.FailureKind.ENGINE.name(), null);
        } catch (Exception ignore) {
            // token stays parked → boot hydration / orphan sweeper
        }
    }

    /**
     * Audit/log mirror for a node that resolved WITHOUT a sandbox (Duckbot start/end). Carries the
     * node's input/output lists so the message + files that crossed the boundary are recoverable from
     * the run's log trail. No container, so no exit code, stdout or language. Never throws.
     */
    private void reportBoundaryLog(ExecutionContext ctx, String nodeId, String status,
                                   String errorMessage, long startedNanos,
                                   SandboxJobResponse.FailureKind failureKind) {
        if (sandboxLogCallback == null || ctx == null
                || isSuppressedByCancellation(ctx.getWorkflowExecutionId(), nodeId)) {
            return;
        }
        try {
            Object input = null;
            Object output = null;
            String nodeName = null;
            NodeDto node = ctx.getCurrentNode();
            if (node != null) {
                // Never push inline file bytes to the control plane sandbox-logs — a 5 MiB GIF base64 times out the
                // channel HTTP client and trips the control plane body limit (status 400).
                input = redactInputPayloads(node.getInputList());
                output = redactOutputPayloads(node.getOutputList());
                if (node.getData() != null) {
                    nodeName = node.getData().getLabel();
                }
            }
            sandboxLogCallback.report(new SandboxLogCallback.SandboxLogRecord(
                    ctx.getWorkflowExecutionId(), nodeId, nodeName, status,
                    null, (System.nanoTime() - startedNanos) / 1_000_000L, null, null,
                    null, null, errorMessage, input, output, null, null,
                    (failureKind == null ? SandboxJobResponse.FailureKind.NONE : failureKind).name(),
                    egressPostureNote()));
        } catch (Exception e) {
            log.debug("scriptDispatchDelegate: boundary-log mirror skipped node={} ({})", nodeId, e.toString());
        }
    }

    /**
     * Terminal, redacted, IO-less failure log for a dispatch that never produced a real sandbox
     * result (context vanished, unresolved language, or any daemon-side exception caught by {@code
     * runDispatch}'s outer catch). Deliberately carries NO {@code input}/{@code output}/{@code
     * stdout}/{@code stderr} — a system-level failure must never surface (or look like) normal node
     * output. {@code errorMessage} MUST already be a non-PHI, redacted string (an exception class
     * name, or an already-{@code Redactor}-scrubbed diagnostic) — this method does no redaction of
     * its own. Never throws; suppressed for a run the {@link CancelledRunGuard} already marked
     * cancelled (a late system-failure log for a cancelled run is exactly the kind of stale report the
     * guard exists to drop).
     */
    private void emitTerminalFailureLog(String workflowExecutionId, String nodeId, String nodeName,
                                        SandboxJobResponse.FailureKind failureKind, String errorMessage,
                                        boolean retryAttempt) {
        if (sandboxLogCallback == null || isSuppressedByCancellation(workflowExecutionId, nodeId)) {
            return;
        }
        try {
            sandboxLogCallback.report(new SandboxLogCallback.SandboxLogRecord(
                    workflowExecutionId, nodeId, nodeName, "FAILED",
                    null, null, null, null,
                    null, null, errorMessage, null, null, null, null, failureKind.name(), null,
                    null, retryAttempt ? Boolean.TRUE : null));
        } catch (Exception e) {
            log.debug("scriptDispatchDelegate: terminal-failure-log mirror skipped node={} ({})",
                    nodeId, e.toString());
        }
    }

    /**
     * Sub-PI log-mode inheritance, asserted at dispatch time. The Activiti PROCESS_STARTED listener
     * ({@code RunStatusEventBridge}) already inherits the parent's mode, but it runs on the engine
     * thread and can fire before {@code HelperExecuteService}'s register call completes; this
     * re-asserts the parent mode from the dispatch thread, where the child → parent link is
     * guaranteed durable. Best-effort, never throws.
     */
    private void inheritParentLogModeIfSubPiChild(ExecutionContext ctx) {
        try {
            if (runLogModeRegistryProvider == null || ctx == null) {
                return;
            }
            RunLogModeRegistry registry = runLogModeRegistryProvider.getIfAvailable();
            if (registry == null) {
                return;
            }
            String childWfeId = ctx.getWorkflowExecutionId();
            if (childWfeId == null || childWfeId.isBlank()) {
                return;
            }
            com.studioquack.sqs.runtime.SubProcessCompletionRegistry subReg =
                    subProcessCompletionRegistry();
            if (subReg == null || !subReg.isSubPiChild(childWfeId)) {
                return;
            }
            String parent = subReg.resolveReportableRunId(childWfeId);
            if (parent == null || parent.equals(childWfeId)) {
                return;
            }
            com.studioquack.sqs.status.RunLogMode parentMode = registry.modeOf(parent);
            if (registry.modeOf(childWfeId) != parentMode) {
                registry.override(childWfeId, parentMode);
                log.info("scriptDispatchDelegate: sub-PI child run={} log-mode corrected to parent mode={} "
                        + "(parent={}) — provider node rows will mirror live", childWfeId, parentMode, parent);
            }
        } catch (Exception e) {
            log.debug("scriptDispatchDelegate: sub-PI log-mode inheritance skipped ({})", e.toString());
        }
    }

    /**
 * The run a dispatch's sandbox logs are persisted under. A function-port sub-PI child's rows are
 * posted under its top-level run (the only run the control plane knows), so a list-jobs row naming the
 * child id would open an empty log view.
 */
    String logRunId(String workflowExecutionId) {
        com.studioquack.sqs.runtime.SubProcessCompletionRegistry subReg = subProcessCompletionRegistry();
        return subReg == null ? workflowExecutionId : subReg.resolveReportableRunId(workflowExecutionId);
    }

    /** Lazy registry probe — null in slice tests that never exercise sub-PI dispatch. */
    private com.studioquack.sqs.runtime.SubProcessCompletionRegistry subProcessCompletionRegistry() {
        try {
            return subProcessRegistryProvider == null ? null : subProcessRegistryProvider.getIfAvailable();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** True when {@code workflowExecutionId} was recently cancelled — a late log for it must be dropped. */
    private boolean isSuppressedByCancellation(String workflowExecutionId, String nodeId) {
        if (cancelledRunGuard != null && cancelledRunGuard.isCancelled(workflowExecutionId)) {
            log.debug("scriptDispatchDelegate: suppressing late sandbox-log for cancelled run wfe={} node={}",
                    workflowExecutionId, nodeId);
            return true;
        }
        return false;
    }

    /** The current node's authored label, or null (diagnostics on a failure path with no full record). */
    private String currentNodeName(ExecutionContext ctx) {
        if (ctx == null || ctx.getCurrentNode() == null || ctx.getCurrentNode().getData() == null) {
            return null;
        }
        return ctx.getCurrentNode().getData().getLabel();
    }

    /**
 * Best-effort mirror of one node's sandbox result to the control plane run index's sandbox-log store. Runs
 * after {@code finalizeOutputs}, so the current node carries its fanned-out input/output lists for
 * the Monitoring "echoes" tab. Never throws — a log push must not fail the dispatch.
 */
    /**
     * Best-effort "sandbox started" mirror emitted just before the sandbox runs, so the live
     * execution animation can flip this node to RUNNING instead of jumping straight to success. The
     * callback pushes it in parallel (non-blocking); never throws — a started mirror must not delay or
     * fail the dispatch.
     */
    private void reportSandboxStarted(ExecutionContext ctx, String nodeId) {
        if (sandboxLogCallback == null || ctx == null
                || isSuppressedByCancellation(ctx.getWorkflowExecutionId(), nodeId)) {
            return;
        }
        try {
            String nodeName = null;
            NodeDto node = ctx.getCurrentNode();
            if (node != null && node.getData() != null) {
                nodeName = node.getData().getLabel();
            }
            sandboxLogCallback.reportStarted(new SandboxLogCallback.SandboxStartRecord(
                    ctx.getWorkflowExecutionId(), nodeId, nodeName, ctx.findCurrentNodeLanguage()));
        } catch (Exception e) {
            log.debug("scriptDispatchDelegate: sandbox-started mirror skipped node={} ({})", nodeId, e.toString());
        }
    }

    private void reportSandboxLog(ExecutionContext ctx, String nodeId, SandboxJobResponse response,
                                  boolean retryAttempt) {
        if (sandboxLogCallback == null || ctx == null
                || isSuppressedByCancellation(ctx.getWorkflowExecutionId(), nodeId)) {
            return;
        }
        try {
            Object input = null;
            Object output = null;
            String nodeName = null;
            NodeDto node = ctx.getCurrentNode();
            if (node != null) {
                input = redactInputPayloads(node.getInputList());
                output = redactOutputPayloads(node.getOutputList());
                if (node.getData() != null) {
                    nodeName = node.getData().getLabel();
                }
            }
            Object appliedInputs = response.appliedInputs() == null || response.appliedInputs().isEmpty()
                    ? null : response.appliedInputs();
            java.util.List<String> unknownInputParameters = ctx.getDroppedInputParameters();
            if (unknownInputParameters != null && unknownInputParameters.isEmpty()) {
                unknownInputParameters = null;
            }
            // Production checkpoint: proves a sandbox-log row was PRODUCED for this node (the
            // channel callback then logs whether the control plane stored it). Shape only — no values.
            // The list SIZES are the tell for "row posted but the I/O tabs are empty".
            log.info("sandbox-log.report run={} node={} status={} retry={} inputType={} outputType={} "
                            + "inputRows={} outputRows={}",
                    ctx.getWorkflowExecutionId(), nodeId,
                    response.ok() ? "COMPLETED" : "FAILED",
                    retryAttempt,
                    input == null ? "null" : input.getClass().getSimpleName(),
                    output == null ? "null" : output.getClass().getSimpleName(),
                    input instanceof java.util.List<?> il ? il.size() : -1,
                    output instanceof java.util.List<?> ol ? ol.size() : -1);
            sandboxLogCallback.report(new SandboxLogCallback.SandboxLogRecord(
                    ctx.getWorkflowExecutionId(),
                    nodeId,
                    nodeName,
                    response.ok() ? "COMPLETED" : "FAILED",
                    response.exitCode(),
                    response.wallMs(),
                    ctx.findCurrentNodeLanguage(),
                    response.containerId(),
                    response.stdout(),
                    response.stderr(),
                    response.errorMessage(),
                    input,
                    output,
                    appliedInputs,
                    unknownInputParameters,
                    response.failureKind().name(),
                    egressPostureNote(),
                    null,
                    retryAttempt ? Boolean.TRUE : null));
        } catch (Exception e) {
            log.debug("scriptDispatchDelegate: sandbox-log mirror skipped node={} ({})", nodeId, e.toString());
        }
    }

    /**
 * (item 0) — the run-log sentence for a node whose container started while overlay-egress
 * enforcement was DEGRADED on this host (e.g. self-hosted macOS/Windows, where host iptables
 * cannot be programmed). Delegates to {@link SandboxEgressManager#degradedRunLogNote};
 * best-effort: a readiness read failure must not break the log mirror.
 */
    private String egressPostureNote() {
        try {
            return sandboxEgressManager == null ? null : sandboxEgressManager.degradedRunLogNote();
        } catch (RuntimeException e) {
            log.debug("scriptDispatchDelegate: egress posture note skipped ({})", e.toString());
            return null;
        }
    }

    /** The current node's fanned-out outputs (empty when the node was not resolved). */
    private List<InOutValueDto> currentOutputs(ExecutionContext ctx) {
        if (ctx.getCurrentNode() == null || ctx.getCurrentNode().getOutputList() == null) {
            return List.of();
        }
        return ctx.getCurrentNode().getOutputList();
    }

    /**
     * Persists the node's fanned-out outputs onto its dispatch row so downstream nodes can read them.
     *
     * <p><b>Elevated, not swallowed.</b> A failed {@code saveOutputs} is a lost-output system failure:
     * silently continuing would report the node COMPLETED/deliver success while the very data
     * downstream nodes depend on never made it to the sealed store. This RETHROWS so {@code
     * runDispatch}'s outer catch classifies it as an ENGINE failure and routes the run through the
     * error gate instead — never through a success path with quietly-missing output.
     */
    private void persistOutputs(ProcessContextRef ref, ExecutionContext ctx) {
        if (ref == null || ref.getCurrentNodeRefId() == null || ctx.getCurrentNode() == null) {
            return;
        }
        java.util.List<InOutValueDto> outputs = ctx.getCurrentNode().getOutputList();
        // Data-flow checkpoint (shape only): proves WHAT was sealed onto the dispatch row — the
        // row the sub-PI harvest + downstream input resolution read back. count=0 here with a
        // non-empty sandbox response means the pipeline dropped every port upstream of the store.
        log.info("persist.outputs node={} ref={} exec={} count={}",
                ctx.getCurrentNodeIdSafe(), ref.getCurrentNodeRefId(),
                ref.getExecutionId(), outputs == null ? -1 : outputs.size());
        contextStore.saveOutputs(ref, outputs);
    }

    private ProcessContextRef readRef(DelegateExecution execution) {
        Object v = execution.getVariable(EngineVariableNames.STUDIOQUACK_REF);
        if (v instanceof ProcessContextRef ref) {
            return ref;
        }
        if (v instanceof Map<?, ?> map) {
            return objectMapper.convertValue(map, ProcessContextRef.class);
        }
        return null;
    }

    /**
 * Merge the function-entry payload (carried on the ref blob, addressed to the sub-process's first
 * node) with the persisted upstream outputs targeting this node. The entry payload is applied only
 * when {@code nodeId} equals {@code refNodeId} (the node the ref was minted for); later sub-process
 * nodes fall back to upstream-only. Dedup by the input wire's identity keeps the upstream (latest
 * persisted) value authoritative on a key overlap. Ported from the control plane ScriptDispatchDelegate.
 *
 * <p><b> repair override ({@code operatorOverrideInputs}).</b> The sealed
 * {@code apply-inputs} override stored for {@code (workflowExecutionId, nodeId)} folds LAST, so
 * the operator's edited values WIN over upstream on a binding-key overlap and newly added
 * parameters land as new keys — on every dispatch of this node in this run, however it was
 * reached (retry, replay). An EMPTY override is the operator clearing the inbound list and
 * returns an empty merge; {@code null} means no override was stored. Rows addressed to another
 * node stay filtered even from an override (defence in depth).
 *
 * <p>Static, no instance state: the single merge rule, also used by the {@code user.task}
 * inbound ({@code UserTaskIo}), which runs no dispatch but must see the same inputs.
 */
    public static List<InOutValueDto> mergeNodeInputs(String nodeId, String refNodeId,
                                               List<InOutValueDto> carriedEntryInputs,
                                               List<InOutValueDto> upstreamInputs,
                                               List<InOutValueDto> operatorOverrideInputs) {
        if (operatorOverrideInputs != null && operatorOverrideInputs.isEmpty()) {
            // The operator explicitly cleared this node's inbound list — honour the clear over
            // every carried/upstream row (otherwise the pre-repair inputs silently resurrect).
            return new ArrayList<>();
        }
        LinkedHashMap<String, InOutValueDto> merged = new LinkedHashMap<>();
        boolean isEntryNode = nodeId != null && nodeId.equalsIgnoreCase(refNodeId);
        List<InOutValueDto> carried = isEntryNode
                ? filterAddressedTo(carriedEntryInputs, nodeId)
                : List.of();
        List<InOutValueDto> upstream = upstreamInputs == null ? List.of() : upstreamInputs;
        // Fold order decides the overlap winner: the LAST writer on a binding key is kept.
        for (InOutValueDto in : carried) {
            if (in != null) {
                merged.put(bindingKey(in), in);
            }
        }
        for (InOutValueDto in : upstream) {
            if (in != null) {
                // normal path: upstream wins (latest persisted).
                merged.put(bindingKey(in), in);
            }
        }
        for (InOutValueDto in : filterAddressedTo(operatorOverrideInputs, nodeId)) {
            if (in != null) {
                // operator repair: the override always wins and adds new keys.
                // Mark the row as operator-supplied (transient — never persisted): the inbound
                // port-schema filter exempts exactly these rows, so a field the operator added
                // reaches the node and the record instead of being stripped as unknown.
                in.setOperatorOverride(true);
                merged.put(bindingKey(in), in);
            }
        }
        return new ArrayList<>(merged.values());
    }

    /**
     * The operator's sealed input override for {@code (workflowExecutionId, nodeId)}, or
     * {@code null} when no store is wired (slice tests) or no override was stored. Never logs
     * values (PHI); a corrupt/tampered row propagates so the dispatch fails honestly rather than
     * silently re-running the pre-repair inputs.
     */
    private List<InOutValueDto> loadOperatorOverride(ProcessContextRef ref, ExecutionContext ctx,
                                                     String nodeId) {
        if (operatorInputOverrideStore == null) {
            return null;
        }
        String runId = ref != null && StringUtils.isNotBlank(ref.getWorkflowExecutionId())
                ? ref.getWorkflowExecutionId()
                : (ctx == null ? null : ctx.getWorkflowExecutionId());
        if (runId == null || nodeId == null) {
            return null;
        }
        return operatorInputOverrideStore.loadOverride(runId, nodeId).orElse(null);
    }

    /**
     * The operator's sealed OUTPUT override for {@code (workflowExecutionId, nodeId)}, or
     * {@code null} when no store is wired (slice tests) or none was stored. Read by the skip
     * short-circuit (the whole output) and by a successful re-run, where the edited ports replace
     * what the node produced. Never logs values (PHI).
     */
    private List<InOutValueDto> loadOperatorOutputOverride(ProcessContextRef ref, ExecutionContext ctx,
                                                           String nodeId) {
        if (operatorInputOverrideStore == null) {
            return null;
        }
        String runId = ref != null && StringUtils.isNotBlank(ref.getWorkflowExecutionId())
                ? ref.getWorkflowExecutionId()
                : (ctx == null ? null : ctx.getWorkflowExecutionId());
        if (runId == null || nodeId == null) {
            return null;
        }
        return operatorInputOverrideStore.loadOutputOverride(runId, nodeId).orElse(null);
    }

    /**
     * Keeps only carried payload items addressed to {@code nodeId} (or unaddressed — the daemon
     * leaves {@code targetNodeId} blank on the first hop into a sub-workflow) so a shared ref
     * never leaks one node's payload onto another.
     */
    private static List<InOutValueDto> filterAddressedTo(List<InOutValueDto> carriedEntryInputs,
                                                         String nodeId) {
        if (carriedEntryInputs == null || carriedEntryInputs.isEmpty()) {
            return List.of();
        }
        List<InOutValueDto> out = new ArrayList<>(carriedEntryInputs.size());
        for (InOutValueDto in : carriedEntryInputs) {
            if (in == null) {
                continue;
            }
            String target = in.getTargetNodeId();
            if (target != null && !target.isBlank() && !target.equalsIgnoreCase(nodeId)) {
                continue;
            }
            out.add(in);
        }
        return out;
    }

    /**
     * Appends DuckTape Add-Node foreign-source messages to the merged inputs. Existing wires win
     * on key overlap: a node that IS directly connected must not be duplicated by its pinned tile.
     */
    private List<InOutValueDto> mergeForeignInputs(List<InOutValueDto> merged, List<InOutValueDto> foreignInputs) {
        if (foreignInputs == null || foreignInputs.isEmpty()) {
            return merged;
        }
        LinkedHashMap<String, InOutValueDto> byKey = new LinkedHashMap<>();
        if (merged != null) {
            for (InOutValueDto in : merged) {
                if (in != null) {
                    byKey.put(bindingKey(in), in);
                }
            }
        }
        for (InOutValueDto in : foreignInputs) {
            if (in != null) {
                byKey.putIfAbsent(bindingKey(in), in);
            }
        }
        return new ArrayList<>(byKey.values());
    }

    /** Single definition lives on the resolver so the pipeline's pinned fold names rows identically. */
    private static String bindingKey(InOutValueDto dto) {
        return ForeignEdgeInputResolver.bindingKey(dto);
    }

    /**
 * resolve {@code $(…)} refs in the current node's configuration-parameter
 * string values on this dispatch-private in-memory snapshot. Runs after inbound /
 * edge-transform prep and <b>before</b> secret-binding verification + JIT secret
 * materialisation. {@code $secretRef} is left untouched by {@link ExpressionResolver}.
 * Bounds (max 64 refs / 2048 chars per value) are enforced by the resolver.
 */
    private void resolveExpressionRefsInConfig(ExecutionContext ctx) {
        ConfigParameterExpressionResolver.resolveInCurrentNode(ctx, expressionResolver);
    }

    private Map<String, Object> toContextMap(ExecutionContext ctx) {
        try {
            return objectMapper.convertValue(ctx, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return Map.of();
        }
    }

    /**
     * The sandbox-facing port map as a PRIVATE snapshot. {@code prepared.inputVariables()} is built
     * from {@code currentNode.inputList} by reference — each port value is the very Map/List the
     * persisted {@link ExecutionContext} holds — so a shallow {@code new LinkedHashMap<>(...)} copy
     * shares every nested block with the live context. The secret resolver, the ContentBlock
     * hydrator (multi-MB {@code data}) and the file materialiser ({@code fileId} → in-container
     * path) all mutate in place; through the shared references they were rewriting the persisted
     * context, the sandbox-log {@code input} and every value carried to downstream nodes. Port
     * values are JSON-shaped and are deep-copied; reserved {@code __sq_*} entries carry typed
     * directive DTOs that later stages read as-is, so those stay by reference.
     */
    Map<String, Object> privateInputSnapshot(Map<String, Object> inputVariables) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (inputVariables == null) {
            return out;
        }
        for (Map.Entry<String, Object> e : inputVariables.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();
            boolean reserved = key != null && key.startsWith("__sq_");
            out.put(key, reserved ? value : deepCopyJsonValue(value));
        }
        return out;
    }

    /** Deep copy of a JSON-shaped value (Map/List → fresh LinkedHashMap/ArrayList); scalars as-is. */
    private Object deepCopyJsonValue(Object value) {
        if (!(value instanceof Map<?, ?>) && !(value instanceof List<?>)) {
            return value;
        }
        try {
            return objectMapper.convertValue(value, Object.class);
        } catch (Exception e) {
            // Unconvertible value: keep the reference rather than drop the port. Logged at debug —
            // this is a shape surprise, not a run failure.
            log.debug("scriptDispatchDelegate: input value deep-copy skipped ({})", e.toString());
            return value;
        }
    }

    /**
     * Deep-copy {@code payload} and strip ContentBlock {@code data} so sandbox-log mirrors stay
     * small. Never mutates the live execution context. Never turns a non-null payload into
     * {@code null} — a failed Jackson convert falls back inside {@link SandboxLogIoRedactor}.
     */
    private Object redactInputPayloads(Object payload) {
        return SandboxLogIoRedactor.redactInput(objectMapper, payload);
    }

    private Object redactOutputPayloads(Object payload) {
        return SandboxLogIoRedactor.redactOutput(objectMapper, payload);
    }

    /**
     * Deploy-store key for secret-binding lookups — matches {@code DeployCommandHandler} /
     * {@link com.studioquack.sqs.command.StartRunCommandHandler#hydrateGraphFromDeploy}.
     */
    private String deployArtifactKey(String workflowId, String processDefinitionId) {
        if (StringUtils.isBlank(workflowId)) {
            return workflowId;
        }
        SqsEngine engine = sqsEngineProvider == null ? null : sqsEngineProvider.getIfAvailable();
        if (engine == null || StringUtils.isBlank(processDefinitionId)) {
            return workflowId;
        }
        BpmnModel model = engine.readDeployedModel(processDefinitionId);
        if (model == null || model.getMainProcess() == null) {
            return workflowId;
        }
        String processKey = model.getMainProcess().getId();
        return workflowId + WorkflowGraph.namespaceSuffixOf(processKey);
    }

    private static String stripPrefix(String activityId) {
        if (activityId != null && activityId.startsWith(NODE_ACTIVITY_PREFIX)) {
            return activityId.substring(NODE_ACTIVITY_PREFIX.length());
        }
        return activityId;
    }

    /** @return how many times this delegate has been invoked (diagnostics/tests). */
    public long getInvocationCount() {
        return invocationCount.get();
    }
}
