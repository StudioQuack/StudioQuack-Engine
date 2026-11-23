package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.BeRuntimeEpoch;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager;
import com.studioquack.sqs.engine.listener.BpmnNodeLogPayloads;
import com.studioquack.sqs.status.RunLogBatchFlusher;
import com.studioquack.sqs.status.RunLogModeRegistry;
import com.studioquack.sqs.status.RunStatusCallback;
import com.studioquack.sqs.status.SandboxLogCallback;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles the BE-pushed {@code start-run} command: start a process instance for an already-deployed workflow,
 * correlated by {@code businessKey == workflowExecutionId}, seeded with REF-ONLY variables, and report a
 * RUNNING status callback. The other half of the channel→engine seam (after {@link DeployCommandHandler}).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StartRunCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "start-run";

    /**
     * Bound on waiting for the {@code deploy} this run depends on. The BE pushes deploy then start-run back to
     * back, but the overlay listener and the heartbeat client each run their own serial lane, so when the two
     * commands arrive on different transports start-run can execute while its deploy is still running on the other
     * lane.
     */
    static final Duration DEPLOY_WAIT = Duration.ofSeconds(30);
    private static final long DEPLOY_POLL_MS = 250;
    /**
     * Package-private so slice tests can shorten the bound.
     */
    Duration deployWait = DEPLOY_WAIT;

    private final ObjectMapper objectMapper;
    private final SqsEngine sqsEngine;
    private final ExecutionContextStore executionContextStore;
    private final BeRuntimeEpoch beRuntimeEpoch;
    private final RunStatusCallback runStatusCallback;
    private final WorkflowDeploymentService workflowDeploymentService;
    private final DeployedArtifactStore artifactStore;
    private final com.studioquack.sqs.files.StartRunFileIngest startRunFileIngest;
    private final RunLogModeRegistry runLogModeRegistry;
    /**
     * Null in slice tests that boot without the library manager.
     */
    @Nullable
    private final BuiltinNodeLibraryManager nodeLibraryManager;

    /**
     * Null when the guard bean is absent (slice tests that {@code new} this handler).
     */
    @Nullable
    private com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;

    /**
     * Optional: records + mirrors startup failures (e.g. node library / Podman down) into sandbox_log.
     */
    @Nullable
    private SandboxLogCallback sandboxLogCallback;

    /**
     * Optional: flushes BATCH startup-failure rows immediately since Activiti never started a PI.
     */
    @Nullable
    private RunLogBatchFlusher runLogBatchFlusher;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setExecutionHaltGuard(com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard) {
        this.executionHaltGuard = executionHaltGuard;
    }

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setSandboxLogCallback(SandboxLogCallback sandboxLogCallback) {
        this.sandboxLogCallback = sandboxLogCallback;
    }

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setRunLogBatchFlusher(RunLogBatchFlusher runLogBatchFlusher) {
        this.runLogBatchFlusher = runLogBatchFlusher;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        StartRunCommandPayload payload;
        try {
            payload = objectMapper.readValue(command.payload(), StartRunCommandPayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("start-run: malformed payload: " + message(parse));
        }
        if (payload == null || !payload.isStartable()) {
            return DaemonCommandOutcome.failed(
                    "start-run: payload needs workflowExecutionId + processKey or processDefinitionId");
        }
        String haltRefusal = executionHaltGuard == null ? null : executionHaltGuard.refusal();
        if (haltRefusal != null) {
            return DaemonCommandOutcome.failed(haltRefusal);
        }

        String workflowExecutionId = payload.workflowExecutionId();
        // Classify the run's log policy BEFORE the engine starts, so the first PROCESS_STARTED /
        // node dispatch (and any startup-failure sandbox log) already sees it.
        // The BE-stamped mode (Designer ⇒ STREAM, every other trigger ⇒ BATCH) takes precedence.
        runLogModeRegistry.registerIfAbsent(workflowExecutionId, payload.logModeOrDefault());
        try {
            if (!awaitDeployedDefinition(payload)) {
                String err = "start-run: workflow " + payload.processKey()
                        + " is not deployed on this engine — its deploy did not complete within "
                        + deployWait.toSeconds() + "s";
                recordStartupFailure(workflowExecutionId, "ENGINE", err);
                return DaemonCommandOutcome.failed(err);
            }
            // Pure engine-native BPMN graphs (e.g. none-start -> none-end) execute entirely inside
            // the embedded Activiti JVM and never launch a container, so they do not require the
            // built-in container library. All other workflows wait for the library and log/publish
            // A FAILED sandbox-log record if it is unavailable (Podman down / build failed).
            if (nodeLibraryManager != null && !nodeLibraryManager.isUsable() && !isPureEngineNativeBpmn(payload)) {
                nodeLibraryManager.awaitUsable(BuiltinNodeLibraryManager.START_RUN_WAIT);
                String libraryBlock = nodeLibraryManager.startRunBlockReason();
                if (libraryBlock != null) {
                    String err = "start-run: " + libraryBlock;
                    recordStartupFailure(workflowExecutionId, "INFRA", err);
                    return DaemonCommandOutcome.failed(err);
                }
            }
            // Reinstall/data-dir-cleanup self-heal: the BE ships deploy-ON-CHANGE, so when the graph is
            // unchanged it pushes ONLY start-run. A daemon that lost its Activiti definition (a fresh local store)
            // would then fail with "no processes deployed with key...". Because start-run always carries
            // the full graph in seedContext, we can deploy it locally here before starting — keeping the
            // BE on deploy-on-change without the post-reinstall failure. No-op on the healthy path.
            ensureDefinitionDeployed(payload);

            Map<String, Object> refOnlyVars = buildRefOnlyVars(payload, workflowExecutionId);

            // businessKey == workflowExecutionId so the status callback + monitoring correlate the run.
            ProcessInstance instance = payload.hasPinnedDefinition()
                    ? sqsEngine.startByDefinitionId(payload.processDefinitionId(), workflowExecutionId, refOnlyVars)
                    : sqsEngine.startLatest(payload.processKey(), workflowExecutionId, refOnlyVars);

            // RUNNING at the authoritative moment: an instance now exists for the run.
            reportRunning(workflowExecutionId, instance.getId());

            StartRunResult result = new StartRunResult(
                    instance.getId(), workflowExecutionId, instance.getProcessDefinitionId());
            log.info("start-run.done workflowExecutionId={} processInstanceId={} definitionId={}",
                    workflowExecutionId, instance.getId(), instance.getProcessDefinitionId());
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("start-run.failed workflowExecutionId={}: {}", workflowExecutionId, ex.toString());
            String err = "start-run failed: " + message(ex);
            recordStartupFailure(workflowExecutionId, "ENGINE", err);
            return DaemonCommandOutcome.failed(err);
        }
    }

    /**
     * True only when the deployed (or seed-carried) graph has at least one node and EVERY node is an engine-native
     * BPMN element with no container/script/docling job. Such graphs never touch Podman or the built-in node
     * library image.
     */
    private boolean isPureEngineNativeBpmn(StartRunCommandPayload payload) {
        try {
            java.util.List<com.studioquack.common.dto.flow.NodeDto> nodes = null;
            ExecutionContext seed = payload.seedContext();
            if (seed != null && seed.getNodes() != null && !seed.getNodes().isEmpty()) {
                nodes = seed.getNodes();
            } else if (seed != null && StringUtils.isNotBlank(seed.getWorkflowId())) {
                String artifactKey = seed.getWorkflowId()
                        + com.studioquack.sqs.engine.WorkflowGraph.namespaceSuffixOf(payload.processKey());
                var deployed = artifactStore.loadDeployedGraph(artifactKey);
                if (deployed.isPresent() && deployed.get().nodes() != null && !deployed.get().nodes().isEmpty()) {
                    nodes = deployed.get().nodes();
                }
            }
            if (nodes == null || nodes.isEmpty()) {
                return false;
            }
            for (var node : nodes) {
                if (node == null || node.getData() == null) {
                    return false;
                }
                var data = node.getData();
                if (!BpmnNodeLogPayloads.isEngineNativeBpmnType(data.getBpmnType())) {
                    return false;
                }
                if (StringUtils.isNotBlank(data.getJobType())) {
                    return false;
                }
                if (StringUtils.isNotBlank(node.findCurrentNodeScript())) {
                    return false;
                }
            }
            return true;
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    /**
     * Persists a terminal FAILED sandbox-log record locally and publishes it to the control plane whenever a run
     * fails before/during engine startup (e.g. Podman stopped, node library broken, missing deploy, engine
     * exception). Best-effort — never throws.
     */
    private void recordStartupFailure(String workflowExecutionId, String failureKind, String errorMessage) {
        if (StringUtils.isBlank(workflowExecutionId)) {
            return;
        }
        try {
            if (sandboxLogCallback != null) {
                sandboxLogCallback.report(new SandboxLogCallback.SandboxLogRecord(
                        workflowExecutionId,
                        "__start__",
                        "Start Run",
                        "FAILED",
                        null,
                        0L,
                        "engine",
                        null,
                        null,
                        errorMessage,
                        errorMessage,
                        null,
                        null,
                        null,
                        null,
                        failureKind,
                        null));
            }
            if (runLogModeRegistry != null && runLogModeRegistry.isBatch(workflowExecutionId)
                    && runLogBatchFlusher != null) {
                runLogBatchFlusher.flush(workflowExecutionId);
            }
            if (runStatusCallback != null) {
                runStatusCallback.report(workflowExecutionId, RunStatusCallback.Status.FAILED, errorMessage);
            }
        } catch (Exception logEx) {
            log.debug("start-run: failed to record startup failure log for run={}: {}",
                    workflowExecutionId, logEx.toString());
        } finally {
            if (runLogModeRegistry != null) {
                runLogModeRegistry.remove(workflowExecutionId);
            }
        }
    }

    /**
     * Builds the REF-ONLY engine variable map: the caller's {@code seedVars}, plus the sealed
     * {@code studioquackRef} when a {@code seedContext} is supplied, plus the daemon boot epoch (orphan-rejection
     * parity with {@code WorkflowDeploymentService.startProcessInstance}). The {@link RefOnlyVariableGuard}
     * enforces the whitelist before anything crosses onto the engine.
     */
    private Map<String, Object> buildRefOnlyVars(StartRunCommandPayload payload, String workflowExecutionId) {
        Map<String, Object> vars = new HashMap<>();
        if (payload.seedVars() != null) {
            vars.putAll(payload.seedVars());
        }
        ExecutionContext seedContext = payload.seedContext();
        if (seedContext != null) {
            if (StringUtils.isBlank(seedContext.getWorkflowExecutionId())) {
                seedContext.setWorkflowExecutionId(workflowExecutionId);
            }
            // REF-only run: the graph ships once at deploy, not on every run. When the seedContext
            // carries only trigger params (no nodes), hydrate the deployed topology from the daemon's
            // own store so the sealed context the dispatch path reads is complete. If the seedContext
            // already carries the graph (transitional/legacy), keep it as-is (fallback).
            hydrateGraphFromDeploy(seedContext, payload.processKey());

            // Fresh seed maps win; deploy-stored maps are the fallback.
            // Only runs on the seedContext path.
            sealDeployVariables(seedContext, payload.processKey());
            // Chat attachments arrive with their bytes inline, because the daemon cannot read backend
            // move them into the sealed file store and leave handles BEFORE the snapshot below,
            // so multi-MB base64 never lands in the sealed execution context — which is written once
            // per run and re-read on every node hop.
            startRunFileIngest.ingest(workflowExecutionId, seedContext);
            ProcessContextRef ref = executionContextStore.snapshot(seedContext, null);
            vars.put(EngineVariableNames.STUDIOQUACK_REF, ref);
        }
        // Stamp the daemon startup epoch so an orphan activation from a previous boot is rejected.
        vars.put(BeRuntimeEpoch.VARIABLE_NAME, beRuntimeEpoch.current());
        // Fail-fast REF-ONLY: only studioquackRef + route__*/error/epoch tokens may reach the engine.
        RefOnlyVariableGuard.assertRefOnly(vars);
        return vars;
    }

    /**
     * Fills a graph-less {@code seedContext} (a REF-only run that carries only trigger params) with the topology
     * persisted at deploy, so the sealed context is self-describing for the dispatch path and
     * {@code studioquack.execute} sub-PIs. No-op when the seed already carries a graph (legacy / transitional) or
     * when nothing was persisted at deploy (then the self-heal / existing definition path applies).
     */
    private void hydrateGraphFromDeploy(ExecutionContext seedContext, String processKey) {
        boolean hasGraph = seedContext.getNodes() != null && !seedContext.getNodes().isEmpty();
        if (hasGraph || StringUtils.isBlank(seedContext.getWorkflowId())) {
            return;
        }
        // A Designer Run stored its graph under its own draft key, so hydrate from the namespace the
        // run is actually starting in — otherwise a draft run would execute the published graph.
        String artifactKey = seedContext.getWorkflowId()
                + com.studioquack.sqs.engine.WorkflowGraph.namespaceSuffixOf(processKey);
        artifactStore.loadDeployedGraph(artifactKey).ifPresent(graph -> {
            seedContext.setNodes(graph.nodes());
            seedContext.setEdges(graph.edges());
            log.debug("start-run.hydrate-graph workflowId={} nodes={} edges={} (REF-only run; graph from deploy)",
                    seedContext.getWorkflowId(),
                    graph.nodes() == null ? 0 : graph.nodes().size(),
                    graph.edges() == null ? 0 : graph.edges().size());
        });
    }

    /**
     * Overlays deploy-persisted variable scopes onto {@code seedContext} before the execution-context snapshot.
     * The BE stamps FRESH wf/project vars onto the start-run seedContext, so a non-empty seed map always wins; the
     * deploy-stored map is only the fallback for a seed that carries none.
     */
    private void sealDeployVariables(ExecutionContext seedContext, String processKey) {
        if (StringUtils.isBlank(seedContext.getWorkflowId())) {
            return;
        }
        String artifactKey = seedContext.getWorkflowId()
                + com.studioquack.sqs.engine.WorkflowGraph.namespaceSuffixOf(processKey);
        artifactStore.loadDeployVariables(artifactKey).ifPresent(scopes -> {
            if (seedContext.getWorkflowVariables() == null || seedContext.getWorkflowVariables().isEmpty()) {
                seedContext.setWorkflowVariables(scopes.globalParams());
            }
            if (seedContext.getProjectVariables() == null || seedContext.getProjectVariables().isEmpty()) {
                seedContext.setProjectVariables(scopes.projectVariables());
            }
            log.debug("start-run.seal-vars workflowId={} wfKeys={} projectKeys={} (seed authoritative, deploy fallback)",
                    seedContext.getWorkflowId(),
                    seedContext.getWorkflowVariables() == null ? 0 : seedContext.getWorkflowVariables().size(),
                    seedContext.getProjectVariables() == null ? 0 : seedContext.getProjectVariables().size());
        });
    }

    /**
     * Waits (bounded by {@link #deployWait}) for the deploy this run depends on to land: the process definition,
     * and for a Designer draft key also the deployed graph, which {@code DeployCommandHandler} persists right
     * after the definition and the graph-less seed needs for hydration. A published run whose definition already
     * exists returns at once, so the healthy path pays one query.
     *
     * @return {@code false} only when nothing can start the run: no definition after the wait and no graph on the
     * seed for the self-heal to deploy. Everything else proceeds as before.
     */
    private boolean awaitDeployedDefinition(StartRunCommandPayload payload) {
        if (payload.hasPinnedDefinition() || StringUtils.isBlank(payload.processKey())) {
            return true;
        }
        String processKey = payload.processKey();
        ExecutionContext seed = payload.seedContext();
        boolean seedHasGraph = seed != null && seed.getNodes() != null && !seed.getNodes().isEmpty();
        String draftArtifactKey = processKey.contains(WorkflowGraph.DRAFT_MARKER) && seed != null
                && StringUtils.isNotBlank(seed.getWorkflowId())
                ? seed.getWorkflowId() + WorkflowGraph.namespaceSuffixOf(processKey)
                : null;
        java.util.function.BooleanSupplier ready = () -> sqsEngine.latestDefinitionExists(processKey)
                && (draftArtifactKey == null || artifactStore.loadDeployedGraph(draftArtifactKey).isPresent());
        if (ready.getAsBoolean()) {
            return true;
        }
        long startedAt = System.nanoTime();
        long deadline = startedAt + deployWait.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                Thread.sleep(DEPLOY_POLL_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
            if (ready.getAsBoolean()) {
                log.info("start-run.awaited-deploy processKey={} waitedMs={}",
                        processKey, (System.nanoTime() - startedAt) / 1_000_000L);
                return true;
            }
        }
        boolean defined = sqsEngine.latestDefinitionExists(processKey);
        log.warn("start-run.deploy-wait-expired processKey={} definitionPresent={} seedHasGraph={}",
                processKey, defined, seedHasGraph);
        return defined || seedHasGraph;
    }

    /**
     * Deploys the workflow from {@code seedContext} when the daemon has no live definition for the target process
     * key (post-reinstall self-heal). Only runs for the "latest" (non-pinned) start path and only when the graph
     * is actually present + the definition is genuinely missing — so the healthy, already-deployed run pays a
     * single cheap {@code latestDefinitionExists} check and nothing else.
     */
    private void ensureDefinitionDeployed(StartRunCommandPayload payload) {
        if (payload.hasPinnedDefinition() || StringUtils.isBlank(payload.processKey())) {
            return;
        }
        ExecutionContext seed = payload.seedContext();
        if (seed == null || seed.getNodes() == null || seed.getNodes().isEmpty()) {
            return;
        }
        if (sqsEngine.latestDefinitionExists(payload.processKey())) {
            return;
        }
        try {
            String version = String.valueOf(seed.getWorkflowVersion());
            WorkflowGraph graph = new WorkflowGraph(
                    seed.getWorkflowId(), seed.getWorkflowName(), version, seed.getNodes(), seed.getEdges());
            DeploymentInfo prior = artifactStore.findPriorDefinition(seed.getWorkflowId());
            DeploymentInfo info = workflowDeploymentService.deploy(graph, prior, false);
            artifactStore.saveDefinition(seed.getWorkflowId(), version, info);
            log.info("start-run.self-heal-deploy processKey={} definitionId={} version={} (no prior definition; "
                            + "deployed from seedContext graph)",
                    payload.processKey(), info.getProcessDefinitionId(), info.getVersion());
        } catch (Exception ex) {
            // Non-fatal: fall through to startLatest, which surfaces the real "no processes deployed"
            // error as a failed command if the self-heal could not establish a definition.
            log.warn("start-run.self-heal-deploy failed processKey={}: {}", payload.processKey(), ex.toString());
        }
    }

    /**
     * Best-effort RUNNING report — a callback hiccup must not fail the (successful) start.
     */
    private void reportRunning(String workflowExecutionId, String processInstanceId) {
        try {
            runStatusCallback.report(workflowExecutionId, RunStatusCallback.Status.RUNNING,
                    "processInstanceId=" + processInstanceId);
        } catch (Exception ex) {
            log.warn("start-run: RUNNING status callback failed for workflowExecutionId={}: {}",
                    workflowExecutionId, ex.toString());
        }
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Success output shipped to the BE: the created {@code processInstanceId}.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record StartRunResult(String processInstanceId, String workflowExecutionId, String processDefinitionId) {
    }
}
