package com.studioquack.sqs.engine.listener;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.command.store.DeployedArtifactStore.DeployedGraph;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder;
import com.studioquack.sqs.status.AsyncSandboxLogDispatcher;
import com.studioquack.sqs.status.SandboxLogCallback;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.event.ActivitiActivityCancelledEvent;
import org.activiti.engine.delegate.event.ActivitiActivityEvent;
import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiErrorEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Emits DuckTales per-node sandbox-log rows for engine-native BPMN elements (gateways, events,
 * business-rule / call-activity) and generator-inserted {@code gwfork_}/{@code gwjoin_} parallels.
 * Script / Docling / Duckbot-boundary nodes already log from {@code ScriptDispatchDelegate} —
 * this listener must not double-write those.
 *
 * <p>Hot-path contract (mirrors {@link SqsExecutionStatusListener}):
 * <ul>
 *   <li>Appended in {@code SqsActivitiEngineConfig}, never replacing Activiti's own listeners.</li>
 *   <li>{@link #isFailOnException()} is {@code false} and {@link #onEvent} is fully guarded.</li>
 *   <li>The engine thread only extracts identifiers and {@code enqueue}s. Graph load, store reads,
 *       payload build and the callback all run inside the dispatcher {@code Runnable}.</li>
 * </ul>
 *
 * <p>Activiti 9 has no {@code ACTIVITY_ACTIVATED}; {@link ActivitiEventType#ACTIVITY_STARTED} is
 * the "node became active" signal. Terminals: {@code ACTIVITY_COMPLETED},
 * {@code ACTIVITY_CANCELLED}, {@code ACTIVITY_ERROR_RECEIVED}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BpmnNodeLogListener implements ActivitiEventListener {

    /** Same prefix {@code ScriptDispatchDelegate} stamps on authored graph nodes. */
    private static final String NODE_ACTIVITY_PREFIX = "node_";

    /** Shared sandbox-error end — synthetic, never a designer node. */
    private static final String SHARED_ERROR_END_ID = "Event_SandboxErrorEnd";

    private static final String PROCESS_KEY_PREFIX = "Process_";

    private static final int MAX_START_ENTRIES = 4096;

    /**
     * Synthetic / structural activity ids the generator emits around a real node. Prefix checks
     * are cheap and run on the engine thread so we never enqueue a double-log.
     */
    /** True sentinels only — generator-inserted {@code gwfork_}/{@code gwjoin_} are real BPMN activities. */
    private static final String[] SKIP_ACTIVITY_PREFIXES = {
            "wait_", "errgate_", "errcode_", "err_", "btimer_", "bmsg_", "bsig_", "besc_",
            "failroute_", "hold_",
            "start_duck_", "end_duck_", "start_sub_", "end_sub_", "flow_"
    };

    /** Generator-inserted parallel fork/join — not in the authored graph; synthesize a parallel.gateway log. */
    private static final String GENERATOR_FORK_PREFIX = "gwfork_";
    private static final String GENERATOR_JOIN_PREFIX = "gwjoin_";
    private static final String PARALLEL_GATEWAY_TYPE = "parallel.gateway";

    /** Generator sentinels that are not authored catalog types. */
    private static final Set<String> SKIP_BPMN_TYPES = Set.of(
            "__sq_duckbot_start_task__",
            "__sq_duckbot_end_task__",
            "__sq_forced_task__");

    private static final String JOB_DOCLING = "docling-converter";
    private static final String JOB_CUSTOM_SCRIPT = "custom-script-executor";
    private static final String JOB_FUNCTION_SCRIPT_PREFIX = "function-script-executor";

    private final SandboxLogCallback sandboxLogCallback;
    private final AsyncSandboxLogDispatcher dispatcher;
    private final BpmnNodeLogPayloads payloads;
    private final ExecutionContextStore contextStore;
    private final DeployedArtifactStore artifactStore;
    private final ObjectProvider<GatewayDecisionRecorder> recorderProvider;
    /** Lazy: used only inside the dispatcher runnable (never on the engine thread). */
    private final ObjectProvider<SqsEngine> engineProvider;

    /**
     * Start nanos keyed by {@code runId + '\0' + nodeId}. Insertion-ordered so the eldest entry
     * drops when the map exceeds {@link #MAX_START_ENTRIES}. Durations only — no payloads.
     */
    private final Map<String, Long> startNanosByKey = Collections.synchronizedMap(new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > MAX_START_ENTRIES;
        }
    });

    /**
     * Bounded set of {@code runId + '\0' + nodeId} entries whose active execution was started
     * by an operator {@code retry-incident} repair (so the terminal log row carries {@code retry=true}).
     */
    private final Map<String, Boolean> retryByKey = Collections.synchronizedMap(new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > MAX_START_ENTRIES;
        }
    });

    @Override
    public boolean isFailOnException() {
        return false;
    }

    @Override
    public void onEvent(ActivitiEvent event) {
        try {
            if (event == null || !(event instanceof ActivitiActivityEvent activity)) {
                return;
            }
            ActivitiEventType type = event.getType();
            if (!isTracked(type)) {
                return;
            }
            String activityId = activity.getActivityId();
            // wait_* is a synthetic park — skip STARTED/COMPLETED. ACTIVITY_CANCELLED on wait_X
            // is the interrupting-boundary signal for authored node X (do not skip).
            boolean waitCancelled = type == ActivitiEventType.ACTIVITY_CANCELLED
                    && activityId != null && activityId.startsWith("wait_");
            if (!waitCancelled && shouldSkipActivityId(activityId)) {
                return;
            }
            String runId = extractBusinessKey(event);
            String processInstanceId = event.getProcessInstanceId();
            // Lane key: businessKey when we have it; otherwise the PI id (resolved inside the runnable).
            String laneRunId = notBlank(runId) ? runId : processInstanceId;
            if (!notBlank(laneRunId)) {
                log.debug("SQS bpmn node-log: no businessKey/processInstanceId on {} — skipping", type);
                return;
            }
            String nodeId = waitCancelled
                    ? activityId.substring("wait_".length())
                    : stripPrefix(activityId);
            if (nodeId == null || nodeId.isBlank()) {
                return;
            }
            String nodeName = activity.getActivityName();
            String processDefinitionId = event.getProcessDefinitionId();
            String status = waitCancelled ? "CANCELED" : terminalStatus(type);
            boolean retryAttempt = status == null && consumeRetryPinOnEngineThread(event, nodeId);
            String errorMessage = waitCancelled
                    ? interruptedByBoundaryMessage(event)
                    : extractErrorMessage(event, type);
            dispatcher.enqueue(laneRunId, nodeId, () -> handleAsync(
                    type, runId, processInstanceId, nodeId, nodeName, processDefinitionId, status, errorMessage,
                    waitCancelled, retryAttempt));
        } catch (Exception ex) {
            // Never let a log write break the engine.
            log.warn("SQS bpmn node-log listener failed for event {}: {}",
                    event == null ? null : event.getType(), ex.getMessage());
        }
    }

    private boolean consumeRetryPinOnEngineThread(ActivitiEvent event, String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return false;
        }
        DelegateExecution execution = executionOf(event);
        if (execution == null) {
            return false;
        }
        String varName = com.studioquack.sqs.engine.runtime.EngineVariableNames.retryNodeVariableName(nodeId);
        Object val = execution.getVariable(varName);
        if (val == null) {
            return false;
        }
        execution.removeVariable(varName);
        return Boolean.TRUE.equals(val) || "true".equalsIgnoreCase(String.valueOf(val));
    }

    /**
     * Store reads + payload + callback. Isolated from the engine thread; never throws out.
     * {@code runId} may be blank when the engine thread only had a processInstanceId — resolve here.
     */
    void handleAsync(ActivitiEventType type, String runId, String processInstanceId, String nodeId,
                     String activityName, String processDefinitionId, String status, String errorMessage,
                     boolean waitCancelled) {
        handleAsync(type, runId, processInstanceId, nodeId, activityName, processDefinitionId,
                status, errorMessage, waitCancelled, false);
    }

    void handleAsync(ActivitiEventType type, String runId, String processInstanceId, String nodeId,
                     String activityName, String processDefinitionId, String status, String errorMessage,
                     boolean waitCancelled, boolean retryAttempt) {
        try {
            String resolvedRunId = resolveRunId(runId, processInstanceId);
            if (!notBlank(resolvedRunId)) {
                log.debug("SQS bpmn node-log: no runId after async resolve node={} type={}", nodeId, type);
                return;
            }
            // wait_ CANCELED must reach script/docling nodes too — they are excluded from the
            // engine-native allowlist, and ScriptDispatchDelegate skips its COMPLETED/FAILED overwrite.
            if (waitCancelled) {
                emitWaitCancelled(resolvedRunId, nodeId, activityName, processDefinitionId, errorMessage);
                return;
            }
            // Generator-inserted parallels are not authored nodes — do not require a graph hit.
            if (isGeneratorInsertedParallel(nodeId)) {
                emitLog(resolvedRunId, nodeId, activityName, PARALLEL_GATEWAY_TYPE, status,
                        errorMessage, null, null, retryAttempt);
                return;
            }
            DeployedGraph graph = loadGraphResolvingWorkflowId(processDefinitionId);
            if (graph == null || graph.nodes() == null) {
                log.debug("SQS bpmn node-log: no deployed graph defId={}", processDefinitionId);
                return;
            }
            NodeDto node = findNode(graph.nodes(), nodeId);
            if (node == null) {
                return;
            }
            TemplateDto data = node.getData();
            String bpmnType = data == null ? null : data.getBpmnType();
            String jobType = data == null ? null : data.getJobType();
            if (isExcludedJobOrSentinel(bpmnType, jobType)) {
                return;
            }
            if (!BpmnNodeLogPayloads.isEngineNativeBpmnType(bpmnType)) {
                return;
            }
            String nodeName = resolveNodeName(node, activityName);
            emitLog(resolvedRunId, nodeId, nodeName, bpmnType, status, errorMessage, node, graph.edges(),
                    retryAttempt);
        } catch (Exception ex) {
            log.debug("SQS bpmn node-log async failed run={} node={} type={}: {}",
                    runId, nodeId, type, ex.toString());
        }
    }

    /** STARTED vs terminal write. Duration map + callback only — no engine I/O. */
    private void emitLog(String runId, String nodeId, String nodeName, String bpmnType,
                         String status, String errorMessage, NodeDto node, List<EdgeDto> edges,
                         boolean retryAttempt) {
        String durationKey = durationKey(runId, nodeId);
        if (status == null) {
            startNanosByKey.put(durationKey, System.nanoTime());
            if (retryAttempt) {
                retryByKey.put(durationKey, Boolean.TRUE);
            }
            sandboxLogCallback.reportStarted(new SandboxLogCallback.SandboxStartRecord(
                    runId, nodeId, nodeName, BpmnNodeLogPayloads.LANGUAGE));
            return;
        }
        Long durationMs = takeDurationMs(durationKey);
        boolean isRetry = retryByKey.remove(durationKey) != null;
        Object incoming = incomingMsg(runId, nodeId, edges);
        GatewayDecisionRecorder.GatewayDecision decision = latestDecision(runId, nodeId);
        // True dead-end: Activiti takes the synthetic default → error end, so the
        // gateway itself fires ACTIVITY_COMPLETED. decision.deadEnd() is the remap
        // signal; an intended default pass (defaultTaken) stays COMPLETED.
        if ("COMPLETED".equals(status) && isConditionMiss(bpmnType, decision)) {
            status = "FAILED";
            if (errorMessage == null || errorMessage.isBlank()) {
                errorMessage = "no outgoing flow matched — condition miss";
            }
        }
        SandboxLogCallback.SandboxLogRecord record = payloads.buildTerminal(
                runId, nodeId, nodeName, bpmnType, status, durationMs,
                incoming, node, decision, errorMessage);
        sandboxLogCallback.report(isRetry ? record.withRetry(Boolean.TRUE) : record);
    }

    /**
     * Exclusive/inclusive COMPLETED remaps to FAILED only on a true dead-end
     * ({@code decision.deadEnd()}). An intended default pass ({@code defaultTaken})
     * stays COMPLETED.
     */
    static boolean isConditionMiss(String bpmnType, GatewayDecisionRecorder.GatewayDecision decision) {
        if (decision == null || !decision.deadEnd()) {
            return false;
        }
        if (bpmnType == null) {
            return false;
        }
        String t = bpmnType.toLowerCase(java.util.Locale.ROOT);
        return "exclusive.gateway".equals(t) || "inclusive.gateway".equals(t);
    }

    static boolean isTracked(ActivitiEventType type) {
        return type == ActivitiEventType.ACTIVITY_STARTED
                || type == ActivitiEventType.ACTIVITY_COMPLETED
                || type == ActivitiEventType.ACTIVITY_CANCELLED
                || type == ActivitiEventType.ACTIVITY_ERROR_RECEIVED
                || type == ActivitiEventType.UNCAUGHT_BPMN_ERROR;
    }

    /** {@code null} means the activated/started half (no terminal status yet). */
    static String terminalStatus(ActivitiEventType type) {
        if (type == ActivitiEventType.ACTIVITY_COMPLETED) {
            return "COMPLETED";
        }
        if (type == ActivitiEventType.ACTIVITY_CANCELLED) {
            return "CANCELED";
        }
        if (type == ActivitiEventType.ACTIVITY_ERROR_RECEIVED
                || type == ActivitiEventType.UNCAUGHT_BPMN_ERROR) {
            return "FAILED";
        }
        return null;
    }

    /** Generator-inserted parallel fork/join — logged even when absent from the authored graph. */
    public static boolean isGeneratorInsertedParallel(String activityId) {
        return activityId != null
                && (activityId.startsWith(GENERATOR_FORK_PREFIX) || activityId.startsWith(GENERATOR_JOIN_PREFIX));
    }

    public static boolean shouldSkipActivityId(String activityId) {
        if (activityId == null || activityId.isBlank()) {
            return true;
        }
        if (SHARED_ERROR_END_ID.equals(activityId)) {
            return true;
        }
        for (String prefix : SKIP_ACTIVITY_PREFIXES) {
            if (activityId.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    static String stripPrefix(String activityId) {
        if (activityId != null && activityId.startsWith(NODE_ACTIVITY_PREFIX)) {
            return activityId.substring(NODE_ACTIVITY_PREFIX.length());
        }
        return activityId;
    }

    /**
     * Cheap parse for {@code Process_<workflowId>:<ver>:<uuid>} or a bare {@code Process_<workflowId>}
     * key. A StrongUuid definition id (bare UUID, no {@code Process_} prefix) is NOT a workflow id —
     * {@link #resolveWorkflowId} then asks {@link SqsEngine} for the key.
     */
    static String workflowIdFromProcessDefinitionId(String processDefinitionId) {
        if (processDefinitionId == null || processDefinitionId.isBlank()) {
            return null;
        }
        int colon = processDefinitionId.indexOf(':');
        String key = colon < 0 ? processDefinitionId : processDefinitionId.substring(0, colon);
        if (key.startsWith(PROCESS_KEY_PREFIX)) {
            return key.substring(PROCESS_KEY_PREFIX.length());
        }
        return key;
    }

    /** Prefer the engine-thread extract; if blank, resolve the PI off-thread via {@link SqsEngine}. */
    private String resolveRunId(String runId, String processInstanceId) {
        if (notBlank(runId)) {
            return runId;
        }
        try {
            SqsEngine engine = engineProvider == null ? null : engineProvider.getIfAvailable();
            return engine == null ? null : engine.findBusinessKey(processInstanceId);
        } catch (RuntimeException ex) {
            log.debug("SQS bpmn node-log: businessKey resolve skipped ({})", ex.toString());
            return null;
        }
    }

    /**
     * One store read on the cheap parse; a StrongUuid definition id misses and we then ask
     * {@link SqsEngine} for {@code ProcessDefinition.getKey()} (still off the engine thread).
     */
    private DeployedGraph loadGraphResolvingWorkflowId(String processDefinitionId) {
        String parsed = workflowIdFromProcessDefinitionId(processDefinitionId);
        DeployedGraph graph = loadGraph(parsed);
        if (graph != null) {
            return graph;
        }
        String fromEngine = workflowIdFromEngine(processDefinitionId);
        if (fromEngine == null || fromEngine.equals(parsed)) {
            return null;
        }
        return loadGraph(fromEngine);
    }

    private String workflowIdFromEngine(String processDefinitionId) {
        try {
            SqsEngine engine = engineProvider == null ? null : engineProvider.getIfAvailable();
            if (engine == null || !notBlank(processDefinitionId)) {
                return null;
            }
            ProcessDefinition def = engine.findProcessDefinition(processDefinitionId);
            if (def == null || !notBlank(def.getKey())) {
                return null;
            }
            return workflowIdFromProcessDefinitionId(def.getKey());
        } catch (RuntimeException ex) {
            log.debug("SQS bpmn node-log: process-definition key resolve skipped ({})", ex.toString());
            return null;
        }
    }

    private DeployedGraph loadGraph(String workflowId) {
        if (!notBlank(workflowId)) {
            return null;
        }
        return artifactStore.loadDeployedGraph(workflowId).orElse(null);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static boolean isExcludedJobOrSentinel(String bpmnType, String jobType) {
        if (bpmnType != null && SKIP_BPMN_TYPES.contains(bpmnType)) {
            return true;
        }
        if (jobType == null || jobType.isBlank()) {
            return false;
        }
        if (JOB_DOCLING.equalsIgnoreCase(jobType) || JOB_CUSTOM_SCRIPT.equalsIgnoreCase(jobType)) {
            return true;
        }
        return jobType.regionMatches(true, 0, JOB_FUNCTION_SCRIPT_PREFIX, 0, JOB_FUNCTION_SCRIPT_PREFIX.length());
    }

    private static NodeDto findNode(List<NodeDto> nodes, String nodeId) {
        for (NodeDto node : nodes) {
            if (node != null && nodeId.equals(node.getId())) {
                return node;
            }
        }
        return null;
    }

    private static String resolveNodeName(NodeDto node, String activityName) {
        TemplateDto data = node.getData();
        if (data != null && data.getLabel() != null && !data.getLabel().isBlank()) {
            return data.getLabel();
        }
        return activityName;
    }

    /**
     * Immediate upstream outputs only — never the whole run. One upstream → that node's
     * outputs; many → a map keyed by upstream node id; none → {@code null}.
     */
    private Object incomingMsg(String runId, String nodeId, List<EdgeDto> edges) {
        Set<String> upstreamIds = new LinkedHashSet<>();
        if (edges != null) {
            for (EdgeDto edge : edges) {
                if (edge == null || edge.getSource() == null || !nodeId.equals(edge.getTarget())) {
                    continue;
                }
                if (isAssociation(edge)) {
                    continue;
                }
                upstreamIds.add(edge.getSource());
            }
        }
        if (upstreamIds.isEmpty()) {
            return null;
        }
        Map<String, List<InOutValueDto>> byNode = contextStore.collectOutputsByNode(runId, upstreamIds);
        if (byNode == null || byNode.isEmpty()) {
            return null;
        }
        if (upstreamIds.size() == 1) {
            return byNode.get(upstreamIds.iterator().next());
        }
        Map<String, List<InOutValueDto>> ordered = new LinkedHashMap<>();
        for (String upstreamId : upstreamIds) {
            List<InOutValueDto> outputs = byNode.get(upstreamId);
            if (outputs != null) {
                ordered.put(upstreamId, outputs);
            }
        }
        return ordered.isEmpty() ? null : ordered;
    }

    private static boolean isAssociation(EdgeDto edge) {
        return edge.getData() != null
                && "ASSOCIATION".equalsIgnoreCase(edge.getData().getFlowType());
    }

    private GatewayDecisionRecorder.GatewayDecision latestDecision(String runId, String nodeId) {
        try {
            GatewayDecisionRecorder recorder = recorderProvider == null ? null : recorderProvider.getIfAvailable();
            if (recorder == null) {
                return null;
            }
            return recorder.latest(runId, nodeId);
        } catch (RuntimeException ex) {
            log.debug("SQS bpmn node-log: gateway decision unavailable run={} node={} ({})",
                    runId, nodeId, ex.toString());
            return null;
        }
    }

    private Long takeDurationMs(String key) {
        Long started = startNanosByKey.remove(key);
        if (started == null) {
            return null;
        }
        return (System.nanoTime() - started) / 1_000_000L;
    }

    private static String durationKey(String runId, String nodeId) {
        return runId + '\0' + nodeId;
    }

    /**
     * Interrupting-boundary cancel of {@code wait_<node>}: status text plus cheap cause if Activiti
     * attached one ({@link ActivitiActivityCancelledEvent#getCause()}).
     */
    private static String interruptedByBoundaryMessage(ActivitiEvent event) {
        String message = "interrupted by boundary";
        if (event instanceof ActivitiActivityCancelledEvent cancelled) {
            Object cause = cancelled.getCause();
            if (cause != null) {
                String causeText = String.valueOf(cause);
                if (!causeText.isBlank() && !"null".equals(causeText)) {
                    message = message + ": " + causeText;
                }
            }
            String activityId = cancelled.getActivityId();
            if (activityId != null && !activityId.isBlank()) {
                message = message + " (" + activityId + ")";
            }
        }
        return message;
    }

    /** Existing {@link #emitLog} path — no second callback. Graph miss still reports CANCELED. */
    private void emitWaitCancelled(String runId, String nodeId, String activityName,
                                   String processDefinitionId, String errorMessage) {
        DeployedGraph graph = loadGraphResolvingWorkflowId(processDefinitionId);
        NodeDto node = (graph != null && graph.nodes() != null) ? findNode(graph.nodes(), nodeId) : null;
        String bpmnType = (node != null && node.getData() != null) ? node.getData().getBpmnType() : null;
        String nodeName = node != null ? resolveNodeName(node, activityName) : activityName;
        emitLog(runId, nodeId, nodeName, bpmnType, "CANCELED", errorMessage, node,
                graph == null ? null : graph.edges(), false);
    }

    private static String extractErrorMessage(ActivitiEvent event, ActivitiEventType type) {
        if (event instanceof ActivitiErrorEvent errorEvent) {
            String code = errorEvent.getErrorCode();
            if (code != null && !code.isBlank()) {
                return code;
            }
            return errorEvent.getErrorId();
        }
        if (type == ActivitiEventType.ACTIVITY_CANCELLED) {
            return "cancelled";
        }
        return null;
    }

    /**
     * Same extract as {@link SqsExecutionStatusListener}: {@link DelegateExecution} on an
     * {@link ActivitiEntityEvent}. Activity events do not carry an entity, so we then try the
     * in-command execution (by {@code executionId} / {@code processInstanceId}).
     */
    private String extractBusinessKey(ActivitiEvent event) {
        return businessKeyOf(executionOf(event));
    }

    private static String businessKeyOf(DelegateExecution execution) {
        if (execution == null) {
            return null;
        }
        String key = execution.getProcessInstanceBusinessKey();
        return notBlank(key) ? key : null;
    }

    private DelegateExecution executionOf(ActivitiEvent event) {
        if (event instanceof ActivitiEntityEvent entityEvent
                && entityEvent.getEntity() instanceof DelegateExecution execution) {
            return execution;
        }
        return executionFromCommandContext(event);
    }

    /** Cheap persistence-session lookup — no extra engine query on a cold context. */
    private DelegateExecution executionFromCommandContext(ActivitiEvent event) {
        try {
            CommandContext ctx = Context.getCommandContext();
            if (ctx == null || ctx.getExecutionEntityManager() == null) {
                return null;
            }
            if (ctx.hasInvolvedExecutions()) {
                for (ExecutionEntity involved : ctx.getInvolvedExecutions()) {
                    if (involved == null) {
                        continue;
                    }
                    if (idsEqual(involved.getId(), event.getExecutionId())
                            || idsEqual(involved.getProcessInstanceId(), event.getProcessInstanceId())) {
                        return involved;
                    }
                }
            }
            if (event.getExecutionId() != null) {
                ExecutionEntity byExecution = ctx.getExecutionEntityManager().findById(event.getExecutionId());
                if (byExecution != null) {
                    return byExecution;
                }
            }
            if (event.getProcessInstanceId() != null) {
                return ctx.getExecutionEntityManager().findById(event.getProcessInstanceId());
            }
        } catch (RuntimeException ex) {
            log.debug("SQS bpmn node-log: command-context execution lookup skipped ({})", ex.toString());
        }
        return null;
    }

    private static boolean idsEqual(String left, String right) {
        return left != null && left.equals(right);
    }
}
