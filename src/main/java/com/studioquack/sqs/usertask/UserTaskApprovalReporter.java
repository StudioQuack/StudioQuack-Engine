package com.studioquack.sqs.usertask;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.listener.BpmnNodeLogPayloads;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.status.SandboxLogCallback;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The values are customer data (assume-PHI): they travel only on this authenticated channel, are
 * size-capped here, and are sealed by the backend. Recipients are names as authored — the backend
 * resolves them inside the workflow's own organization and drops anything outside it.
 */
@Slf4j
@Component
public class UserTaskApprovalReporter {

    /** Bounds matching the backend's notification title/body columns. */
    static final int MAX_SUBJECT = 256;
    static final int MAX_MESSAGE = 2048;
    static final int MAX_RECIPIENTS_FIELD = 4096;
    /** Values above this serialized size are not sent; the modal says they were too large. */
    static final int MAX_VALUES_BYTES = 32 * 1024;
    /** Sweep page size — far above any realistic count of simultaneously parked approvals. */
    private static final int SWEEP_LIMIT = 500;
    private static final Duration[] BACKOFF = {
            Duration.ZERO, Duration.ofSeconds(2), Duration.ofSeconds(10), Duration.ofSeconds(30),
            Duration.ofMinutes(2)
    };

    private final ObjectProvider<SqsEngine> engineProvider;
    private final UserTaskIo io;
    private final DeployedArtifactStore artifactStore;
    private final UserTaskApprovalSink sink;
    private final ObjectProvider<SandboxLogCallback> sandboxLogCallback;
    private final ObjectMapper mapper;

    /** Task ids the backend acknowledged (or refused for good) in this process. */
    private final Set<String> settled = ConcurrentHashMap.newKeySet();
    /** Task ids with a delivery attempt in flight — keeps the sweep from doubling a retry loop. */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public UserTaskApprovalReporter(ObjectProvider<SqsEngine> engineProvider,
                                    UserTaskIo io,
                                    DeployedArtifactStore artifactStore,
                                    UserTaskApprovalSink sink,
                                    ObjectProvider<SandboxLogCallback> sandboxLogCallback,
                                    ObjectMapper mapper) {
        this.engineProvider = engineProvider;
        this.io = io;
        this.artifactStore = artifactStore;
        this.sink = sink;
        this.sandboxLogCallback = sandboxLogCallback;
        this.mapper = mapper;
    }

    /** A task just parked (after commit): flag it waiting on the live view, then announce it. */
    public void onParked(ParkedUserTask parked) {
        if (parked == null) {
            return;
        }
        reportStarted(parked);
        reportAsync(parked);
    }

    /** Announces {@code parked} off the engine thread. Never throws. */
    public void reportAsync(ParkedUserTask parked) {
        if (parked == null || StringUtils.isBlank(parked.taskId()) || !inFlight.add(parked.taskId())) {
            return;
        }
        Thread.ofVirtual().name("user-task-report-" + parked.taskId()).start(() -> deliver(parked));
    }

    /**
     * Re-announces parked tasks not yet acknowledged in this process and forgets settled ids whose
     * task is gone, so the set stays bounded by the parked count.
     */
    @Scheduled(fixedDelayString = "${sqs.user-task.reconcile-ms:300000}",
            initialDelayString = "${sqs.user-task.reconcile-initial-ms:60000}")
    public void reconcile() {
        SqsEngine engine = engineProvider.getIfAvailable();
        if (engine == null) {
            return;
        }
        try {
            List<Task> parked = engine.listActiveUserTasks(SWEEP_LIMIT);
            Set<String> activeIds = new HashSet<>();
            for (Task task : parked) {
                activeIds.add(task.getId());
                if (!settled.contains(task.getId())) {
                    reportAsync(new ParkedUserTask(task.getId(), task.getProcessInstanceId(),
                            task.getProcessDefinitionId(), task.getTaskDefinitionKey(), task.getName(),
                            engine.findBusinessKey(task.getProcessInstanceId())));
                }
            }
            if (parked.size() < SWEEP_LIMIT) {
                settled.retainAll(activeIds);
            }
        } catch (RuntimeException ex) {
            log.warn("user-task.approval: reconcile sweep failed: {}", ex.toString());
        }
    }

    private void deliver(ParkedUserTask parked) {
        try {
            Map<String, Object> report = buildReport(parked);
            if (report == null) {
                settled.add(parked.taskId());
                return;
            }
            for (Duration wait : BACKOFF) {
                if (!wait.isZero()) {
                    Thread.sleep(wait);
                }
                UserTaskApprovalSink.Outcome outcome = sink.open(report);
                if (outcome != UserTaskApprovalSink.Outcome.RETRY) {
                    settled.add(parked.taskId());
                    log.info("user-task.approval: task={} node={} announced outcome={}",
                            parked.taskId(), report.get("nodeId"), outcome);
                    return;
                }
            }
            log.warn("user-task.approval: task={} not acknowledged yet — the reconcile sweep retries",
                    parked.taskId());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException ex) {
            log.warn("user-task.approval: report failed task={}: {}", parked.taskId(), ex.toString());
        } finally {
            inFlight.remove(parked.taskId());
        }
    }

    /**
     * The wire body, or {@code null} when the task is not a designer node of a deployed workflow
     * (nothing the backend could address).
     */
    Map<String, Object> buildReport(ParkedUserTask parked) {
        SqsEngine engine = engineProvider.getIfAvailable();
        if (engine == null) {
            return null;
        }
        String nodeId = StringUtils.removeStart(parked.taskDefinitionKey(), SqsEngine.NODE_ELEMENT_ID_PREFIX);
        ProcessDefinition def = engine.findProcessDefinition(parked.processDefinitionId());
        String workflowId = def == null ? null : WorkflowGraph.workflowIdOf(def.getKey());
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(workflowId)) {
            log.warn("user-task.approval: task={} has no workflow/node address — not announced", parked.taskId());
            return null;
        }
        Optional<UserTaskIo.Inbound> inbound = resolveInbound(engine, parked.processInstanceId(), nodeId);
        NodeDto node = inbound.map(UserTaskIo.Inbound::node)
                .orElseGet(() -> deployedNode(workflowId + WorkflowGraph.namespaceSuffixOf(def.getKey()), nodeId));

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("taskId", parked.taskId());
        report.put("workflowId", workflowId);
        report.put("nodeId", nodeId);
        report.put("workflowExecutionId", parked.businessKey());
        report.put("processInstanceId", parked.processInstanceId());
        report.put("nodeLabel", cap(parked.name(), MAX_SUBJECT));
        report.put("subject", cap(param(node, "subject"), MAX_SUBJECT));
        report.put("message", cap(param(node, "message"), MAX_MESSAGE));
        report.put("assignees", cap(param(node, "assignees"), MAX_RECIPIENTS_FIELD));
        report.put("teams", cap(param(node, "teams"), MAX_RECIPIENTS_FIELD));
        Map<String, Object> values = inbound.map(i -> UserTaskIo.values(i.inputVariables())).orElse(Map.of());
        if (!values.isEmpty()) {
            if (serializedSize(values) <= MAX_VALUES_BYTES) {
                report.put("values", values);
            } else {
                log.warn("user-task.approval: task={} values exceed {} bytes — not sent",
                        parked.taskId(), MAX_VALUES_BYTES);
                report.put("valuesOmitted", true);
            }
        }
        return report;
    }

    /** The task's inbound as the pipeline resolves it; empty when the run has no loadable context. */
    private Optional<UserTaskIo.Inbound> resolveInbound(SqsEngine engine, String processInstanceId, String nodeId) {
        try {
            return io.resolveInbound(engine.getProcessVariable(processInstanceId, EngineVariableNames.STUDIOQUACK_REF),
                    nodeId);
        } catch (RuntimeException ex) {
            log.warn("user-task.approval: inbound for node={} not resolvable: {}", nodeId, ex.toString());
            return Optional.empty();
        }
    }

    /** The node as this daemon deployed it — used when the run's own context is gone. */
    private NodeDto deployedNode(String artifactKey, String nodeId) {
        return artifactStore.loadDeployedGraph(artifactKey)
                .map(DeployedArtifactStore.DeployedGraph::nodes)
                .flatMap(nodes -> nodes.stream().filter(n -> n != null && nodeId.equals(n.getId())).findFirst())
                .orElse(null);
    }

    /** Live view only: the task is now waiting. Never throws. */
    private void reportStarted(ParkedUserTask parked) {
        SandboxLogCallback callback = sandboxLogCallback.getIfAvailable();
        if (callback == null || StringUtils.isBlank(parked.businessKey())) {
            return;
        }
        try {
            callback.reportStarted(new SandboxLogCallback.SandboxStartRecord(parked.businessKey(),
                    StringUtils.removeStart(parked.taskDefinitionKey(), SqsEngine.NODE_ELEMENT_ID_PREFIX),
                    parked.name(), BpmnNodeLogPayloads.LANGUAGE));
        } catch (RuntimeException ex) {
            log.debug("user-task.approval: started mirror skipped task={} ({})", parked.taskId(), ex.toString());
        }
    }

    private static String param(NodeDto node, String shortCode) {
        if (node == null || node.getData() == null) {
            return null;
        }
        return StringUtils.trimToNull(node.getData().findParameterValueByShortCode(shortCode));
    }

    private int serializedSize(Map<String, Object> values) {
        try {
            return mapper.writeValueAsString(values).getBytes(StandardCharsets.UTF_8).length;
        } catch (Exception ex) {
            return Integer.MAX_VALUE;
        }
    }

    private static String cap(String value, int max) {
        return value == null || value.length() <= max ? value : value.substring(0, max);
    }

    /** Visible for tests: whether this process has settled the task. */
    boolean isSettled(String taskId) {
        return settled.contains(taskId);
    }
}
