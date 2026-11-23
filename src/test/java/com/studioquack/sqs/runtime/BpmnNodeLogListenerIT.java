package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.listener.BpmnNodeLogListener;
import com.studioquack.sqs.engine.listener.BpmnNodeLogPayloads;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.support.FakeSandboxConfig;
import com.studioquack.sqs.runtime.support.FakeSandboxExecutionPort;
import com.studioquack.sqs.status.AsyncSandboxLogDispatcher;
import com.studioquack.sqs.status.log.SandboxLogEntity;
import com.studioquack.sqs.status.log.SandboxLogRepository;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.event.ActivitiActivityEvent;
import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.spring.boot.ProcessEngineConfigurationConfigurer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real-engine repro for Mystery M2: {@link BpmnNodeLogListener} must emit STARTED + terminal
 * sandbox-log rows for an authored exclusive gateway. The unit-test mock is also an
 * {@link ActivitiEntityEvent}; live Activiti gateway events are typically activity-only, so this
 * IT is the one that fails when {@code extractBusinessKey} silently skips.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-bpmn-node-log-it;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-bpmn-node-log-it.wal",
        "sqs.orphan-sweeper.enabled=false"
})
@ActiveProfiles("base")
@Import({FakeSandboxConfig.class, BpmnNodeLogListenerIT.ProbeConfig.class})
class BpmnNodeLogListenerIT {

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private ExecutionContextStore contextStore;
    @Autowired private FakeSandboxExecutionPort fakeSandbox;
    @Autowired private DeployedArtifactStore artifactStore;
    @Autowired private SandboxLogRepository sandboxLogRepository;
    @Autowired private AsyncSandboxLogDispatcher dispatcher;
    @Autowired private ActivityEventProbe probe;

    @BeforeEach
    void resetFake() {
        fakeSandbox.reset();
        probe.rows.clear();
    }

    @Test
    @DisplayName("exclusive gateway emits STARTED + terminal language=bpmn on a real engine")
    void exclusiveGatewayEmitsStartedAndTerminalBpmnLogs() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String scriptN = "decideN";
        String gw = "gw";
        NodeDto gateway = gatewayNode(gw, "hitEnd", "= route__" + gw + " == 'HIT'");
        WorkflowGraph graph = new WorkflowGraph("wfXor-" + wfeId, "xor", "1",
                List.of(start(), sandboxNode(scriptN), gateway, end("hitEnd"), end("missEnd")),
                List.of(edge("startN", scriptN), edge(scriptN, gw),
                        edge(gw, "hitEnd"), edge(gw, "missEnd")));
        fakeSandbox.succeed(scriptN, Map.of("out", "HIT"));

        seedSaveAndStart(graph, scriptN, wfeId);
        assertTrue(awaitCompleted(wfeId), "exclusive-gateway flow completed");
        assertTrue(dispatcher.awaitIdle(Duration.ofSeconds(5)), "sandbox-log dispatcher drained");

        List<SandboxLogEntity> allRows = sandboxLogRepository
                .findByWorkflowExecutionIdOrderByCdateAsc(wfeId);
        List<SandboxLogEntity> gwRows = allRows.stream()
                .filter(row -> gw.equals(row.getNodeId()))
                .toList();
        List<ActivityEventProbe.Row> gwEvents = probe.rows.stream()
                .filter(row -> ("node_" + gw).equals(row.activityId()))
                .toList();
        List<String> rowSummary = allRows.stream()
                .map(row -> row.getNodeId() + "/" + row.getStatus() + "/" + row.getLanguage())
                .toList();

        assertThat(gwRows)
                .as("exclusive gateway sandbox-log rows; probe=%s allRows=%s", gwEvents, rowSummary)
                .anyMatch(row -> "STARTED".equals(row.getStatus())
                        && BpmnNodeLogPayloads.LANGUAGE.equals(row.getLanguage()))
                .anyMatch(row -> ("COMPLETED".equals(row.getStatus()) || "FAILED".equals(row.getStatus()))
                        && BpmnNodeLogPayloads.LANGUAGE.equals(row.getLanguage()));
    }

    @Test
    @DisplayName("generator-inserted gwfork_/gwjoin_ emit STARTED + terminal language=bpmn")
    void generatorParallelGatewaysEmitStartedAndTerminalBpmnLogs() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String forkSrc = "forkSrc";
        String left = "leftN";
        String right = "rightN";
        String joinSink = "joinSink";
        // Two unconditional outgoing edges → generator inserts gwfork_forkSrc;
        // two incoming parallel branches → generator inserts gwjoin_joinSink.
        WorkflowGraph graph = new WorkflowGraph("wfPar-" + wfeId, "par", "1",
                List.of(start(), sandboxNode(forkSrc), sandboxNode(left), sandboxNode(right),
                        sandboxNode(joinSink), end("endN")),
                List.of(edge("startN", forkSrc),
                        edge(forkSrc, left), edge(forkSrc, right),
                        edge(left, joinSink), edge(right, joinSink),
                        edge(joinSink, "endN")));
        fakeSandbox.succeed(forkSrc, Map.of("out", "split"));
        fakeSandbox.succeed(left, Map.of("out", "L"));
        fakeSandbox.succeed(right, Map.of("out", "R"));
        fakeSandbox.succeed(joinSink, Map.of("out", "joined"));

        seedSaveAndStart(graph, forkSrc, wfeId);
        assertTrue(awaitCompleted(wfeId), "parallel fork/join flow completed");
        assertTrue(dispatcher.awaitIdle(Duration.ofSeconds(5)), "sandbox-log dispatcher drained");

        List<SandboxLogEntity> rows = sandboxLogRepository
                .findByWorkflowExecutionIdOrderByCdateAsc(wfeId);
        assertGatewayPair(rows, "gwfork_" + forkSrc, probe.rows);
        assertGatewayPair(rows, "gwjoin_" + joinSink, probe.rows);
    }

    @Test
    @DisplayName("all-conditioned miss: gateway terminal is FAILED with dead-end echoes")
    void allConditionedMissEmitsFailedGatewayRowWithDeadEndEchoes() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String agentN = "agentN";
        String gw = "gw";
        NodeDto gateway = simpleNode(gw, "exclusive.gateway", null, "gateway");
        WorkflowGraph graph = new WorkflowGraph("wfDeadEnd-" + wfeId, "dead-end", "1",
                List.of(start(), agentNode(agentN), gateway, end("endA"), end("endB")),
                List.of(edge("startN", agentN), edge(agentN, gw),
                        typedGwEdge(gw, "endA", "@ai_agent.main_output.final_answer == \" \""),
                        typedGwEdge(gw, "endB", "@ai_agent.main_output.final_answer == \"nope\"")));
        fakeSandbox.succeed(agentN, Map.of("main_output", Map.of("final_answer", "hello")));

        seedSaveAndStart(graph, agentN, wfeId);
        assertTrue(awaitCompleted(wfeId), "all-conditioned miss reaches a terminal state");
        assertTrue(dispatcher.awaitIdle(Duration.ofSeconds(5)), "sandbox-log dispatcher drained");

        List<SandboxLogEntity> gwRows = sandboxLogRepository
                .findByWorkflowExecutionIdOrderByCdateAsc(wfeId).stream()
                .filter(row -> gw.equals(row.getNodeId()))
                .toList();
        assertThat(gwRows)
                .as("gateway must emit a FAILED terminal row with dead-end echoes")
                .anyMatch(row -> "FAILED".equals(row.getStatus())
                        && BpmnNodeLogPayloads.LANGUAGE.equals(row.getLanguage())
                        && row.getStdout() != null
                        && row.getStdout().contains("activated: dead-end")
                        && row.getStdout().contains("@ai_agent.main_output.final_answer == \" \"")
                        && row.getErrorMessage() != null
                        && row.getErrorMessage().contains("no outgoing flow matched"));
    }

    private static void assertGatewayPair(List<SandboxLogEntity> rows, String activityId,
                                          List<ActivityEventProbe.Row> probeRows) {
        List<SandboxLogEntity> match = rows.stream()
                .filter(row -> activityId.equals(row.getNodeId()))
                .toList();
        List<ActivityEventProbe.Row> events = probeRows.stream()
                .filter(row -> activityId.equals(row.activityId()))
                .toList();
        assertThat(match)
                .as("sandbox-log rows for %s; probe=%s", activityId, events)
                .anyMatch(row -> "STARTED".equals(row.getStatus())
                        && BpmnNodeLogPayloads.LANGUAGE.equals(row.getLanguage()))
                .anyMatch(row -> ("COMPLETED".equals(row.getStatus()) || "FAILED".equals(row.getStatus()))
                        && BpmnNodeLogPayloads.LANGUAGE.equals(row.getLanguage()));
    }

    private ProcessInstance seedSaveAndStart(WorkflowGraph graph, String sandboxNodeId, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-bpmn-log-it");
        ctx.setCurrentNodeId(sandboxNodeId);
        ctx.setNodes(new ArrayList<>(graph.nodes()));
        ctx.setEdges(new ArrayList<>(graph.edges()));
        ProcessContextRef ref = contextStore.snapshot(ctx, null);

        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        // Deploy command persists the authored graph; the listener's findNode path needs that row.
        artifactStore.saveDefinition(graph.workflowId(), graph.workflowVersion(), info);
        artifactStore.saveDeployedGraph(graph.workflowId(), graph.nodes(), graph.edges());

        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(pi, "process instance started");
        return pi;
    }

    private NodeDto start() {
        return simpleNode("startN", "start.event", null, "start");
    }

    private NodeDto end(String id) {
        return simpleNode(id, "end.event", null, "end");
    }

    private NodeDto sandboxNode(String id) {
        NodeDto n = simpleNode(id, "custom.sandbox", "custom-script-executor", "serviceTask");
        n.getData().setTemplateScript(new TemplateScriptDto(id + "-s", id + "-t", "python",
                "studioquack.return_result([{\"port\": \"out\", \"value\": \"x\"}])\n"));
        PortDto out = new PortDto();
        out.setId("out");
        out.setName("out");
        out.setValue("out");
        out.setType(PortType.OUTPUT);
        n.getData().setPorts(new ArrayList<>(List.of(out)));
        return n;
    }

    /** Producer matching the live graph: shortCode {@code ai_agent}, port {@code main_output}. */
    private NodeDto agentNode(String id) {
        NodeDto n = sandboxNode(id);
        n.getData().setNodeShortCode("ai_agent");
        PortDto out = new PortDto();
        out.setId("main_output");
        out.setName("main_output");
        out.setValue("main_output");
        out.setType(PortType.OUTPUT);
        n.getData().setPorts(new ArrayList<>(List.of(out)));
        return n;
    }

    private EdgeDto typedGwEdge(String gw, String target, String condition) {
        EdgeDto e = edge(gw, target);
        EdgeDataDto data = new EdgeDataDto();
        data.setCondition(condition);
        e.setData(data);
        return e;
    }

    private NodeDto gatewayNode(String id, String conditionalTargetNodeId, String feelCondition) {
        NodeDto n = simpleNode(id, "exclusive.gateway", null, "gateway");
        EdgeTransformDto cond = new EdgeTransformDto();
        cond.setTargetNodeId(conditionalTargetNodeId);
        cond.setSourcePath("conditionExpression");
        cond.setSourcePathValue(feelCondition);
        n.setEdgeTransformListForCurrentNode(new ArrayList<>(List.of(cond)));
        return n;
    }

    private NodeDto simpleNode(String id, String bpmnType, String jobType, String nodeType) {
        TemplateDto t = new TemplateDto();
        t.setBpmnType(bpmnType);
        t.setJobType(jobType);
        t.setNodeType(nodeType);
        t.setLabel(id);
        NodeDto n = new NodeDto();
        n.setId(id);
        n.setData(t);
        return n;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto e = new EdgeDto();
        e.setId("e_" + source + "_" + target);
        e.setSource(source);
        e.setTarget(target);
        return e;
    }

    private boolean awaitCompleted(String wfeId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            if (sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0
                    && sqsEngine.historicProcessInstances().processInstanceBusinessKey(wfeId).finished().count() > 0) {
                return true;
            }
            Thread.sleep(100);
        }
        return sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0;
    }

    @TestConfiguration
    static class ProbeConfig {
        @Bean
        ActivityEventProbe activityEventProbe() {
            return new ActivityEventProbe();
        }

        @Bean
        ProcessEngineConfigurationConfigurer activityEventProbeConfigurer(ActivityEventProbe probe) {
            return cfg -> {
                List<ActivitiEventListener> listeners = new ArrayList<>();
                if (cfg.getEventListeners() != null) {
                    listeners.addAll(cfg.getEventListeners());
                }
                listeners.add(probe);
                cfg.setEventListeners(listeners);
            };
        }
    }

    /**
     * Records the live event shape + businessKey extract for gateway-like activity ids so a
     * failing assertion names the exact listener pipeline step that dropped the row.
     */
    static final class ActivityEventProbe implements ActivitiEventListener {
        record Row(String activityId, String type, String eventClass, boolean entityEvent,
                   boolean commandContextPresent, String businessKey, String processDefinitionId) {
        }

        final List<Row> rows = new CopyOnWriteArrayList<>();

        @Override
        public void onEvent(ActivitiEvent event) {
            if (!(event instanceof ActivitiActivityEvent activity) || !isTracked(event.getType())) {
                return;
            }
            String activityId = activity.getActivityId();
            if (!isGatewayLike(activityId)) {
                return;
            }
            boolean entity = event instanceof ActivitiEntityEvent
                    && ((ActivitiEntityEvent) event).getEntity() instanceof DelegateExecution;
            CommandContext ctx = Context.getCommandContext();
            rows.add(new Row(activityId, String.valueOf(event.getType()), event.getClass().getName(),
                    entity, ctx != null, extractBusinessKey(event), event.getProcessDefinitionId()));
        }

        @Override
        public boolean isFailOnException() {
            return false;
        }

        private static boolean isTracked(ActivitiEventType type) {
            return type == ActivitiEventType.ACTIVITY_STARTED
                    || type == ActivitiEventType.ACTIVITY_COMPLETED
                    || type == ActivitiEventType.ACTIVITY_CANCELLED
                    || type == ActivitiEventType.ACTIVITY_ERROR_RECEIVED;
        }

        private static boolean isGatewayLike(String activityId) {
            return activityId != null && (activityId.startsWith("node_gw")
                    || activityId.startsWith("gwfork_")
                    || activityId.startsWith("gwjoin_")
                    || "node_gw".equals(activityId));
        }

        /** Same extract the listener uses today — entity first, then in-command execution. */
        private static String extractBusinessKey(ActivitiEvent event) {
            if (event instanceof ActivitiEntityEvent entityEvent
                    && entityEvent.getEntity() instanceof DelegateExecution execution) {
                return execution.getProcessInstanceBusinessKey();
            }
            try {
                CommandContext ctx = Context.getCommandContext();
                if (ctx == null || ctx.getExecutionEntityManager() == null) {
                    return null;
                }
                if (event.getExecutionId() != null) {
                    ExecutionEntity byExecution = ctx.getExecutionEntityManager().findById(event.getExecutionId());
                    if (byExecution != null) {
                        return byExecution.getProcessInstanceBusinessKey();
                    }
                }
                if (event.getProcessInstanceId() != null) {
                    ExecutionEntity byPi = ctx.getExecutionEntityManager().findById(event.getProcessInstanceId());
                    return byPi == null ? null : byPi.getProcessInstanceBusinessKey();
                }
            } catch (RuntimeException ignored) {
                return null;
            }
            return null;
        }
    }
}
