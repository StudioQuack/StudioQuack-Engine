package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.sqs.SqsClientDaemonApplication;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Timer durability proof — a boundary timer is a persisted job ({@code ACT_RU_TIMER_JOB}), not an
 * in-memory scheduled task. The test boots the real daemon application twice against one named
 * in-memory database (kept alive across boots by {@code DB_CLOSE_DELAY=-1})
 */
class BoundaryEventTimerRestartTest {

    /** The boundary duration — due long before boot B's executor finishes its first acquire cycle. */
    private static final String TIMER_DURATION = "PT2S";

    @Test
    void a_timer_boundary_survives_an_engine_restart_and_fires() throws Exception {
        String dbUrl = "jdbc:h2:mem:sqs-timer-restart-" + UUID.randomUUID().toString().substring(0, 8)
                + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
        String wfeId = UUID.randomUUID().toString();
        String processInstanceId;

        // ---- Boot A: deploy, start, park, prove the job is persisted, then shut down. ----
        try (ConfigurableApplicationContext ctxA = boot(dbUrl)) {
            WorkflowDeploymentService deploymentService = ctxA.getBean(WorkflowDeploymentService.class);
            SqsEngine engineA = ctxA.getBean(SqsEngine.class);

            WorkflowGraph graph = timerBoundaryGraph(wfeId);
            DeploymentInfo info = deploymentService.deploy(graph, null, true);
            // Bare ref → the dispatch delegate parks the ReceiveTask without a sandbox result.
            ProcessContextRef bareRef = ProcessContextRef.builder()
                    .executionId(wfeId).workflowExecutionId(wfeId).envId("env-restart").schemaVersion(1).build();
            ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                    Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), true);
            assertNotNull(pi, "process instance started");
            processInstanceId = pi.getId();

            assertNotNull(awaitParked(engineA, processInstanceId, "wait_scriptN"),
                    "the sandbox step parked on its wait state");
            assertTrue(await(() -> engineA.countTimerJobsForProcessInstance(processInstanceId) == 1),
                    "the timer boundary scheduled a persisted job for the parked run");
        }

        // ---- Boot B: same database, fresh engine. The durable job must fire on its own. ----
        try (ConfigurableApplicationContext ctxB = boot(dbUrl)) {
            SqsEngine engineB = ctxB.getBean(SqsEngine.class);

            assertTrue(await(() -> reachedActivity(engineB, processInstanceId, "node_handlerN"), 60_000),
                    "after the restart the persisted timer job fired and routed to the boundary handler");
            assertTrue(await(() -> engineB.historicProcessInstances()
                            .processInstanceId(processInstanceId).finished().count() > 0),
                    "the run completed in the second boot");
            assertFalse(reachedActivity(engineB, processInstanceId, "errgate_scriptN"),
                    "the interrupting timer cancelled the wait before the error gate");
            assertEquals(0, engineB.countTimerJobsForProcessInstance(processInstanceId),
                    "the fired timer job was consumed, not left behind");
        }
    }

    // ---- fixtures ------------------------------------------------------------

    /**
     * Boots the real daemon application against {@code dbUrl} (random web port, base profile).
     * Overrides go as COMMAND-LINE arguments, not default properties: profile-specific YAML ({@code
     * application-base.yml}) outranks {@code SpringApplicationBuilder.properties}, and the base
     * file embedded database URL must never win over the test's in-memory one.
     */
    private ConfigurableApplicationContext boot(String dbUrl) {
        return new SpringApplicationBuilder(SqsClientDaemonApplication.class)
                .web(WebApplicationType.SERVLET)
                .run(
                        "--spring.profiles.active=base",
                        "--spring.datasource.url=" + dbUrl,
                        "--spring.datasource.driver-class-name=org.h2.Driver",
                        "--spring.datasource.username=sa",
                        "--spring.datasource.password=",
                        "--spring.flyway.enabled=false",
                        "--otel.sdk.disabled=true",
                        "--server.port=0",
                        "--sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-timer-restart-jobs.wal",
                        "--sqs.sandbox.warmpool.enabled=false",
                        "--sqs.orphan-sweeper.enabled=false");
    }

    /** start → sandbox(scriptN, interrupting timer boundary → handlerN) → endN. */
    private WorkflowGraph timerBoundaryGraph(String wfeId) {
        BoundaryEventConfig cfg = BoundaryEventConfig.timer(TIMER_DURATION);
        cfg.setId("b_TIMER");
        NodeDto sandbox = sandboxNode("scriptN");
        sandbox.getData().setBoundaryEventConfigs(List.of(cfg));
        return new WorkflowGraph("wfTimerRestart-" + wfeId, "timerRestart", "1",
                List.of(start(), sandbox, end("handlerN"), end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN"),
                        boundaryEdge("scriptN", "handlerN", cfg.getId())));
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
        out.setId("out"); out.setName("out"); out.setValue("out"); out.setType(PortType.OUTPUT);
        n.getData().setPorts(new ArrayList<>(List.of(out)));
        return n;
    }

    private NodeDto simpleNode(String id, String bpmnType, String jobType, String nodeType) {
        TemplateDto t = new TemplateDto();
        t.setBpmnType(bpmnType); t.setJobType(jobType); t.setNodeType(nodeType); t.setLabel(id);
        NodeDto n = new NodeDto();
        n.setId(id); n.setData(t);
        return n;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto e = new EdgeDto();
        e.setId("e_" + source + "_" + target); e.setSource(source); e.setTarget(target);
        return e;
    }

    private EdgeDto boundaryEdge(String source, String target, String boundaryEventId) {
        EdgeDto e = edge(source, target);
        EdgeDataDto data = new EdgeDataDto();
        data.setBoundaryEventId(boundaryEventId);
        e.setData(data);
        return e;
    }

    private boolean reachedActivity(SqsEngine engine, String processInstanceId, String activityId) {
        return engine.historicActivityInstances(processInstanceId).list().stream()
                .anyMatch(a -> activityId.equals(a.getActivityId()));
    }

    private String awaitParked(SqsEngine engine, String pi, String waitActivityId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            String exec = engine.findUnfinishedWaitExecution(pi, waitActivityId);
            if (exec != null) {
                return exec;
            }
            Thread.sleep(50);
        }
        return null;
    }

    private boolean await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        return await(cond, 20000);
    }

    private boolean await(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(100);
        }
        return cond.getAsBoolean();
    }
}
