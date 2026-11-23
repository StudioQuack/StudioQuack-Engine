package com.studioquack.sqs.gate;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.delegate.ScriptDispatchDelegate;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.support.FakeSandboxConfig;
import com.studioquack.sqs.runtime.support.FakeSandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PERFORMANCE — concurrent load. Starts {@value #LOAD} workflows in a simultaneous burst through
 * the engine + dispatch pool and asserts the default async pool (core 8 / max 32) copes: every run
 * completes (no deadlock / starvation), none dead-letters, and the whole burst drains within a
 * generous liveness budget.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-gate-load-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-gate-load-jobs.wal",
        "sqs.orphan-sweeper.enabled=false",
        // Size the JDBC pool to the concurrency under test (client burst + async executor max 32 +
        // dispatch pool) so the test measures ENGINE liveness, not a starved default 10-conn pool.
        "spring.datasource.hikari.maximum-pool-size=100",
        "spring.datasource.hikari.connection-timeout=20000"
})
@ActiveProfiles("base")
@Import(FakeSandboxConfig.class)
class ConcurrentLoadPerfTest {

    /** N >= 20 concurrent runs (the gate floor). */
    private static final int LOAD = 24;
    /** Generous liveness budget for the whole burst to drain (NOT a per-run latency assertion). */
    private static final long DRAIN_BUDGET_MS = 60_000;

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private ExecutionContextStore contextStore;
    @Autowired private FakeSandboxExecutionPort fakeSandbox;
    @Autowired private ScriptDispatchDelegate scriptDispatchDelegate;

    @BeforeEach
    void resetFake() {
        fakeSandbox.reset();
    }

    @Test
    void twenty_plus_concurrent_runs_all_complete_without_deadlock() throws Exception {
        String nodeId = "loadN";
        fakeSandbox.program(nodeId, req ->
                SandboxJobResponse.success(0, "ok", "", Map.of("out", "ok"), 1L, "fake"));

        // Deploy ONE definition, then start LOAD instances of it (each with its own sealed context ref).
        WorkflowGraph graph = linearGraph("wfLoad", nodeId);
        DeploymentInfo info = deploymentService.deploy(graph, null, true);

        long dispatchesBefore = scriptDispatchDelegate.getInvocationCount();
        List<String> wfeIds = new ArrayList<>();
        for (int i = 0; i < LOAD; i++) {
            wfeIds.add(UUID.randomUUID().toString());
        }

        // Fire all starts simultaneously from a client thread pool to simulate a real burst; the
        // latch releases every worker at once so the engine sees genuine concurrency.
        ExecutorService clients = Executors.newFixedThreadPool(LOAD);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger startFailures = new AtomicInteger();
        long t0 = System.currentTimeMillis();
        try {
            for (String wfeId : wfeIds) {
                clients.submit(() -> {
                    try {
                        go.await();
                        startRun(graph, info, nodeId, wfeId);
                    } catch (Exception e) {
                        startFailures.incrementAndGet();
                    }
                    return null;
                });
            }
            go.countDown(); // release the burst
            clients.shutdown();
            assertTrue(clients.awaitTermination(30, TimeUnit.SECONDS), "all starts were submitted");
        } finally {
            clients.shutdownNow();
        }
        assertEquals(0, startFailures.get(), "every run started cleanly under concurrent load");

        // Liveness: the whole burst drains within the generous budget — nothing stuck.
        assertTrue(awaitAllDrained(DRAIN_BUDGET_MS),
                "all " + LOAD + " runs drained (no active PIs) within the liveness budget");
        long elapsed = System.currentTimeMillis() - t0;

        // Every run finished cleanly in history; none dead-lettered; the dispatch path fired per run.
        long finished = sqsEngine.historicProcessInstances().finished().count();
        assertTrue(finished >= LOAD, "at least " + LOAD + " runs finished in history (was " + finished + ")");
        assertEquals(0, sqsEngine.processInstances().count(), "no runtime process instances remain");
        assertEquals(0, sqsEngine.deadLetterJobs().count(), "no run dead-lettered under load");
        assertTrue(scriptDispatchDelegate.getInvocationCount() - dispatchesBefore >= LOAD,
                "the dispatch delegate fired at least once per run");
        assertTrue(elapsed < DRAIN_BUDGET_MS, "the burst drained within the budget (elapsed=" + elapsed + "ms)");
    }

    // ---- fixtures ---------------------------------------------------------------------------

    /** Seals a fresh per-run context and starts one instance of the pre-deployed definition. */
    private void startRun(WorkflowGraph graph, DeploymentInfo info, String nodeId, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-load");
        ctx.setCurrentNodeId(nodeId);
        ctx.setNodes(new ArrayList<>(graph.nodes()));
        ctx.setEdges(new ArrayList<>(graph.edges()));
        ProcessContextRef ref = contextStore.snapshot(ctx, null);
        deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
    }

    private WorkflowGraph linearGraph(String workflowId, String nodeId) {
        return new WorkflowGraph(workflowId, "load", "1",
                List.of(simpleNode("startN", "start.event", null, "start"),
                        sandboxNode(nodeId),
                        simpleNode("endN", "end.event", null, "end")),
                List.of(edge("startN", nodeId), edge(nodeId, "endN")));
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

    private boolean awaitAllDrained(long budgetMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + budgetMs;
        while (System.currentTimeMillis() < deadline) {
            if (sqsEngine.processInstances().count() == 0
                    && sqsEngine.historicProcessInstances().finished().count() >= LOAD) {
                return true;
            }
            Thread.sleep(100);
        }
        return sqsEngine.processInstances().count() == 0;
    }
}
