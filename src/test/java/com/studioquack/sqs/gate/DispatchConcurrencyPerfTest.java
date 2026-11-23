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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PERFORMANCE tests #4 + #6 — the non-blocking ReceiveTask cutover means a parked sandbox job holds
 * no engine async-executor thread, and a slow node never blocks unrelated runs. Both are relative /
 * liveness assertions (no brittle absolute latencies).
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-gate-perf-conc-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-gate-perf-conc-jobs.wal",
        "sqs.orphan-sweeper.enabled=false",
        // Deliberately tiny engine async pool: proves a parked token does NOT pin an engine thread.
        "sqs.activiti.async-executor.core-pool-size=2",
        "sqs.activiti.async-executor.max-pool-size=2"
        // The dispatch executor is virtual-thread-per-task (uncapped): N sandbox sleeps always
        // run concurrently off the engine threads — no pool-size knob remains to pin here.
})
@ActiveProfiles("base")
@Import(FakeSandboxConfig.class)
class DispatchConcurrencyPerfTest {

    private static final int CONCURRENT_RUNS = 8;       // > the 2-thread async pool on purpose
    private static final long PARKED_SANDBOX_MS = 2500;  // sandbox wall-time to widen the all-parked window
    private static final long SLOW_SANDBOX_MS = 6000;    // #6 slow node — far longer than a fast run

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private ExecutionContextStore contextStore;
    @Autowired private FakeSandboxExecutionPort fakeSandbox;

    @BeforeEach
    void resetFake() {
        fakeSandbox.reset();
    }

    // -----------------------------------------------------------------------------------------
    // #4 — many concurrent parked ReceiveTasks with a tiny async pool (no thread pinning).
    // -----------------------------------------------------------------------------------------

    @Test
    void concurrent_runs_all_park_without_pinning_the_small_async_pool() throws Exception {
        String nodeId = "concN";
        // Every run shares this node id → one programmed response drives them all. The sandbox sleeps
        // on the dispatch pool (not the engine thread), simulating real container wall-time.
        fakeSandbox.program(nodeId, req -> {
            sleepQuietly(PARKED_SANDBOX_MS);
            return SandboxJobResponse.success(0, "ok", "", Map.of("out", "done"), 1L, "fake");
        });

        List<String> piIds = new ArrayList<>();
        List<String> wfeIds = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_RUNS; i++) {
            String wfeId = UUID.randomUUID().toString();
            WorkflowGraph graph = linearGraph("wfConc-" + i + "-" + wfeId, nodeId);
            ProcessInstance pi = seedAndStart(graph, nodeId, wfeId);
            piIds.add(pi.getId());
            wfeIds.add(wfeId);
        }

        // Sample the parked count until every run is simultaneously parked (or we give up). If a
        // parked token pinned an engine thread, at most 2 (the async pool size) could be parked/in
        // flight at once and this peak could never reach CONCURRENT_RUNS.
        int peakParked = 0;
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline && peakParked < CONCURRENT_RUNS) {
            peakParked = Math.max(peakParked, countParked(piIds, "wait_" + nodeId));
            Thread.sleep(20);
        }
        assertEquals(CONCURRENT_RUNS, peakParked,
                "all " + CONCURRENT_RUNS + " tokens parked at once despite a 2-thread async pool "
                        + "(a parked ReceiveTask holds no engine thread)");

        // Liveness: every run then resumes and completes (no deadlock / starvation).
        for (String wfeId : wfeIds) {
            assertTrue(awaitCompleted(wfeId), "run " + wfeId + " completed after its sandbox returned");
        }
        assertEquals(0, sqsEngine.deadLetterJobs().count(), "no run dead-lettered");
    }

    // -----------------------------------------------------------------------------------------
    // #6 — a slow node does not block unrelated fast runs (parked token frees the engine thread).
    // -----------------------------------------------------------------------------------------

    @Test
    void a_slow_node_does_not_block_unrelated_fast_runs() throws Exception {
        String slowNode = "slowN";
        String fastNode = "fastN";
        fakeSandbox.program(slowNode, req -> {
            sleepQuietly(SLOW_SANDBOX_MS);
            return SandboxJobResponse.success(0, "ok", "", Map.of("out", "slow-done"), 1L, "fake");
        });
        fakeSandbox.succeed(fastNode, Map.of("out", "fast-done")); // returns immediately

        // Start the slow run FIRST so it is already occupying a dispatch slot when the fast runs land.
        String slowWfe = UUID.randomUUID().toString();
        WorkflowGraph slowGraph = linearGraph("wfSlow-" + slowWfe, slowNode);
        ProcessInstance slowPi = seedAndStart(slowGraph, slowNode, slowWfe);

        int fastCount = 5;
        List<String> fastWfes = new ArrayList<>();
        for (int i = 0; i < fastCount; i++) {
            String wfeId = UUID.randomUUID().toString();
            WorkflowGraph fastGraph = linearGraph("wfFast-" + i + "-" + wfeId, fastNode);
            seedAndStart(fastGraph, fastNode, wfeId);
            fastWfes.add(wfeId);
        }

        // All fast runs complete (generous liveness timeout — we do NOT bound how fast, only that
        // they finish before the slow one). No wall-clock latency assertion → robust on slow CI.
        for (String wfeId : fastWfes) {
            assertTrue(awaitCompleted(wfeId), "fast run " + wfeId + " completed");
        }

        // The crisp isolation proof: at the instant every fast run is done, the slow run is STILL
        // running. The only way this fails is if the fast runs waited on the slow node (>6s) —
        // i.e. the slow node blocked them. It did not: its parked token freed the engine thread.
        assertTrue(sqsEngine.isProcessInstanceActive(slowPi.getId()),
                "the slow run is still in flight while all fast runs already completed (no blocking)");

        // Liveness: the slow run also eventually completes.
        assertTrue(awaitCompleted(slowWfe), "the slow run itself eventually completed");
        assertEquals(0, sqsEngine.deadLetterJobs().count(), "no run dead-lettered");
    }

    // ---- fixtures ---------------------------------------------------------------------------

    private int countParked(List<String> piIds, String waitActivityId) {
        int parked = 0;
        for (String pi : piIds) {
            if (sqsEngine.findUnfinishedWaitExecution(pi, waitActivityId) != null) {
                parked++;
            }
        }
        return parked;
    }

    private ProcessInstance seedAndStart(WorkflowGraph graph, String sandboxNodeId, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-perf");
        ctx.setCurrentNodeId(sandboxNodeId);
        ctx.setNodes(new ArrayList<>(graph.nodes()));
        ctx.setEdges(new ArrayList<>(graph.edges()));
        ProcessContextRef ref = contextStore.snapshot(ctx, null);

        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(pi, "process instance started");
        return pi;
    }

    private WorkflowGraph linearGraph(String workflowId, String nodeId) {
        return new WorkflowGraph(workflowId, "perf", "1",
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

    private boolean awaitCompleted(String wfeId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            if (sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0
                    && sqsEngine.historicProcessInstances().processInstanceBusinessKey(wfeId)
                    .finished().count() > 0) {
                return true;
            }
            Thread.sleep(50);
        }
        return sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
