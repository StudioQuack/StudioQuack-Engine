package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDataDto;
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
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.support.FakeSandboxConfig;
import com.studioquack.sqs.runtime.support.FakeSandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import org.activiti.engine.runtime.Execution;
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Acceptance suite — every boundary-event type the designer can attach, fired for real on the
 * embedded engine (embedded database, base profile, real async executor) with an in-process {@link
 * FakeSandboxExecutionPort} standing in for the container. Complements {@link
 * TypedErrorRepairConformanceTest} (which owns the per-code error routing and the fired-event
 * payloads) with the cells its matrix left unproven
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-boundary-e2e-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-boundary-e2e-jobs.wal",
        "sqs.sandbox.warmpool.enabled=false",
        "sqs.orphan-sweeper.enabled=false"
})
@ActiveProfiles("base")
@Import(FakeSandboxConfig.class)
class BoundaryEventEndToEndTest {

    /** Long enough for the boundary to fire before the sandbox result could ever resume the wait. */
    private static final long LATE_SANDBOX_MS = 30_000;
    /** Long enough to park + fire the boundary, short enough to keep the test under its budget. */
    private static final long SLOW_SANDBOX_MS = 6_000;

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private ExecutionContextStore contextStore;
    @Autowired private FakeSandboxExecutionPort fakeSandbox;
    @Autowired private DispatchCancellationRegistry cancellationRegistry;

    @BeforeEach
    void resetFake() {
        fakeSandbox.reset();
    }

    // ================================================================
    // MESSAGE — interrupting cancels the host; non-interrupting does not.
    // ================================================================

    @Test
    void interrupting_message_boundary_cancels_the_parked_step() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String messageName = "CancelRun_" + wfeId.substring(0, 8);
        WorkflowGraph graph = nonErrorBoundaryGraph(wfeId, BoundaryEventConfig.message(messageName, "=wfe"));
        parkSandboxFor("scriptN", LATE_SANDBOX_MS);

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);
        assertNotNull(awaitParked(pi.getId(), "wait_scriptN"), "the sandbox step parked, awaiting a result");
        Execution subscription = awaitMessageSubscription(wfeId, messageName);

        sqsEngine.messageEventReceived(messageName, subscription.getId(), Map.of());

        assertTrue(awaitCompleted(wfeId), "the correlated message ended the run");
        assertTrue(reachedActivity(pi, "node_handlerN"), "the interrupting message routed to its handler");
        assertNull(sqsEngine.findUnfinishedWaitExecution(pi.getId(), "wait_scriptN"),
                "an interrupting boundary cancels the attached wait state");
        assertFalse(reachedActivity(pi, "errgate_scriptN"),
                "cancelling the wait skips the error gate — a diverted step is never re-read as a sandbox error");
        assertFalse(reachedActivity(pi, "node_endN"), "the main flow was abandoned with the parked step");
        assertTrue(await(() -> cancellationRegistry.isStepCancelled(pi.getId(), "scriptN")),
                "interrupting boundary cancelStep marks the parked step");
    }

    @Test
    void non_interrupting_message_boundary_leaves_the_step_parked_and_the_result_completes_the_run()
            throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String messageName = "NotifyRun_" + wfeId.substring(0, 8);
        BoundaryEventConfig cfg = BoundaryEventConfig.message(messageName, "=wfe").nonInterrupting();
        WorkflowGraph graph = nonErrorBoundaryGraph(wfeId, cfg);
        // Slow but finite: the boundary fires while parked, then the real result resumes the main flow.
        parkSandboxFor("scriptN", SLOW_SANDBOX_MS);

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);
        assertNotNull(awaitParked(pi.getId(), "wait_scriptN"), "the sandbox step parked");
        Execution subscription = awaitMessageSubscription(wfeId, messageName);

        sqsEngine.messageEventReceived(messageName, subscription.getId(), Map.of());

        assertTrue(await(() -> reachedActivity(pi, "node_handlerN")),
                "a non-interrupting message still runs its handler branch");
        assertNotNull(sqsEngine.findUnfinishedWaitExecution(pi.getId(), "wait_scriptN"),
                "a non-interrupting boundary must NOT cancel the sandbox wait state");

        assertTrue(awaitCompleted(wfeId), "the late sandbox result resumed the wait and completed the run");
        assertTrue(reachedActivity(pi, "errgate_scriptN"),
                "the real sandbox result still flowed through the error gate (success = pass-through)");
        assertTrue(reachedActivity(pi, "node_endN"), "the main flow completed after the boundary branch");
        assertFalse(cancellationRegistry.isStepCancelled(pi.getId(), "scriptN"),
                "a non-interrupting boundary must not cancelStep the host");
    }

    // ================================================================
    // SIGNAL — non-interrupting host preservation + broadcast fan-out.
    // ================================================================

    @Test
    void non_interrupting_signal_boundary_leaves_the_step_parked() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String signalName = "NotifyAll_" + wfeId.substring(0, 8);
        BoundaryEventConfig cfg = BoundaryEventConfig.signal(signalName).nonInterrupting();
        WorkflowGraph graph = nonErrorBoundaryGraph(wfeId, cfg);
        parkSandboxFor("scriptN", SLOW_SANDBOX_MS);

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);
        assertNotNull(awaitParked(pi.getId(), "wait_scriptN"), "the sandbox step parked");
        assertTrue(await(() -> sqsEngine.findSignalEventExecution(wfeId, signalName) != null),
                "the boundary registered its signal subscription");

        sqsEngine.signalEventReceived(signalName, Map.of());

        assertTrue(await(() -> reachedActivity(pi, "node_handlerN")),
                "a non-interrupting signal still runs its handler branch");
        assertNotNull(sqsEngine.findUnfinishedWaitExecution(pi.getId(), "wait_scriptN"),
                "a non-interrupting signal must NOT cancel the sandbox wait state");
        assertTrue(awaitCompleted(wfeId), "the late sandbox result completed the run afterwards");
    }

    @Test
    void a_broadcast_signal_reaches_every_subscribed_instance() throws Exception {
        String wfeIdA = UUID.randomUUID().toString();
        String wfeIdB = UUID.randomUUID().toString();
        String signalName = "AbortAll_" + UUID.randomUUID().toString().substring(0, 8);
        // One deployed graph, two parked runs — the signal boundary is interrupting, so a delivered
        // signal ends whichever run it reaches.
        BoundaryEventConfig cfg = BoundaryEventConfig.signal(signalName);
        cfg.setId("b_SIGNAL");
        NodeDto sandbox = sandboxNode("scriptN");
        sandbox.getData().setBoundaryEventConfigs(List.of(cfg));
        WorkflowGraph graph = new WorkflowGraph("wfSigFanout-" + wfeIdA, "sigFanout", "1",
                List.of(start(), sandbox, end("handlerN"), end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN"),
                        boundaryEdge("scriptN", "handlerN", cfg.getId())));
        parkSandboxFor("scriptN", LATE_SANDBOX_MS);

        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessInstance piA = startInstance(graph, info, wfeIdA);
        ProcessInstance piB = startInstance(graph, info, wfeIdB);
        assertNotNull(awaitParked(piA.getId(), "wait_scriptN"), "run A parked");
        assertNotNull(awaitParked(piB.getId(), "wait_scriptN"), "run B parked");
        assertTrue(await(() -> sqsEngine.findSignalEventExecution(wfeIdA, signalName) != null
                        && sqsEngine.findSignalEventExecution(wfeIdB, signalName) != null),
                "both runs registered their own subscription to the same signal");

        sqsEngine.signalEventReceived(signalName, Map.of());

        assertTrue(awaitCompleted(wfeIdA), "the broadcast ended run A");
        assertTrue(awaitCompleted(wfeIdB), "the broadcast ended run B — not just the first subscriber");
        assertTrue(reachedActivity(piA, "node_handlerN"), "run A routed through its signal boundary");
        assertTrue(reachedActivity(piB, "node_handlerN"), "run B routed through its signal boundary");
    }

    // ================================================================
    // ERROR — caught down its own flow; uncaught still fails the run.
    // ================================================================

    @Test
    void error_boundary_catches_a_typed_sandbox_error_and_routes_down_its_own_flow() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        NodeDto sandbox = sandboxNode("scriptN");
        BoundaryEventConfig cfg = BoundaryEventConfig.error("VALIDATION_FAILED");
        cfg.setId("b_VALIDATION_FAILED");
        sandbox.getData().setBoundaryEventConfigs(List.of(cfg));
        WorkflowGraph graph = new WorkflowGraph("wfErrCaught-" + wfeId, "errCaught", "1",
                List.of(start(), sandbox, end("handlerN"), end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN"),
                        boundaryEdge("scriptN", "handlerN", cfg.getId())));
        fakeSandbox.fail("scriptN", "VALIDATION_FAILED", "bad input");

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);

        assertTrue(awaitCompleted(wfeId), "the caught-error flow reached a terminal state");
        assertTrue(reachedActivity(pi, "node_handlerN"),
                "the matched error boundary routed down its own flow to the handler");
        assertFalse(reachedErrorEnd(pi),
                "a caught error must NOT fall through to the catch-all shared error end");
        assertFalse(reachedActivity(pi, "node_endN"), "the failed step's main flow was not taken");
    }

    @Test
    void an_uncaught_sandbox_failure_holds_the_root_run_for_repair() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        // No configured error boundary: only the always-emitted catch-all stands between the
        // sandbox failure and the fail-route decision.
        WorkflowGraph graph = new WorkflowGraph("wfErrUncaught-" + wfeId, "errUncaught", "1",
                List.of(start(), sandboxNode("scriptN"), end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN")));
        fakeSandbox.fail("scriptN", "SANDBOX_FAILED", "boom");

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);

        // The catch-all fail-route HOLDS a failed ROOT run alive as a dead-letter incident (the
        // repair surface) instead of ending it at the shared error end.
        assertTrue(await(() -> sqsEngine.deadLetterJobsForProcessInstance(pi.getId()).count() > 0),
                "an uncaught failure dead-letters the hold job → a genuine Activiti incident");
        // Runtime execution query, not historic activity: a first-execution dead-letter rolls the
        // job's transaction back, so the hold task leaves no historic activity record.
        assertTrue(sqsEngine.executions().processInstanceId(pi.getId())
                        .activityId("hold_scriptN").count() > 0,
                "the token parks at the failure-hold task");
        assertFalse(reachedErrorEnd(pi), "a held ROOT run never reaches the shared error end");
        assertFalse(reachedActivity(pi, "node_endN"), "the success path was not taken");
        // The run-level FAILED signal: the sentinel survives on the live held instance, which is
        // exactly what marks the run FAILED (and what a repair clears before re-queueing).
        assertTrue(await(() -> sqsEngine.historicVariables(pi.getId())
                        .variableName(EngineVariableNames.ERROR_CODE).list().stream()
                        .anyMatch(v -> EngineVariableNames.isFailureSentinel(v.getValue()))),
                "the failure sentinel survives on the held instance — the run reads as FAILED");
    }

    // ---- fixtures ------------------------------------------------------------

    /** start → sandbox(scriptN, one non-error boundary → handlerN) → endN. */
    private WorkflowGraph nonErrorBoundaryGraph(String wfeId, BoundaryEventConfig cfg) {
        NodeDto sandbox = sandboxNode("scriptN");
        cfg.setId("b_" + cfg.getType());
        sandbox.getData().setBoundaryEventConfigs(List.of(cfg));
        return new WorkflowGraph("wfBoundaryE2E-" + wfeId, "boundaryE2E", "1",
                List.of(start(), sandbox, end("handlerN"), end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN"),
                        boundaryEdge("scriptN", "handlerN", cfg.getId())));
    }

    /** Seals the graph context (so a fired boundary can publish), deploys and starts one run. */
    private ProcessInstance seedAndStart(WorkflowGraph graph, String sandboxNodeId, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-boundary-e2e");
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

    /** Starts a second run of an already-deployed graph (own sealed context, own businessKey). */
    private ProcessInstance startInstance(WorkflowGraph graph, DeploymentInfo info, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-boundary-e2e");
        ctx.setCurrentNodeId("scriptN");
        ctx.setNodes(new ArrayList<>(graph.nodes()));
        ctx.setEdges(new ArrayList<>(graph.edges()));
        ProcessContextRef ref = contextStore.snapshot(ctx, null);
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(pi, "process instance started");
        return pi;
    }

    /** Programs the sandbox step to return a success after {@code delayMs} — a parked, then resuming, step. */
    private void parkSandboxFor(String nodeId, long delayMs) {
        fakeSandbox.program(nodeId, req -> {
            sleep(delayMs);
            return SandboxJobResponse.success(0, "ok", "", Map.of(), 1L, "fake");
        });
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

    /** The boundary's handler edge — the shape a deploy actually carries ({@code data.boundaryEventId}). */
    private EdgeDto boundaryEdge(String source, String target, String boundaryEventId) {
        EdgeDto e = edge(source, target);
        EdgeDataDto data = new EdgeDataDto();
        data.setBoundaryEventId(boundaryEventId);
        e.setData(data);
        return e;
    }

    private boolean reachedErrorEnd(ProcessInstance pi) {
        return reachedActivity(pi, ActivitiBpmnElementHelper.SHARED_ERROR_END_ID);
    }

    private boolean reachedActivity(ProcessInstance pi, String activityId) {
        return sqsEngine.historicActivityInstances(pi.getId()).list().stream()
                .anyMatch(a -> activityId.equals(a.getActivityId()));
    }

    private String awaitParked(String pi, String waitActivityId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            String exec = sqsEngine.findUnfinishedWaitExecution(pi, waitActivityId);
            if (exec != null) {
                return exec;
            }
            Thread.sleep(50);
        }
        return null;
    }

    /** Awaits the boundary's message-event subscription — the durable marker correlation consumes. */
    private Execution awaitMessageSubscription(String wfeId, String messageName) throws InterruptedException {
        assertTrue(await(() -> sqsEngine.findMessageEventExecution(wfeId, messageName) != null),
                "the boundary registered a message-event subscription for the run");
        return sqsEngine.findMessageEventExecution(wfeId, messageName);
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

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(100);
        }
        return cond.getAsBoolean();
    }
}
