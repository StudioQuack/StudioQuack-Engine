package com.studioquack.sqs.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.CancelInstanceCommandHandler;
import com.studioquack.sqs.command.ReplayPiCommandHandler;
import com.studioquack.sqs.command.RetryIncidentCommandHandler;
import com.studioquack.sqs.command.SkipStepCommandHandler;
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
import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ReceiveTask;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.engine.delegate.JavaDelegate;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Conformance — the typed sandbox-error → boundary → incident → repair chain end-to-end on the
 * embedded engine (embedded database, base profile), with an in-process {@link
 * FakeSandboxExecutionPort} standing in for the container. Covers
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-typederr-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-typederr-jobs.wal",
        "sqs.orphan-sweeper.enabled=false"
})
@ActiveProfiles("base")
@Import(FakeSandboxConfig.class)
class TypedErrorRepairConformanceTest {

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private ExecutionContextStore contextStore;
    @Autowired private FakeSandboxExecutionPort fakeSandbox;
    @Autowired private IncidentService incidentService;
    @Autowired private ObjectMapper mapper;

    @Autowired private RetryIncidentCommandHandler retryHandler;
    @Autowired private SkipStepCommandHandler skipHandler;
    @Autowired private ReplayPiCommandHandler replayHandler;
    @Autowired private CancelInstanceCommandHandler cancelHandler;

    /** Always throws with a message carrying a typed code + PHI (email/phone) → redaction target. */
    public static class AlwaysFailWithPhiDelegate implements JavaDelegate {
        @Override
        public void execute(org.activiti.engine.delegate.DelegateExecution execution) {
            throw new RuntimeException(
                    "SANDBOX_FAILED: notify john@example.com or +90 555 123 45 67 about run");
        }
    }

    /** Throws on its FIRST invocation only, so a retry succeeds deterministically. */
    public static class FlakyOnceRepairDelegate implements JavaDelegate {
        static final AtomicInteger CALLS = new AtomicInteger();

        @Override
        public void execute(org.activiti.engine.delegate.DelegateExecution execution) {
            if (CALLS.getAndIncrement() == 0) {
                throw new RuntimeException("transient boom");
            }
        }
    }

    @BeforeEach
    void resetFake() {
        fakeSandbox.reset();
    }

    // ================================================================
    // Typed error → per-code boundary (matched) vs catch-all (unmatched / untyped).
    // ================================================================

    @Test
    void typed_error_routes_to_matching_per_code_boundary() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        WorkflowGraph graph = boundaryGraph(wfeId, "VALIDATION_FAILED", "handlerN");
        // Simulated studioquack.fail("VALIDATION_FAILED", …): the fake returns a typed failure.
        fakeSandbox.fail("scriptN", "VALIDATION_FAILED", "bad input");

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);
        assertTrue(awaitCompleted(wfeId), "typed-error flow reached a terminal state");
        assertTrue(reachedActivity(pi, "node_handlerN"),
                "the matched code routed to its configured per-code handler node");
        assertFalse(reachedErrorEnd(pi),
                "a matched per-code boundary must NOT fall through to the catch-all shared error end");
    }

    @Test
    void unmatched_code_falls_through_to_catch_all_and_holds_the_root_run() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        WorkflowGraph graph = boundaryGraph(wfeId, "VALIDATION_FAILED", "handlerN");
        // A code with NO per-code boundary declared → catch-all.
        fakeSandbox.fail("scriptN", "SOME_OTHER_CODE", "unmapped");

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);
        // The catch-all fail-route HOLDS a failed ROOT run alive as a dead-letter incident (the
        // repair surface) instead of ending it at the shared error end.
        assertTrue(await(() -> sqsEngine.deadLetterJobsForProcessInstance(pi.getId()).count() > 0),
                "an unmatched code falls through to the catch-all and the ROOT run is held as an incident");
        assertTrue(parkedAt(pi.getId(), "hold_scriptN"), "the token parks at the failure-hold task");
        assertFalse(reachedErrorEnd(pi), "a held ROOT run never reaches the shared error end");
        assertFalse(reachedActivity(pi, "node_handlerN"), "the per-code handler was NOT taken for an unmatched code");
    }

    @Test
    void untyped_sandbox_failed_crash_routes_to_catch_all_and_holds_the_root_run() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        WorkflowGraph graph = boundaryGraph(wfeId, "VALIDATION_FAILED", "handlerN");
        // Untyped crash → SANDBOX_FAILED (the catch-all code), even with a per-code boundary present.
        fakeSandbox.fail("scriptN", "SANDBOX_FAILED", "untyped crash");

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);
        assertTrue(await(() -> sqsEngine.deadLetterJobsForProcessInstance(pi.getId()).count() > 0),
                "an untyped SANDBOX_FAILED crash routes to the catch-all and the ROOT run is held as an incident");
        assertTrue(parkedAt(pi.getId(), "hold_scriptN"), "the token parks at the failure-hold task");
        assertFalse(reachedErrorEnd(pi), "a held ROOT run never reaches the shared error end");
        assertFalse(reachedActivity(pi, "node_handlerN"), "the per-code handler was NOT taken for an untyped crash");
    }

    // ================================================================
    // TIMER / MESSAGE / SIGNAL boundaries on the sandbox wait state.
    // ================================================================

    @Test
    void interrupting_timer_boundary_times_out_a_parked_sandbox_step() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        ProcessInstance pi = startParked(nonErrorBoundaryGraph(wfeId, BoundaryEventConfig.timer("PT1S")), wfeId);
        assertNotNull(awaitParked(pi.getId(), "wait_scriptN"), "the sandbox step parked, awaiting a result");

        assertTrue(awaitCompleted(wfeId), "the timer fired and the run reached a terminal state");
        assertTrue(reachedActivity(pi, "node_handlerN"),
                "an interrupting timer routes to its configured handler");
        assertFalse(reachedActivity(pi, "errgate_scriptN"),
                "cancelling the wait state skips the error gate, so a timeout is never re-read as a "
                        + "typed sandbox error");
    }

    @Test
    void non_interrupting_timer_boundary_leaves_the_sandbox_step_parked() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        BoundaryEventConfig cfg = BoundaryEventConfig.timer("PT1S").nonInterrupting();
        ProcessInstance pi = startParked(nonErrorBoundaryGraph(wfeId, cfg), wfeId);
        assertNotNull(awaitParked(pi.getId(), "wait_scriptN"), "the sandbox step parked");

        assertTrue(await(() -> reachedActivity(pi, "node_handlerN")),
                "a non-interrupting timer still runs its handler branch");
        assertNotNull(sqsEngine.findUnfinishedWaitExecution(pi.getId(), "wait_scriptN"),
                "a non-interrupting timer must NOT cancel the sandbox wait state");
    }

    @Test
    void message_boundary_diverts_a_parked_sandbox_step() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String messageName = "CancelRun_" + wfeId.substring(0, 8);
        ProcessInstance pi = startParked(
                nonErrorBoundaryGraph(wfeId, BoundaryEventConfig.message(messageName, "=wfe")), wfeId);
        assertNotNull(awaitParked(pi.getId(), "wait_scriptN"), "the sandbox step parked");
        assertTrue(await(() -> sqsEngine.findMessageEventExecution(wfeId, messageName) != null),
                "the boundary registered a message-event subscription for the run");

        Execution parked = sqsEngine.findMessageEventExecution(wfeId, messageName);
        sqsEngine.messageEventReceived(messageName, parked.getId(), Map.of());

        assertTrue(awaitCompleted(wfeId), "the correlated message ended the run");
        assertTrue(reachedActivity(pi, "node_handlerN"), "the message boundary routes to its handler");
    }

    @Test
    void signal_boundary_diverts_a_parked_sandbox_step() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        // A unique name per run: a boundary signal is globally scoped, which is what makes it
        // deliverable at all (SignalEventReceivedCmd skips processInstance-scoped subscriptions).
        String signalName = "AbortRun_" + wfeId.substring(0, 8);
        ProcessInstance pi = startParked(
                nonErrorBoundaryGraph(wfeId, BoundaryEventConfig.signal(signalName)), wfeId);
        assertNotNull(awaitParked(pi.getId(), "wait_scriptN"), "the sandbox step parked");

        sqsEngine.signalEventReceived(signalName, Map.of());

        assertTrue(awaitCompleted(wfeId), "the broadcast signal ended the run");
        assertTrue(reachedActivity(pi, "node_handlerN"), "the signal boundary routes to its handler");
    }

    // ================================================================
    // A fired boundary publishes what its port's output schema promises.
    // ================================================================

    @Test
    void timer_boundary_publishes_its_promised_variables_to_the_handler() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        BoundaryEventConfig cfg = BoundaryEventConfig.timer("PT1S");
        WorkflowGraph graph = nonErrorBoundaryGraph(wfeId, cfg);
        parkOnALateSandbox("scriptN");

        seedAndStart(graph, "scriptN", wfeId);

        Map<String, Object> published = awaitPublished(wfeId, "handlerN");
        assertEquals("b_TIMER", published.get("eventId"), "the handler is told WHICH boundary fired");
        assertEquals("duration", published.get("timerType"), "the configured timer kind is carried");
        assertEquals("PT1S", published.get("timerValue"), "the configured due value is carried");
        assertNotNull(OffsetDateTime.parse(String.valueOf(published.get("firedAt"))),
                "firedAt is an ISO-8601 instant the mapping can read");
    }

    @Test
    void message_boundary_publishes_its_name_and_correlation_key() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String messageName = "CancelRun_" + wfeId.substring(0, 8);
        WorkflowGraph graph = nonErrorBoundaryGraph(wfeId, BoundaryEventConfig.message(messageName, "=wfe"));
        parkOnALateSandbox("scriptN");

        seedAndStart(graph, "scriptN", wfeId);
        assertTrue(await(() -> sqsEngine.findMessageEventExecution(wfeId, messageName) != null),
                "the boundary registered its message subscription");
        Execution parked = sqsEngine.findMessageEventExecution(wfeId, messageName);
        sqsEngine.messageEventReceived(messageName, parked.getId(), Map.of());

        Map<String, Object> published = awaitPublished(wfeId, "handlerN");
        assertEquals(messageName, published.get("messageName"), "the message the boundary caught is carried");
        assertEquals(wfeId, published.get("correlationKey"),
                "correlationKey is the run business key the message actually correlated on");
    }

    @Test
    void signal_boundary_publishes_the_signal_it_caught() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String signalName = "AbortRun_" + wfeId.substring(0, 8);
        WorkflowGraph graph = nonErrorBoundaryGraph(wfeId, BoundaryEventConfig.signal(signalName));
        parkOnALateSandbox("scriptN");

        seedAndStart(graph, "scriptN", wfeId);
        assertTrue(await(() -> sqsEngine.findSignalEventExecution(wfeId, signalName) != null),
                "the boundary registered its signal subscription");
        sqsEngine.signalEventReceived(signalName, Map.of());

        assertEquals(signalName, awaitPublished(wfeId, "handlerN").get("signalName"),
                "the signal the boundary caught is carried to its handler");
    }

    @Test
    void error_boundary_publishes_the_code_message_and_source_node() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        WorkflowGraph graph = boundaryGraph(wfeId, "VALIDATION_FAILED", "handlerN");
        fakeSandbox.fail("scriptN", "VALIDATION_FAILED", "bad input");

        seedAndStart(graph, "scriptN", wfeId);

        Map<String, Object> published = awaitPublished(wfeId, "handlerN");
        assertEquals("b_VALIDATION_FAILED", published.get("eventId"), "the boundary identifies itself");
        assertEquals("VALIDATION_FAILED", published.get("errorCode"), "the raised code reaches the handler");
        assertEquals("bad input", published.get("errorMessage"), "the failure detail reaches the handler");
        assertEquals("scriptN", published.get("errorSource"), "the handler is told which step failed");
    }

    @Test
    void two_boundaries_on_one_node_publish_under_separate_keys() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        NodeDto sandbox = sandboxNode("scriptN");
        // Both non-interrupting: an interrupting boundary abandons the step, so it is the only one that
        // can ever fire on it — two events reach the same handler only when the first leaves it running.
        BoundaryEventConfig fast = BoundaryEventConfig.timer("PT1S").nonInterrupting();
        fast.setId("b_timer_fast");
        BoundaryEventConfig slow = BoundaryEventConfig.timer("PT2S").nonInterrupting();
        slow.setId("b_timer_slow");
        sandbox.getData().setBoundaryEventConfigs(List.of(fast, slow));
        WorkflowGraph graph = new WorkflowGraph("wfTwoBoundaries-" + wfeId, "twoBoundaries", "1",
                List.of(start(), sandbox, end("handlerN"), end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN"),
                        boundaryEdge("scriptN", "handlerN", fast.getId()),
                        boundaryEdge("scriptN", "handlerN", slow.getId())));
        parkOnALateSandbox("scriptN");

        seedAndStart(graph, "scriptN", wfeId);

        // Both feed the SAME handler: the two payloads must survive as separate wires, not collapse.
        assertTrue(await(() -> publishedTo(wfeId, "handlerN").size() == 2),
                () -> "expected one wire per boundary, got " + publishedTo(wfeId, "handlerN"));
        List<InOutValueDto> wires = publishedTo(wfeId, "handlerN");
        assertEquals(List.of("b_timer_fast", "b_timer_slow"),
                wires.stream().map(InOutValueDto::getPortValue).sorted().toList(),
                "each boundary publishes under its own port value");
        assertEquals(List.of("b_timer_fast", "b_timer_slow"),
                wires.stream().map(w -> String.valueOf(payloadOf(w).get("eventId"))).sorted().toList(),
                "and each payload names its own boundary");
    }

    @Test
    void a_boundary_without_a_handler_publishes_nothing() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        NodeDto sandbox = sandboxNode("scriptN");
        BoundaryEventConfig cfg = BoundaryEventConfig.timer("PT1S");
        cfg.setId("b_orphan");
        sandbox.getData().setBoundaryEventConfigs(List.of(cfg));
        // No boundary edge at all → the boundary routes to the shared error end, so there is no
        // handler to address a payload to.
        WorkflowGraph graph = new WorkflowGraph("wfOrphanBoundary-" + wfeId, "orphanBoundary", "1",
                List.of(start(), sandbox, end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN")));
        parkOnALateSandbox("scriptN");

        ProcessInstance pi = seedAndStart(graph, "scriptN", wfeId);
        assertTrue(await(() -> reachedErrorEnd(pi)), "the handler-less boundary took the shared error end");
        assertTrue(contextStore.collectOutputsByNode(wfeId).values().stream()
                        .flatMap(List::stream)
                        .noneMatch(v -> "b_orphan".equals(v.getPortValue())),
                "nothing is sealed for a boundary that has nowhere to send it");
    }

    // ================================================================
    // Incident (dead-letter) visibility + redaction before persist/log.
    // ================================================================

    @Test
    void incident_is_visible_with_redacted_context_and_typed_code() throws Exception {
        String key = "Process_inc_" + UUID.randomUUID().toString().substring(0, 8);
        sqsEngine.deploy("inc", Map.of("inc.bpmn",
                asyncTaskThenEndXml(key, AlwaysFailWithPhiDelegate.class.getName())));
        String wfeId = UUID.randomUUID().toString();
        ProcessInstance pi = sqsEngine.startLatest(key, wfeId, Map.of());

        assertTrue(await(() -> incidentService.countForProcessInstance(pi.getId()) > 0),
                "the failing async job dead-lettered → an incident is visible");

        List<IncidentService.IncidentView> incidents = incidentService.listForProcessInstance(pi.getId());
        assertEquals(1, incidents.size(), "exactly one incident for the run");
        IncidentService.IncidentView inc = incidents.get(0);

        assertEquals(pi.getId(), inc.processInstanceId(), "incident is keyed by process-instance identity");
        assertEquals("SANDBOX_FAILED", inc.errorCode(), "the typed code is carried on the incident");
        assertTrue(inc.message().contains("SANDBOX_FAILED"), "the code text is visible in the message");
        assertFalse(inc.message().contains("john@example.com"),
                "PHI (email) MUST be redacted before the incident is surfaced/persisted");
        assertTrue(inc.message().contains("[redacted-email]"), "the email was redacted by the Redactor");
        assertFalse(inc.message().contains("555 123 45 67"), "PHI (phone) MUST be redacted too");
    }

    // ================================================================
    // Repair handlers — each executes the right SqsEngine/engine op.
    // ================================================================

    @Test
    void retry_incident_command_recovers_a_dead_letter() throws Exception {
        FlakyOnceRepairDelegate.CALLS.set(0);
        String key = "Process_retryCmd_" + UUID.randomUUID().toString().substring(0, 8);
        sqsEngine.deploy("retryCmd", Map.of("r.bpmn",
                asyncTaskThenEndXml(key, FlakyOnceRepairDelegate.class.getName())));
        String wfeId = UUID.randomUUID().toString();
        ProcessInstance pi = sqsEngine.startLatest(key, wfeId, Map.of());
        assertTrue(await(() -> sqsEngine.deadLetterJobsForProcessInstance(pi.getId()).count() > 0),
                "the flaky first run dead-lettered");

        DaemonCommandOutcome outcome = retryHandler.handle(
                cmd(RetryIncidentCommandHandler.TYPE, Map.of("processInstanceId", pi.getId())));

        assertTrue(outcome.succeeded(), () -> "retry-incident error: " + outcome.error());
        assertTrue(await(() -> sqsEngine.processInstances().processInstanceId(pi.getId()).count() == 0),
                "after retry-incident the job re-ran and the run completed");
    }

    @Test
    void skip_step_command_skips_a_parked_sandbox_step() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        WorkflowGraph graph = twoNodeSandbox(wfeId, "workN");
        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        // Bare ref → the dispatch delegate parks the ReceiveTask WITHOUT delivering a result (stays parked).
        ProcessContextRef bareRef = ProcessContextRef.builder()
                .executionId(wfeId).workflowExecutionId(wfeId).envId("env").schemaVersion(1).build();
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), true);
        assertNotNull(awaitParked(pi.getId(), "wait_workN"), "the sandbox step parked before skip");

        DaemonCommandOutcome outcome = skipHandler.handle(cmd(SkipStepCommandHandler.TYPE,
                Map.of("processInstanceId", pi.getId(), "targetFlowNodeId", "workN")));

        assertTrue(outcome.succeeded(), () -> "skip-step error: " + outcome.error());
        // An operator skip must never be held: SQ_SKIP routes through the same catch-all as any
        // failure, but the run ENDS at the shared error end exactly as it did before the hold
        // path existed — a skip is a deliberate abandon-node decision, not a repairable incident.
        assertTrue(await(() -> reachedErrorEnd(pi)),
                "skip-step raised SQ_SKIP → the run ends at the shared error end");
        assertFalse(parkedAt(pi.getId(), "hold_workN"),
                "an operator skip never parks at the failure-hold task");
    }

    @Test
    void replay_pi_command_restarts_from_seed_by_process_instance_id() throws Exception {
        String key = "Process_replayCmd_" + UUID.randomUUID().toString().substring(0, 8);
        sqsEngine.deploy("replayCmd", Map.of("rp.bpmn", startReceiveEndXml(key)));
        String wfeId = UUID.randomUUID().toString();

        // A recoverable seed ref (sealed context) so the replay guard can reconstruct the seed.
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setCurrentNodeId("seed");
        ProcessContextRef seedRef = contextStore.snapshot(ctx, null);
        ProcessInstance original = sqsEngine.startLatest(key, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, seedRef));
        assertNotNull(awaitParked(original.getId(), "wait_r"), "original run parked and is running");

        DaemonCommandOutcome outcome = replayHandler.handle(
                cmd(ReplayPiCommandHandler.TYPE, Map.of("processInstanceId", original.getId())));

        assertTrue(outcome.succeeded(), () -> "replay-pi error: " + outcome.error());
        assertTrue(await(() -> sqsEngine.processInstances().processInstanceId(original.getId()).count() == 0),
                "replay-pi cancelled the original instance");
        ProcessInstance replayed = sqsEngine.processInstances().processInstanceBusinessKey(wfeId).singleResult();
        assertNotNull(replayed, "a replayed instance exists for the same workflowExecutionId business key");
        assertNotEquals(original.getId(), replayed.getId(), "replay started a NEW process instance");
    }

    @Test
    void cancel_instance_command_terminates_a_run() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        WorkflowGraph graph = twoNodeSandbox(wfeId, "workN");
        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessContextRef bareRef = ProcessContextRef.builder()
                .executionId(wfeId).workflowExecutionId(wfeId).envId("env").schemaVersion(1).build();
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), true);
        assertNotNull(awaitParked(pi.getId(), "wait_workN"), "parked before cancel");

        DaemonCommandOutcome outcome = cancelHandler.handle(cmd(CancelInstanceCommandHandler.TYPE,
                Map.of("processInstanceId", pi.getId(), "reason", "test cancel")));

        assertTrue(outcome.succeeded(), () -> "cancel-instance error: " + outcome.error());
        assertTrue(await(() -> sqsEngine.processInstances().processInstanceId(pi.getId()).count() == 0),
                "cancel-instance removed the run from the runtime");
        assertTrue(sqsEngine.historicProcessInstances().processInstanceId(pi.getId()).count() > 0,
                "the cancelled run is recorded in history");
    }

    // ---- fixtures ------------------------------------------------------------

    /** start → sandbox(scriptN, per-code ERROR boundary code→handler) → endN; + structural handler node. */
    private WorkflowGraph boundaryGraph(String wfeId, String errorCode, String handlerNodeId) {
        NodeDto sandbox = sandboxNode("scriptN");
        BoundaryEventConfig cfg = BoundaryEventConfig.error(errorCode);
        cfg.setId("b_" + errorCode);
        sandbox.getData().setBoundaryEventConfigs(List.of(cfg));
        return new WorkflowGraph("wfBoundary-" + wfeId, "boundary", "1",
                List.of(start(), sandbox, end(handlerNodeId), end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN"),
                        boundaryEdge("scriptN", handlerNodeId, cfg.getId())));
    }

    /** start → sandbox(scriptN, one non-error boundary → handlerN) → endN. */
    private WorkflowGraph nonErrorBoundaryGraph(String wfeId, BoundaryEventConfig cfg) {
        NodeDto sandbox = sandboxNode("scriptN");
        cfg.setId("b_" + cfg.getType());
        sandbox.getData().setBoundaryEventConfigs(List.of(cfg));
        return new WorkflowGraph("wfNonErrBoundary-" + wfeId, "nonErrBoundary", "1",
                List.of(start(), sandbox, end("handlerN"), end("endN")),
                List.of(edge("startN", "scriptN"), edge("scriptN", "endN"),
                        boundaryEdge("scriptN", "handlerN", cfg.getId())));
    }

    /** Starts with a bare ref, so the dispatch delegate parks the ReceiveTask without a result. */
    private ProcessInstance startParked(WorkflowGraph graph, String wfeId) {
        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessContextRef bareRef = ProcessContextRef.builder()
                .executionId(wfeId).workflowExecutionId(wfeId).envId("env").schemaVersion(1).build();
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), true);
        assertNotNull(pi, "process instance started");
        return pi;
    }

    private WorkflowGraph twoNodeSandbox(String wfeId, String nodeId) {
        return new WorkflowGraph("wfSkip-" + wfeId, "skip", "1",
                List.of(start(), sandboxNode(nodeId), end("endN")),
                List.of(edge("startN", nodeId), edge(nodeId, "endN")));
    }

    private ProcessInstance seedAndStart(WorkflowGraph graph, String sandboxNodeId, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-typederr");
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

    /**
     * The boundary's handler edge, carrying the bare boundary-event id. This — NOT
     * {@code BoundaryEventConfig.targetNodeId} — is the shape a deploy actually receives: the config
     * field is transient and is dropped when the workflow document is stored.
     */
    private EdgeDto boundaryEdge(String source, String target, String boundaryEventId) {
        EdgeDto e = edge(source, target);
        EdgeDataDto data = new EdgeDataDto();
        data.setBoundaryEventId(boundaryEventId);
        e.setData(data);
        return e;
    }

    /** Raw BPMN: start → async serviceTask(delegateClass) → end (dead-letters on throw). */
    private String asyncTaskThenEndXml(String processKey, String delegateClass) {
        BpmnModel model = new BpmnModel();
        model.setTargetNamespace("http://studioquack.com/bpmn");
        Process p = new Process();
        p.setId(processKey);
        p.setExecutable(true);
        model.addProcess(p);
        StartEvent s = new StartEvent();
        s.setId("s");
        p.addFlowElement(s);
        ServiceTask t = new ServiceTask();
        t.setId("t");
        t.setName("work");
        t.setAsynchronous(true);
        t.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_CLASS);
        t.setImplementation(delegateClass);
        p.addFlowElement(t);
        EndEvent e = new EndEvent();
        e.setId("e");
        p.addFlowElement(e);
        p.addFlowElement(flow("f_s_t", "s", "t"));
        p.addFlowElement(flow("f_t_e", "t", "e"));
        return toXml(model);
    }

    /** Raw BPMN: start → receiveTask(wait_r) → end. Parks (never triggered) so the run stays running. */
    private String startReceiveEndXml(String processKey) {
        BpmnModel model = new BpmnModel();
        model.setTargetNamespace("http://studioquack.com/bpmn");
        Process p = new Process();
        p.setId(processKey);
        p.setExecutable(true);
        model.addProcess(p);
        StartEvent s = new StartEvent();
        s.setId("s");
        p.addFlowElement(s);
        ReceiveTask r = new ReceiveTask();
        r.setId("wait_r");
        r.setName("park");
        p.addFlowElement(r);
        EndEvent e = new EndEvent();
        e.setId("e");
        p.addFlowElement(e);
        p.addFlowElement(flow("f_s_r", "s", "wait_r"));
        p.addFlowElement(flow("f_r_e", "wait_r", "e"));
        return toXml(model);
    }

    private SequenceFlow flow(String id, String from, String to) {
        SequenceFlow f = new SequenceFlow(from, to);
        f.setId(id);
        return f;
    }

    private String toXml(BpmnModel model) {
        return new String(new BpmnXMLConverter().convertToXML(model), StandardCharsets.UTF_8);
    }

    private DaemonCommand cmd(String type, Map<String, Object> payload) throws Exception {
        return new DaemonCommand(UUID.randomUUID().toString(), type, null, mapper.writeValueAsString(payload));
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

    /**
     * Runtime-execution parked check for the failure-hold task. NOT the historic-activity query:
     * a first-execution dead-letter rolls the job's transaction back, so the hold task leaves no
     * historic activity record while the execution stays parked.
     */
    private boolean parkedAt(String pi, String activityId) {
        return sqsEngine.executions().processInstanceId(pi).activityId(activityId).count() > 0;
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

    /**
     * Parks a sandbox step the way a real slow container does, rather than with {@code startParked}'s
     * bare ref: a fired boundary reads the run's sealed context, which a bare ref cannot address. The
     * return is late enough to outlast the async executor's timer-acquire cycle, so the step is still
     * parked when a one-second boundary timer is picked up.
     */
    private void parkOnALateSandbox(String nodeId) {
        fakeSandbox.program(nodeId, req -> {
            sleep(30_000);
            return SandboxJobResponse.success(0, "ok", "", Map.of(), 1L, "fake");
        });
    }

    /** The wires a boundary sealed for {@code handlerNodeId} — what its own dispatch would read. */
    private List<InOutValueDto> publishedTo(String wfeId, String handlerNodeId) {
        ProcessContextRef byExecution = ProcessContextRef.builder().executionId(wfeId).build();
        return contextStore.loadInputsTargetingNode(byExecution, handlerNodeId);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> payloadOf(InOutValueDto wire) {
        return wire.getValue() instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    /** Waits for the single payload a fired boundary addressed to {@code handlerNodeId}. */
    private Map<String, Object> awaitPublished(String wfeId, String handlerNodeId) throws InterruptedException {
        assertTrue(await(() -> !publishedTo(wfeId, handlerNodeId).isEmpty()),
                "the fired boundary sealed a payload addressed to its handler");
        List<InOutValueDto> wires = publishedTo(wfeId, handlerNodeId);
        assertEquals(1, wires.size(), "exactly one boundary payload for this handler");
        return payloadOf(wires.get(0));
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
