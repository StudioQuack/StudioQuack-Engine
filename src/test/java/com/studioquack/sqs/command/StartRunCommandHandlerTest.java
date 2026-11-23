package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import com.studioquack.sqs.status.RunStatusCallback;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code start-run} command handler. After a deploy, a pushed {@code start-run} command starts
 * a process instance with {@code businessKey == workflowExecutionId} and REF-ONLY variables, and
 * reports a RUNNING status callback.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-startrun-cmd-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
@Import(StartRunCommandHandlerTest.RecordingConfig.class)
class StartRunCommandHandlerTest {

    @Autowired
    private StartRunCommandHandler startRunHandler;
    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private RecordingRunStatusCallback callback;
    @Autowired
    private com.studioquack.sqs.command.store.DeployedArtifactStore artifactStore;
    /** Mocked so a test can make the library unusable; default mock = no block reason. */
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager nodeLibraryManager;

    @Test
    void starts_pi_with_business_key_and_ref_only_vars_and_reports_running() throws Exception {
        String workflowId = "wfStartRun";
        deployUserTaskWorkflow(workflowId);
        String wfeId = UUID.randomUUID().toString();

        StartRunCommandPayload payload = new StartRunCommandPayload(
                wfeId, "Process_" + workflowId, null, Map.of("route__seed", "ok"), null);
        DaemonCommandOutcome outcome = startRunHandler.handle(
                new DaemonCommand("c-start-1", StartRunCommandHandler.TYPE, null, mapper.writeValueAsString(payload)));

        assertTrue(outcome.succeeded(), () -> "start-run error: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        String processInstanceId = (String) out.get("processInstanceId");
        assertNotNull(processInstanceId, "processInstanceId returned");

        // businessKey == workflowExecutionId, and exactly one running instance.
        assertEquals(1L, sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count(),
                "exactly one running PI with businessKey == workflowExecutionId");
        assertEquals(wfeId, sqsEngine.processInstances().processInstanceBusinessKey(wfeId)
                .singleResult().getBusinessKey());

        // REF-ONLY vars only reached the engine (Mapping Invariant 15).
        List<String> varNames = sqsEngine.historicVariables(processInstanceId).list().stream()
                .map(v -> v.getVariableName()).toList();
        assertTrue(varNames.stream().allMatch(RefOnlyVariableGuard::isAllowed),
                "only REF-ONLY / routing / epoch tokens on the engine, got: " + varNames);
        assertTrue(varNames.contains("route__seed"), "the seeded route token carried through");

        // RUNNING status callback reported at start.
        assertTrue(callback.received(wfeId, RunStatusCallback.Status.RUNNING),
                "RUNNING reported for the run; events=" + callback.events());
    }

    @Test
    void seeds_ref_context_and_places_studioquack_ref_on_the_instance() throws Exception {
        String workflowId = "wfStartRunSeed";
        deployUserTaskWorkflow(workflowId);
        String wfeId = UUID.randomUUID().toString();

        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowId(workflowId);
        seed.setEnvId("env-startrun");
        seed.setCurrentNodeId("taskN");

        StartRunCommandPayload payload = new StartRunCommandPayload(
                wfeId, "Process_" + workflowId, null, null, seed);
        DaemonCommandOutcome outcome = startRunHandler.handle(
                new DaemonCommand("c-start-seed", StartRunCommandHandler.TYPE, null, mapper.writeValueAsString(payload)));

        assertTrue(outcome.succeeded(), () -> "start-run error: " + outcome.error());
        String processInstanceId = (String) mapper.readValue(outcome.output(), Map.class).get("processInstanceId");

        // The seed context was sealed into the store and reduced to a studioquackRef on the engine.
        List<String> varNames = sqsEngine.historicVariables(processInstanceId).list().stream()
                .map(v -> v.getVariableName()).toList();
        assertTrue(varNames.contains(EngineVariableNames.STUDIOQUACK_REF),
                "sealed seed handle placed on the instance; vars=" + varNames);
        assertTrue(varNames.stream().allMatch(RefOnlyVariableGuard::isAllowed),
                "REF-ONLY still holds with a seeded context, got: " + varNames);
        assertTrue(callback.received(wfeId, RunStatusCallback.Status.RUNNING));
    }

    @Test
    void rejects_phi_seed_var_by_ref_only_guard() throws Exception {
        String workflowId = "wfStartRunPhi";
        deployUserTaskWorkflow(workflowId);
        String wfeId = UUID.randomUUID().toString();

        // A PHI-bearing (non-whitelisted) seed var must be rejected before it can reach ACT_*_VARIABLE.
        StartRunCommandPayload payload = new StartRunCommandPayload(
                wfeId, "Process_" + workflowId, null, Map.of("prompt", "PATIENT-SECRET-9f3a"), null);
        DaemonCommandOutcome outcome = startRunHandler.handle(
                new DaemonCommand("c-phi", StartRunCommandHandler.TYPE, null, mapper.writeValueAsString(payload)));

        assertFalse(outcome.succeeded(), "PHI seed var must fail the REF-ONLY guard");
        assertNotNull(outcome.error());
        assertTrue(outcome.error().contains("REF-ONLY"), "error names the REF-ONLY violation: " + outcome.error());
        assertEquals(0L, sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count(),
                "no PI started when the seed vars are rejected");
    }

    /**
     * The Designer race: start-run arrives on one command lane while its deploy is still running on
     * the other. The handler must wait for the draft definition AND its graph, then start normally.
     */
    @Test
    void draft_start_run_waits_for_a_deploy_still_in_flight() throws Exception {
        String workflowId = "wfStartRunRace";
        String draftToken = "race" + UUID.randomUUID().toString().substring(0, 8);
        String processKey = "Process_" + workflowId + WorkflowGraph.DRAFT_MARKER + draftToken;
        String wfeId = UUID.randomUUID().toString();
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowId(workflowId);
        seed.setEnvId("env-startrun");

        Thread lateDeploy = new Thread(() -> {
            try {
                Thread.sleep(600);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            deployUserTaskWorkflow(workflowId, draftToken);
        });
        lateDeploy.start();

        StartRunCommandPayload payload = new StartRunCommandPayload(wfeId, processKey, null, null, seed);
        DaemonCommandOutcome outcome = startRunHandler.handle(
                new DaemonCommand("c-start-race", StartRunCommandHandler.TYPE, null, mapper.writeValueAsString(payload)));
        lateDeploy.join();

        assertTrue(outcome.succeeded(), () -> "start-run error: " + outcome.error());
        assertEquals(1L, sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count(),
                "the run started once its deploy landed");
        assertTrue(callback.received(wfeId, RunStatusCallback.Status.RUNNING));
    }

    @Test
    void draft_start_run_fails_clearly_when_its_deploy_never_arrives() throws Exception {
        java.time.Duration original = startRunHandler.deployWait;
        startRunHandler.deployWait = java.time.Duration.ofMillis(400);
        try {
            String processKey = "Process_wfNeverDeployed" + WorkflowGraph.DRAFT_MARKER + "gone";
            ExecutionContext seed = new ExecutionContext();
            seed.setWorkflowId("wfNeverDeployed");
            StartRunCommandPayload payload = new StartRunCommandPayload(
                    UUID.randomUUID().toString(), processKey, null, null, seed);
            DaemonCommandOutcome outcome = startRunHandler.handle(
                    new DaemonCommand("c-start-gone", StartRunCommandHandler.TYPE, null, mapper.writeValueAsString(payload)));

            assertFalse(outcome.succeeded());
            assertTrue(outcome.error().contains("is not deployed on this engine"), outcome.error());
        } finally {
            startRunHandler.deployWait = original;
        }
    }

    /**
     * Start/end nodes come from the built-in library too, so while it is not usable no run starts —
     * not even a start → end graph with no other node.
     */
    @Test
    void unusable_node_library_refuses_even_a_start_end_only_graph() throws Exception {
        org.mockito.Mockito.when(nodeLibraryManager.isUsable()).thenReturn(false);
        org.mockito.Mockito.when(nodeLibraryManager.startRunBlockReason())
                .thenReturn("built-in node library is not ready yet (building)");
        String workflowId = "wfStartEndOnly";
        String draftToken = "se" + UUID.randomUUID().toString().substring(0, 8);
        NodeDto start = node("startN", "start.event", null, "start");
        NodeDto end = node("endN", "end.event", null, "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Start End", "1",
                List.of(start, end), List.of(edge("startN", "endN")), draftToken);
        deploymentService.deploy(graph, null, true);
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowId(workflowId);
        String wfeId = UUID.randomUUID().toString();

        StartRunCommandPayload payload = new StartRunCommandPayload(
                wfeId, "Process_" + workflowId + WorkflowGraph.DRAFT_MARKER + draftToken, null, null, seed);
        DaemonCommandOutcome outcome = startRunHandler.handle(
                new DaemonCommand("c-start-se", StartRunCommandHandler.TYPE, null, mapper.writeValueAsString(payload)));

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("built-in node library"), outcome.error());
        assertEquals(0L, sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count());
    }

    @Test
    void unstartable_payload_returns_failed() throws Exception {
        StartRunCommandPayload payload = new StartRunCommandPayload(
                null, null, null, null, null);
        DaemonCommandOutcome outcome = startRunHandler.handle(
                new DaemonCommand("c-empty", StartRunCommandHandler.TYPE, null, mapper.writeValueAsString(payload)));
        assertFalse(outcome.succeeded());
        assertNotNull(outcome.error());
    }

    // ---- fixtures -------------------------------------------------------

    /** Deploys {@code start → userTask → end}, which parks on start (container-independent). */
    private void deployUserTaskWorkflow(String workflowId) {
        deployUserTaskWorkflow(workflowId, null);
    }

    /** Draft variant persists the graph too, exactly as {@code DeployCommandHandler} does. */
    private void deployUserTaskWorkflow(String workflowId, String draftToken) {
        deployDraft(workflowId, draftToken, node("taskN", "user.task", null, null));
    }

    private void deployDraft(String workflowId, String draftToken, NodeDto task) {
        NodeDto start = node("startN", "start.event", null, "start");
        NodeDto end = node("endN", "end.event", null, "end");
        List<NodeDto> nodes = List.of(start, task, end);
        List<EdgeDto> edges = List.of(edge("startN", "taskN"), edge("taskN", "endN"));
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Start Run Flow", "1", nodes, edges, draftToken);
        com.studioquack.common.dto.flow.DeploymentInfo info = deploymentService.deploy(graph, null, true);
        if (draftToken != null) {
            // Same order as DeployCommandHandler: the graph rides the definition row.
            String artifactKey = workflowId + graph.namespaceSuffix();
            artifactStore.saveDefinition(artifactKey, "1", info);
            artifactStore.saveDeployedGraph(artifactKey, nodes, edges);
        }
    }

    private NodeDto node(String id, String bpmnType, String jobType, String nodeType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setJobType(jobType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        return edge;
    }

    /** Registers the recording callback as the primary {@link RunStatusCallback} for this context. */
    @TestConfiguration
    static class RecordingConfig {
        @Bean
        @Primary
        RecordingRunStatusCallback recordingRunStatusCallback() {
            return new RecordingRunStatusCallback();
        }
    }
}
