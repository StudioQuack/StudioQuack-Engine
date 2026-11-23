package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code undeploy} command handler against the embedded engine on an in-memory database (base
 * profile). Proves the lifecycle half that was missing when undeploy only dropped the server's SQL
 * row
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-undeploy-cmd-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class UndeployCommandHandlerTest {

    @Autowired
    private DeployCommandHandler deployHandler;
    @Autowired
    private UndeployCommandHandler undeployHandler;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private DeployedArtifactStore artifactStore;
    @Autowired
    private ObjectMapper mapper;

    @Test
    void undeploy_deletes_definition_artifacts_and_frees_the_message_start_name() throws Exception {
        String messageName = "undeploy-test-msg";
        // Flow A: message.start → user.task → end, deployed (never started).
        deploy("wfUndeployA", messageStartNodes(messageName), messageStartEdges());

        assertTrue(sqsEngine.latestDefinitionExists("Process_wfUndeployA"), "flow A deployed");
        assertNotNull(artifactStore.findPriorDefinition("wfUndeployA"), "definition cache row written");

        DaemonCommandOutcome outcome = undeployHandler.handle(command("u-1",
                new UndeployCommandHandler.UndeployCommandPayload("wfUndeployA", false)));

        assertTrue(outcome.succeeded(), () -> "undeploy outcome error: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        assertEquals(Boolean.TRUE, out.get("engineDeploymentDeleted"), "engine deployment deleted");

        assertFalse(sqsEngine.latestDefinitionExists("Process_wfUndeployA"),
                "process definition gone from the engine");
        assertNull(artifactStore.findPriorDefinition("wfUndeployA"), "definition cache row removed");

        // The freed subscription: flow B can now deploy with the SAME message-start name — before
        // undeploy existed this failed the guard with "already subscribed by another deployed
        // workflow" even though the user had undeployed flow A in the FE.
        DaemonCommandOutcome redeploy = deploy("wfUndeployB", messageStartNodes(messageName), messageStartEdges());
        assertTrue(redeploy.succeeded(),
                () -> "message-start name must be reusable after undeploy: " + redeploy.error());
    }

    @Test
    void undeploy_refuses_while_instances_run_and_force_cascades() throws Exception {
        String workflowId = "wfUndeployRunning";
        deploy(workflowId, parkingNodes(), parkingEdges());
        ProcessInstance instance = sqsEngine.startLatest("Process_" + workflowId, "run-undeploy-1", null);
        assertNotNull(instance, "run started and parked on the user task");

        DaemonCommandOutcome refused = undeployHandler.handle(command("u-2",
                new UndeployCommandHandler.UndeployCommandPayload(workflowId, false)));
        assertFalse(refused.succeeded(), "undeploy must refuse while an instance runs");
        assertNotNull(refused.error());
        assertTrue(refused.error().contains("running instance"), "refusal names the running count: " + refused.error());
        assertTrue(sqsEngine.latestDefinitionExists("Process_" + workflowId),
                "refused undeploy leaves the definition in place");

        DaemonCommandOutcome forced = undeployHandler.handle(command("u-3",
                new UndeployCommandHandler.UndeployCommandPayload(workflowId, true)));
        assertTrue(forced.succeeded(), () -> "forced undeploy error: " + forced.error());
        assertFalse(sqsEngine.latestDefinitionExists("Process_" + workflowId),
                "forced undeploy cascades the definition away");
        assertNull(sqsEngine.processInstances().processInstanceId(instance.getId()).singleResult(),
                "forced undeploy dropped the live instance");
    }

    @Test
    void undeploy_is_idempotent_when_the_daemon_holds_nothing() throws Exception {
        DaemonCommandOutcome outcome = undeployHandler.handle(command("u-4",
                new UndeployCommandHandler.UndeployCommandPayload("wfNeverDeployed", false)));

        assertTrue(outcome.succeeded(), () -> "idempotent undeploy error: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        assertEquals(Boolean.FALSE, out.get("engineDeploymentDeleted"),
                "nothing deployed → nothing deleted, still ok");
    }

    @Test
    void malformed_payload_returns_failed_outcome_not_throw() {
        DaemonCommandOutcome outcome = undeployHandler.handle(
                new DaemonCommand("u-bad", UndeployCommandHandler.TYPE, null, "{ not-json"));
        assertFalse(outcome.succeeded(), "malformed payload must fail");
        assertNotNull(outcome.error(), "failure carries an error message for the BE");
    }

    // ---- fixtures (mirror DeployCommandHandlerTest / MessageSenderExecutionListenerTest) ----

    private DaemonCommandOutcome deploy(String workflowId, List<NodeDto> nodes, List<EdgeDto> edges)
            throws Exception {
        DeployCommandPayload payload = new DeployCommandPayload(
                workflowId, "1", "Undeploy Test Flow", nodes, edges, null, null, false, false);
        DaemonCommandOutcome outcome = deployHandler.handle(new DaemonCommand(
                "c-" + workflowId, DeployCommandHandler.TYPE, null, mapper.writeValueAsString(payload)));
        assertTrue(outcome.succeeded(), () -> "deploy of " + workflowId + " failed: " + outcome.error());
        return outcome;
    }

    private List<NodeDto> messageStartNodes(String messageName) {
        NodeDto start = node("startN", "message.start.event", "start");
        ParameterDto nameParam = new ParameterDto();
        nameParam.setShortCode("name");
        nameParam.setValue(messageName);
        start.getData().setConfigurationParameters(new ArrayList<>(List.of(nameParam)));
        return List.of(start, node("taskN", "user.task", null), node("endN", "end.event", "end"));
    }

    private List<EdgeDto> messageStartEdges() {
        return List.of(edge("startN", "taskN"), edge("taskN", "endN"));
    }

    private List<NodeDto> parkingNodes() {
        return List.of(node("startN", "start.event", "start"),
                node("taskN", "user.task", null),
                node("endN", "end.event", "end"));
    }

    private List<EdgeDto> parkingEdges() {
        return List.of(edge("startN", "taskN"), edge("taskN", "endN"));
    }

    private NodeDto node(String id, String bpmnType, String nodeType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
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

    private DaemonCommand command(String id, UndeployCommandHandler.UndeployCommandPayload payload)
            throws Exception {
        return new DaemonCommand(id, UndeployCommandHandler.TYPE, null, mapper.writeValueAsString(payload));
    }
}
