package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.command.store.DeployedScriptEntity;
import com.studioquack.sqs.engine.SqsEngine;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code deploy} command handler against the embedded engine on an in-memory database. Proves
 * the channel→engine seam
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-deploy-cmd-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class DeployCommandHandlerTest {

    @Autowired
    private DeployCommandHandler deployHandler;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private DeployedArtifactStore artifactStore;
    @Autowired
    private ObjectMapper mapper;

    @Test
    void deploy_command_compiles_bpmn_into_h2_and_persists_template_snapshot() throws Exception {
        String workflowId = "wfDeployCmd";
        String version = "1";
        List<NodeDto> nodes = twoNodeWorkflowNodes();
        List<EdgeDto> edges = List.of(edge("startN", "scriptN"), edge("scriptN", "endN"));

        DeployCommandPayload payload = new DeployCommandPayload(
                workflowId, version, "Deploy Cmd Flow", nodes, edges, null, null, false, false);
        DaemonCommandOutcome outcome = deployHandler.handle(command("c-deploy-1", payload));

        assertTrue(outcome.succeeded(), () -> "deploy outcome error: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        assertNotNull(out.get("processDefinitionId"), "pinned processDefinitionId returned");
        assertTrue(((Number) out.get("version")).intValue() >= 1, "engine version returned");

        // BPMN deployed into embedded database — a ProcessDefinition exists for the main process.
        assertTrue(sqsEngine.latestDefinitionExists("Process_" + workflowId),
                "main process definition deployed in H2");

        // Snapshot persisted (all 3 node templates + the python script).
        assertTrue(artifactStore.countTemplates(workflowId, version) >= 3,
                "node templates snapshotted for offline self-provisioning");
        assertEquals(1L, artifactStore.countScripts(workflowId, version), "one script snapshotted (python node)");
        List<DeployedScriptEntity> scripts = artifactStore.scripts(workflowId, version);
        assertTrue(scripts.stream().anyMatch(s -> "python".equals(s.getLanguage())
                        && s.getScript() != null && s.getScript().contains("return_result")),
                "the python node's script body was persisted verbatim");
    }

    @Test
    void second_identical_deploy_reuses_definition_no_new_version() throws Exception {
        String workflowId = "wfDeployReuse";
        String version = "1";
        List<NodeDto> nodes = twoNodeWorkflowNodes();
        List<EdgeDto> edges = List.of(edge("startN", "scriptN"), edge("scriptN", "endN"));
        DeployCommandPayload payload = new DeployCommandPayload(
                workflowId, version, "Reuse Flow", nodes, edges, null, null, false, false);
        String payloadJson = mapper.writeValueAsString(payload);

        DaemonCommandOutcome first = deployHandler.handle(
                new DaemonCommand("c-reuse-1", DeployCommandHandler.TYPE, null, payloadJson));
        DaemonCommandOutcome second = deployHandler.handle(
                new DaemonCommand("c-reuse-2", DeployCommandHandler.TYPE, null, payloadJson));

        assertTrue(first.succeeded(), () -> "first deploy error: " + first.error());
        assertTrue(second.succeeded(), () -> "second deploy error: " + second.error());

        Map<?, ?> firstOut = mapper.readValue(first.output(), Map.class);
        Map<?, ?> secondOut = mapper.readValue(second.output(), Map.class);
        // Reuse (bpmn-signature:v7 guard): identical graph → same pinned definition + same version.
        assertEquals(firstOut.get("processDefinitionId"), secondOut.get("processDefinitionId"),
                "identical graph reuses the pinned definition id");
        assertEquals(firstOut.get("version"), secondOut.get("version"),
                "identical graph creates NO new engine version");
    }

    /**
     * A Designer Run must deploy EVERY time, even when the graph has not changed. Its deployment is
     * throwaway — the reaper deletes it once the run ends — so there is no steady deployment record
     * to reuse.
     */
    @Test
    void designer_run_redeploys_even_when_graph_is_unchanged() throws Exception {
        String workflowId = "wfDesignerRedeploy";
        List<NodeDto> nodes = twoNodeWorkflowNodes();
        List<EdgeDto> edges = List.of(edge("startN", "scriptN"), edge("scriptN", "endN"));
        DeployCommandPayload ephemeral = new DeployCommandPayload(
                workflowId, "1", "Designer Flow", nodes, edges, null, null, false, true);
        String payloadJson = mapper.writeValueAsString(ephemeral);

        DaemonCommandOutcome first = deployHandler.handle(
                new DaemonCommand("c-design-1", DeployCommandHandler.TYPE, null, payloadJson));
        DaemonCommandOutcome second = deployHandler.handle(
                new DaemonCommand("c-design-2", DeployCommandHandler.TYPE, null, payloadJson));

        assertTrue(first.succeeded(), () -> "first designer deploy error: " + first.error());
        assertTrue(second.succeeded(), () -> "second designer deploy error: " + second.error());

        Map<?, ?> firstOut = mapper.readValue(first.output(), Map.class);
        Map<?, ?> secondOut = mapper.readValue(second.output(), Map.class);
        assertNotEquals(firstOut.get("processDefinitionId"), secondOut.get("processDefinitionId"),
                "an unchanged designer run must still produce a FRESH definition, not reuse the reaped one");
    }

    /**
     * A Designer Run must not write into the durable reuse cache: that record is what published runs
     * consult, and pointing it at a definition the reaper is about to delete corrupts it.
     */
    @Test
    void designer_run_does_not_poison_the_deployed_definition_cache() throws Exception {
        String workflowId = "wfDesignerNoCache";
        List<NodeDto> nodes = twoNodeWorkflowNodes();
        List<EdgeDto> edges = List.of(edge("startN", "scriptN"), edge("scriptN", "endN"));

        DaemonCommandOutcome outcome = deployHandler.handle(new DaemonCommand(
                "c-design-cache", DeployCommandHandler.TYPE, null,
                mapper.writeValueAsString(new DeployCommandPayload(
                        workflowId, "1", "Designer Flow", nodes, edges, null, null, false, true))));

        assertTrue(outcome.succeeded(), () -> "designer deploy error: " + outcome.error());
        assertNull(artifactStore.findPriorDefinition(workflowId),
                "a throwaway designer deployment must leave no durable definition record");
    }

    @Test
    void malformed_payload_returns_failed_outcome_not_throw() {
        DaemonCommandOutcome outcome = deployHandler.handle(
                new DaemonCommand("c-bad", DeployCommandHandler.TYPE, null, "{ not-json"));
        assertFalse(outcome.succeeded(), "malformed payload must fail");
        assertNotNull(outcome.error(), "failure carries an error message for the BE");
    }

    // Fixtures (mirror the node/edge fixtures)

    private List<NodeDto> twoNodeWorkflowNodes() {
        NodeDto start = node("startN", "start.event", null, "start", "tpl-start", null, null);
        NodeDto script = node("scriptN", "custom.sandbox", "custom-script-executor", "serviceTask",
                "tpl-script", "python",
                "studioquack.return_result([{\"port\": \"out\", \"value\": \"ok\"}])\n");
        NodeDto end = node("endN", "end.event", null, "end", "tpl-end", null, null);
        return List.of(start, script, end);
    }

    private NodeDto node(String id, String bpmnType, String jobType, String nodeType,
                         String templateId, String language, String script) {
        TemplateDto template = new TemplateDto();
        template.setId(templateId);
        template.setBpmnType(bpmnType);
        template.setJobType(jobType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        template.setName(id);
        if (script != null) {
            template.setTemplateScript(new TemplateScriptDto(id + "-s", templateId, language, script));
            PortDto out = new PortDto();
            out.setId("out");
            out.setName("out");
            out.setValue("out");
            out.setType(PortType.OUTPUT);
            template.setPorts(new ArrayList<>(List.of(out)));
        }
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

    private DaemonCommand command(String id, DeployCommandPayload payload) throws Exception {
        return new DaemonCommand(id, DeployCommandHandler.TYPE, "2026-07-12T00:00:00Z",
                mapper.writeValueAsString(payload));
    }
}
