package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime proof for the call.activity deploy-time target check: the strategy mints the authored
 * {@code processId} into the daemon's process-key space (bare id → {@code Process_<id>}, draft
 * namespace appended), and {@link WorkflowDeploymentService} refuses the deploy when a literal
 * target is not deployed on this daemon — instead of letting the run fail at the node
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-call-activity-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class CallActivityDeployValidationTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;

    @Autowired
    private DeployedArtifactStore artifactStore;

    @Autowired
    private SqsEngine sqsEngine;

    @Test
    void failed_validation_rolls_back_the_activiti_deployment() {
        // Regression (Sep 2026 incident): the call.activity check runs AFTER the Activiti deploy
        // commits; without a rollback the broken definition + its message-start subscription leak —
        // the leftover then answers inbound messages with the broken graph and blocks the published
        // deploy via the duplicate-message guard.
        String missing = "missing-" + UUID.randomUUID();
        String callerWf = "wfCallerRb-" + UUID.randomUUID();

        assertThrows(IllegalStateException.class,
                () -> deploymentService.deploy(callerGraph(callerWf, missing, null), null, true));
        assertFalse(sqsEngine.latestDefinitionExists("Process_" + callerWf),
                "a failed post-deploy validation must roll back the Activiti deployment");
    }

    @Test
    void failed_draft_validation_rolls_back_the_ephemeral_deployment() {
        // Same rollback for a Designer Run: the leaked draft must not keep its namespace.
        String callerWf = "wfCallerRbDraft-" + UUID.randomUUID();

        assertThrows(IllegalStateException.class,
                () -> deploymentService.deploy(
                        callerGraph(callerWf, "missing-" + UUID.randomUUID(), "tokRb"), null, true, true));
        assertFalse(sqsEngine.latestDefinitionExists("Process_" + callerWf + "__draft_tokRb"),
                "the failed ephemeral deploy must be rolled back too");
    }

    @Test
    void deploy_fails_loudly_when_the_call_activity_target_is_not_deployed() {
        String missing = "missing-" + UUID.randomUUID();
        WorkflowGraph caller = callerGraph("wfCallerMiss-" + UUID.randomUUID(), missing, null);

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> deploymentService.deploy(caller, null, true),
                "an unresolved literal target must fail the deploy, not the run");
        assertTrue(ex.getMessage().contains("Process_" + missing),
                "the failure names the unresolved process key: " + ex.getMessage());
    }

    @Test
    void deploy_passes_when_the_call_activity_target_is_deployed() {
        String targetWf = "wfTarget-" + UUID.randomUUID();
        deploymentService.deploy(targetGraph(targetWf), null, true);

        WorkflowGraph caller = callerGraph("wfCallerOk-" + UUID.randomUUID(), targetWf, null);
        assertDoesNotThrow(() -> deploymentService.deploy(caller, null, true),
                "a target deployed on the same daemon resolves");
    }

    @Test
    void draft_run_cannot_call_the_published_target() {
        String targetWf = "wfTargetPub-" + UUID.randomUUID();
        deploymentService.deploy(targetGraph(targetWf), null, true);

        WorkflowGraph draftCaller = callerGraph("wfCallerDraft-" + UUID.randomUUID(), targetWf, "tok1");
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> deploymentService.deploy(draftCaller, null, true, true),
                "a Designer Run targets Process_<wf>__draft_tok1 — the published process is off-limits");
        assertTrue(ex.getMessage().contains("__draft_tok1"),
                "the failure names the draft-namespaced key: " + ex.getMessage());
    }

    @Test
    void deploy_refuses_expression_targets() {
        // A ${…} calledElement would be evaluated as JUEL inside the daemon JVM at runtime.
        WorkflowGraph caller = callerGraph("wfCallerExpr-" + UUID.randomUUID(), "${targetProcess}", null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> deploymentService.deploy(caller, null, true),
                "an expression target is an EL injection surface and must fail the deploy");
        assertTrue(ex.getMessage().contains("literal process key"), ex.getMessage());
    }

    @Test
    void deploy_resolves_a_workflow_short_code_target_via_the_store() {
        // The strategy's generation-time fallback: a graph carrying workflowShortCode (and no
        // processId) resolves against the artifact store when the code is globally unique here.
        String targetWf = "wfTargetSc-" + UUID.randomUUID();
        DeploymentInfo info = deploymentService.deploy(targetGraph(targetWf), null, true);
        artifactStore.saveDefinition(targetWf, "1", info);
        artifactStore.saveWebhookAddress(targetWf, "sales_ops", 1, "order_flow");

        WorkflowGraph caller = callerGraphByShortCode("wfCallerSc-" + UUID.randomUUID(), "order_flow");
        assertDoesNotThrow(() -> deploymentService.deploy(caller, null, true),
                "a globally unique short code resolves to the deployed target");
    }

    @Test
    void deploy_fails_loudly_when_neither_process_id_nor_short_code_is_set() {
        WorkflowGraph caller = callerGraphByShortCode("wfCallerBlank-" + UUID.randomUUID(), null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> deploymentService.deploy(caller, null, true),
                "both target parameters blank must fail the deploy, not the run");
        assertTrue(ex.getMessage().contains("Process Id"), ex.getMessage());
    }

    /** {@code start → call.activity(workflowShortCode=code) → end}; no processId param value. */
    private WorkflowGraph callerGraphByShortCode(String workflowId, String shortCode) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto call = node("callN", "call.activity", null);
        param(call, "workflowShortCode", shortCode);
        NodeDto end = node("endN", "end.event", "end");
        return new WorkflowGraph(workflowId, "Caller", "1",
                List.of(start, call, end),
                List.of(edge("startN", "callN"), edge("callN", "endN")),
                null);
    }

    /** {@code start → call.activity(processId=target) → end}; {@code draftToken} null = published. */
    private WorkflowGraph callerGraph(String workflowId, String targetProcessId, String draftToken) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto call = node("callN", "call.activity", null);
        param(call, "processId", targetProcessId);
        NodeDto end = node("endN", "end.event", "end");
        return new WorkflowGraph(workflowId, "Caller", "1",
                List.of(start, call, end),
                List.of(edge("startN", "callN"), edge("callN", "endN")),
                draftToken);
    }

    /** A minimal deployable target workflow: {@code start → end}. */
    private WorkflowGraph targetGraph(String workflowId) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto end = node("endN", "end.event", "end");
        return new WorkflowGraph(workflowId, "Target", "1",
                List.of(start, end),
                List.of(edge("startN", "endN")));
    }

    private static void param(NodeDto node, String shortCode, String value) {
        ParameterDto param = new ParameterDto();
        param.setShortCode(shortCode);
        param.setValue(value);
        List<ParameterDto> existing = node.getData().getConfigurationParameters();
        List<ParameterDto> merged = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
        merged.add(param);
        node.getData().setConfigurationParameters(merged);
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
}
