package com.studioquack.sqs.command;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.command.store.DeployedDefinitionEntity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * call.activity short-code targeting ({@link CallActivityShortCodeResolver}): the deploy payload
 * rewrite that stamps a literal {@code processId} resolved from an authored
 * {@code workflowShortCode}, with the enclosing deploy's project/version as the disambiguation
 * context. Unknown and ambiguous codes fail the deploy loudly.
 */
class CallActivityShortCodeResolverTest {

    private final DeployedArtifactStore store = mock(DeployedArtifactStore.class);
    private final CallActivityShortCodeResolver resolver = new CallActivityShortCodeResolver(store);

    @Test
    void explicit_process_id_wins_and_the_store_is_never_touched() {
        NodeDto call = callActivityNode("callN", "wf-explicit", null);
        DeployCommandPayload payload = payload(List.of(call), "sales_ops", 3);

        resolver.resolveDeployPayload(payload);

        assertEquals("wf-explicit", paramValue(call, "processId"));
        verify(store, never()).findDeployedByWorkflowShortCode(anyString());
    }

    @Test
    void both_parameters_blank_fail_the_deploy_loudly() {
        NodeDto call = callActivityNode("callN", null, null);
        DeployCommandPayload payload = payload(List.of(call), "sales_ops", 3);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> resolver.resolveDeployPayload(payload));
        assertTrue(ex.getMessage().contains("callN"), "the failure names the node: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("Process Id"), ex.getMessage());
    }

    @Test
    void short_code_resolves_to_the_single_deployed_workflow() {
        when(store.findDeployedByWorkflowShortCode("order_flow"))
                .thenReturn(List.of(row("wf-child-1", "sales_ops", 3)));
        NodeDto call = callActivityNode("callN", null, "Order_Flow"); // raw input is normalised
        DeployCommandPayload payload = payload(List.of(call), "sales_ops", 3);

        resolver.resolveDeployPayload(payload);

        assertEquals("wf-child-1", paramValue(call, "processId"));
    }

    @Test
    void unknown_short_code_fails_with_a_deploy_first_hint() {
        when(store.findDeployedByWorkflowShortCode("ghost")).thenReturn(List.of());
        NodeDto call = callActivityNode("callN", null, "ghost");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> resolver.resolveDeployPayload(payload(List.of(call), "sales_ops", 3)));
        assertTrue(ex.getMessage().contains("ghost"), ex.getMessage());
        assertTrue(ex.getMessage().contains("deploy the target workflow first"), ex.getMessage());
    }

    @Test
    void same_project_context_disambiguates_a_shared_short_code() {
        when(store.findDeployedByWorkflowShortCode("order_flow")).thenReturn(List.of(
                row("wf-other-project", "billing", 1),
                row("wf-same-project", "sales_ops", 3)));
        NodeDto call = callActivityNode("callN", null, "order_flow");

        resolver.resolveDeployPayload(payload(List.of(call), "sales_ops", 3));

        assertEquals("wf-same-project", paramValue(call, "processId"));
    }

    @Test
    void same_version_context_prefers_the_sibling_clone() {
        // A project-version deploy's call activity targets the SAME version's clone — the clone
        // keeps the short code even though its workflow id changes per version.
        when(store.findDeployedByWorkflowShortCode("order_flow")).thenReturn(List.of(
                row("wf-v2-clone", "sales_ops", 2),
                row("wf-v3-clone", "sales_ops", 3)));
        NodeDto call = callActivityNode("callN", null, "order_flow");

        resolver.resolveDeployPayload(payload(List.of(call), "sales_ops", 3));

        assertEquals("wf-v3-clone", paramValue(call, "processId"));
    }

    @Test
    void ambiguity_surviving_the_context_fails_with_a_process_id_hint() {
        when(store.findDeployedByWorkflowShortCode("order_flow")).thenReturn(List.of(
                row("wf-a", "sales_ops", 2),
                row("wf-b", "sales_ops", 2)));
        NodeDto call = callActivityNode("callN", null, "order_flow");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> resolver.resolveDeployPayload(payload(List.of(call), "sales_ops", 2)));
        assertTrue(ex.getMessage().contains("Process Id"), ex.getMessage());
    }

    @Test
    void unscoped_resolution_requires_global_uniqueness() {
        when(store.findDeployedByWorkflowShortCode("order_flow")).thenReturn(List.of(
                row("wf-a", "sales_ops", 2),
                row("wf-b", "billing", 1)));

        assertThrows(IllegalArgumentException.class, () -> resolver.resolve("order_flow", null, null));
    }

    @Test
    void non_call_activity_nodes_are_untouched() {
        NodeDto script = node("scriptN", "studioquack.script");
        DeployCommandPayload payload = payload(List.of(script), "sales_ops", 3);

        resolver.resolveDeployPayload(payload);

        assertTrue(script.getData().getConfigurationParameters() == null
                || script.getData().getConfigurationParameters().isEmpty());
        verify(store, never()).findDeployedByWorkflowShortCode(anyString());
    }

    // ------------------------------------------------------------------

    private static DeployedDefinitionEntity row(String workflowId, String projectShortCode, Integer projectVersion) {
        DeployedDefinitionEntity entity = new DeployedDefinitionEntity();
        entity.setWorkflowId(workflowId);
        entity.setProjectShortCode(projectShortCode);
        entity.setProjectVersion(projectVersion);
        entity.setWorkflowShortCode("order_flow");
        return entity;
    }

    private static DeployCommandPayload payload(List<NodeDto> nodes, String projectShortCode, Integer projectVersion) {
        return new DeployCommandPayload("wf-caller", "1", "Caller", nodes, List.of(), null, null,
                false, false, null, projectShortCode, projectVersion, null, null, null);
    }

    private static NodeDto callActivityNode(String id, String processId, String shortCode) {
        NodeDto node = node(id, "call.activity");
        List<ParameterDto> params = new ArrayList<>();
        params.add(param("processId", processId));
        params.add(param("workflowShortCode", shortCode));
        node.getData().setConfigurationParameters(params);
        return node;
    }

    private static NodeDto node(String id, String bpmnType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setLabel(id);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private static ParameterDto param(String shortCode, String value) {
        ParameterDto param = new ParameterDto();
        param.setShortCode(shortCode);
        param.setValue(value);
        return param;
    }

    private static String paramValue(NodeDto node, String shortCode) {
        return node.getData().getConfigurationParameters().stream()
                .filter(p -> shortCode.equals(p.getShortCode()))
                .map(p -> p.getValue() == null ? null : String.valueOf(p.getValue()))
                .findFirst()
                .orElse(null);
    }
}
