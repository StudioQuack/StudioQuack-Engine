package com.studioquack.sqs.sandbox.secrets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The deployed-binding guard's allow/deny contract (fail-closed). The deny cases are the
 * security regression proofs: a ref that was not bound on the node at deploy time must never
 * reach the OS store — whether it arrives on a parameter, in the input variables, or from a
 * stale engine's leftover context.
 */
class DeployedSecretBindingGuardTest {

    private static final String BOUND = "sec_aaaaaaaaaaaaaaaaaaaaaaaa:v1";
    private static final String FOREIGN = "sec_bbbbbbbbbbbbbbbbbbbbbbbb:v1";

    private final DeployedArtifactStore store = mock(DeployedArtifactStore.class);
    private final DeployedSecretBindingGuard guard = new DeployedSecretBindingGuard(store, new ObjectMapper());

    private static NodeDto node(String id, Object paramValue) {
        ParameterDto p = new ParameterDto();
        p.setId("p1");
        p.setShortCode("db_password");
        p.setType("password");
        p.setValue(paramValue);
        TemplateDto data = new TemplateDto();
        data.setConfigurationParameters(List.of(p));
        NodeDto n = new NodeDto();
        n.setId(id);
        n.setData(data);
        return n;
    }

    private void deployedGraph(NodeDto... nodes) {
        when(store.loadDeployedGraph("wf-1"))
                .thenReturn(Optional.of(new DeployedArtifactStore.DeployedGraph(List.of(nodes), List.of())));
    }

    @Test
    void deploy_bound_ref_resolves() {
        NodeDto deployed = node("n1", Map.of("$secretRef", BOUND));
        deployedGraph(deployed);

        assertDoesNotThrow(() -> guard.verifyNodeBindings(
                "wf-1", "n1", node("n1", Map.of("$secretRef", BOUND)), List.of(), Map.of()));
    }

    @Test
    void a_ref_not_bound_at_deploy_is_rejected() {
        deployedGraph(node("n1", Map.of("$secretRef", BOUND)));

        DeployedSecretBindingGuard.SecretBindingViolation v = assertThrows(
                DeployedSecretBindingGuard.SecretBindingViolation.class,
                () -> guard.verifyNodeBindings(
                        "wf-1", "n1", node("n1", Map.of("$secretRef", FOREIGN)), List.of(), Map.of()));
        assertTrue(v.getMessage().contains(FOREIGN), "the offending ref id is named for forensics");
        assertTrue(v.getMessage().contains("n1"));
    }

    @Test
    void a_crafted_ref_in_the_input_variables_is_rejected() {
        // Cross-node exfil attempt: an upstream output carries a handle to a secret that was
        // only ever bound on a DIFFERENT node — the current node must not resolve it.
        deployedGraph(node("n1", Map.of("$secretRef", BOUND)));

        assertThrows(DeployedSecretBindingGuard.SecretBindingViolation.class,
                () -> guard.verifyNodeBindings("wf-1", "n1",
                        node("n1", Map.of("$secretRef", BOUND)), List.of(),
                        Map.of("upstream", Map.of("$secretRef", FOREIGN))));
    }

    @Test
    void an_input_variable_may_carry_a_ref_bound_on_this_same_node() {
        deployedGraph(node("n1", Map.of("$secretRef", BOUND)));

        assertDoesNotThrow(() -> guard.verifyNodeBindings("wf-1", "n1",
                node("n1", Map.of("$secretRef", BOUND)), List.of(),
                Map.of("upstream", Map.of("$secretRef", BOUND))));
    }

    @Test
    void a_compact_ref_embedded_in_the_deployed_script_text_still_resolves() {
        NodeDto deployed = node("n1", null);
        TemplateScriptDto script = new TemplateScriptDto();
        script.setLanguage("python");
        script.setScript("token = \"$secretRef:" + BOUND + "\"");
        deployed.getData().setTemplateScript(script);
        deployedGraph(deployed);

        NodeDto current = node("n1", null);
        current.getData().setTemplateScript(script);

        assertDoesNotThrow(() -> guard.verifyNodeBindings("wf-1", "n1", current, List.of(), Map.of()));
    }

    @Test
    void the_context_graph_is_the_fallback_when_no_deployed_graph_was_persisted() {
        when(store.loadDeployedGraph("wf-1")).thenReturn(Optional.empty());
        NodeDto fallback = node("n1", Map.of("$secretRef", BOUND));

        assertDoesNotThrow(() -> guard.verifyNodeBindings(
                "wf-1", "n1", node("n1", Map.of("$secretRef", BOUND)), List.of(fallback), Map.of()));
        assertThrows(DeployedSecretBindingGuard.SecretBindingViolation.class,
                () -> guard.verifyNodeBindings(
                        "wf-1", "n1", node("n1", Map.of("$secretRef", FOREIGN)), List.of(fallback), Map.of()));
    }

    @Test
    void no_binding_source_fails_closed() {
        when(store.loadDeployedGraph("wf-1")).thenReturn(Optional.empty());

        assertThrows(DeployedSecretBindingGuard.SecretBindingViolation.class,
                () -> guard.verifyNodeBindings(
                        "wf-1", "n1", node("n1", Map.of("$secretRef", BOUND)), List.of(), Map.of()));
    }

    @Test
    void designer_draft_deploy_key_is_used_not_the_published_row() {
        String draftKey = "wf-1" + com.studioquack.sqs.engine.WorkflowGraph.DRAFT_MARKER + "tok";
        when(store.loadDeployedGraph("wf-1"))
                .thenReturn(Optional.of(new DeployedArtifactStore.DeployedGraph(
                        List.of(node("n1", Map.of("$secretRef", FOREIGN))), List.of())));
        when(store.loadDeployedGraph(draftKey))
                .thenReturn(Optional.of(new DeployedArtifactStore.DeployedGraph(
                        List.of(node("n1", Map.of("$secretRef", BOUND))), List.of())));

        assertDoesNotThrow(() -> guard.verifyNodeBindings(
                draftKey, "n1", node("n1", Map.of("$secretRef", BOUND)), List.of(), Map.of()));
    }

    @Test
    void a_node_with_no_refs_never_reaches_the_store() {
        when(store.loadDeployedGraph("wf-1")).thenReturn(Optional.empty());

        assertDoesNotThrow(() -> guard.verifyNodeBindings(
                "wf-1", "n1", node("n1", "plain-text"), List.of(), Map.of("k", "v")));
    }
}
