package com.studioquack.sqs.sandbox.secrets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deployed-binding guard — the last check before any {@code $secretRef} is decrypted into a
 * sandbox-facing context. Rule: a ref may resolve on a node only when that exact ref was bound
 * SOMEWHERE ON THAT SAME NODE in the deployed graph (the deploy-time artifact store is the
 * authority; the graph carried on the execution context is the fallback when no deployed graph was
 * persisted).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeployedSecretBindingGuard {

    /** Tamper-event type the caller reports on a violation. */
    public static final String TAMPER_TYPE = "secret.ref.mismatch";

    private static final Pattern COMPACT = Pattern.compile("^\\$secretRef:(sec_[0-9a-f]{24}:v[1-9][0-9]{0,5})$");
    private static final Pattern JSON_WRAPPER =
            Pattern.compile("^\\{\"\\$secretRef\":\"(sec_[0-9a-f]{24}:v[1-9][0-9]{0,5})\"}$");
    private static final Pattern BARE = Pattern.compile("^(sec_[0-9a-f]{24}:v[1-9][0-9]{0,5})$");

    /** Same secret-shaped type discriminators as {@link SecretResolver}. */
    private static final Set<String> SECRET_SHAPED_TYPES = Set.of(
            "password", "api_key", "apikey", "token", "secret", "credential", "credentials",
            "connection_string", "connectionstring", "private_key", "privatekey");

    private final DeployedArtifactStore artifactStore;
    private final ObjectMapper objectMapper;

    /**
     * Verifies every ref the dispatch is about to resolve against the deployed binding.
     *
     * @param deployArtifactKey Deploy-store key ({@code workflowId} + draft namespace suffix when.
     * @param nodeId The node being dispatched.
     * @param currentNode The node about to be resolved (post-inbound-pipeline state).
     */
    public void verifyNodeBindings(String deployArtifactKey, String nodeId, NodeDto currentNode,
                                   List<NodeDto> graphFallback, Map<String, Object> inputVars) {
        Set<String> attempted = new LinkedHashSet<>();
        collectRefs(toTree(currentNode), attempted);
        collectRefs(inputVars, attempted);
        if (attempted.isEmpty()) {
            return;
        }

        NodeDto deployedNode = findNode(deployArtifactKey, nodeId, graphFallback);
        if (deployedNode == null) {
            throw new SecretBindingViolation(nodeId, attempted,
                    "no deployed binding source for the node — refusing to resolve any secret");
        }
        Set<String> bound = new LinkedHashSet<>();
        collectRefs(toTree(deployedNode), bound);

        Set<String> foreign = new TreeSet<>(attempted);
        foreign.removeAll(bound);
        if (!foreign.isEmpty()) {
            throw new SecretBindingViolation(nodeId, foreign,
                    "ref(s) not bound on this node at deploy time");
        }
    }

    /** The deployed graph's node, else the fallback graph's node, else null. */
    private NodeDto findNode(String deployArtifactKey, String nodeId, List<NodeDto> graphFallback) {
        List<NodeDto> deployed = artifactStore.loadDeployedGraph(deployArtifactKey)
                .map(DeployedArtifactStore.DeployedGraph::nodes)
                .filter(nodes -> !nodes.isEmpty())
                .orElse(null);
        NodeDto hit = findById(deployed, nodeId);
        return hit != null ? hit : findById(graphFallback, nodeId);
    }

    private static NodeDto findById(List<NodeDto> nodes, String nodeId) {
        if (nodes == null || nodeId == null) {
            return null;
        }
        for (NodeDto n : nodes) {
            if (n != null && nodeId.equals(n.getId())) {
                return n;
            }
        }
        return null;
    }

    /** DTOs → generic tree so the walk covers every nested shape the resolver understands. */
    private Object toTree(Object dto) {
        if (dto == null) {
            return null;
        }
        try {
            return objectMapper.convertValue(dto, Object.class);
        } catch (IllegalArgumentException e) {
            log.warn("secret-binding-guard: could not convert node to a walkable tree: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Collects every resolvable ref under {@code tree} — the same four forms
     * {@link SecretResolver} resolves (object wrapper, secret-typed bare value, compact
     * string, JSON-stringified wrapper). Collect-only; never mutates.
     */
    @SuppressWarnings("unchecked")
    private void collectRefs(Object tree, Set<String> out) {
        if (tree instanceof Map<?, ?> rawMap) {
            Map<String, Object> map = (Map<String, Object>) rawMap;
            Object refVal = map.get("$secretRef");
            if (refVal instanceof String refStr) {
                addIfRef(refStr, out);
            }
            Object typeVal = map.get("type");
            Object valueVal = map.get("value");
            if (typeVal instanceof String t && SECRET_SHAPED_TYPES.contains(t.trim().toLowerCase())
                    && valueVal instanceof String vs) {
                addIfRef(vs.trim(), out);
            }
            for (Object v : map.values()) {
                collectRefs(v, out);
            }
            return;
        }
        if (tree instanceof List<?> list) {
            for (Object v : list) {
                collectRefs(v, out);
            }
            return;
        }
        if (tree instanceof String s) {
            Matcher m;
            if ((m = COMPACT.matcher(s)).matches()) {
                addIfRef(m.group(1), out);
            } else if ((m = JSON_WRAPPER.matcher(s)).matches()) {
                addIfRef(m.group(1), out);
            }
        }
    }

    private static void addIfRef(String candidate, Set<String> out) {
        if (candidate == null) {
            return;
        }
        SecretRef.parse(candidate.trim()).ifPresent(ref -> out.add(ref.wire()));
    }

    /**
     * Fail-closed rejection of a dispatch whose refs are not deploy-bound. Carries only ref ids and
     * the node id — never values — so the message is safe for logs, sandbox-log ingest, and the tamper
     * detail.
     */
    public static final class SecretBindingViolation extends RuntimeException {

        private SecretBindingViolation(String nodeId, Set<String> refs, String reason) {
            super("secret binding violation on node=" + nodeId + ": " + reason + " refs=" + refs);
        }
    }
}
