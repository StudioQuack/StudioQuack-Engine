package com.studioquack.sqs.webhook;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import org.apache.commons.lang3.StringUtils;

/**
 * Deterministic Activiti BPMN message name conventions for parameter-driven webhook ingress
 * nodes ({@code LISTEN}, {@code MESSAGE_CATCH}, {@code SEND_AND_WAIT}) and {@code INGRESS_NODE}
 * message boundary events.
 *
 * <p>Both the BPMN compiler ({@code BpmnGenerationService} / {@code ActivitiBoundaryEventHelper})
 * and the deploy-time binding index ({@code DeployedWebhookBindingStore}) derive message names
 * through this single class so subscriptions and runtime routing can never drift.
 */
public final class WebhookMessageNames {

    /** Reserved prefix for parameter-driven webhook hook message names. */
    public static final String HOOK_PREFIX = "__sq_hook_";

    private WebhookMessageNames() {
    }

    /** True when {@code messageName} was minted for a parameter-driven webhook ingress binding. */
    public static boolean isHookMessageName(String messageName) {
        return messageName != null && messageName.startsWith(HOOK_PREFIX);
    }

    /**
     * Deterministic BPMN message name for a node-level webhook listener/catch/send-and-wait:
     * {@code __sq_hook_<workflowId><namespaceSuffix>_<nodeId>}.
     */
    public static String forNode(String workflowId, String nodeId, String namespaceSuffix) {
        String wf = sanitize(workflowId);
        String ns = StringUtils.isBlank(namespaceSuffix) ? "" : sanitize(namespaceSuffix);
        String node = sanitize(nodeId);
        return HOOK_PREFIX + wf + ns + "_" + node;
    }

    /** Convenience overload for published deployments (empty namespace suffix). */
    public static String forNode(String workflowId, String nodeId) {
        return forNode(workflowId, nodeId, "");
    }

    /**
     * Deterministic BPMN message name for a {@code MESSAGE} boundary event configured with
     * {@code messageSource = INGRESS_NODE}. Honours an explicit {@code config.messageName} when
     * authored; otherwise synthesises {@code __sq_hook_<workflowId><ns>_<hostNodeId>_<boundaryId>}.
     */
    public static String forBoundary(String workflowId, String hostNodeId,
                                     BoundaryEventConfig cfg, String namespaceSuffix) {
        if (cfg != null && StringUtils.isNotBlank(cfg.getMessageName())) {
            return cfg.getMessageName().trim();
        }
        String boundaryId = cfg == null ? "" : cfg.getId();
        String wf = sanitize(workflowId);
        String ns = StringUtils.isBlank(namespaceSuffix) ? "" : sanitize(namespaceSuffix);
        String host = sanitize(hostNodeId);
        String bid = sanitize(boundaryId);
        return HOOK_PREFIX + wf + ns + "_" + host + "_" + bid;
    }

    private static String sanitize(String raw) {
        if (raw == null || raw.isBlank()) {
            return "unknown";
        }
        return raw.trim().replaceAll("[^A-Za-z0-9_.-]", "_");
    }
}
