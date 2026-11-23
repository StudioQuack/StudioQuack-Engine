package com.studioquack.sqs.sandbox.helperchan;

import java.util.Map;

/**
 * Conversation-stream coordinates for one helper job: how a graph-routed live frame reaching a
 * Duckbot sink is attributed in the conversation (run id, conversation id, message key, source
 * node + label).
 */
public final class ConversationStreamCoordinator {

    private ConversationStreamCoordinator() { }

    /**
     * Reads Duckbot coordinates from the helper job's execution-context map (Jackson-serialized
     * {@code AgenticInfo} lands as a nested map under {@code agenticInfo}).
     */
    public static AgenticCoords coordsOf(HelperJob job) {
        Map<String, Object> ec = job.executionContext();
        if (ec == null || ec.isEmpty()) {
            return null;
        }
        String wfeId = str(ec.get("workflowExecutionId"));
        String nodeId = str(ec.get("currentNodeId"));
        if (wfeId.isEmpty() || nodeId.isEmpty()) {
            return null;
        }
        Object raw = ec.get("agenticInfo");
        if (!(raw instanceof Map<?, ?> ai)) {
            return null;
        }
        String conversationId = str(ai.get("conversationId"));
        if (conversationId.isEmpty()) {
            return null;
        }
        return new AgenticCoords(wfeId, nodeId, conversationId, emptyToNull(str(ai.get("agentMessageKey"))),
                nodeLabelOf(ec, nodeId));
    }

    /** Emitting node is {@code currentNode} here — {@code NodeDto.data.label}, else nodeId. */
    private static String nodeLabelOf(Map<String, Object> ec, String nodeId) {
        Object currentNode = ec.get("currentNode");
        if (currentNode instanceof Map<?, ?> cn && cn.get("data") instanceof Map<?, ?> data) {
            String label = str(data.get("label"));
            if (!label.isBlank()) {
                return label;
            }
        }
        return nodeId;
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    record AgenticCoords(String workflowExecutionId, String nodeId,
                         String conversationId, String agentMessageKey, String nodeLabel) {
    }
}
