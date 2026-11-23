package com.studioquack.sqs.status;

/**
 * Seam that POSTs live Duckbot stream frames to the control plane so the chat can render tokens as
 * a node produces them — the incremental counterpart of {@link ConversationOutputCallback}.
 */
public interface ConversationStreamCallback {

    /** Routes one stream frame to the BE conversation. Never throws. */
    void emit(ConversationStreamEvent event);

    /**
     * {@code delta} is omitted on start/end. {@code msgOrder} is optional — the backend uses it when
     * present to order the live update.
     */
    record ConversationStreamEvent(
            String workflowExecutionId,
            String conversationId,
            String agentMessageKey,
            String nodeId,
            String event,
            String channel,
            String delta,
            int seq,
            Integer msgOrder,
            String nodeLabel) {
    }
}
