package com.studioquack.sqs.sandbox.helperchan;

import java.util.Map;

/**
 * Sink for graph-routed live stream frames. A node's {@code emit-partial} that names its own
 * source port travels the workflow graph (edge resolution + per-frame JOLT in {@link
 * StreamEdgeRouter}) and lands here when an edge target is a live-render end point (the Duckbot
 * end node).
 */
public interface StreamRouteSink {

    /**
     * One routed frame: a single delta travelling from {@code sourceNodeId}'s port along an
     * edge into {@code targetNodeId}. {@code channel} is the producer-declared stream channel
     * ({@code thinking}/{@code answer}); the sink owns any normalisation.
     */
    record StreamFrame(String workflowExecutionId, String conversationId, String agentMessageKey,
                       String sourceNodeId, String targetNodeId, String channel, String delta,
                       String nodeLabel) {
    }

    /** @return true when the target node (serialized NodeDto map) is a live-render end point. */
    boolean isStreamTarget(Map<String, Object> targetNode);

    /** Renders one delta, opening the per-(source node, channel) session lazily. */
    void onFrame(StreamFrame frame);

    /** Closes every open session the source node owns on this run ({@code stream-end}). */
    void onSourceFinished(String workflowExecutionId, String sourceNodeId);

    /**
     * @return true when a live stream for (workflowExecutionId, conversationId, sourceNodeId,
     *         channel) already rendered recently — the Duckbot end then skips re-rendering
     *         that source's assembled value for the same channel on the same run. Keyed per
     *         run so turn 1's live paint never suppresses turn 2's fallback (or its
     *         stream-end) on the same conversation.
     */
    boolean hadRecentStream(String workflowExecutionId, String conversationId, String sourceNodeId,
                            String channel);

    /**
     * Renders an assembled (non-streamed) value as one fallback stream on the given channel, appending
     * across invocations within the conversation turn. Used when the producer ran non-streaming so the
     * live path never fired.
     */
    void renderFallback(String workflowExecutionId, String conversationId, String agentMessageKey,
                        String rendererNodeId, String nodeLabel, String channel, String text);

    /** Ends any fallback session for the conversation (called when the final answer persists). */
    void endFallback(String conversationId);
}
