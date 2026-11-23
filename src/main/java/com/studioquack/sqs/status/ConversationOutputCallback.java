package com.studioquack.sqs.status;

/**
 * Seam that routes a Duckbot/agentic workflow's final answer back to the control plane so it lands
 * in the conversation ({@code conversation_contents}) and is pushed to the Duckbot screen over
 * SSE.
 */
public interface ConversationOutputCallback {

    /**
     * @return Stored files attachment refs the backend stored for the answer's files (so the daemon
     * can rewrite its local history row for browser downloads), or an empty list when none / the push
     * did not land
     */
    java.util.List<com.studioquack.common.dto.flow.ConversationFileRefDto> report(ConversationOutput output);

    /**
     * One Duckbot end-node message. {@code appPageId} non-blank routes to the application store;
     * otherwise it is stored as an {@code assistant} conversation message.
     */
    /**
     * @param msgOrder The message's position in the conversation, counted by the daemon that owns the
     * thread.
     * @param files A {@code sq-file:} handle is meaningless to the backend — it cannot read the client
     * file store — so the end bridge resolves each handle and sends content.
     * @param voices Audio ContentBlocks from the end node's {@code voices} port (same
     * {@link OutputFile} shape as {@code files}).
     * @param nodeId Source/renderer node that produced this answer (the inbound
     * {@code InOutValueDto.nodeId}).
     */
    record ConversationOutput(
            String workflowExecutionId,
            String conversationId,
            String agentMessageKey,
            String appPageId,
            String role,
            String message,
            int msgOrder,
            java.util.List<OutputFile> files,
            java.util.List<OutputFile> voices,
            String nodeId,
            java.util.List<ThinkingSection> thinkingSections) {
    }

    /**
     * One agent's reasoning block, persisted with the answer so the bubble survives the next turn, a
     * reload.
     *
     * @param nodeId
     * @param nodeLabel
     * @param text
     */
    record ThinkingSection(String nodeId, String nodeLabel, String text) {
    }

    /**
     * @param contentBase64 Required — this is the whole point of the record; a reference would not
     * survive the client boundary.
     */
    record OutputFile(
            String name,
            String mimeType,
            String kind,
            String contentBase64) {
    }
}
