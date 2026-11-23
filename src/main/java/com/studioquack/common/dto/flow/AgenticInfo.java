package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AgenticInfo implements Serializable {
    private String conversationId;
    private String agentMessageKey;
    private String userPrompt;
    private Map<String, Object> flowInputs;
    private String appPageId;
    private boolean isMessageStartEvent;

    /**
     * Refs for files attached to the triggering user message.
     * Only the refs travel here; the bytes stay in the file store.
     */
    private List<ConversationFileRefDto> attachments;

    /**
     * Optional voice clip on the triggering user message (kind typically {@code audio}).
     * Arrives on start-run with {@code contentBase64} inlined; {@code StartRunFileIngest}
     * moves the bytes into the client file store (same as attachments). The Duckbot start
     * bridge re-encodes those bytes to a base64 string on the start node's {@code voices}
     * OUTPUT key — not an {@code sq-file:} handle — so downstream nodes receive audio
     * inline. Absent when the turn has no voice.
     */
    private ConversationFileRefDto voice;

    /**
     * When true, Duckbot start publishes the full client-store thread. When false, only the
     * current turn text (leading {@code @agent} already stripped by the control plane) is
     * published — the explicit Completion choice from Chat settings. Conversation (full
     * history) is the default everywhere, 1:1 and multi-party alike.
     */
    private boolean includeFullHistory;

    /** Set only on the re-run a prompt edit triggers; null on every ordinary turn. */
    private PromptEditSupersede promptEdit;

    public AgenticInfo(String conversationId, String agentMessageKey, String userPrompt, boolean isMessageStartEvent) {
        this.conversationId = conversationId;
        this.agentMessageKey = agentMessageKey;
        this.userPrompt = userPrompt;
        this.isMessageStartEvent = isMessageStartEvent;
    }

    public AgenticInfo(String conversationId, String agentMessageKey, Map<String, Object> flowInputs, String appPageId, boolean isMessageStartEvent) {
        this.conversationId = conversationId;
        this.agentMessageKey = agentMessageKey;
        this.flowInputs = flowInputs;
        this.appPageId = appPageId;
        this.isMessageStartEvent = isMessageStartEvent;
    }
}
