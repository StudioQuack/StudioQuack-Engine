package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.conversation.ConversationMessageEntity;
import com.studioquack.sqs.conversation.ConversationStore;
import com.studioquack.sqs.status.ConversationOutputCallback;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serves Duckbot conversation history to the chat UI, and honours a user's delete. Conversation history is
 * stored on this daemon.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConversationCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "conversation";

    private final ObjectMapper objectMapper;
    private final ConversationStore conversationStore;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        Payload payload;
        try {
            payload = objectMapper.readValue(command.payload(), Payload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("conversation: malformed payload: " + parse.getMessage());
        }
        if (payload == null || payload.conversationId() == null || payload.conversationId().isBlank()) {
            return DaemonCommandOutcome.failed("conversation: conversationId is required");
        }
        String op = payload.op() == null ? "history" : payload.op().trim().toLowerCase();
        try {
            return switch (op) {
                case "history" -> ok(history(payload.conversationId(), payload.userId()));
                case "delete" -> ok(delete(payload.conversationId(), payload.userId()));
                default -> DaemonCommandOutcome.failed("conversation: unknown op '" + op + "'");
            };
        } catch (Exception ex) {
            // Never throw out of a command handler: an unhandled exception would surface to the
            // operator as an opaque dispatch failure rather than a readable command error.
            log.warn("conversation: op={} conversationId={} failed: {}",
                    op, payload.conversationId(), ex.toString());
            return DaemonCommandOutcome.failed("conversation: " + op + " failed: " + ex.getMessage());
        }
    }

    private Map<String, Object> history(String conversationId, String userId) {
        List<Map<String, Object>> messages = new ArrayList<>();
        int msgOrder = 0;
        for (ConversationMessageEntity entity : conversationStore.historyForUser(conversationId, userId)) {
            Map<String, Object> message = new LinkedHashMap<>();
            // The daemon owns the thread, so its row order IS the message order — stamp it so the
            // chat can order/anchor rows exactly as the live updates did.
            message.put("msgOrder", ++msgOrder);
            message.put("role", entity.getRole());
            message.put("content", entity.getContent());
            message.put("createdAt", entity.getCreatedAt() == null ? null : entity.getCreatedAt().toString());
            message.put("appPageId", entity.getAppPageId());
            // Attachments travel as the refs they were stored with, not as content: the chat fetches a
            // file on demand, so a thread with large attachments stays cheap to open.
            message.put("attachments", conversationStore.attachmentsOf(entity));
            // Thinking persists with the answer row: a reload shows the same reasoning bubble the
            // live turn painted, instead of losing it with the ephemeral stream buffer.
            var thinkingSections = conversationStore.thinkingSectionsOf(entity);
            if (!thinkingSections.isEmpty()) {
                message.put("thinkingSections", thinkingSections);
                message.put("thinking", thinkingSections.stream()
                        .map(ConversationOutputCallback.ThinkingSection::text)
                        .filter(java.util.Objects::nonNull)
                        .reduce((a, b) -> a + "\n\n" + b).orElse(null));
            }
            messages.add(message);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("conversationId", conversationId);
        result.put("messages", messages);
        return result;
    }

    private Map<String, Object> delete(String conversationId, String userId) {
        long deleted = conversationStore.deleteConversationForUser(conversationId, userId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("conversationId", conversationId);
        result.put("deleted", deleted);
        return result;
    }

    private DaemonCommandOutcome ok(Map<String, Object> body) throws Exception {
        return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(body));
    }

    /**
     * @param op {@code history} (default) or {@code delete}
     * @param userId scopes the operation to one person's messages. Sent by the backend for a user-originated
     * request; absent for engine-originated ones.
     */
    record Payload(String op, String conversationId, String userId) { }
}
