package com.studioquack.sqs.conversation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ConversationFileRefDto;
import com.studioquack.sqs.status.ConversationOutputCallback;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Client-owned Duckbot conversation history. The Duckbot start node reads the conversation so far;
 * the end node appends the answer.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ConversationStore {

    private static final TypeReference<List<ConversationFileRefDto>> ATTACHMENT_LIST =
            new TypeReference<>() { };
    private static final TypeReference<List<ConversationOutputCallback.ThinkingSection>> THINKING_LIST =
            new TypeReference<>() { };

    private final ConversationMessageRepository repository;
    private final ObjectMapper objectMapper;

    /**
     * Appends one message and returns it.
     *
     * @param role {@code user} or {@code assistant}
     */
    @Transactional
    public ConversationMessageEntity append(String conversationId, String userId, String messageKey,
                                            String appPageId, String role, String content,
                                            List<ConversationFileRefDto> attachments) {
        return append(conversationId, userId, messageKey, appPageId, role, content, attachments, null);
    }

    /**
     * Appends one message, persisting the turn's thinking sections alongside an assistant answer.
     *
     * @param thinkingSections per-agent reasoning in production order; null/empty for user turns and
     *                         answers that produced none
     */
    @Transactional
    public ConversationMessageEntity append(String conversationId, String userId, String messageKey,
                                            String appPageId, String role, String content,
                                            List<ConversationFileRefDto> attachments,
                                            List<ConversationOutputCallback.ThinkingSection> thinkingSections) {
        if (conversationId == null || conversationId.isBlank()) {
            throw new IllegalArgumentException("conversationId is required to append a message");
        }
        ConversationMessageEntity entity = ConversationMessageEntity.builder()
                .id(UUID.randomUUID().toString())
                .conversationId(conversationId)
                .userId(userId)
                .messageKey(messageKey)
                .appPageId(appPageId)
                .role(role == null || role.isBlank() ? "user" : role)
                .content(content)
                .attachmentsJson(writeAttachments(attachments))
                .thinkingJson(writeThinking(thinkingSections))
                .createdAt(OffsetDateTime.now())
                .build();
        ConversationMessageEntity saved = repository.save(entity);
        log.debug("conversation.append conversationId={} role={} attachments={} thinkingSections={}",
                conversationId, saved.getRole(), attachments == null ? 0 : attachments.size(),
                thinkingSections == null ? 0 : thinkingSections.size());
        return saved;
    }

    /** How many messages the conversation now holds — the ordering key for a live chat update. */
    @Transactional(readOnly = true)
    public int messageCount(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) {
            return 0;
        }
        return (int) repository.countByConversationId(conversationId);
    }

    /** The conversation in order — the Duckbot start node's view of "what has been said". */
    @Transactional(readOnly = true)
    public List<ConversationMessageEntity> history(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) {
            return List.of();
        }
        return repository.findByConversationIdOrderByCreatedAtAsc(conversationId);
    }

    /**
     * For requests originating from a person, as opposed to the engine reading its own run's thread. A
     * conversation id behaves like a bearer token and this store holds the only copy, so ownership is
     * re-checked here rather than trusted from the caller.
     */
    @Transactional(readOnly = true)
    public List<ConversationMessageEntity> historyForUser(String conversationId, String userId) {
        if (conversationId == null || conversationId.isBlank()) {
            return List.of();
        }
        if (userId == null || userId.isBlank()) {
            return history(conversationId);
        }
        return repository.findByConversationIdAndUserIdOrderByCreatedAtAsc(conversationId, userId);
    }

    /** Attachments of a stored message, or an empty list when it had none / the JSON is unreadable. */
    public List<ConversationFileRefDto> attachmentsOf(ConversationMessageEntity entity) {
        if (entity == null || entity.getAttachmentsJson() == null || entity.getAttachmentsJson().isBlank()) {
            return List.of();
        }
        try {
            return objectMapper.readValue(entity.getAttachmentsJson(), ATTACHMENT_LIST);
        } catch (Exception e) {
            // A message with unreadable attachment metadata is still worth showing.
            log.warn("conversation.attachments unreadable for message {}: {}", entity.getId(), e.toString());
            return List.of();
        }
    }

    /**
     * Rewrites a message's attachment refs after the backend stores the answer's files in stored
     * files. The end bridge first records client {@code sq-file:} handles; those are meaningless to
     * the browser download endpoint.
     */
    @Transactional
    public void replaceAttachments(String messageId, List<ConversationFileRefDto> attachments) {
        if (messageId == null || messageId.isBlank() || attachments == null || attachments.isEmpty()) {
            return;
        }
        ConversationMessageEntity entity = repository.findById(messageId).orElse(null);
        if (entity == null) {
            return;
        }
        entity.setAttachmentsJson(writeAttachments(attachments));
        repository.save(entity);
    }

    /**
     * Drops the user turn a prompt edit replaced, plus the turns it superseded, so the re-run's thread
     * reads as if the edited prompt had been sent instead. The turn is the {@code turnOrdinal}-th user
     * row recorded for {@code messageKey}.
     *
     * @param dropLaterUserTurns True removes every later row; false removes only later non-user rows
     * (a room with people keeps their turns)
     */
    @Transactional
    public int supersedeTurn(String conversationId, String messageKey, int turnOrdinal,
                             String previousPrompt, boolean dropLaterUserTurns) {
        if (conversationId == null || conversationId.isBlank() || turnOrdinal < 1 || previousPrompt == null) {
            return 0;
        }
        List<ConversationMessageEntity> thread = history(conversationId);
        int start = -1;
        int seen = 0;
        for (int i = 0; i < thread.size(); i++) {
            ConversationMessageEntity row = thread.get(i);
            if ("user".equals(row.getRole()) && Objects.equals(messageKey, row.getMessageKey())
                    && ++seen == turnOrdinal) {
                start = i;
                break;
            }
        }
        if (start < 0 || !previousPrompt.trim().equals(Objects.toString(thread.get(start).getContent(), "").trim())) {
            log.info("conversation.supersede skipped conversationId={} reason={}",
                    conversationId, start < 0 ? "turn-not-found" : "content-drift");
            return 0;
        }
        List<ConversationMessageEntity> superseded = new ArrayList<>();
        superseded.add(thread.get(start));
        for (int i = start + 1; i < thread.size(); i++) {
            ConversationMessageEntity row = thread.get(i);
            if (dropLaterUserTurns || !"user".equals(row.getRole())) {
                superseded.add(row);
            }
        }
        repository.deleteAll(superseded);
        log.info("conversation.superseded conversationId={} messages={}", conversationId, superseded.size());
        return superseded.size();
    }

    /** User deleted the conversation in the Duckbot modal. @return rows removed. */
    @Transactional
    public long deleteConversation(String conversationId) {
        return deleteConversationForUser(conversationId, null);
    }

    /**
     * Deletes a conversation, refusing to touch one that belongs to someone else when {@code userId}
     * is supplied. A blank user is the unscoped engine/admin path.
     */
    @Transactional
    public long deleteConversationForUser(String conversationId, String userId) {
        if (conversationId == null || conversationId.isBlank()) {
            return 0;
        }
        long removed = (userId == null || userId.isBlank())
                ? repository.deleteByConversationId(conversationId)
                : repository.deleteByConversationIdAndUserId(conversationId, userId);
        log.info("conversation.deleted conversationId={} messages={} scoped={}",
                conversationId, removed, userId != null && !userId.isBlank());
        return removed;
    }

    /** Scoped erasure for one user on this daemon. @return rows removed. */
    @Transactional
    public long deleteForUser(String userId) {
        if (userId == null || userId.isBlank()) {
            return 0;
        }
        long removed = repository.deleteByUserId(userId);
        log.info("conversation.deleted_for_user userId={} messages={}", userId, removed);
        return removed;
    }

    /** Thinking sections of a stored assistant message, or an empty list when it had none. */
    public List<ConversationOutputCallback.ThinkingSection> thinkingSectionsOf(ConversationMessageEntity entity) {
        if (entity == null || entity.getThinkingJson() == null || entity.getThinkingJson().isBlank()) {
            return List.of();
        }
        try {
            return objectMapper.readValue(entity.getThinkingJson(), THINKING_LIST);
        } catch (Exception e) {
            // A message with unreadable thinking metadata still shows its answer.
            log.warn("conversation.thinking unreadable for message {}: {}", entity.getId(), e.toString());
            return List.of();
        }
    }

    private String writeAttachments(List<ConversationFileRefDto> attachments) {
        if (attachments == null || attachments.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(attachments);
        } catch (Exception e) {
            // Losing the attachment metadata is bad; losing the message body with it would be worse.
            log.warn("conversation.append could not serialise {} attachment(s): {}",
                    attachments.size(), e.toString());
            return null;
        }
    }

    private String writeThinking(List<ConversationOutputCallback.ThinkingSection> thinkingSections) {
        if (thinkingSections == null || thinkingSections.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(thinkingSections);
        } catch (Exception e) {
            // Same trade as attachments: never lose the answer over its reasoning metadata.
            log.warn("conversation.append could not serialise {} thinking section(s): {}",
                    thinkingSections.size(), e.toString());
            return null;
        }
    }

}
