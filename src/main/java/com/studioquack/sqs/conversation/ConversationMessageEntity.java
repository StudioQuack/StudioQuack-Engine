package com.studioquack.sqs.conversation;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * One Duckbot conversation message stored on this daemon. Rows are user content, so log retention
 * does not delete them; they stay until the user deletes the conversation.
 */
@Entity
@Table(name = "conversation_message", indexes = {
        // The dominant read is "the whole conversation, oldest first", issued by every Duckbot start
        // node, so the conversation + ordering columns are indexed together.
        @Index(name = "idx_conv_msg_conversation", columnList = "conversation_id, created_at"),
        @Index(name = "idx_conv_msg_user", columnList = "user_id")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConversationMessageEntity {

    /** Surrogate id; a conversation is an ordered sequence, so the message needs its own key. */
    @Id
    @Column(name = "id", length = 64, nullable = false)
    private String id;

    /** Groups the messages of one chat (or one App transaction). */
    @Column(name = "conversation_id", length = 128, nullable = false)
    private String conversationId;

    /** Owner. Retained so a delete-my-data request can be scoped without consulting the backend. */
    @Column(name = "user_id", length = 128)
    private String userId;

    /** The workflow's Duckbot message key, mirroring the backend record this replaces. */
    @Column(name = "message_key", length = 128)
    private String messageKey;

    /** Set when the message belongs to an App page rather than a chat conversation. */
    @Column(name = "app_page_id", length = 128)
    private String appPageId;

    /** {@code user} or {@code assistant}. */
    @Column(name = "role", length = 32, nullable = false)
    private String role;

    /** Message body. {@code text} because an assistant answer has no useful length ceiling. */
    @Column(name = "content", columnDefinition = "text")
    private String content;

    /**
     * Attachments as a JSON array. Stored denormalised rather than in a child table: it is always read
     * and written with its message and never queried across conversations, so a join would buy nothing.
     */
    @Column(name = "attachments_json", columnDefinition = "text")
    private String attachmentsJson;

    /**
     * The turn's thinking sections as a JSON array of {@code {nodeId, nodeLabel, text}}, persisted
     * with the assistant answer they belong to.
     */
    @Column(name = "thinking_json", columnDefinition = "text")
    private String thinkingJson;

    /** Ordering key for the conversation. */
    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;
}
