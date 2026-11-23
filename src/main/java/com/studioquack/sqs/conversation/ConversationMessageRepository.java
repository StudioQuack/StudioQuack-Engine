package com.studioquack.sqs.conversation;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * JPA access to Duckbot conversation messages stored on this daemon.
 */
@Repository
public interface ConversationMessageRepository extends JpaRepository<ConversationMessageEntity, String> {

    /** The whole conversation in order — what a Duckbot start node reads on every run. */
    List<ConversationMessageEntity> findByConversationIdOrderByCreatedAtAsc(String conversationId);

    /**
     * The same read, scoped to one user. Used when the request came from a person rather than from the
     * engine.
     */
    List<ConversationMessageEntity> findByConversationIdAndUserIdOrderByCreatedAtAsc(
            String conversationId, String userId);

    /** User-initiated delete of one conversation from the Duckbot modal. */
    long deleteByConversationId(String conversationId);

    /** Scoped delete — cannot remove a conversation belonging to someone else. */
    long deleteByConversationIdAndUserId(String conversationId, String userId);

    /** Scoped erasure: everything belonging to one user on this daemon. */
    long deleteByUserId(String userId);

    long countByConversationId(String conversationId);
}
