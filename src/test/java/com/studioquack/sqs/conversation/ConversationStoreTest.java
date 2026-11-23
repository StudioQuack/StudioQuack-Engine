package com.studioquack.sqs.conversation;

import com.studioquack.common.dto.flow.ConversationFileRefDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client-owned Duckbot conversation history against a real embedded database schema. Runs on the
 * embedded engine's own datasource so the {@code ddl-auto} mapping is exercised too — a store whose
 * entity does not actually materialise into a table would pass any mock-based test.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-conversation-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class ConversationStoreTest {

    @Autowired
    private ConversationStore store;

    private static ConversationFileRefDto ref(String id, String name) {
        return new ConversationFileRefDto(id, name, "application/pdf", 10L, "document", Instant.now().toString());
    }

    @Test
    @DisplayName("messages come back in the order they were appended")
    void history_isOrdered() {
        // A Duckbot start node reads this as "what has been said so far", so ordering is the contract.
        String conversationId = "conv-order";
        store.append(conversationId, "u1", "mk", null, "user", "first", null);
        store.append(conversationId, "u1", "mk", null, "assistant", "second", null);
        store.append(conversationId, "u1", "mk", null, "user", "third", null);

        List<ConversationMessageEntity> history = store.history(conversationId);

        assertEquals(List.of("first", "second", "third"),
                history.stream().map(ConversationMessageEntity::getContent).toList());
        assertEquals(List.of("user", "assistant", "user"),
                history.stream().map(ConversationMessageEntity::getRole).toList());
    }

    @Test
    @DisplayName("conversations are isolated from one another")
    void history_isScopedToItsConversation() {
        store.append("conv-a", "u1", "mk", null, "user", "in a", null);
        store.append("conv-b", "u1", "mk", null, "user", "in b", null);

        assertEquals(1, store.history("conv-a").size());
        assertEquals("in a", store.history("conv-a").get(0).getContent());
    }

    @Test
    @DisplayName("attachments survive a round trip")
    void attachments_roundTrip() {
        String conversationId = "conv-files";
        store.append(conversationId, "u1", "mk", null, "user", "see attached",
                List.of(ref("64aa", "invoice.pdf"), ref("64bb", "scan.pdf")));

        ConversationMessageEntity stored = store.history(conversationId).get(0);
        List<ConversationFileRefDto> attachments = store.attachmentsOf(stored);

        assertEquals(2, attachments.size());
        assertEquals("64aa", attachments.get(0).getFileId());
        assertEquals("invoice.pdf", attachments.get(0).getName());
    }

    @Test
    @DisplayName("a message with no attachments reads back as an empty list, never null")
    void noAttachments_isEmptyList() {
        store.append("conv-plain", "u1", "mk", null, "user", "hello", null);

        assertTrue(store.attachmentsOf(store.history("conv-plain").get(0)).isEmpty());
        assertTrue(store.attachmentsOf(null).isEmpty());
    }

    @Test
    @DisplayName("deleting a conversation removes only that conversation")
    void deleteConversation_isScoped() {
        // The user-facing delete in the Duckbot modal. Over-deleting here destroys unrelated history.
        store.append("conv-doomed", "u1", "mk", null, "user", "bye", null);
        store.append("conv-doomed", "u1", "mk", null, "assistant", "ok", null);
        store.append("conv-kept", "u1", "mk", null, "user", "still here", null);

        assertEquals(2, store.deleteConversation("conv-doomed"));

        assertTrue(store.history("conv-doomed").isEmpty());
        assertEquals(1, store.history("conv-kept").size());
    }

    @Test
    @DisplayName("deleting by user removes that user's history across conversations")
    void deleteForUser_isScopedToTheUser() {
        store.append("conv-u1-a", "user-1", "mk", null, "user", "a", null);
        store.append("conv-u1-b", "user-1", "mk", null, "user", "b", null);
        store.append("conv-u2", "user-2", "mk", null, "user", "c", null);

        assertEquals(2, store.deleteForUser("user-1"));

        assertTrue(store.history("conv-u1-a").isEmpty());
        assertEquals(1, store.history("conv-u2").size(), "another user's history is untouched");
    }

    @Test
    @DisplayName("an App-page message keeps its page id")
    void appPageId_isRetained() {
        store.append("txn-1", "u1", "mk", "page-7", "assistant", "done", null);

        assertEquals("page-7", store.history("txn-1").get(0).getAppPageId());
    }

    @Test
    @DisplayName("a blank role defaults rather than violating the not-null column")
    void blankRole_defaults() {
        store.append("conv-role", "u1", "mk", null, "  ", "x", null);

        assertEquals("user", store.history("conv-role").get(0).getRole());
    }

    @Test
    @DisplayName("appending without a conversation id is rejected")
    void missingConversationId_isRejected() {
        // Silently accepting it would create history nothing can ever read back.
        assertThrows(IllegalArgumentException.class,
                () -> store.append(null, "u1", "mk", null, "user", "orphan", null));
        assertThrows(IllegalArgumentException.class,
                () -> store.append("  ", "u1", "mk", null, "user", "orphan", null));
    }

    @Test
    @DisplayName("reads and deletes of unknown ids are harmless")
    void unknownIds_areNoOps() {
        assertTrue(store.history("nope").isEmpty());
        assertTrue(store.history(null).isEmpty());
        assertEquals(0, store.deleteConversation("nope"));
        assertEquals(0, store.deleteConversation(null));
        assertEquals(0, store.deleteForUser(null));
    }
}
