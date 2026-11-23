package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ConversationFileRefDto;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.conversation.ConversationStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Serving conversation history to the chat UI through the daemon. The user-scoping assertions
 * matter most: the backend checks access before proxying, but a conversation id behaves like a
 * bearer token and the daemon holds the only copy, so it re-checks.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-conv-cmd-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class ConversationCommandHandlerTest {

    @Autowired
    private ConversationCommandHandler handler;
    @Autowired
    private ConversationStore store;
    @Autowired
    private ObjectMapper mapper;

    private DaemonCommandOutcome run(String json) {
        return handler.handle(new DaemonCommand("c-1", ConversationCommandHandler.TYPE, null, json));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> messagesOf(DaemonCommandOutcome outcome) throws Exception {
        Map<String, Object> body = mapper.readValue(outcome.output(), Map.class);
        return (List<Map<String, Object>>) body.get("messages");
    }

    @Test
    @DisplayName("history returns the thread in order")
    void history_returnsOrderedThread() throws Exception {
        store.append("conv-h", "u1", "mk", null, "user", "question", null);
        store.append("conv-h", "u1", "mk", null, "assistant", "answer", null);

        DaemonCommandOutcome outcome = run("{\"op\":\"history\",\"conversationId\":\"conv-h\"}");

        assertTrue(outcome.succeeded(), outcome.error());
        List<Map<String, Object>> messages = messagesOf(outcome);
        assertEquals(2, messages.size());
        assertEquals("user", messages.get(0).get("role"));
        assertEquals("question", messages.get(0).get("content"));
        assertEquals("assistant", messages.get(1).get("role"));
    }

    @Test
    @DisplayName("history defaults to the history op when none is given")
    void missingOp_defaultsToHistory() throws Exception {
        store.append("conv-default", "u1", "mk", null, "user", "hi", null);

        DaemonCommandOutcome outcome = run("{\"conversationId\":\"conv-default\"}");

        assertTrue(outcome.succeeded(), outcome.error());
        assertEquals(1, messagesOf(outcome).size());
    }

    @Test
    @DisplayName("a scoped read cannot see another user's conversation")
    void history_isScopedToTheRequester() throws Exception {
        store.append("conv-owned", "owner", "mk", null, "user", "private", null);

        DaemonCommandOutcome outcome = run(
                "{\"op\":\"history\",\"conversationId\":\"conv-owned\",\"userId\":\"someone-else\"}");

        assertTrue(outcome.succeeded(), outcome.error());
        assertTrue(messagesOf(outcome).isEmpty(), "another user's thread must not be returned");
    }

    @Test
    @DisplayName("attachments travel as refs, so opening a thread stays cheap")
    void history_carriesAttachmentRefs() throws Exception {
        store.append("conv-files", "u1", "mk", null, "user", "see attached",
                List.of(new ConversationFileRefDto("64aa", "a.pdf", "application/pdf", 3L,
                        "document", Instant.now().toString())));

        DaemonCommandOutcome outcome = run("{\"op\":\"history\",\"conversationId\":\"conv-files\"}");

        String rendered = outcome.output();
        assertTrue(rendered.contains("64aa"), rendered);
        assertFalse(rendered.contains("contentBase64"), "content is fetched on demand, not inlined");
    }

    @Test
    @DisplayName("history carries the persisted thinking sections with the answer row")
    void history_carriesThinkingSections() throws Exception {
        store.append("conv-think", "u1", "mk", null, "assistant", "the answer", null,
                List.of(new com.studioquack.sqs.status.ConversationOutputCallback.ThinkingSection(
                        "agent-1", "Tax Bot", "plan the lookup")));

        DaemonCommandOutcome outcome = run("{\"op\":\"history\",\"conversationId\":\"conv-think\"}");

        assertTrue(outcome.succeeded(), outcome.error());
        List<Map<String, Object>> messages = messagesOf(outcome);
        assertEquals(1, messages.size());
        assertEquals("plan the lookup", messages.get(0).get("thinking"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sections =
                (List<Map<String, Object>>) messages.get(0).get("thinkingSections");
        assertEquals(1, sections.size());
        assertEquals("agent-1", sections.get(0).get("nodeId"));
        assertEquals("Tax Bot", sections.get(0).get("nodeLabel"));
        assertEquals("plan the lookup", sections.get(0).get("text"));
    }

    @Test
    @DisplayName("delete removes the conversation and reports how much went")
    void delete_removesTheConversation() throws Exception {
        store.append("conv-del", "u1", "mk", null, "user", "a", null);
        store.append("conv-del", "u1", "mk", null, "assistant", "b", null);

        DaemonCommandOutcome outcome = run("{\"op\":\"delete\",\"conversationId\":\"conv-del\"}");

        assertTrue(outcome.succeeded(), outcome.error());
        assertEquals(2, mapper.readValue(outcome.output(), Map.class).get("deleted"));
        assertTrue(store.history("conv-del").isEmpty());
    }

    @Test
    @DisplayName("a scoped delete cannot remove someone else's conversation")
    void delete_isScopedToTheRequester() throws Exception {
        // Guessing an id must not be enough to destroy another person's history.
        store.append("conv-theirs", "owner", "mk", null, "user", "keep me", null);

        DaemonCommandOutcome outcome = run(
                "{\"op\":\"delete\",\"conversationId\":\"conv-theirs\",\"userId\":\"attacker\"}");

        assertTrue(outcome.succeeded(), outcome.error());
        assertEquals(0, mapper.readValue(outcome.output(), Map.class).get("deleted"));
        assertEquals(1, store.history("conv-theirs").size(), "the owner's thread survives");
    }

    @Test
    @DisplayName("an unknown conversation is an empty thread, not an error")
    void unknownConversation_isEmpty() throws Exception {
        DaemonCommandOutcome outcome = run("{\"op\":\"history\",\"conversationId\":\"nope\"}");

        assertTrue(outcome.succeeded(), outcome.error());
        assertTrue(messagesOf(outcome).isEmpty());
    }

    @Test
    @DisplayName("a missing conversationId is a readable failure")
    void missingConversationId_fails() {
        DaemonCommandOutcome outcome = run("{\"op\":\"history\"}");

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("conversationId"), outcome.error());
    }

    @Test
    @DisplayName("an unknown op is refused rather than silently treated as a read")
    void unknownOp_fails() {
        DaemonCommandOutcome outcome = run("{\"op\":\"drop-everything\",\"conversationId\":\"c\"}");

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("unknown op"), outcome.error());
    }

    @Test
    @DisplayName("a malformed payload fails the command instead of throwing")
    void malformedPayload_fails() {
        DaemonCommandOutcome outcome = run("{ not json");

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("malformed"), outcome.error());
    }
}
