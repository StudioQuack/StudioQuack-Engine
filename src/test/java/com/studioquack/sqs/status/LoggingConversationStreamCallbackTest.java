package com.studioquack.sqs.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * {@link LoggingConversationStreamCallback} must never throw, including on null/blank frames.
 * INFO logs SHAPE only for start/end; deltas are DEBUG — this test does not assert log contents (no logger spy).
 */
class LoggingConversationStreamCallbackTest {

    private final LoggingConversationStreamCallback callback = new LoggingConversationStreamCallback();

    @Test
    @DisplayName("emit(null) does not throw")
    void emit_null_doesNotThrow() {
        assertThatCode(() -> callback.emit(null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("blank conversationId / workflowExecutionId is skipped without throwing")
    void emit_blankIds_doesNotThrow() {
        assertThatCode(() -> callback.emit(new ConversationStreamCallback.ConversationStreamEvent(
                "", " ", "msg-1", "node-1", "stream-delta", "answer", "secret", 1, 1, null)))
                .doesNotThrowAnyException();
        assertThatCode(() -> callback.emit(new ConversationStreamCallback.ConversationStreamEvent(
                "wx-1", "", "msg-1", "node-1", "stream-delta", "answer", "secret", 1, 1, null)))
                .doesNotThrowAnyException();
        assertThatCode(() -> callback.emit(new ConversationStreamCallback.ConversationStreamEvent(
                "wx-1", "conv-1", "msg-1", "node-1", null, "answer", "secret", 1, 1, null)))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("valid frame takes the emit path without throwing")
    void emit_valid_doesNotThrow() {
        assertThatCode(() -> callback.emit(new ConversationStreamCallback.ConversationStreamEvent(
                "wx-1", "conv-1", "msg-1", "node-1", "stream-delta", "answer", "secret token", 4, 2, null)))
                .doesNotThrowAnyException();
    }
}
