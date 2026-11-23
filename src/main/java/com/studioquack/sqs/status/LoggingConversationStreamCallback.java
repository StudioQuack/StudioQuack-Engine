package com.studioquack.sqs.status;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Default {@link ConversationStreamCallback}: a logging no-op used when the secure channel is off
 * ({@code sqs.channel.enabled=false}, the default). {@code matchIfMissing=true} so a config that
 * never mentions {@code sqs.channel.enabled} still gets this default;
 * {@link ChannelConversationStreamCallback} replaces it when the channel is enabled.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "false", matchIfMissing = true)
public class LoggingConversationStreamCallback implements ConversationStreamCallback {

    @Override
    public void emit(ConversationStreamEvent event) {
        try {
            if (event == null || event.event() == null
                    || isBlank(event.conversationId()) || isBlank(event.workflowExecutionId())) {
                return;
            }
            String format = "duckbot-stream[local] conversationId={} event={} channel={} seq={} deltaChars={}";
            Object[] args = {
                    event.conversationId(), event.event(), event.channel(), event.seq(),
                    event.delta() == null ? 0 : event.delta().length()
            };
            // Per-delta chatter is DEBUG; start/end are session open/close (INFO).
            if ("stream-delta".equals(event.event())) {
                log.debug(format, args);
            } else {
                log.info(format, args);
            }
        } catch (RuntimeException ignored) {
            // Logging no-op must never fail a node.
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
