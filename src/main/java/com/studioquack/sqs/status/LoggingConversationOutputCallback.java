package com.studioquack.sqs.status;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Default {@link ConversationOutputCallback}: a logging no-op used when the secure channel is off
 * ({@code sqs.channel.enabled=false}, the default). {@code matchIfMissing=true} so a config that
 * never mentions {@code sqs.channel.enabled} still gets this default;
 * {@link ChannelConversationOutputCallback} replaces it when the channel is enabled.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "false", matchIfMissing = true)
public class LoggingConversationOutputCallback implements ConversationOutputCallback {

    @Override
    public java.util.List<com.studioquack.common.dto.flow.ConversationFileRefDto> report(
            ConversationOutput output) {
        if (output == null) {
            return java.util.List.of();
        }
        log.info("duckbot-output[local] conversationId={} role={} chars={}",
                output.conversationId(), output.role(),
                output.message() == null ? 0 : output.message().length());
        return java.util.List.of();
    }
}
