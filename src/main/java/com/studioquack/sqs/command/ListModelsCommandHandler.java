package com.studioquack.sqs.command;

import com.studioquack.sqs.aimodel.AiModelProviderClient;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Goal A — {@code list-models}: proxies the configured OpenAI-compatible provider's models path and returns
 * the reply verbatim (the backend normalises it to {@code [{id,label}]}). Returns an empty JSON array (never
 * fails) when no provider is configured or the provider is unreachable, so the picker/tab degrades gracefully.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ListModelsCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; matches the control plane.
     */
    public static final String TYPE = "list-models";

    private final AiModelProviderClient client;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        return DaemonCommandOutcome.ok(client.listModelsRaw());
    }
}
