package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelCapabilityState;
import com.studioquack.sqs.aimodel.AiModelLoadState;
import com.studioquack.sqs.aimodel.AiModelProviderClient;
import com.studioquack.sqs.aimodel.AiModelProviderConfig;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.ProviderDialGuard;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Goal A — {@code set-ai-model}: applies the backend-pushed OpenAI-compatible provider connection to the live
 * {@link AiModelProviderHolder}. Auto-discovered by {@code DaemonCommandRegistry} like every other
 * {@link DaemonCommandHandler}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SetAiModelCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; matches the control plane.
     */
    public static final String TYPE = "set-ai-model";

    private final ObjectMapper objectMapper;
    private final AiModelProviderHolder holder;
    private final AiModelLoadState loadState;
    private final AiModelCapabilityState capabilityState;
    private final AiModelProviderClient client;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        String payload = command == null ? null : command.payload();
        if (payload == null || payload.isBlank()) {
            return DaemonCommandOutcome.failed("set-ai-model: missing provider payload");
        }
        AiModelProviderConfig config;
        try {
            config = objectMapper.readValue(payload, AiModelProviderConfig.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("set-ai-model: malformed provider payload: " + message(parse));
        }
        if (config == null) {
            return DaemonCommandOutcome.failed("set-ai-model: empty provider payload");
        }
        if (config.configured()) {
            String blocked = firstBlocked(config.modelsUrl(), config.chatUrl(),
                    config.loadUrl(), config.unloadUrl(), config.downloadUrl(),
                    config.downloadStatusUrl());
            if (blocked != null) {
                return DaemonCommandOutcome.failed("set-ai-model: " + blocked);
            }
        }
        // A server change invalidates the loaded-model record: the old provider took its
        // in-memory models with it, so the picker/guard must not keep offering them.
        // Key- or path-only edit against the SAME server leaves the record intact.
        String previousServer = holder.current().server();
        String nextServer = config.server();
        boolean serverChanged = previousServer == null ? nextServer != null : !previousServer.equals(nextServer);
        holder.apply(config);
        if (serverChanged) {
            loadState.clear();
            capabilityState.reset();
        }
        // Probe the load lifecycle of the just-applied provider: a config can carry a load path the
        // real server ignores (vLLM), 404s (single-model llama-server) or rejects (401) — in every
        // such case the provider must fall to "Managed By Provider" so the operator never faces a
        // load button that silently does nothing. This is done at Save time, per the design.
        client.refreshCapability();
        // Reconcile the loaded record from the provider itself: for a managed provider from its
        // loaded-models endpoint (LM Studio's /api/v1/models), for a provider-managed one from the


        client.reconcileLoadedModels(holder.current());
        try {
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    new Ack("applied", config.server(), config.configured())));
        } catch (Exception e) {
            return DaemonCommandOutcome.ok("{\"applied\":true}");
        }
    }

    private static String firstBlocked(String... urls) {
        for (String url : urls) {
            String blocked = ProviderDialGuard.refusalIfPresent(url);
            if (blocked != null) {
                return blocked;
            }
        }
        return null;
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Tiny ack the BE stores in its recent-results ring (never carries the key).
     */
    private record Ack(String status, String server, boolean configured) {
    }
}
