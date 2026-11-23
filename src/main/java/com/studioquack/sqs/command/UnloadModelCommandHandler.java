package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelCapabilityState;
import com.studioquack.sqs.aimodel.AiModelLoadState;
import com.studioquack.sqs.aimodel.AiModelProviderClient;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * {@code unload-model}: the operator action behind the Engine Settings → AI Model unload toggle — frees the
 * provider's memory for a model the operator no longer wants pinned. Mirror of {@link LoadModelCommandHandler}
 * : POSTs the provider's configured unload path and, on a 2xx, drops the model from {@link AiModelLoadState}
 * so the picker stops offering it and the no-JIT guard refuses further chat calls for it (fail-fast instead of
 * a mid-run reload).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UnloadModelCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; matches the control plane.
     */
    public static final String TYPE = "unload-model";

    private final ObjectMapper objectMapper;
    private final AiModelProviderClient client;
    private final AiModelLoadState loadState;
    private final AiModelProviderHolder holder;
    private final AiModelCapabilityState capabilityState;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        String model = LoadModelCommandHandler.extractModel(command == null ? null : command.payload());
        if (model == null) {
            return DaemonCommandOutcome.failed("unload-model: missing 'model' in payload");
        }

        // Report honestly instead of POSTing a dead route.
        if (capabilityState.isProbed() && !capabilityState.managed()) {
            return ack(model, "Model '" + model + "' is managed by the provider — "
                    + "the provider controls eviction; no explicit unload is performed.");
        }
        // Mirror of the load guard: when the provider (reconciled live) does not report the
        // model as loaded, there is nothing to unload — succeed quietly instead of surfacing
        // the provider's "instance not found" as an operator-facing failure.
        client.reconcileLoadedModels(holder.current());
        if (!loadState.isLoaded(model)) {
            return ack(model, "Model '" + model + "' is not loaded on the provider.");
        }
        AiModelProviderClient.ModelActionResult result = client.unloadModel(model);
        if (!result.ok()) {
            return DaemonCommandOutcome.failed(result.message());
        }
        loadState.markUnloaded(model);
        return ack(model, result.message());
    }

    private DaemonCommandOutcome ack(String model, String message) {
        try {
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    objectMapper.createObjectNode()
                            .put("model", model)
                            .put("loaded", false)
                            .put("message", message)));
        } catch (Exception e) {
            return DaemonCommandOutcome.ok("{\"loaded\":false}");
        }
    }
}
