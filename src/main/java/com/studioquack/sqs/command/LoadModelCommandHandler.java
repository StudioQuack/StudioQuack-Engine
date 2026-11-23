package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
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
 * {@code load-model}: loads a provider model into memory AHEAD of execution — the operator action behind the
 * Engine Settings → AI Model load toggle. The daemon POSTs the provider's configured load path and, on a 2xx,
 * records the model in {@link AiModelLoadState} so the settings panel shows it loaded, the designer picker
 * offers it, and the llm proxy's no-JIT guard admits it.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LoadModelCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; matches the control plane.
     */
    public static final String TYPE = "load-model";

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
        String model = extractModel(command == null ? null : command.payload());
        if (model == null) {
            return DaemonCommandOutcome.failed("load-model: missing 'model' in payload");
        }
        // Provider-managed provider (no working load route): the daemon must NOT pretend to load
        // the provider owns residency (vLLM's fixed model, Ollama/Unsloth JIT-on-request). Report a
        // clean, honest result so the operator never sees a Load action that silently does nothing.
        if (capabilityState.isProbed() && !capabilityState.managed()) {
            return ack(model, "Model '" + model + "' is managed by the provider — it loads on demand; "
                    + "no explicit load is performed.");
        }
        // Double-load guard: reconcile from the provider first, and when it already reports the
        // model in memory, skip the load call entirely. A blind re-load on LM Studio-class servers
        // maps the weights AGAIN as a second instance — the operator's RAM pays for our stale view.
        client.reconcileLoadedModels(holder.current());
        if (loadState.isLoaded(model)) {
            return ack(model, "Model '" + model + "' is already loaded on the provider.");
        }
        AiModelProviderClient.ModelActionResult result = client.loadModel(model);
        if (!result.ok()) {
            return DaemonCommandOutcome.failed(result.message());
        }
        loadState.markLoaded(model);
        return ack(model, result.message());
    }

    private DaemonCommandOutcome ack(String model, String message) {
        try {
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    objectMapper.createObjectNode()
                            .put("model", model)
                            .put("loaded", true)
                            .put("message", message)));
        } catch (Exception e) {
            return DaemonCommandOutcome.ok("{\"loaded\":true}");
        }
    }

    /**
     * The {@code model} field of the JSON payload, or {@code null} when absent/blank/unparseable.
     */
    static String extractModel(String payload) {
        if (payload == null || payload.isBlank()) {
            return null;
        }
        try {
            JsonNode node = new ObjectMapper().readTree(payload);
            JsonNode model = node == null ? null : node.get("model");
            return model != null && model.isTextual() && !model.asText().isBlank()
                    ? model.asText() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
