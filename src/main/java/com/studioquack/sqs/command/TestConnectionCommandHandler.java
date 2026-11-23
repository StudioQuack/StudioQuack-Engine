package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.aimodel.AiModelCapabilityState;
import com.studioquack.sqs.aimodel.AiModelProviderClient;
import com.studioquack.sqs.aimodel.AiModelProviderConfig;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Goal A / ITEM 6 — {@code test-connection}: probes an OpenAI-compatible provider and ships back a structured
 * {@code {ok,status,message?}} which the backend relays verbatim to the AI Model tab. When the command carries
 * a provider-config payload (the operator's DRAFT, forwarded by the BE so a just-picked preset can be tested
 * BEFORE Save) we probe THAT config; otherwise we fall back to the daemon's STORED config (the legacy no-arg
 * behaviour, so an untouched save path still works and the operator never has to resend a stored key).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TestConnectionCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; matches the control plane.
     */
    public static final String TYPE = "test-connection";

    private final ObjectMapper objectMapper;
    private final AiModelProviderClient client;
    private final AiModelProviderHolder holder;
    private final AiModelCapabilityState capabilityState;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        AiModelProviderConfig override = parseOverride(command.payload());
        // ITEM 6: draft payload (BE-forwarded form) wins over the daemon's stored provider.
        // LM Studio preset probe URL = GET {server}/v1/models → http://127.0.0.1:1234/v1/models.
        if (override != null) {
            log.info("test-connection: probing DRAFT provider url={}", override.modelsUrl());
        } else {
            log.info("test-connection: probing STORED provider (no draft payload / no server)");
        }
        AiModelProviderClient.ProbeResult result =
                override != null ? client.probe(override) : client.probe();
        // Probe the load-lifecycle too, so the panel can show "Managed By Provider" the moment a
        // provider is tested — before Save — instead of a Load button that would silently do nothing.
        // A DRAFT probe (preset not yet saved) must NOT mutate the live verdict; a stored-config test
        // refreshes it so the current provider's lifecycle stays truthful.
        AiModelProviderConfig probed = override != null ? override : holder.current();
        AiModelCapabilityState.Capability capability = client.probeCapability(probed);
        if (override == null) {
            capabilityState.set(capability);
        }
        ObjectNode node = objectMapper.createObjectNode();
        node.put("ok", result.ok());
        node.put("status", result.status());
        if (result.message() != null && !result.message().isBlank()) {
            node.put("message", result.message());
        }
        node.put("lifecycle", capability.lifecycle().name());
        node.put("managed", capability.managed());
        if (capability.reason() != null && !capability.reason().isBlank()) {
            node.put("lifecycleReason", capability.reason());
        }
        // Same GET body the probe already fetched — FE renders Available models
        // before Save without a stored-config list-models round-trip.
        node.set("models", client.modelsFromListBody(result.body()));
        return DaemonCommandOutcome.ok(node.toString());
    }

    /**
     * Parse the OPTIONAL draft provider config the BE forwards. Returns {@code null} — so the handler probes the
     * STORED config — when the payload is absent, unparseable, or carries no {@code server}, keeping the legacy
     * no-arg behaviour intact and never throwing.
     */
    private AiModelProviderConfig parseOverride(String payload) {
        if (payload == null || payload.isBlank()) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(payload);
            if (node == null || !node.isObject() || node.path("server").asText("").isBlank()) {
                return null;
            }
            return objectMapper.treeToValue(node, AiModelProviderConfig.class);
        } catch (Exception e) {
            log.debug("test-connection: ignoring unparseable provider payload: {}", e.toString());
            return null;
        }
    }
}
