package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager.DeleteResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * ITEM 5 — handles the BE-pushed {@code delete-oci-image} command: {@code podman rmi} the given ref and purge
 * any legacy uploaded blob for it. The BE removes its own canonical registry row separately, so this is a
 * best-effort daemon-store cleanup.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeleteOciImageCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "delete-oci-image";

    private final ObjectMapper objectMapper;
    private final StatefulInstanceManager manager;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        JsonNode payload;
        try {
            payload = objectMapper.readTree(command.payload() == null ? "{}" : command.payload());
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("delete-oci-image: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String ref = text(payload, "ref");
        if (ref == null || ref.isBlank()) {
            return DaemonCommandOutcome.failed("delete-oci-image: payload missing 'ref'");
        }
        try {
            DeleteResult result = manager.deleteImage(envId, ref);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("delete-oci-image.failed ref={}: {}", ref, ex.toString());
            return DaemonCommandOutcome.failed("delete-oci-image failed: " + message(ex));
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
