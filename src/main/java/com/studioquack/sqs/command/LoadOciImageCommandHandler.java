package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager.LoadResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles the BE-pushed {@code load-oci-image} command: {@code podman load} the daemon-local blob-store
 * tarball for a custom image ref and report the resolved content digest so the BE flips the
 * {@code env_custom_oci_images} row to LOADED. Wire contract (matches the control plane + the BE registry):
 * payload: {envId, ref, displayName} result: {ref, digest, status} status ∈ {LOADED, FAILED} Never throws out
 * of {@link #handle}: a malformed payload or a load failure becomes a clean {@code ok}-result carrying
 * {@code status:FAILED} (the BE reads the status field), while an unparseable payload is a {@code failed}
 * outcome.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LoadOciImageCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "load-oci-image";

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
            return DaemonCommandOutcome.failed("load-oci-image: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String ref = text(payload, "ref");
        String displayName = text(payload, "displayName");
        if (ref == null || ref.isBlank()) {
            return DaemonCommandOutcome.failed("load-oci-image: payload missing 'ref'");
        }
        try {
            LoadResult result = manager.loadImage(envId, ref, displayName);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("load-oci-image.failed ref={}: {}", ref, ex.toString());
            return DaemonCommandOutcome.failed("load-oci-image failed: " + message(ex));
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
