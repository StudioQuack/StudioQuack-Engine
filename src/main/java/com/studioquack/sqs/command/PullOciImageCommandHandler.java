package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.OciImageDigestReporter;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager.LoadResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * ITEM 5 — handles the BE-pushed {@code pull-oci-image} command: {@code podman pull} the given Docker-Hub ref
 * anonymously, resolve its content digest, and report it back so the BE flips the
 * {@code env_custom_oci_images} row DOWNLOADING → ACTIVE (or FAILED). Supersedes the tarball
 * {@code load-oci-image} upload flow for Docker-Hub images.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PullOciImageCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "pull-oci-image";

    private final ObjectMapper objectMapper;
    private final StatefulInstanceManager manager;
    private final OciImageDigestReporter digestReporter;

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
            return DaemonCommandOutcome.failed("pull-oci-image: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String ref = text(payload, "ref");
        String displayName = text(payload, "displayName");
        if (ref == null || ref.isBlank()) {
            return DaemonCommandOutcome.failed("pull-oci-image: payload missing 'ref'");
        }
        try {
            LoadResult result = manager.pullImage(envId, ref, displayName);
            // Report ACTIVE/FAILED (+ digest) so the BE registry row flips.
            // The command result below is the canonical fallback when the channel is off.
            digestReporter.report(envId, ref, result.digest(), result.status());
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result));
        } catch (Exception ex) {
            log.warn("pull-oci-image.failed ref={}: {}", ref, ex.toString());
            return DaemonCommandOutcome.failed("pull-oci-image failed: " + message(ex));
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
